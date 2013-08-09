/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::cell::*;
//use std::comm::*;
use std::task::*;
//use std::rt::uv::*;
use extra::priority_queue::*;
use extra::arc::*;
use extra::time::*;

use util::*;
use conn::*;
use conn_node::NodeConnection;

use bson::encode::*;

// TODO go through error handling more meticulously
// XXX due to io/concurrency issues in Rust 0.7, the spawned reconnection
//      thread does not always perform as expected. Until these issues
//      are resolved, it is probably better simply to run the reconnection in
//      a single thread (with relation to sends, recvs, etc.).

#[deriving(Clone, Eq)]
pub enum ServerType {
    PRIMARY = 0,
    SECONDARY = 1,
    OTHER = 2,
}

pub struct ReplicaSetConnection {
    //seed : ~RWARC<~[(~str, uint)]>, // once RWARC corrected
    //seed : ~ARC<~[(~str, uint)]>, // XXX for now, no adding seeds//RWARC appears to have bug and require impl *Clone* explicitly
    seed : Cell<~ARC<~[(~str, uint)]>>,
    state : Cell<ReplicaSetData>, // XXX RWARC?
    write_to : Cell<NodeConnection>,
    read_from : Cell<NodeConnection>,
    priv port_state : Cell<Port<ReplicaSetData>>,
    priv chan_reconn : Cell<Chan<bool>>,
    read_pref : Cell<READ_PREFERENCE>,
    read_pref_changed : Cell<bool>,
    timeout : Cell<u64>,
}

/**
 * All `Send`able data associated with `ReplicaSetConnection`s.
 * (NodeConnections are not `Send`able due to their managed pointers.)
 */
struct ReplicaSetData {
    pri : Option<NodeData>,         // discovered primary
    sec : PriorityQueue<NodeData>,  // discovered secondaries
    err : Option<MongoErr>,         // any errors along the way
}
// not actually needed for now but probably good to have anyway
impl Clone for ReplicaSetData {
    pub fn clone(&self) -> ReplicaSetData {
        let mut sec = PriorityQueue::new(); // PriorityQueue doesn't impl Clone
        for self.sec.iter().advance |&elem| {
            sec.push(elem);
        }

        ReplicaSetData {
            pri : self.pri.clone(),
            sec : sec,
            err : self.err.clone(),
        }
    }
}
impl ReplicaSetData {
    pub fn new( pri : Option<NodeData>,
                sec : PriorityQueue<NodeData>,
                err : Option<MongoErr>) -> ReplicaSetData {
        ReplicaSetData {
            pri : pri,
            sec : sec,
            err : err,
        }
    }
}

impl Connection for ReplicaSetConnection {
    pub fn connect(&self) -> Result<(), MongoErr> {
        if !(self.chan_reconn.is_empty() && self.port_state.is_empty()) {
            Err(MongoErr::new(
                    ~"conn_replica::connect",
                    ~"cannot connect",
                    ~"already connected; call reconnect or refresh instead"))
        } else {
            let seed_arc = self.seed.take();

            /* BEGIN BLOCK to include with new io (edit as appropriate) */
            // po/ch for kill of reconnect thread
            /*let (port_reconn, chan_reconn) = stream();
            self.chan_reconn.put_back(chan_reconn);
            let port = ReplicaSetConnection::spawn_reconnect(
                                    //&self.seed,
                                    &seed_arc,
                                    port_reconn);
            self.seed.put_back(seed_arc);
            self.port_state.put_back(port);
            self.refresh()*/
            /* END BLOCK to include with new io (edit as appropriate) */

            /* BEGIN BLOCK to remove with new io */
            let state = ReplicaSetConnection::reconnect_with_seed(&seed_arc);
            self.seed.put_back(seed_arc);
            self.refresh_with_state(Some(state))
            /* END BLOCK to remove with new io */
        }
    }

    pub fn disconnect(&self) -> Result<(), MongoErr> {
        let mut err_str = ~"";

        // kill reconnect thread, empty port, empty state
        if !self.chan_reconn.is_empty() {
            self.chan_reconn.take().send(false);
        }
        if !self.port_state.is_empty() { self.port_state.take(); }
        if !self.state.is_empty() { self.state.take(); }

        // disconnect from write_to and read_from as appropriate
        if !self.write_to.is_empty() {
            match self.write_to.take().disconnect() {
                Ok(_) => (),
                Err(e) => err_str.push_str(e.to_str()),
            }
        }
        if !self.read_from.is_empty() {
            match self.read_from.take().disconnect() {
                Ok(_) => (),
                Err(e) => err_str.push_str(e.to_str()),
            }
        }

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                    ~"conn_replica::disconnect",
                    ~"error while disconnecting",
                    err_str)) }
    }

    pub fn reconnect(&self) -> Result<(), MongoErr> {
        self.disconnect();
        self.connect()
    }

    pub fn send(&self, data : &[u8], read : bool) -> Result<(), MongoErr> {
        let mut err = None;
        let t = precise_time_s();

        loop {
            if precise_time_s() - t >= self.get_timeout() as float { break; }

            // until timing out, try to send (which refreshes internally)
            match self.try_send(data, read) {
                Ok(_) => return Ok(()),
                Err(e) => err = Some(e),
            }
        }

        Err(MongoErr::new(
                ~"conn_replica::send",
                ~"timed out trying to send",
                fmt!("last error: %s", err.unwrap().to_str())))
    }

    pub fn recv(&self, buf : &mut ~[u8], read : bool) -> Result<uint, MongoErr> {
        // should not recv without having issued send earlier
        // choose correct server from which to recv
        let server_cell = if read { &self.read_from } else { &self.write_to };
        if server_cell.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::recv",
                        ~"cannot recv",
                        fmt!("no server from which to receive; %s op",
                            if read { "read" } else { "write" })));
        }
        let server = server_cell.take();
        debug!("[recv] server: %?", server);

        // even if found server, if ping is empty, server down
        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::recv",
                        ~"cannot receive",
                        ~"server down"));
        }

        // otherwise recv and then put everything back
        let result = server.recv(buf, read);
        server_cell.put_back(server);
        result
    }

    pub fn set_timeout(&self, timeout : u64) -> u64 {
        let prev = self.timeout.take();
        self.timeout.put_back(timeout);
        prev
    }

    pub fn get_timeout(&self) -> u64 {
        self.timeout.clone().take()
    }
}

// XXX integrate with NodeConnection; integration may depend on sendability
#[deriving(Clone)]
struct NodeData {
    ip : ~str,
    port : uint,
    typ : ServerType,   // already separated before cmp; for convenience
    ping : Option<u64>,
    tagset : TagSet,
}
// Inequalities seem all backwards because max-heaps.
impl Ord for NodeData {
    pub fn lt(&self, other : &NodeData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(t1), Some(t2)) => t1 >= t2,
        }
    }
    pub fn le(&self, other : &NodeData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(t1), Some(t2)) => t1 > t2,
        }
    }
    pub fn gt(&self, other : &NodeData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(t1), Some(t2)) => t1 <= t2,
        }
    }
    pub fn ge(&self, other : &NodeData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(t1), Some(t2)) => t1 < t2,
        }
    }
}
impl Eq for NodeData {
    pub fn eq(&self, other : &NodeData) -> bool {
            self.ip == other.ip
        &&  self.port == other.port
        &&  self.tagset == other.tagset
    }
    pub fn ne(&self, other : &NodeData) -> bool {
            self.ip != other.ip
        ||  self.port != other.port
        ||  self.tagset != other.tagset
    }
}
impl NodeData {
    pub fn new( ip : ~str,
                port : uint,
                typ : ServerType,
                ping : Option<u64>,
                tagset : Option<TagSet>) -> NodeData {
        NodeData {
            ip : ip,
            port : port,
            typ : typ,
            ping : ping,
            tagset : match tagset {
                None => TagSet::new(~[]),
                Some(ts) => ts,
            },
        }
    }
}

impl Eq for ReplicaSetData {
    pub fn eq(&self, other : &ReplicaSetData) -> bool {
        if         self.pri != other.pri
                || self.sec.len() != other.sec.len()
                || self.err != other.err {
            return false;
        }

        let mut it = self.sec.iter().zip(other.sec.iter());
        for it.advance |(&x, &y)| {
            if x != y { return false; }
        }

        true
    }
    pub fn ne(&self, other : &ReplicaSetData) -> bool {
        if         self.pri != other.pri
                || self.sec.len() != other.sec.len()
                || self.err != self.err {
            return true;
        }

        let mut it = self.sec.iter().zip(other.sec.iter());
        for it.advance |(&x, &y)| {
            if x != y { return true; }
        }

        false
    }
}

impl ReplicaSetConnection {
    pub fn new(seed : &[(~str, uint)]) -> ReplicaSetConnection {
        let mut seed_arc = ~[];
        for seed.iter().advance |&(ip,port)| {
            seed_arc.push((ip, port));
        }
        ReplicaSetConnection {
            //seed : ~RWARC(seed),    // RWARC corrected
            //seed : ~ARC(seed),
            seed : Cell::new(~ARC(seed_arc)),
            state : Cell::new_empty(),
            write_to : Cell::new_empty(),
            read_from : Cell::new_empty(),
            port_state : Cell::new_empty(),
            chan_reconn : Cell::new_empty(),
            read_pref : Cell::new(PRIMARY_ONLY),
            read_pref_changed : Cell::new(true),
            timeout : Cell::new(MONGO_TIMEOUT_SECS),
        }
    }

    /**
     * Given a seed list (here in ARC form), connects and gets snapshot of
     * replica set packaged as ReplicaSetData.
     */
    //fn reconnect_with_seed(seed : &~RWARC<~[(~str, uint)]>) -> ReplicaSetData {   // RWARC corrected
    fn reconnect_with_seed(seed : &~ARC<~[(~str, uint)]>) -> ReplicaSetData {
        let hosts = match ReplicaSetConnection::_get_host_list(seed) {
            Ok(l) => l,
            Err(e) => return ReplicaSetData::new(None, PriorityQueue::new(), Some(e)),
        };

        ReplicaSetConnection::_get_replica_set_data(hosts)
    }

    /**
     * Gets host list given a seed list (ARC).
     *
     * # Arguments
     * `seed` - seed list to use for node discovery
     *
     * # Returns
     * a list of hosts if it is found from any seed, MongoErr otherwise
     * errors that may have occurred during node discovery
     */
    //fn _get_host_list(seed : &~RWARC<~[(~str, uint)]>)    // RWARC corrected
    fn _get_host_list(seed : &~ARC<~[(~str, uint)]>)
                -> Result<~[(~str, uint)], MongoErr> {
        /* BEGIN BLOCK to remove with new io */
        let mut err_str = ~"";
        let seed_list = seed.get();
        for seed_list.iter().advance |&(ip, port)| {
            let server = @NodeConnection::new(ip, port);
            let server_list = server._check_master_and_do(
                    |bson_doc : &~BsonDocument| -> Result<~[(~str, uint)], MongoErr> {
                let mut list = ~[];
                let mut err = None;

                let mut list_doc = None;
                match bson_doc.find(~"hosts") {
                    None => (),
                    Some(doc) => {
                        match doc {
                            &Array(ref l) => list_doc = Some(l.clone()),
                            _ => err = Some(MongoErr::new(
    ~"conn_replica::reconnect_with_seed",
    ~"ismaster response in unexpected format",
    fmt!("hosts field %?, expected encode::Array of hosts", *doc))),
                        }

                        if err.is_none() {
                            let fields = list_doc.unwrap().fields;
                            let mut host_str = ~"";
                            for fields.iter().advance |&(_, @host_doc)| {
                                match host_doc {
                                    UString(s) => host_str = s,
                                    _ => err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("hosts field %?, expected list of host ~str", *doc))),
                                }

                                if err.is_some() { break }
                                match parse_host(host_str.as_slice()) {
                                    Ok(p) => list.push(p),
                                    Err(e) => err = Some(e),
                                }
                            }
                        }
                    }
                }

                if err.is_none() { Ok(list) }
                else { Err(err.unwrap()) }
            });
            match server_list {
                Ok(l) => if l.len() > 0 { return Ok(l); },
                Err(e) => err_str.push_str(fmt!("%s\n", e.to_str())),
            }
        }

        Err(MongoErr::new(
                ~"conn_replica::reconnect_with_seed",
                fmt!("could not get host list from seed %?", seed),
                err_str))
        /* END BLOCK to remove with new io */

        /* BEGIN BLOCK to include with new io */
        /*// remember number of expected responses
        let n = seed.get().len();
        //let n = seed.read(|&list| -> uint { list.len() });    // RWARC corrected

        // po/ch for sending host list
        let (port_hosts, chan_hosts) = stream();
        let chan_hosts = SharedChan::new(chan_hosts);

        // po/ch for sending seed list
        let (port_seed, chan_seed) = stream();
        chan_seed.send(seed.clone()); // ARC

        // check seeds
        do spawn_supervised {
            let seed_arc = port_seed.recv();
            let seed_list = seed_arc.get();

        //seed_arc.read( |&seed_list| -> () {   // RWARC corrected---indent block
            for seed_list.iter().advance |&seed| {
                // po/ch for sending this seed
                let (port_pair, chan_pair) = stream();
                chan_pair.send(seed.clone());

                // ch for sending host list for this seed
                let chan_hosts_tmp = chan_hosts.clone();

                // spawn checker for this seed
                do spawn {
                    let (ip, port) = port_pair.recv();
                    let server = @NodeConnection::new(ip, port);

                    // attempt to get list of hosts from this server
                    let server_result = server._check_master_and_do(
                            |bson_doc : &~BsonDocument| -> Result<~[(~str, uint)], MongoErr> {
                        let mut list = ~[];
                        let mut err = None;

                        let mut list_doc = None;
                        match bson_doc.find(~"hosts") {
                            None => (),
                            Some(doc) => {
                                match doc {
                                    &Array(ref l) => list_doc = Some(l.clone()),
                                    _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("hosts field %?, expected encode::Array of hosts", *doc))),
                                }

                                if err.is_none() {
                                    let fields = list_doc.unwrap().fields;
                                    let mut host_str = ~"";
                                    for fields.iter().advance |&(_, @host_doc)| {
                                        match host_doc {
                                            UString(s) => host_str = s,
                                            _ => {
                                                err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("hosts field %?, expected list of host ~str", *doc)));
                                                break;
                                            }
                                        }

                                        match parse_host(host_str.as_slice()) {
                                            Ok(p) => list.push(p),
                                            Err(e) => {
                                                err = Some(e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if err.is_none() { Ok(list) }
                        else { Err(err.unwrap()) }
                    });

                    // check if successfully got a list
                    match server_result {
                        Ok(list) => {
                            if list.len() > 0 {
                                chan_hosts_tmp.send(Ok(list));
                                fail!(); // take down whole thread
                            } else { chan_hosts_tmp.send(Ok(~[])); }
                        }
                        Err(e) => chan_hosts_tmp.send(Err(e)),
                    }
                }
            }
        //});   // RWARC corrected---end indent block
        }

        // try to recv a host list
        let mut err_str = ~"";
        for n.times {
            match port_hosts.recv() {
                Ok(l) => if l.len() > 0 { return Ok(l); },
                Err(e) => err_str.push_str(e.to_str()),
            }
        }

        Err(MongoErr::new(
                    ~"conn_replica::reconnect_with_seed",
                    ~"no host list found",
                    err_str))*/
        /* END BLOCK to include with new io */

    }

    /**
     * Gets data pertaining to replica set connection, given a host list.
     *
     * # Arguments
     * `hosts` - list of hosts
     *
     * # Returns
     * ReplicaSetData struct with discovered primary,
     * discovered secondaries, and any errors that may have come up
     */
    fn _get_replica_set_data(hosts : ~[(~str, uint)]) -> ReplicaSetData {
        let mut pri = None;
        let mut sec = PriorityQueue::new::<NodeData>();

        /* BEGIN BLOCK to remove with new io */
        let mut err = None;
        let mut err_str = ~"";
        for hosts.iter().advance |&(ip, port)| {
            let server = @NodeConnection::new(ip.clone(), port);

            // get server stats packaged in a NodeData
            let server_stats = server._check_master_and_do(
                    |bson_doc : &~BsonDocument|
                            -> Result<NodeData, MongoErr> {
                let mut typ = None;
                let mut err = None;

                // check if server is master
                match bson_doc.find(~"ismaster") {
                    None => err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("no \"ismaster\" field in %?", bson_doc))),
                    Some(doc) => {
                        match doc {
                            &Bool(ref val) => if *val { typ = Some(PRIMARY); },
                            _ => err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("expected boolean \"ismaster\" field, found %?", doc))),
                        }
                    }
                }

                // check if server is secondary
                if typ.is_none() && err.is_none() {
                    match bson_doc.find(~"secondary") {
                        None => err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("no \"secondary\" field in %?", bson_doc))),
                        Some(doc) => {
                            match doc {
                                &Bool(ref val) => if *val { typ = Some(SECONDARY); },
                                _ => err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("expected boolean \"secondary\" field, found %?", doc))),
                            }
                        }
                    }
                }

                // get tags for this server
                let mut tags = TagSet::new(~[]);
                if err.is_none() {
                    match bson_doc.find(~"tags") {
                        None => (),
                        Some(doc) => {
                            match doc {
                                &Embedded(ref val) => {
                                    for val.fields.iter().advance |&(@k,@v)| {
                                        match v {
                                            UString(val) => tags.set(k,val),
                                            _ => {
                                                err = Some(MongoErr::new(
        ~"conn_replica::reconnect_with_seed",
        ~"ismaster response in unexpected format",
        fmt!("expected UString value for tag, found %?", v)));
                                                break;
                                            }
                                        }
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                }

                // package into a NodeData
                if err.is_none() {
                    let (data_typ, data_tags) = match typ {
                        Some(t) => (t, tags),
                        None => (OTHER, tags),
                    };
                    Ok(NodeData::new(
                            ip.clone(), port,
                            data_typ,
                            Some(server.ping.take()),
                            Some(data_tags)))
                } else { Err(err.unwrap()) }
            });

            // given the NodeData, process into ReplicaSetData
            match server_stats {
                Ok(stats) => {
                    if stats.typ == PRIMARY {
                        if pri.is_some() {
                            err = Some(MongoErr::new(
                                ~"conn_replica::reconnect_with_seed",
                                ~"error while connecting to hosts",
                                ~"multiple primaries"));
                            pri = None;
                            break;
                        } else { pri = Some(stats); }
                    } else if stats.typ == SECONDARY {
                        sec.push(stats);
                    }
                }
                Err(e) => err_str.push_str(fmt!("%s\n", e.to_str())),
            }
        }
        if err_str.len() > 0 {
            err = Some(MongoErr::new(
                        ~"conn_replica::reconnect_with_seed",
                        ~"error while connecting to hosts",
                        err_str));
        }
        /* END BLOCK to remove with new io */

        /* BEGIN BLOCK to include with new io */
        /*// remember expected number of responses
        let n = hosts.len();

        // po/ch for receiving NodeData
        let (port_server, chan_server) = stream();
        let chan_server = SharedChan::new(chan_server);

        // determine type and ping time of each host
        for hosts.iter().advance |&pair| {
            let chan_server_tmp = chan_server.clone();

            // po/ch for sending pair
            let (port_pair, chan_pair) = stream();
            chan_pair.send(pair);

            // spawn checker for this host
            do spawn_supervised {   // just in case
                let (ip, port) = port_pair.recv();
                let server = @NodeConnection::new(ip.clone(), port);
                let server_result = server._check_master_and_do(
                        |bson_doc : &~BsonDocument|
                                -> Result<(ServerType, TagSet), MongoErr> {
                    let mut typ = None;
                    let mut err = None;

                    // check if is master
                    match bson_doc.find(~"ismaster") {
                        None => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("no \"ismaster\" field in %?", bson_doc))),
                        Some(doc) => {
                            match doc {
                                &Bool(ref val) => if *val { typ = Some(PRIMARY); },
                                _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("expected boolean \"ismaster\" field, found %?", doc))),
                            }
                        }
                    }

                    // XXX would normally just return early, but can't in closures
                    // check if is secondary
                    if typ.is_none() && err.is_none() {
                        match bson_doc.find(~"secondary") {
                            None => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("no \"secondary\" field in %?", bson_doc))),
                            Some(doc) => {
                                match doc {
                                    &Bool(ref val) => if *val { typ = Some(SECONDARY); },
                                    _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("expected boolean \"secondary\" field, found %?", doc))),
                                }
                            }
                        }
                    }

                    // get tags
                    let mut tags = TagSet::new(~[]);
                    if err.is_none() {
                        match bson_doc.find(~"tags") {
                            None => (),
                            Some(doc) => {
                                match doc {
                                    &Embedded(ref val) => {
                                        for val.fields.iter().advance |&(@k,@v)| {
                                            match v {
                                                UString(val) => tags.set(k,val),
                                                _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("expected UString value for tag, found %?", v))),
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }

                    // return type and tags if all well
                    if err.is_none() {
                        match typ {
                            Some(t) => Ok((t, tags)),
                            None => Ok((OTHER, tags)),
                        }
                    } else { Err(err.unwrap()) }
                });

                // send result from server packaged into a NodeData to stream
                match server_result {
                    Ok((t, tags)) => {
                        let stats = NodeData::new(
                            ip, port, t, Some(server.ping.take()), Some(tags)
                        );
                        chan_server_tmp.send(Ok(stats));
                    }
                    Err(e) => chan_server_tmp.send(Err(e)),
                }
            }
        }

        // properly insert now-typed hosts into respective locations
        let mut err = None;
        let mut err_str = ~"";

        // wait for acks from all hosts
        for n.times {
            match port_server.recv() {
                Ok(stats) => {
                    if stats.typ == PRIMARY {
                        if pri.is_some() {
                            err = Some(MongoErr::new(
                                ~"conn_replica::reconnect_with_seed",
                                ~"error while connecting to hosts",
                                ~"multiple primaries"));
                            pri = None;
                            break;
                        } else { pri = Some(stats); }
                    } else if stats.typ == SECONDARY {
                        sec.push(stats);
                    }
                }
                Err(e) => {
                    err_str.push_str(fmt!("%s\n", e.to_str()));
                }
            }
        }

        if err_str.len() > 0 {
            err = Some(MongoErr::new(
                ~"conn_replica::reconnect_with_seed",
                ~"error while connecting to hosts",
                err_str));
        }*/
        /* END BLOCK to include with new io */
        ReplicaSetData::new(pri, sec, err)
    }

    /*
     * Given a seed list, spawns a reconnect thread that periodically
     * sends a ReplicaSetState to the refresh stream; this stream is what
     * gets pulled from when actual refreshes happen. Presently, due to
     * IO bugginess, instead of maintaining a thread, refreshes happen
     * synchronously, when needed (before a write).
     */
    // XXX do not call until new io
    //fn spawn_reconnect( seed : &~RWARC<~[(~str, uint)]>,  // RWARC corrected
    fn spawn_reconnect( seed : &~ARC<~[(~str, uint)]>,
                        port_reconn : Port<bool>)
                -> Port<ReplicaSetData> {
        // po/ch for seed list
        let (port_seed, chan_seed) = stream();
        chan_seed.send(seed.clone());
        // po/ch for ReplicaSetData
        let (port_state, chan_state) = stream();

        // actually spawn reconnection thread
        do spawn_supervised {
            do spawn { if !port_reconn.recv() { fail!(); } }

            let seed = port_seed.recv();
            loop {
                // pick up seed and state, then send up channel
                let state = ReplicaSetConnection::reconnect_with_seed(&seed);
                debug!("~~~sending state~~~\n%?\n~~~~~~~~~~~~~~~~~~~", state);
                chan_state.send(state);
                // sleep : new io
            }
        }

        // send back port to pick up reconnection states
        port_state
    }

    /*
     * Given a state (which may be none), refresh the replica set.
     * This may be necessary if the state changes or if the read
     * preference changes.
     */
    fn refresh_with_state(&self, tmp_state : Option<ReplicaSetData>)
                -> Result<(), MongoErr> {
        debug!("self before refresh:\n%?", self);

        // by end, read_pref will have been accounted for; note updated
        let pref_changed = self.read_pref_changed.take();
        self.read_pref_changed.put_back(false);

        // only update read_from or write_to if
        //  0) no prior state
        //      old_state.is_none() && tmp_state.is_some()
        //  1) state has changed
        //      old_state.is_some() && tmp_state.is_some() && old_state != tmp_state
        //  2) read preference has changed
        //      a) tmp_state.is_none() => refresh according to old_state
        //      b) tmp_state.is_some() => refresh according to tmp_state
        let old_state = if self.state.is_empty() {
            None
        } else {
            Some(self.state.take())
        };  // self.state now certainly empty

        debug!("refreshing:\nold state:\n===\n%?\n===\nnew state:\n===\n%?\n===",
            old_state, tmp_state);

        // get state according which to update
        let state = match (old_state, tmp_state) {
            (None, None) => {
                // should never reach here
                fail!("reached unreachable state: empty old_ AND new_state");
            }
            (Some(os), None) => {
                // no new state
                if !pref_changed {
                    // read_pref unchanged; no refresh needed
                    self.state.put_back(os);
                    return Ok(());
                }
                // refresh according to old state
                os
            }
            (None, Some(ns)) => {
                // first state;
                // refresh according to new state
                ns
            }
            (Some(os), Some(ns)) => {
                // new state and state to update;
                // check that *need* to refresh
                if !pref_changed && os == ns {
                    // read_pref unchanged, and no change to state;
                    // no refresh needed
                    self.state.put_back(os);
                    return Ok(());
                } else {
                    // otherwise need to refresh according to new state
                    ns
                } }
        };

        // update write_to if it would change
        let mut server = if !self.write_to.is_empty() {
            Some(self.write_to.take())
        } else { None };
        match (server, self._refresh_write_to(&state)) {
            (None, None) => (),
            (Some(a), None) => { a.disconnect(); }
            (None, Some(b)) => { b.connect(); self.write_to.put_back(b); }
            (Some(a), Some(b)) => {
                if b != a {
                    a.disconnect();
                    b.connect();
                    self.write_to.put_back(b);
                } else { self.write_to.put_back(a); }
            }
        }

        // update read_from if it would change
        server = if !self.read_from.is_empty() {
            Some(self.read_from.take())
        } else { None };
        match (server, self._refresh_read_from(&state)) {
            (None, None) => (),
            (Some(a), None) => { a.disconnect(); }
            (None, Some(b)) => { b.connect(); self.read_from.put_back(b); }
            (Some(a), Some(b)) => {
                if b != a {
                    a.disconnect();
                    b.connect();
                    self.read_from.put_back(b);
                } else { self.read_from.put_back(a); }
            }
        }

        // replace state
        self.state.put_back(state);

        debug!("self after refresh:\n%?", self);

        Ok(())
    }

    /*
     * Refreshes server to which to write, given a ReplicaSetData.
     * Essentially, initializes a NodeConnection that then (if
     * necessary) will be connected to and replace the former
     * write_to.
     */
    fn _refresh_write_to(&self, state : &ReplicaSetData)
                -> Option<NodeConnection> {
        // write_to is always primary
        if state.pri.is_none() { return None; }
        let dat = state.pri.clone().unwrap();
        let pri = NodeConnection::new(dat.ip.clone(), dat.port);
        pri.tags.take();
        pri.tags.put_back(dat.tagset.clone());
        pri.ping.put_back(dat.ping.unwrap());
        Some(pri)
    }

    /*
     * Refreshes server from which to read, given a ReplicaSetData.
     * Similarly to _refresh_write_to, initializes a NodeConnection
     * based on read preference and available servers that then (if
     * necessary) will be connected to and replace the former
     * read_from.
     */
    fn _refresh_read_from(&self, state : &ReplicaSetData)
                -> Option<NodeConnection> {
        let read_pref = self.read_pref.take();

        // list of candidate servers
        let mut servers = ~[];

        let pri = match state.clone().pri { None => ~[], Some(s) => ~[s] };
        let mut sec = PriorityQueue::new();

        for state.sec.iter().advance |&s| {
            sec.push(s);
        }

        let mut result;
        {
            // parse read_preference to determine server choices
            let ts_list = match read_pref {
                PRIMARY_ONLY => {
                    servers.push_all_move(pri);
                    &None
                }
                PRIMARY_PREF(ref ts) => {
                    servers.push_all_move(pri);
                    let ordered = sec.to_sorted_vec();
                    for ordered.rev_iter().advance |&s| {
                        servers.push(s);
                    }
                    ts
                }
                SECONDARY_ONLY(ref ts) => {
                    let ordered = sec.to_sorted_vec();
                    for ordered.rev_iter().advance |&s| {
                        servers.push(s);
                    }
                    ts
                }
                SECONDARY_PREF(ref ts) => {
                    let ordered = sec.to_sorted_vec();
                    for ordered.rev_iter().advance |&s| {
                        servers.push(s);
                    }
                    servers.push_all_move(pri);
                    ts
                }
                NEAREST(ref ts) => {
                    for pri.iter().advance |&s| { sec.push(s); }
                    let ordered = sec.to_sorted_vec();
                    for ordered.rev_iter().advance |&s| {
                        servers.push(s);
                    }
                    ts
                }
            };

            result = self._find_server(servers, ts_list);
        }

        self.read_pref.put_back(read_pref);
        result
    }

    /*
     * Creates a NodeConnection based on a list of candidate servers,
     * satisfying the provided tags.
     */
    fn _find_server(&self,  servers : ~[NodeData],
                            tagsets : &Option<~[TagSet]>)
                -> Option<NodeConnection> {
        let tmp = ~[TagSet::new(~[])];
        let ts_list = match tagsets {
            &None => &tmp,
            &Some(ref l) => l,
        };

        // iterate through available servers, checking if they match
        //      given tagset
        for servers.iter().advance |&server| {
            for ts_list.iter().advance |ts| {
                if server.tagset.matches(ts) {
                    let result = NodeConnection::new(
                                    server.ip.clone(),
                                    server.port);
                    result.tags.take();
                    result.tags.put_back(server.tagset.clone());
                    result.ping.put_back(server.ping.clone().unwrap());
                    return Some(result);
                }
            }
        }

        None
    }


    /**
     * Refreshes replica set connection data.
     *
     * Refreshes by fishing out latest state sent from reconnection task
     * to main task and checking if the write_to or read_from servers
     * need to be updated (due to primaries/secondaries, their tags, or the
     * read preference having changed). Connection must be connected
     * while calling refresh.
     *
     * Called before every send or recv.
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn refresh(&self) -> Result<(), MongoErr> {
        if self.port_state.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::refresh",
                        ~"could not refresh",
                        ~"reconnect thread dead"));
        }

        debug!("===starting refresh===");
        // fish out most up-to-date information
        let port_state = self.port_state.take();
        let mut tmp_state = None;
        if !self.state.is_empty() {
            // has been updated before; get latest in stream
            while port_state.peek() {
                tmp_state = Some(port_state.recv());
            }
        } else {
            // first update; wait if needed to pick up first state
            tmp_state = Some(port_state.recv());
        }
        self.port_state.put_back(port_state);

        debug!("===found below state===\n%?\n=======================", tmp_state);

        // refresh according to new state
        self.refresh_with_state(tmp_state)
    }

    /*
     * Body of send, wrapped by timeout mechanism.
     */
    fn try_send(&self, data : &[u8], read : bool) -> Result<(), MongoErr> {
        //self.refresh();   // uncomment with new io
        /* BEGIN BLOCK to remove with new io */
        let seed_arc = self.seed.take();
        let state = ReplicaSetConnection::reconnect_with_seed(&seed_arc);
        self.seed.put_back(seed_arc);
        match self.refresh_with_state(Some(state)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };
        /* END BLOCK to remove with new io */

        // choose correct server to which to send
        let server_cell = if read { &self.read_from } else { &self.write_to };
        if server_cell.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"no primary found"));
        }
        let server = server_cell.take();
        debug!("[send] server: %?", server);

        // even if found server, if ping is empty, server down
        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"server down"));
        }

        // otherwise send and then put everything back
        let result = server.send(data, read);
        server_cell.put_back(server);
        result
    }
}
