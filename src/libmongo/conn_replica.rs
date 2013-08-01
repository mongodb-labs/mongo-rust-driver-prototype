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
use std::comm::*;
use std::task::*;
use std::int::*;
use std::rt::uv::*;
use extra::priority_queue::*;
use extra::arc::*;
use extra::uv::*;
use extra::time::*;
use extra::timer::*;

use util::*;
use conn::*;
use conn_node::NodeConnection;

use bson::encode::*;

// TODO go through error handling more meticulously

#[deriving(Clone, Eq)]
pub enum ServerType {
    PRIMARY = 0,
    SECONDARY = 1,
    OTHER = 2,
}

pub struct ReplicaSetConnection {
    //seed : ~RWARC<~[(~str, uint)]>,       // once RWARC corrected
    //seed : ~ARC<~[(~str, uint)]>,       // XXX for now, no adding seeds//RWARC appears to have bug and require impl *Clone* explicitly
    seed : Cell<~ARC<~[(~str, uint)]>>,
    state : Cell<ReplicaSetData>,      // XXX RWARC?
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
 */
struct ReplicaSetData {
    pri : Option<NodeData>,
    sec : PriorityQueue<NodeData>,
    err : Option<MongoErr>,
}
// not actually needed for now but probably good to have anyway
impl Clone for ReplicaSetData {
    pub fn clone(&self) -> ReplicaSetData {
        let mut sec = PriorityQueue::new();
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
            // po/ch for kill of reconnect thread
            let (port_reconn, chan_reconn) = stream();
            self.chan_reconn.put_back(chan_reconn);
            let seed_arc = self.seed.take();

            let port = ReplicaSetConnection::spawn_reconnect(
                                    //&self.seed,
                                    &seed_arc,
                                    port_reconn);
            self.seed.put_back(seed_arc);
            self.port_state.put_back(port);
            self.refresh()
            /*let state = ReplicaSetConnection::reconnect_with_seed(&seed_arc);
            self.seed.put_back(seed_arc);
            self.refresh_with_state(Some(state))*/
        }
    }

    pub fn disconnect(&self) -> Result<(), MongoErr> {
        let mut err_str = ~"";

        // kill reconnect thread
        if !self.chan_reconn.is_empty() {
            self.chan_reconn.take().send(false);
        } else {
            err_str.push_str(MongoErr::new(
                    ~"conn_replica::disconnect",
                    ~"unexpected state of reconnect thread",
                    ~"reconnect thread already dead").to_str());
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

    pub fn send(&self, data : ~[u8], read : bool) -> Result<(), MongoErr> {
        // refresh server data: first try via refresh, then try via reconnect
        let mut err = None;
        let t = precise_time_ns();
        loop {
            if precise_time_ns() - t >= self.get_timeout()*1000000000 {
                break;
            }

            match self.try_send(data.clone(), read) {
                Ok(_) => return Ok(()),
                Err(e) => err = Some(e),
            }
        }
        Err(MongoErr::new(
                ~"conn_replica::send",
                ~"timed out trying to send",
                fmt!("last error: %s", err.unwrap().to_str())))
    }

    pub fn recv(&self, read : bool) -> Result<~[u8], MongoErr> {
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
        let result = server.recv(read);
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
    pub fn new(seed : &[(&str, uint)]) -> ReplicaSetConnection {
        let mut seed_arc = ~[];
        for seed.iter().advance |&(ip,port)| {
            seed_arc.push((ip.to_owned(), port));
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
        // remember number of expected responses
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
                    match server._check_master_and_do(
                            |bson_doc : &~BsonDocument| -> Result<~[(~str, uint)], MongoErr> {
                        let mut list = ~[];
                        let mut err = None;

                        let mut list_doc = None;
                        match bson_doc.find(~"hosts") {
                            None => (),
                            Some(doc) => {
                                let tmp_doc = copy *doc;
                                match tmp_doc {
                                    Array(l) => list_doc = Some(l),
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

                                        match parse_host(&host_str) {
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
                    }) {
                        Ok(list) => {
                            if list.len() > 0 {
println(fmt!("found list %?", list));
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
                    err_str))
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

        // remember expected number of responses
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
                match server._check_master_and_do(
                        |bson_doc : &~BsonDocument|
                                -> Result<(ServerType, TagSet), MongoErr> {
                    let mut typ = None;
                    let mut err = None;

                    let cpy = copy *bson_doc;
                    match bson_doc.find(~"ismaster") {
                        None => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("no \"ismaster\" field in %?", cpy))),
                        Some(doc) => {
                            match copy *doc {
                                Bool(val) => if val { typ = Some(PRIMARY); },
                                _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("expected boolean \"ismaster\" field, found %?", *doc))),
                            }
                        }
                    }

                    if typ.is_none() && err.is_none() {
                        match bson_doc.find(~"secondary") {
                            None => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("no \"secondary\" field in %?", cpy))),
                            Some(doc) => {
                                match copy *doc {
                                    Bool(val) => if val { typ = Some(SECONDARY); },
                                    _ => err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("expected boolean \"secondary\" field, found %?", *doc))),
                                }
                            }
                        }
                    }

                    if err.is_none() {
                        let mut tags = TagSet::new(~[]);
                        match bson_doc.find(~"tags") {
                            None => (),
                            Some(doc) => {
                                match copy *doc {
                                    Embedded(val) => {
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

                        match typ {
                            Some(t) => Ok((t, tags)),
                            None => Ok((OTHER, tags)),
                        }
                    } else {
                        Err(err.unwrap())
                    }
                }) {
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
                    err_str.push_str(e.to_str());
                }
            }
        }

        if pri.is_none() {
            err = Some(MongoErr::new(
                        ~"conn_replica::reconnect_with_seed",
                        ~"error while connecting to hosts",
                        ~"no primary"));
        }

        if err_str.len() > 0 {
            err = Some(MongoErr::new(
                ~"conn_replica::reconnect_with_seed",
                ~"error while connecting to hosts",
                err_str));
        }
        ReplicaSetData::new(pri, sec, err)
    }

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
                let mut lp = Loop::new();
                let mut timer = TimerWatcher::new(&mut lp);
                // pick up seed and state
                let state = ReplicaSetConnection::reconnect_with_seed(&seed);
                println(fmt!("~~~sending state~~~\n%?\n~~~~~~~~~~~~~~~~~~~", state));
                chan_state.send(state);
                //sleep(&global_loop::get(), MONGO_RECONN_MSECS as uint);
                do timer.start(MONGO_RECONN_MSECS, 0) |timer,_| {
                    timer.close(||());
                }
                lp.run();
                lp.close();
            }
        }

        // send back port to pick up reconnection states
        port_state
    }

    fn refresh_with_state(&self, tmp_state : Option<ReplicaSetData>)
                -> Result<(), MongoErr> {
        // by end, read_pref will have been accounted for; note updated
        let pref_changed = self.read_pref_changed.take();
        self.read_pref_changed.put_back(false);

        // only update read_from or write_to if:
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

//println(fmt!("refreshing:\nold state:\n===\n%?\n===\nnew state:\n===\n%?\n===", old_state, tmp_state));

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
                }
            }
        };

        // do actual refresh
        let mut err_str = ~"";
        if !self.write_to.is_empty() { self.write_to.take().disconnect(); }
        if !self.read_from.is_empty() { self.read_from.take().disconnect(); }
        if state.pri.is_some() {
            match self._refresh_write_to(&state) {
                Ok(_) => (),
                Err(e) => err_str.push_str(e.to_str()),
            }
        } else {
            return Err(MongoErr::new(
                        ~"conn_replica::refresh",
                        ~"no primary; see state error",
                        state.err.clone().unwrap().to_str()));
        }
        match self._refresh_read_from(&state) {
            Ok(_) => (),
            Err(e) => err_str.push_str(e.to_str()),
        }

        // replace state
        self.state.put_back(state);

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                    ~"conn_replica::refresh",
                    ~"error while refreshing",
                    err_str)) }
    }

    fn _refresh_write_to(&self, state : &ReplicaSetData)
                -> Result<(), MongoErr> {
        // write_to is always primary, and flow cannot reach here
        //      unless non-empty primary
        let dat = state.pri.clone().unwrap();
        let pri = NodeConnection::new(dat.ip.clone(), dat.port);
        pri.tags.take();
        pri.tags.put_back(dat.tagset.clone());
        pri.ping.put_back(dat.ping.unwrap());
        let result = pri.connect();
        self.write_to.put_back(pri);
        result
    }

    fn _refresh_read_from(&self, state : &ReplicaSetData)
                -> Result<(), MongoErr> {
        let read_pref = self.read_pref.take();

        let mut servers = ~[];

        let pri = match state.clone().pri { None => ~[], Some(s) => ~[s] };
        let mut sec = PriorityQueue::new();

        for state.sec.iter().advance |&s| {
            sec.push(s);
        }

        // parse read_preference to determine server choices
        let (pref_str, ts_list) = match read_pref {
            PRIMARY_ONLY => {
                servers.push_all_move(pri);
                (~"PRIMARY_ONLY", &None)
            }
            PRIMARY_PREF(ref ts) => {
                servers.push_all_move(pri);
                let ordered = sec.to_sorted_vec();
                for ordered.rev_iter().advance |&s| {
                    servers.push(s);
                }
                (~"PRIMARY_PREF", ts)
            }
            SECONDARY_ONLY(ref ts) => {
                let ordered = sec.to_sorted_vec();
                for ordered.rev_iter().advance |&s| {
                    servers.push(s);
                }
                (~"SECONDARY_ONLY", ts)
            }
            SECONDARY_PREF(ref ts) => {
                let ordered = sec.to_sorted_vec();
                for ordered.rev_iter().advance |&s| {
                    servers.push(s);
                }
                servers.push_all_move(pri);
                (~"SECONDARY_PREF", ts)
            }
            NEAREST(ref ts) => {
                for pri.iter().advance |&s| { sec.push(s); }
                let ordered = sec.to_sorted_vec();
                for ordered.rev_iter().advance |&s| {
                    servers.push(s);
                }
                (~"NEAREST", ts)
            }
        };
        self.read_pref.put_back(read_pref);

        let server = match self._find_server(pref_str, servers, ts_list) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        let result = server.connect();
        self.read_from.put_back(server);
        result
    }

    fn _find_server(&self,  pref : ~str,
                            servers : ~[NodeData],
                            tagsets : &Option<~[TagSet]>)
                -> Result<NodeConnection, MongoErr> {
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
                    return Ok(result);
                }
            }
        }

        Err(MongoErr::new(
                ~"conn_replica::_get_read_server",
                ~"could not find server matching tagset",
                fmt!("tagset: %?, preference: %?", ts_list, pref)))
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

    fn try_send(&self, data : ~[u8], read : bool) -> Result<(), MongoErr> {
        self.refresh();

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

pub fn parse_host(host_str : &~str) -> Result<(~str, uint), MongoErr> {
    let mut port_str = fmt!("%?", MONGO_DEFAULT_PORT);
    let mut ip_str = match host_str.find_str(":") {
        None => host_str.to_owned(),
        Some(i) => {
            port_str = host_str.slice_from(i+1).to_owned();
            host_str.slice_to(i).to_owned()
        }
    };

    if ip_str == ~"localhost" { ip_str = LOCALHOST.to_owned(); }    // XXX must exist better soln

    match from_str(port_str) {
        None => Err(MongoErr::new(
                        ~"conn_replica::parse_host",
                        ~"unexpected host string format",
                        fmt!("host string should be \"[IP ~str]:[uint]\",
                                    found %s:%s", ip_str, port_str))),
        Some(k) => Ok((ip_str, k as uint)),
    }
}
