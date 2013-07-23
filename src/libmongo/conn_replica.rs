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
use extra::priority_queue::*;
use extra::uv::*;
use extra::timer::*;
use extra::arc::*;

use util::*;
use conn::*;
use conn_node::NodeConnection;

use bson::encode::*;

#[deriving(Clone, Eq)]
pub enum ServerType {
    PRIMARY = 0,
    SECONDARY = 1,
    OTHER = 2,
}

pub struct ReplicaSetConnection {
    seed : ~ARC<~[(~str, uint)]>,       // XXX for now, no adding seeds//RWARC appears to have bug and require impl *Clone* explicitly
    state : Cell<ReplicaSetData>,      // XXX RWARC
    write_to : Cell<NodeConnection>,
    read_from : Cell<NodeConnection>,
    port_state : Cell<Port<ReplicaSetData>>,
    chan_reconn : Cell<Chan<bool>>,
    read_pref : Cell<READ_PREFERENCE>,
    read_pref_changed : Cell<bool>,
}

/**
 * All `Send`able data associated with `ReplicaSetConnection`s.
 */
struct ReplicaSetData {
    pri : Option<NodeConnectionData>,
    sec : ~PriorityQueue<NodeConnectionData>,
    err : Option<MongoErr>,
}
// not actually needed for now but probably good to have anyway
impl Clone for ReplicaSetData {
    pub fn clone(&self) -> ReplicaSetData {
        let mut sec = ~PriorityQueue::new();
        for self.sec.iter().advance |&elem| {
            sec.push(elem.clone());
        }
        ReplicaSetData {
            pri : self.pri.clone(),
            sec : sec,
            err : self.err.clone(),
        }
    }
}
impl ReplicaSetData {
    pub fn new( pri : Option<NodeConnectionData>,
                sec : ~PriorityQueue<NodeConnectionData>,
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
                    ~"already connected; call reconnect instead"))
        } else {
            // po/ch for kill of reconnect thread
            let (port_reconn, chan_reconn) = stream();
            // po/ch for port for recving kill of reconnect thread
            let (port_port_reconn, chan_port_reconn) = stream();
            self.chan_reconn.put_back(chan_reconn);
            let port = ReplicaSetConnection::spawn_reconnect(
                                    &self.seed,
                                    port_reconn,
                                    (port_port_reconn, chan_port_reconn));

            self.port_state.put_back(port);
            self.reconnect()
        }
    }

    pub fn disconnect(&self) -> Result<(), MongoErr> {
        let mut err_str = ~"";

        // kill reconnect thread
        if !self.chan_reconn.is_empty() {
println("sent disconnect ln 112");
            self.chan_reconn.take().send(false);
        } else {
            err_str.push_str(MongoErr::new(
                    ~"conn_replica::disconnect",
                    ~"unexpected state of reconnect thread",
                    ~"reconnect thread already dead").to_str());
        }

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

    pub fn send(&self, data : ~[u8], read : bool) -> Result<(), MongoErr> {
        // refresh server data via reconnect
        match self.reconnect() {
            Ok(_) => (),
            Err(e) => return Err(e),
        }
println("[send] reconnected");

        // choose correct server to which to send
        let server_cell = if read { &self.read_from } else { &self.write_to };
println("[send] chose server_cell");
        if server_cell.is_empty() {
println("[send] EMPTY SERVER");
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"no primary found"));
        }
        let server = server_cell.take();
println(fmt!("[send] server: %?", server));

        // even if found server, if ping is empty, server down
        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"server down"));
        }

println("pre-send");
        // otherwise send and then put everything back
        let result = server.send(data, read);
println("post-send");
        server_cell.put_back(server);
        result
    }

    pub fn recv(&self, read : bool) -> Result<~[u8], MongoErr> {
        // refresh server data via reconnect
        match self.reconnect() {
            Ok(_) => (),
            Err(e) => return Err(e),
        }
println("[recv] reconnected");

        // choose correct server from which to recv
        let server_cell = if read { &self.read_from } else { &self.write_to };
println("[recv] chose server_cell");
        if server_cell.is_empty() {
println("[recv] EMPTY SERVER");
            return Err(MongoErr::new(
                        ~"conn_replica::recv",
                        ~"cannot recv",
                        fmt!("no server from which to receive; %s op",
                            if read { "read" } else { "write" })));
        }
        let server = server_cell.take();
println(fmt!("[recv] server: %?", server));

        // even if found server, if ping is empty, server down
        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot receive",
                        ~"server down"));
        }

println("pre-recv");
        // otherwise recv and then put everything back
        let result = server.recv(read);
println("post-recv");
        server_cell.put_back(server);
        result
    }
}

#[deriving(Clone)]
struct NodeConnectionData {
    ip : ~str,
    port : uint,
    typ : ServerType,   // already separated before cmp; for convenience
    ping : Option<u64>,
    tagset : TagSet,
}
// Inequalities seem all backwards because max-heaps.
impl Ord for NodeConnectionData {
    pub fn lt(&self, other : &NodeConnectionData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(t1), Some(t2)) => t1 >= t2,
        }
    }
    pub fn le(&self, other : &NodeConnectionData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(t1), Some(t2)) => t1 > t2,
        }
    }
    pub fn gt(&self, other : &NodeConnectionData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(t1), Some(t2)) => t1 <= t2,
        }
    }
    pub fn ge(&self, other : &NodeConnectionData) -> bool {
        match (self.ping, other.ping) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(t1), Some(t2)) => t1 < t2,
        }
    }
}
impl Eq for NodeConnectionData {
    pub fn eq(&self, other : &NodeConnectionData) -> bool {
            self.ip == other.ip
        &&  self.port == other.port
        &&  self.tagset == other.tagset
    }
    pub fn ne(&self, other : &NodeConnectionData) -> bool {
            self.ip != other.ip
        ||  self.port != other.port
        ||  self.tagset != other.tagset
    }
}
impl NodeConnectionData {
    pub fn new( ip : ~str,
                port : uint,
                typ : ServerType,
                ping : Option<u64>,
                tagset : Option<TagSet>) -> NodeConnectionData {
        NodeConnectionData {
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
                || self.sec.len() != other.sec.len() {
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
                || self.sec.len() != other.sec.len() {
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
    pub fn new(seed : ~[(~str, uint)]) -> ReplicaSetConnection {
        ReplicaSetConnection {
            seed : ~ARC(seed),
            state : Cell::new_empty(),
            write_to : Cell::new_empty(),
            read_from : Cell::new_empty(),
            port_state : Cell::new_empty(),
            chan_reconn : Cell::new_empty(),
            read_pref : Cell::new(PRIMARY_ONLY),
            read_pref_changed : Cell::new(true),
        }
    }

    fn reconnect_with_seed(seed : &~ARC<~[(~str, uint)]>) -> ReplicaSetData {
        let hosts = match ReplicaSetConnection::_get_host_list(seed) {
            Ok(l) => l,
            Err(e) => return ReplicaSetData::new(None, ~PriorityQueue::new(), Some(e)),
        };

        ReplicaSetConnection::_get_replica_set_data(hosts)
    }

    fn _get_host_list(seed : &~ARC<~[(~str, uint)]>)
                -> Result<~[(~str, uint)], MongoErr> {
        // remember number of expected responses
        let n = seed.get().len();

        // po/ch for sending host list
        let (port_hosts, chan_hosts) = stream();
        let chan_hosts = SharedChan::new(chan_hosts);

        // po/ch for sending seed list
        let (port_seed, chan_seed) = stream();
        chan_seed.send(seed.clone());

        // check seeds
        do spawn_supervised {
            let seed_arc = port_seed.recv();
            let seed_list = seed_arc.get();

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

                                if err.clone().is_none() {
                                    let fields = copy list_doc.unwrap().fields;
                                    let mut host_str = ~"";
                                    for fields.iter().advance |&(_, @host_doc)| {
                                        match host_doc {
                                            UString(s) => host_str = s.clone(),
                                            _ => {
                                                err = Some(MongoErr::new(
            ~"conn_replica::reconnect_with_seed",
            ~"ismaster response in unexpected format",
            fmt!("hosts field %?, expected list of host ~str", *doc)));
                                                break;
                                            }
                                        }

                                        match _parse_host(host_str.clone()) {
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
                                chan_hosts_tmp.send(Ok(list));
println("failing ln 408");
                                fail!(); // take down whole thread
                            } else { chan_hosts_tmp.send(Ok(~[])); }
                        }
                        Err(e) => chan_hosts_tmp.send(Err(e)),
                    }
                }
            }
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

    fn _get_replica_set_data(hosts : ~[(~str, uint)]) -> ReplicaSetData {
        let mut pri = None;
        let mut sec = ~PriorityQueue::new::<NodeConnectionData>();

        // remember expected number of responses
        let n = hosts.len();

        // po/ch for receiving NodeConnectionData
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
                                                UString(val) => tags.set_tag((k,val)),
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
                        let stats = NodeConnectionData::new(
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
                                ~"error while connecting to host",
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

        if err_str.len() > 0 {
            err = Some(MongoErr::new(
                ~"conn_replica::reconnect_with_seed",
                ~"error while connecting to host",
                err_str.clone()));
        }
        ReplicaSetData::new(pri, sec, err)
    }

    fn spawn_reconnect( seed : &~ARC<~[(~str, uint)]>,
                        port_reconn : Port<bool>,
                        (port_port_reconn, chan_port_reconn) : (Port<Port<bool>>, Chan<Port<bool>>))
                -> Port<ReplicaSetData> {
        // send off port for recving kill of reconnect thread
        chan_port_reconn.send(port_reconn);

        // po/ch for seed list
        let (port_seed, chan_seed) = stream();
        chan_seed.send(seed.clone());
        // po/ch for ReplicaSetData
        let (port_state, chan_state) = stream();

        // actually spawn reconnection thread
        do spawn_supervised {
            // pick up port for recving kill of reconnect thread
            let port_reconn = port_port_reconn.recv();
            do spawn {
println("before fail ln 604");
                if !port_reconn.recv() {
println("\"after\" fail ln 606");
                    fail!();
                }
            }

            let seed = port_seed.recv();
println("received seedlist");
//let iotask = global_loop::get();
            loop {
println("looping ln 607");
                // pick up seed and state
                let state = ReplicaSetConnection::reconnect_with_seed(&seed);
println(fmt!("sending state %?", state));
                chan_state.send(state);
                //sleep(&global_loop::get(), 1000 * 60 * 5);    // XXX new io
                sleep(&global_loop::get(), 10);                 // XXX new io
                //sleep(&iotask, 10);                 // XXX new io
            }
        }

        // send back port to pick up reconnection states
        port_state
    }

    /**
     * Reconnects to replica set.
     *
     * Reconnects by fishing out latest state sent from reconnection task
     * to main task, and checking if the write_to or read_from servers
     * need to be updated (because primaries/secondaries, their tags,
     * or read preference has changed).
     *
     * Called before every send or recv.
     */
    pub fn reconnect(&self) -> Result<(), MongoErr> {
        if self.port_state.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::reconnect",
                        ~"could not reconnect",
                        ~"reconnect thread dead"));
        }

println(fmt!("in reconnection; self %?", self));

        // fish out most up-to-date information
        let port_state = self.port_state.take();
        let mut tmp_state = None;
        if !self.state.is_empty() {
println("updated before");
            // has been updated before; get latest in stream
            while port_state.peek() {
                tmp_state = Some(port_state.recv());
            }
        } else {
println("first update");
            // first update; wait if needed to pick up first state
            tmp_state = Some(port_state.recv());
        }
        self.port_state.put_back(port_state);
println(fmt!("tmp_state : %?", tmp_state));

        // by end, read_pref will have been accounted for; note updated
        let pref_changed = self.read_pref_changed.take();
        self.read_pref_changed.put_back(false);

        // only update read_from or write_to if:
        //  0) no prior state (read_from and write_to)
        //      old_state.is_none() && tmp_state.is_some()
        //  1) state has changed (read_from and write_to)
        //      old_state.is_some() && tmp_state.is_some() && old_state != tmp_state
        //  2) read preference has changed (read_from)
        //      a) tmp_state.is_none() => refresh according to old_state
        //      b) tmp_state.is_some() => refresh according to tmp_state
        let old_state = if self.state.is_empty() {
            None
        } else {
            Some(self.state.take())
        };  // self.state now certainly empty

        let state = match (old_state, tmp_state) {
            (None, None) => {
                // no state to update but read_pref "changed"
                // read_pref already marked updated; undo that mark
                self.read_pref_changed.take();
                self.read_pref_changed.put_back(true);
                return Ok(());
            }
            (Some(os), None) => {
                // no new state
                if !pref_changed {
                    // read_pref unchanged; no refresh needed
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
        let maybe_err = state.clone().err;
        if maybe_err.is_none() {
            match self._refresh_write_to(state.clone()) {
                Ok(_) => (),
                Err(e) => err_str.push_str(e.to_str()),
            }
            match self._refresh_read_from(state.clone()) {
                Ok(_) => (),
                Err(e) => err_str.push_str(e.to_str()),
            }
        } else {
            return Err(maybe_err.unwrap());
        }

        self.state.put_back(state);

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                    ~"conn_replica::reconnect",
                    ~"error while reconnecting",
                    err_str)) }






















/*
        if tmp_state.is_none() {
            // no new state; nothing to update
            return Ok(());
        }

        let state = tmp_state.unwrap();
        let new_state = state.clone();

println(fmt!("post reconnection prelude; self %?", self));

        // refresh write_to and read_from servers as needed
        let mut err_str = ~"";
        if         self.state.is_empty()
                || state != self.state.take()
                || self.read_pref_changed.take() {
println("updating...");
            if !self.write_to.is_empty() { self.write_to.take().disconnect(); }
            if !self.read_from.is_empty() { self.read_from.take().disconnect(); }

            let maybe_err = state.err.clone();
            if maybe_err.is_none() {
                // update write_to
                match self._refresh_write_to(state.clone()) {
                    Ok(_) => (),
                    Err(e) => err_str.push_str(e.to_str()),
                };

                // update read_from
                match self._refresh_read_from(state.clone()) {
                    Ok(_) => (),
                    Err(e) => err_str.push_str(e.to_str()),
                };
            } else {
println(fmt!("error?!?! %?", maybe_err.clone().unwrap()));
                return Err(maybe_err.unwrap());
            }
        }

        // update state and read_pref_changed
        if !self.read_pref_changed.is_empty() {
            self.read_pref_changed.take();
            self.read_pref_changed.put_back(false);
        }
        self.state.put_back(new_state);

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                    ~"conn_replica::reconnect",
                    ~"error while reconnecting",
                    err_str)) }
*/
    }

    fn _refresh_write_to(&self, state : ReplicaSetData)
                -> Result<(), MongoErr> {
        let dat = state.pri.clone().unwrap();
        let pri = NodeConnection::new(dat.ip.clone(), dat.port);
        pri.tags.take();
        pri.tags.put_back(dat.tagset.clone());
        pri.ping.put_back(dat.ping.unwrap());
        let result = pri.connect();
        self.write_to.put_back(pri);
        result
    }

    fn _refresh_read_from(&self, state : ReplicaSetData)
                -> Result<(), MongoErr> {
        let read_pref = self.read_pref.take();

        let mut servers = ~[];

        let pri = state.clone().pri.unwrap();
        let mut sec = ~PriorityQueue::new();

        for state.sec.iter().advance |&s| {
            sec.push(s.clone());
        }
println(fmt!("PRI:%?\nSEC:%?", pri, sec));

        let (pref_str, ts_list) = match read_pref {
            PRIMARY_ONLY => {
                servers.push(pri);
                (~"PRIMARY_ONLY", &None)
            }
            PRIMARY_PREF(ref ts) => {
                servers.push(pri);
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
                servers.push(pri);
                (~"SECONDARY_PREF", ts)
            }
            NEAREST(ref ts) => {
                sec.push(pri);
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
                            servers : ~[NodeConnectionData],
                            tagsets : &Option<~[TagSet]>)
                -> Result<NodeConnection, MongoErr> {
        let ts_list = match (*tagsets).clone() {
            None => ~[TagSet::new(~[])],
            Some(l) => l,
        };

        // iterate through available servers, checking if they match
        //      given tagset
        for servers.iter().advance |&server| {
            for ts_list.iter().advance |ts| {
                if server.tagset.matches(ts) {
                    let result = NodeConnection::new(server.ip.clone(), server.port);
                    result.tags.take();
                    result.tags.put_back(ts.clone());
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
}

fn _parse_host(host_str : ~str) -> Result<(~str, uint), MongoErr> {
    let mut port_str = fmt!("%?", MONGO_DEFAULT_PORT);
    let mut ip_str = match host_str.find_str(":") {
        None => host_str,
        Some(i) => {
            port_str = host_str.slice_from(i+1).to_owned();
            host_str.slice_to(i).to_owned()
        }
    };

    if ip_str == ~"localhost" { ip_str = ~"127.0.0.1"; }    // XXX must exist better soln

    match from_str(port_str) {
        None => Err(MongoErr::new(
                        ~"conn_replica::_parse_host",
                        ~"unexpected host string format",
                        fmt!("host string should be \"[IP ~str]:[uint]\",
                                    found %s:%s", ip_str, port_str))),
        Some(k) => Ok((ip_str, k as uint)),
    }
}
