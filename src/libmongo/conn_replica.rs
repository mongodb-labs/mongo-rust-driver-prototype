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
    state : ~Cell<ReplicaSetData>,      // XXX RWARC
    write_to : ~Cell<NodeConnection>,
    read_from : ~Cell<NodeConnection>,
    port_state : ~Cell<Port<ReplicaSetData>>,
    chan_reconn : ~Cell<Chan<bool>>,
    read_pref : ~Cell<READ_PREFERENCE>,
}

/**
 * All `Send`able data associated with `ReplicaSetConnection`s.
 */
struct ReplicaSetData {
    pri : Option<NodeConnectionData>,
    sec : ~PriorityQueue<NodeConnectionData>,
    update_write_to : bool,
    update_read_from : bool,
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
            update_write_to : self.update_write_to,
            update_read_from : self.update_read_from,
            err : self.err.clone(),
        }
    }
}
impl ReplicaSetData {
    pub fn new( pri : Option<NodeConnectionData>,
                sec : ~PriorityQueue<NodeConnectionData>,
                update_write_to : bool,
                update_read_from : bool,
                err : Option<MongoErr>) -> ReplicaSetData {
        ReplicaSetData {
            pri : pri,
            sec : sec,
            update_write_to : update_write_to,
            update_read_from : update_read_from,
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
            //let (port, _) = ReplicaSetConnection::spawn_reconnect(
            let port = ReplicaSetConnection::spawn_reconnect(
                                    &self.seed,
                                    port_reconn,
                                    (port_port_reconn, chan_port_reconn));

            self.state.take();
            //self.state.put_back(result);
            self.port_state.put_back(port);
            self.reconnect();
            Ok(())
        }
        /*if !self.hosts.is_empty() {
            Err(MongoErr::new(
                    ~"conn_replica::connect",
                    ~"cannot connect",
                    ~"already connected; call reconnect instead"))
        } else {
            Ok(())*/
/*
            // TODO spawn reconnect thread
            match self.reconnect() {
                Ok(_) => {
                    // port and chan for kill of reconnection thread
                    let (port_reconn, chan_reconn) = stream();
                    // port and chan for port for recving kill of connection thread
                    let (port_port_reconn, chan_port_reconn) = stream();
                    chan_port_reconn.send(port_reconn);
                    if !self.chan_reconn.is_empty() { self.chan_reconn.take(); }
                    self.chan_reconn.put_back(chan_reconn);

                    // port and chan for seed list
                    let (port_seed, chan_seed) = stream();
                    chan_seed.send(self.seed.clone().take());

                    // now actually spawn
                    do spawn_supervised {
                        // pick up port for recving kill of connection thread
                        let port_reconn = port_port_reconn.recv();
                        do spawn { assert!(port_reconn.recv()); }

                        let iotask = global_loop::get();
                        loop {
                            sleep(&iotask, 60*5);
                            let seed = port_seed.recv();
                            match ReplicaSetConnection::reconnect_with_seed(seed) {
                                Ok(_) => (),
                                Err(e) => fail!(e.to_str()),
                            }
                        }
                    }

                    Ok(())
                }
                Err(e) => Err(e),
            }
*/
        //}
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
/*
        let mut err_str = ~"";

        // kill reconnect thread
        if self.chan_reconn.is_empty() {
            err_str.push_str(MongoErr::new(
                    ~"conn_replica::disconnect",
                    ~"unexpected state of reconnect thread",
                    ~"reconnect thread already dead").to_str());
        } else {
            let chan_reconn = self.chan_reconn.take();
            chan_reconn.send(false);
        }

        // empty out send and recv servers
        if !self.write_to.is_empty() {
            match self.write_to.take().disconnect() {
                Ok(_) => (),
                Err(e) => err_str.push_str(fmt!("\n%s", e.to_str())),
            }
        }

        if !self.read_from.is_empty() {
            match self.read_from.take().disconnect() {
                Ok(_) => (),
                Err(e) => err_str.push_str(fmt!("\n%s", e.to_str())),
            }
        }

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                        ~"conn_replica::disconnect",
                        ~"error disconnecting from server[s]",
                        err_str)) }
*/
    }

    pub fn send(&self, data : ~[u8], read : bool) -> Result<(), MongoErr> {
        let server_cell = if read { &self.read_from } else { &self.write_to };

        if server_cell.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"no primary found"));
        }

        let server = server_cell.take();

        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot send",
                        ~"server down"));
        }

        let result = server.send(data, true);
        server_cell.put_back(server);
        result
    }

    pub fn recv(&self) -> Result<~[u8], MongoErr> {
        // XXX
        if self.read_from.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot receive",
                        ~"no receiving server found"));
        }

        let server = self.read_from.take();

        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"cannot receive",
                        ~"server down"));
        }

        let result = server.recv();
        self.read_from.put_back(server);
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
            state : ~Cell::new(ReplicaSetData {
                pri : None,
                sec : ~PriorityQueue::new(),
                update_write_to : true,
                update_read_from : true,
                err : None,
            }),
            write_to : ~Cell::new_empty(),
            read_from : ~Cell::new_empty(),
            port_state : ~Cell::new_empty(),
            chan_reconn : ~Cell::new_empty(),
            read_pref : ~Cell::new(PRIMARY_ONLY),
        }
    }

    fn reconnect_with_seed(seed : &~ARC<~[(~str, uint)]>) -> ReplicaSetData {
        let hosts = match ReplicaSetConnection::_get_host_list(seed) {
            Ok(l) => l,
            Err(e) => return ReplicaSetData::new(None, ~PriorityQueue::new(), true, true, Some(e)),
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
                                assert!(false); // take down whole thread
                            } else { chan_hosts_tmp.send(Ok(~[])); }
                        }
                        Err(e) => chan_hosts_tmp.send(Err(e)),
                    }
                }
            }
        }

        // try to recv a host list
        let mut i = 0;
        let mut err_str = ~"";
        loop {
            match port_hosts.recv() {
                Ok(l) => if l.len() > 0 { return Ok(l); },
                Err(e) => err_str.push_str(e.to_str()),
            }

            i += 1;
            if i >= n {
                // received all acks from seeds; no host list
                return Err(MongoErr::new(
                            ~"conn_replica::reconnect_with_seed",
                            ~"no host list found",
                            err_str));
            }
        }
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
                        |bson_doc : &~BsonDocument| -> Result<ServerType, MongoErr> {
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
                        match typ {
                            Some(t) => Ok(t),
                            None => Ok(OTHER),
                        }
                    } else {
                        Err(err.unwrap())
                    }
                }) {
                    Ok(t) => {
                        let stats = NodeConnectionData::new(
                            ip, port, t, Some(server.ping.take()), None
                        );
                        chan_server_tmp.send(Ok(stats));
                    }
                    Err(e) => chan_server_tmp.send(Err(e)),
                }
            }
        }

        // properly insert now-typed hosts into respective locations
        let mut i = 0;
        let mut err = None;
        let mut err_str = ~"";
        loop {
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

            i += 1;
            if i >= n {
                // received all acks from hosts; done
                if err_str.len() > 0 {
                    err = Some(MongoErr::new(
                        ~"conn_replica::reconnect_with_seed",
                        ~"error while connecting to host",
                        err_str.clone()));
                }
                break;
            }
        }

        ReplicaSetData::new(pri, sec, true, true, err)
    }

    fn spawn_reconnect( seed : &~ARC<~[(~str, uint)]>,
                        port_reconn : Port<bool>,
                        (port_port_reconn, chan_port_reconn) : (Port<Port<bool>>, Chan<Port<bool>>))
                //-> (Port<ReplicaSetData>, ReplicaSetData) {
                -> Port<ReplicaSetData> {
        // send off port for recving kill of reconnect thread
        chan_port_reconn.send(port_reconn);

        // po/ch for seed list
        let (port_seed, chan_seed) = stream();
        chan_seed.send(seed.clone());
        // po/ch for ReplicaSetData
        let (port_result, chan_result) = stream();

        // actually spawn reconnection thread
        do spawn_supervised {
            // pick up port for recving kill of reconnect thread
            let port_reconn = port_port_reconn.recv();
            do spawn { assert!(port_reconn.recv()); }

            let seed = port_seed.recv();
            let iotask = &global_loop::get();
            loop {
                // pick up seed and state
                let result = ReplicaSetConnection::reconnect_with_seed(&seed);
                chan_result.send(result);
                sleep(iotask, 1000 * 60 * 5);
            }
        }

        // just get first
        //let state = port_result.recv();
//println(fmt!("state...%?", copy state));
        //(port_result, state)
        port_result
    }

    pub fn reconnect(&self) -> Result<(), MongoErr> {
        if self.port_state.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::reconnect",
                        ~"could not reconnect",
                        ~"reconnect thread dead"));
        }

        let port_state = self.port_state.take();
        let mut tmp_state = port_state.try_recv();
println(fmt!("tmp_state (fst)...%?\n", tmp_state));
        if tmp_state.is_none() { return Ok(()); }
        else {
            while port_state.peek() {
                tmp_state = port_state.try_recv();
            }
        }
println(fmt!("tmp_state (snd)...%?\n", tmp_state));

        /*while !tmp_state.is_none() {
            tmp_state = port_state.try_recv();
println(fmt!("tmp_state...%?\n", tmp_state));
            if !port_state.peek() { return Ok(()); }
        }*/

println("before let state = tmp_state.unwrap();");
        let state = tmp_state.unwrap();
        let new_state = state.clone();
println(fmt!("also, cloned state: %?\n", new_state));

        let mut err_str = ~"";
println(fmt!("before possible update: %?\n", self));
        if self.state.is_empty() || state != self.state.take() {
            if !self.write_to.is_empty() { println("disconnected write_to\n"); self.write_to.take().disconnect(); }
            if !self.read_from.is_empty() { println("disconnected read_from\n"); self.read_from.take().disconnect(); }

            let maybe_err = state.err.clone();
            if maybe_err.is_none() {
println("no err, possibly updating write_to and read_from");

                // update write_to
                match self._refresh_write_to(state.clone()) {
                    Ok(_) => println("refreshed write_to"),
                    Err(e) => err_str.push_str(e.to_str()),
                };

                // update read_from
                match self._refresh_read_from(state.clone()) {
                    Ok(_) => println("refreshed read_from"),
                    Err(e) => err_str.push_str(e.to_str()),
                };
            } else {
println("before return Err(maybe_err.unwrap());");
                return Err(maybe_err.unwrap());
            }
        }

        self.state.put_back(new_state);
println(fmt!("after possible update: %?\n", self));

        if err_str.len() == 0 { Ok(()) }
        else { Err(MongoErr::new(
                    ~"conn_replica::reconnect",
                    ~"error while reconnecting",
                    err_str)) }
    }

    fn _refresh_write_to(&self, state : ReplicaSetData) -> Result<(), MongoErr> {
println("before let dat = state.pri.clone().unwrap();");
        let dat = state.pri.clone().unwrap();
        let pri = NodeConnection::new(dat.ip.clone(), dat.port);
        // XXX also get and put in tags
        pri.ping.put_back(dat.ping.unwrap());
println("just put ping in");
        let result = pri.connect();
println("about to put pri back");
        self.write_to.put_back(pri);
        result
    }

    fn _refresh_read_from(&self, state : ReplicaSetData) -> Result<(), MongoErr> {
        let read_pref = self.read_pref.take();

        let mut servers = ~[];

        let pri = state.clone().pri.unwrap();
        let mut sec = ~PriorityQueue::new();

        for state.sec.iter().advance |&s| {
            sec.push(s.clone());
        }

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
println("about to put back tags");
                    result.tags.take();
                    result.tags.put_back(ts.clone());
println("about to put back ping");
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
