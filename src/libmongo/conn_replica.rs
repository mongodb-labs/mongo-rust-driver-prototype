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

use std::int;
use std::cell::*;
use extra::priority_queue::*;

use util::*;
use conn::Connection;
use conn_node::NodeConnection;

use bson::encode::*;

static NSERVER_TYPES : uint = 3;

pub enum ServerType {
    PRIMARY = 0,
    SECONDARY = 1,
    /*ARBITER = 2,
    PASSIVE = 3,*/
    OTHER = 2,
}

pub struct ReplicaSetConnection {
    /*priv seed : ~[NodeConnection],
    priv hosts: ~Cell<~[~PriorityQueue<@NodeConnection>]>,  // TODO RWARC?
    priv hosts_unord : ~Cell<~[~[@NodeConnection]]>,        // TODO RWARC?
    priv send_to : ~Cell<@NodeConnection>,                  // convenience
    priv recv_from : ~Cell<@NodeConnection>,                // XXX placeholder
    read_mode : ~Cell<READ_PREFERENCE>,*/
    seed : ~[NodeConnection],
    hosts: ~Cell<~[~PriorityQueue<@NodeConnection>]>,  // TODO RWARC?
    hosts_unord : ~Cell<~[~[@NodeConnection]]>,        // TODO RWARC?
    send_to : ~Cell<@NodeConnection>,                  // convenience
    recv_from : ~Cell<@NodeConnection>,                // XXX placeholder
    read_mode : ~Cell<READ_PREFERENCE>,
}

impl Connection for ReplicaSetConnection {
    /**
     * Connect to the ReplicaSetConnection.
     *
     * Goes through the seed list, finds the hosts, connects to each of the hosts,
     * and records the primary and secondaries.
     */
    pub fn connect(&self) -> Result<(), MongoErr> {
        if !self.hosts.is_empty() {
            Err(MongoErr::new(~"conn_replica::connect", ~"already connected", ~""))
        } else {
            self.reconnect()
        }
    }

    /**
     * Disconnects from replica set, emptying the cell holding the hosts as well.
     */
    pub fn disconnect(&self) -> Result<(), MongoErr> {
        let mut err = ~"";

        // disconnect from each of hosts; order doesn't matter here
        if !self.hosts.is_empty() {
            let host_mat = self.hosts.take();
            for host_mat.iter().advance |&host_type| {
                for host_type.iter().advance |&server| {
                    match server.disconnect() {
                        Ok(_) => (),
                        Err(e) => err.push_str(fmt!("\n\t%s", e.to_str())),
                    }
                }
            }
        }
        // XXX above dumb; would do below but cannot vec::concat
        //      since NodeConnection does not fufill Copy
        /*if !self.hosts.is_empty() {
            let hosts = vec::concat(self.hosts.take());
            for hosts.iter().advance |&server| {
                match server.disconnect() {
                    Ok(_) => (),
                    Err(e) => err.push_str(fmt!("\n\t%s", e.to_str())),
                }
            }
        }*/

        // empty out send_to and recv_from
        if !self.send_to.is_empty() { self.send_to.take(); }
        if !self.recv_from.is_empty() { self.recv_from.take(); }

        match err.len() {
            0 => Ok(()),
            _ => Err(MongoErr::new(
                    ~"conn_replica::disconnect",
                    ~"error disconnecting",
                    err)),
        }
    }

    pub fn send(&self, data : ~[u8]) -> Result<(), MongoErr> {
        if self.send_to.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"no send_to server",
                        ~"no server specified to which to send"));
        }
        let server = self.send_to.take();

        if server.ping.is_empty() {
            return Err(MongoErr::new(
                        ~"conn_replica::send",
                        ~"no send_to server",
                        ~"server down"));
        }
        let result = server.send(data);
        self.send_to.put_back(server);
        result
    }

    pub fn recv(&self) -> Result<~[u8], MongoErr> {
        match self._get_read_server() {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let server = self.recv_from.take();
        let result = server.recv();
        self.recv_from.put_back(server);
        result
    }
}

impl ReplicaSetConnection {
    pub fn new(seed_pairs : ~[(~str, uint)]) -> ReplicaSetConnection {
        let mut seed : ~[NodeConnection] = ~[];
        for seed_pairs.iter().advance |&(host, port)| {
            seed.push(NodeConnection::new(host.clone(), port));
        }
        ReplicaSetConnection::new_from_conn(seed)
    }

    fn new_from_conn(seed : ~[NodeConnection]) -> ReplicaSetConnection {
        ReplicaSetConnection {
            seed : seed,
            hosts_unord : ~Cell::new_empty(),
            hosts : ~Cell::new_empty(),
            recv_from : ~Cell::new_empty(),
            send_to : ~Cell::new_empty(),
            read_mode : ~Cell::new(PRIMARY_ONLY),
        }
    }

    /*pub fn add_node(&mut self, node : NodeConnection, server_type : ServerType) {
    }*/

    /**
     * Reconnects to the ReplicaSetConnection.
     */
    // XXX
    pub fn reconnect(&self) -> Result<(), MongoErr> {
        let mut host_list : ~[(~str, uint)] = ~[];
        let mut hosts : ~[~PriorityQueue<@NodeConnection>] = ~[];
        for NSERVER_TYPES.times {
            hosts.push(~PriorityQueue::new::<@NodeConnection>());
        }

        if !self.hosts.is_empty() { self.hosts.take(); }

        // get hosts by iterating through seeds
        for self.seed.iter().advance |&server| {
            // TODO spawn
            host_list = match (@server)._check_master_and_do(
                    |bson_doc : &~BsonDocument| -> Result<~[(~str, uint)], MongoErr> {
                let mut list = ~[];
                let mut err = None;

                let mut list_doc = None;
                let mut host_str = ~"";
                let mut pair = (~"", 0);

                // XXX rearrange once block functions can early return
                match bson_doc.find(~"hosts") {
                    None => (),
                    Some(doc) => {
                        let tmp_doc = copy *doc;
                        match tmp_doc {
                            Array(list) => list_doc = Some(list),
                            _ => err = Some(MongoErr::new(
                                        ~"conn_replica::connect",
                                        ~"isMaster runcommand response in unexpected format",
                                        fmt!("hosts field %?, expected encode::Array of hosts", *doc))),
                        }

                        if (copy err).is_none() {
                            let fields = copy list_doc.unwrap().fields;
                            for fields.iter().advance |&(_, @host_doc)| {
                                match host_doc {
                                    UString(s) => host_str = copy s,
                                    _ => err = Some(MongoErr::new(
                                            ~"conn_replica::connect",
                                            ~"isMaster runcommand response in unexpected format",
                                            fmt!("hosts field %?, expected list of host ~str", *doc))),
                                }

                                if (copy err).is_some() { break; }

                                match self._parse_host(copy host_str) {
                                    Ok(p) => pair = p,
                                    Err(e) => err = Some(MongoErr::new(
                                            ~"conn_replica::connect",
                                            ~"error parsing hosts",
                                            fmt!("-->\n%s", e.to_str()))),
                                }

                                if (copy err).is_none() { list.push(copy pair); }
                            }
                        }
                    }
                }

                if err.is_none() { Ok(list) }
                else { Err(err.unwrap()) }
            }) {
                Ok(list) => list,
                Err(e) => return Err(e),
            };

            if host_list.len() != 0 { break; }
        }

        // go through hosts to determine primary and secondaries
        for host_list.iter().advance |&(server_str, server_port)| {
            let server = @NodeConnection::new(server_str, server_port);

            let server_type = server._check_master_and_do(
                    |bson_doc : &~BsonDocument| -> Result<ServerType, MongoErr> {
                // check if is master
                let mut err = None;
                let mut is_master = false;
                let mut is_secondary = false;

                match bson_doc.find(~"ismaster") {
                    None => err = Some(MongoErr::new(
                                        ~"conn_replica::connect",
                                        ~"isMaster runcommand response in unexpected format",
                                        ~"no \"ismaster\" field")),
                    Some(doc) => {
                        match copy *doc {
                            Bool(val) => is_master = val,
                            _ => err = Some(MongoErr::new(
                                            ~"conn_replica::connect",
                                            ~"isMaster runcommand response in unexpected format",
                                            ~"\"ismaster\" field non-boolean")),
                        }
                    }
                }

                // check if is secondary
                if err.is_none() && !is_master {
                    match bson_doc.find(~"secondary") {
                        None => err = Some(MongoErr::new(
                                            ~"conn_replica::connect",
                                            ~"isMaster runcommand response in unexpected format",
                                            ~"no \"secondary\" field")),
                        Some(doc) => {
                            match copy *doc {
                                Bool(val) => is_secondary = val,
                                _ => err = Some(MongoErr::new(
                                                ~"conn_replica::connect",
                                                ~"isMaster runcommand response in unexpected format",
                                                ~"\"secondary\" field non-boolean")),
                            }
                        }
                    }
                }

                if err.is_none() {
                    if is_master { Ok(PRIMARY) }
                    else if is_secondary { Ok(SECONDARY) }
                    else { Ok(OTHER) }    // XXX not quite...?
                } else { Err(err.unwrap()) }
            });

            // record type of this server (primary or secondary) XXX
            match server_type {
                Ok(typ) => match typ {
                    PRIMARY => hosts[PRIMARY as int].push(server),
                    SECONDARY => hosts[SECONDARY as int].push(server),
                    OTHER => (),
                },
                Err(e) => return Err(e),
            }
        }

        // empty out everything first   // XXX
        if !self.send_to.is_empty() { self.send_to.take(); }
        if !self.recv_from.is_empty() { self.recv_from.take(); }

        // connect to primary iff 1
        let result = if hosts[PRIMARY as int].len() == 1 {
            // put alias in send_to
            self.send_to.put_back(*(hosts[PRIMARY as int].top()));

            // connect to primary
            let tmp = hosts[PRIMARY as int].top().connect();

            // put hosts back in
            self.hosts.put_back(hosts);

            tmp
        } else if hosts[PRIMARY as int].len() < 1 {
            Err(MongoErr::new(
                ~"conn_replica::connect",
                ~"no primary",
                ~"could not find primary"))
        } else {
            Err(MongoErr::new(
                ~"conn_replica::connect",
                ~"multiple primaries",
                ~"replica set cannot contain multiple primaries"))
        };

        result
    }

    pub fn _get_read_server(&self) -> Result<(), MongoErr> {
        let pref =  if !self.read_mode.is_empty() {
            self.read_mode.take()
        } else {
            return Err(MongoErr::new(
                            ~"conn_replica::_get_read_server",
                            ~"could not get server from which to read",
                            ~"no read preference specified"));
        };

        let hosts = self.hosts.take();

        let server = {
            let mut pri = None;

            // hosts borrowed here, but in block so given back afterwards
            let pri_tmp = hosts[PRIMARY as int].maybe_top();
            if pri_tmp.is_some() && !pri_tmp.unwrap().ping.is_empty() {
                 pri = pri_tmp;
            }
            let sec_list = (copy hosts[SECONDARY as int]).to_sorted_vec();

            if !self.recv_from.is_empty() { self.recv_from.take(); }

            // determine which server to set based on preference and tagsets
            let mut servers = ~[];
            let (pref_str, ts_list) = match pref {
                PRIMARY_ONLY => {
                    if pri.is_some() { servers.push(*pri.unwrap()); }
                    (~"PRIMARY_ONLY", &None)
                }
                PRIMARY_PREF(ref ts) => {
                    if pri.is_some() { servers.push(*pri.unwrap()); }
                    for sec_list.rev_iter().advance |&s| {
                        if s.ping.is_empty() { break; }
                        servers.push(s);
                    }
                    (~"PRIMARY_PREF", ts)
                }
                SECONDARY_ONLY(ref ts) => {
                    for sec_list.rev_iter().advance |&s| {
                        if s.ping.is_empty() { break; }
                        servers.push(s);
                    }
                    (~"SECONDARY_ONLY", ts)
                }
                SECONDARY_PREF(ref ts) => {
                    for sec_list.rev_iter().advance |&s| {
                        if s.ping.is_empty() { break; }
                        servers.push(s);
                    }
                    if pri.is_some() { servers.push(*pri.unwrap()); }
                    (~"SECONDARY_PREF", ts)
                }
                NEAREST(ref ts) => {
                    let mut tmp = copy *hosts[SECONDARY as int];
                    if pri.is_some() { tmp.push(*pri.unwrap()); }
                    let ordered = tmp.to_sorted_vec();
                    for ordered.rev_iter().advance |&s| {
                        if s.ping.is_empty() { break; }
                        servers.push(s);
                    }
                    (~"NEAREST", ts)
                }
            };

            match self._find_server(pref_str, servers, ts_list) {
                Ok(s) => s,
                Err(e) => return Err(e),
            }
        };

        server.connect();
        self.recv_from.put_back(server);

        // put everything back where found
        self.hosts.put_back(hosts);
        self.read_mode.put_back(pref);

        Ok(())
    }
    /**
     * Find server from which to read, given a list of
     * (available) servers and an optional list of tagsets.
     */
    fn _find_server(&self,  pref : ~str,
                            servers : ~[@NodeConnection],
                            tagsets : &Option<~[TagSet]>)
            -> Result<@NodeConnection, MongoErr> {
        let ts_list = match copy *tagsets {
            None => ~[TagSet::new(~[])],
            Some(l) => l,
        };

        // iterate through available servers, checking
        //      if they match the given tagset
        for servers.iter().advance |&server| {
            let server_tags = if server.tags.is_empty() {
                TagSet::new(~[])
            } else {
                server.tags.take()
            };
            server.tags.put_back(copy server_tags);

            for ts_list.iter().advance |ts| {
                if server_tags.matches(ts) {
                    return Ok(server);
                }
            }
        }

        Err(MongoErr::new(
                ~"conn_replica::_get_read_server",
                ~"could not find server matching tagset",
                fmt!("tagset: %?, preference: %?", ts_list, pref)))
    }

    /**
     * Parse host string found from isMaster command into host and port
     * (if specified, otherwise uses default 27017).
     *
     * # Arguments
     * `host_str` - string containing host information
     *
     * # Returns
     * host IP string/port pair on success, MongoErr on failure
     */
    fn _parse_host(&self, host_str : ~str) -> Result<(~str, uint), MongoErr> {
        let mut port_str = fmt!("%?", MONGO_DEFAULT_PORT);
        let mut ip_str = match host_str.find_str(":") {
            None => host_str,
            Some(i) => {
                port_str = host_str.slice_from(i+1).to_owned();
                host_str.slice_to(i).to_owned()
            }
        };

        if ip_str == ~"localhost" { ip_str = ~"127.0.0.1"; }    // XXX must exist better soln

        match int::from_str(port_str) {
            None => Err(MongoErr::new(
                            ~"conn_replica::_parse_host",
                            ~"unexpected host string format",
                            fmt!("host string should be \"[IP ~str]:[uint]\",
                                        found %s:%s", ip_str, port_str))),
            Some(k) => Ok((ip_str, k as uint)),
        }
    }
}
