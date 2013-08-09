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
//use std::rt::io::net::*;
//use std::rt::io::net::tcp::*;
use extra::net::ip::*;
use extra::net::tcp::*;
use extra::uv::*;
use extra::time::*;

use util::*;
use conn::*;
use client::Client;

use bson::encode::*;

/**
 * Connection between Client and Server. Routes user requests to
 * server and hands back wrapped result for Client to parse.
 */
pub struct NodeConnection {
    priv server_ip_str : ~str,
    priv server_port : uint,
    priv server_ip : Cell<IpAddr>,
    /* BEGIN BLOCK to remove with new io */
    priv sock : Cell<@Socket>,
    priv port : Cell<@GenericPort<PortResult>>,
    /* END BLOCK to remove with new io */
    //priv stream : Cell<TcpStream>, /* BLOCK to include with new io */
    ping : Cell<u64>,
    tags : Cell<TagSet>,
    timeout : Cell<u64>,
}

impl Connection for NodeConnection {
    pub fn connect(&self) -> Result<(), MongoErr> {
        // sanity check: should not connect if already connected (?)
        if !(self.sock.is_empty() && self.port.is_empty()) {
        //if !self.stream.is_empty() {
            return Err(MongoErr::new(
                            ~"conn::connect",
                            ~"pre-existing stream",
                            ~"cannot override existing stream"));
        }

        // parse IP addr
        let ip = match v4::try_parse_addr(self.server_ip_str) {
            Err(e) => return Err(MongoErr::new(
                                    ~"conn::connect",
                                    ~"IP parse err",
                                    e.err_msg.clone())),
            Ok(addr) => addr,
        };

        /* BEGIN BLOCK to include with new io */
        /*let stream = match TcpStream::connect(copy ip) {
            None => return Err(MongoErr::new(
                                    ~"conn::connect",
                                    ~"could not connect to TcpStream",
                                    fmt!("%?", ip.clone()))),
            Some(s) => s,
        };
        self.stream.put_back(stream);*/
        /* END BLOCK to include with new io */

        /* BEGIN BLOCK to remove with new io */
        // set up the socket --- for now, just v4
        // XXX
        let sock = match connect(ip, self.server_port, &global_loop::get()) {
            Err(GenericConnectErr(ename, emsg)) => return Err(MongoErr::new(~"conn::connect", ename, emsg)),
            Err(ConnectionRefused) => return Err(MongoErr::new(~"conn::connect", ~"EHOSTNOTFOUND", ~"Invalid IP or port")),
            Ok(sock) => @sock as @Socket
        };

        // start the read port
        let port = match sock.read_start() {
            Err(e) => return Err(MongoErr::new(
                                    ~"conn::connect",
                                    e.err_name.clone(),
                                    e.err_msg.clone())),
            Ok(p) => p as @GenericPort<PortResult>,
        };

        // hand initialized fields to self
        self.sock.put_back(sock);
        self.port.put_back(port);
        /* END BLOCK to remove with new io */

        self.server_ip.put_back(ip);

        Ok(())
    }

    pub fn disconnect(&self) -> Result<(), MongoErr> {
        // NO sanity check: don't really care if disconnect unconnected connection

        // nuke port first (we can keep the ip)
        if !self.port.is_empty() { self.port.take(); }

        if !self.sock.is_empty() {
            match self.sock.take().read_stop() {
                Err(e) => return Err(MongoErr::new(
                                        ~"conn::disconnect",
                                        e.err_name.clone(),
                                        e.err_msg.clone())),
                Ok(_) => (),
            }
        }
        //if !self.stream.is_empty() { self.stream.take(); }
        if !self.ping.is_empty() { self.ping.take(); }

        Ok(())
    }

    pub fn reconnect(&self) -> Result<(), MongoErr> {
        self.disconnect();
        self.connect()
    }

    pub fn send(&self, data : &[u8], _ : bool) -> Result<(), MongoErr> {
        /* BEGIN BLOCK to remove with new io */
        if self.sock.is_empty() {
            return Err(MongoErr::new(
                            ~"node_conn::send",
                            ~"unknown send err",
                            ~"cannot send on null socket"));
        } else {
            let sock = self.sock.take();
            let result = match sock.write_future(data.to_owned()).get() {
                Err(e) => Err(MongoErr::new(
                            ~"node_conn::send",
                            e.err_name.clone(),
                            e.err_msg.clone())),
                Ok(_) => Ok(()),
            };
            self.sock.put_back(sock);
            result
        }
        /* END BLOCK to remove with new io */

        /* BEGIN BLOCK to include with new io */
        /*if self.stream.is_empty() {
            return Err(MongoErr::new(
                            ~"conn::send",
                            ~"no stream",
                            ~"cannot write to null stream"));
        } else {
            let stream = self.stream.take();
            stream.write(data);
            self.stream.put_back(stream);
            Ok(())
        }*/
        /* END BLOCK to include with new io */
    }

    /* BEGIN BLOCK to remove with new io */
    pub fn recv(&self, buf : &mut ~[u8], _ : bool) -> Result<uint, MongoErr> {
         // sanity check and unwrap: should not send on an unconnected connection
        if self.port.is_empty() {
            return Err(MongoErr::new(
                            ~"conn::recv",
                            ~"unknown recv err",
                            ~"cannot receive from null port"));
        } else {
            let port = self.port.take();
            let result = match port.recv() {
                Err(e) => Err(MongoErr::new(
                                ~"conn::recv",
                                e.err_name.clone(),
                                e.err_msg.clone())),
                Ok(msg) => {
                    for msg.iter().advance |&b| {
                        buf.push(b);
                    }
                    Ok(buf.len())
                }
            };
            self.port.put_back(port);
            result
        }
    }
    /* END BLOCK to remove with new io */
    /* BEGIN BLOCK to include with new io */
    /*pub fn recv(&self, buf : &mut [u8], _ : bool) -> Result<uint, MongoErr> {
        if self.stream.is_empty() {
            return Err(MongoErr::new(
                            ~"conn::recv",
                            ~"no stream",
                            ~"cannot receive from null stream"));
        } else {
            let stream = self.stream.take();
            let result = match stream.read(buf) {
                Some(n) => Ok(n),
                None => return Err(MongoErr::new(
                            ~"conn::recv",
                            ~"no bytes read",
                            ~"")),
            };
            self.stream.put_back(stream);
            result
        }
    }*/
    /* BEGIN BLOCK to include with new io */

    pub fn set_timeout(&self, timeout : u64) -> u64 {
        let prev = self.timeout.take();
        self.timeout.put_back(timeout);
        prev
    }

    pub fn get_timeout(&self) -> u64 {
        self.timeout.clone().take()
    }
}

impl Eq for NodeConnection {
    pub fn eq(&self, other : &NodeConnection) -> bool {
           self.server_ip_str == other.server_ip_str
        && self.server_port == other.server_port
        && self.tags == other.tags
    }

    pub fn ne(&self, other : &NodeConnection) -> bool {
        !( self.server_ip_str == other.server_ip_str
        && self.server_port == other.server_port
        && self.tags == other.tags)
    }
}

impl NodeConnection {
    /**
     * Create a new NodeConnection with given IP and port.
     *
     * # Arguments
     * `server_ip_str` - string representing IP of server
     * `server_port` - uint representing port on server
     *
     * # Returns
     * NodeConnection that can be connected to server node
     * indicated.
     */
    pub fn new(server_ip_str : &str, server_port : uint) -> NodeConnection {
        NodeConnection {
            server_ip_str : server_ip_str.to_owned(),
            server_port : server_port,
            server_ip : Cell::new_empty(),
            /* BEGIN BLOCK to remove with new io */
            sock : Cell::new_empty(),
            port : Cell::new_empty(),
            /* END BLOCK to remove with new io */
            //stream : Cell::new_empty(), /* BLOCK to include with new io */
            ping : Cell::new_empty(),
            tags : Cell::new(TagSet::new(~[])),
            timeout : Cell::new(MONGO_TIMEOUT_SECS),
        }
    }

    pub fn get_ip(&self) -> ~str { self.server_ip_str.clone() }
    pub fn get_port(&self) -> uint { self.server_port }
    pub fn is_master(&self) -> Result<bool, MongoErr> {
        let tmp = NodeConnection::new(self.server_ip_str.clone(), self.server_port);
        (@tmp)._check_master_and_do(
                            |bson_doc : &~BsonDocument|
                                    -> Result<bool, MongoErr> {
            match bson_doc.find(~"ismaster") {
                None => Err(MongoErr::new(
                                ~"conn_replica::connect",
                                ~"isMaster runcommand response in unexpected format",
                                ~"no \"ismaster\" field")),
                Some(doc) => {
                    match doc {
                        &Bool(ref val) => Ok(*val),
                        _ => Err(MongoErr::new(
                                ~"conn_replica::connect",
                                ~"isMaster runcommand response in unexpected format",
                                ~"\"ismaster\" field non-boolean")),
                    }
                }
            }
        })
    }

    /**
     * Run admin isMaster command and pass document into callback to process.
     * Helper function.
     */
    pub fn _check_master_and_do<T>(&self, cb : &fn(bson : &~BsonDocument) -> Result<T, MongoErr>)
                -> Result<T, MongoErr> {
        let client = @Client::new();

        let server = @NodeConnection::new(self.get_ip(), self.get_port());
        match client._connect_to_conn(
                fmt!("client::connect[%s:%?]", self.server_ip_str, self.server_port),
                server as @Connection) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let admin = client.get_admin();
        if !self.ping.is_empty() { self.ping.take(); }
        let mut ping = precise_time_ns();
        let resp = match admin.run_command(SpecNotation(~"{ \"ismaster\":1 }")) {
            Ok(bson) => bson,
            Err(e) => return Err(e),
        };
        ping = precise_time_ns() - ping;
        self.ping.put_back(ping);

        let result = match cb(&resp) {
            Ok(ret) => Ok(ret),
            Err(e) => Err(e),
        };

        match client.disconnect() {
            Ok(_) => result,
            Err(e) => Err(e),
        }
    }

/*
    pub fn is_master(&self) -> Result<bool, MongoErr> {
        self._check_master_and_do(
                |bson_doc : ~BsonDocument| -> Result<bool, MongoErr> {
            let mut err = None;
            let mut is_master = false;

            match bson_doc.find(~"ismaster") {
                None =>  err = Some(MongoErr::new(
                                    ~"conn_node::is_master",
                                    ~"isMaster runcommand response in unexpected format",
                                    ~"no \"ismaster\" field")),
                Some(doc) => {
                    match doc {
                        &Bool(ref val) => is_master = *val,
                        _ => err = Some(MongoErr::new(
                                        ~"conn_node::is_master",
                                        ~"isMaster runcommand response in unexpected format",
                                        ~"\"ismaster\" field non-boolean")),
                    }
                }
            }

            if err.is_none() { Ok(is_master) }
            else { Err(err.unwrap()) }
        })
    }
*/
}

/**
 * Comparison of `NodeConnection`s based on their ping times.
 */
// Inequalities seem all backwards because max-heaps.
impl Ord for NodeConnection {
    pub fn lt(&self, other : &NodeConnection) -> bool {
        if self.ping.is_empty() { return true; }
        let ping0 = self.ping.take();

        if other.ping.is_empty() {
            self.ping.put_back(ping0);
            return false;
        }
        let ping1 = other.ping.take();

        let retval = ping0 > ping1;
        self.ping.put_back(ping0);
        other.ping.put_back(ping1);
        retval
    }

    pub fn le(&self, other : &NodeConnection) -> bool {
        if self.ping.is_empty() { return true; }
        let ping0 = self.ping.take();

        if other.ping.is_empty() {
            self.ping.put_back(ping0);
            return false;
        }
        let ping1 = other.ping.take();

        let retval = ping0 >= ping1;
        self.ping.put_back(ping0);
        other.ping.put_back(ping1);
        retval
    }

    pub fn gt(&self, other : &NodeConnection) -> bool {
        if self.ping.is_empty() { return false; }
        let ping0 = self.ping.take();

        if other.ping.is_empty() {
            self.ping.put_back(ping0);
            return true;
        }
        let ping1 = other.ping.take();

        let retval = ping0 < ping1;
        self.ping.put_back(ping0);
        other.ping.put_back(ping1);
        retval
    }

    pub fn ge(&self, other : &NodeConnection) -> bool {
        if self.ping.is_empty() { return false; }
        let ping0 = self.ping.take();

        if other.ping.is_empty() {
            self.ping.put_back(ping0);
            return true;
        }
        let ping1 = other.ping.take();

        let retval = ping0 <= ping1;
        self.ping.put_back(ping0);
        other.ping.put_back(ping1);
        retval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::comm::GenericPort;
    use std::cell::*;
    use extra::net::tcp::*;
    use extra::future;
    use tools::mockable;
    use conn::*;

    struct MockPort {
        state: int
    }

    struct MockSocket {
        state: int
    }

    impl GenericPort<PortResult> for MockPort {
        fn recv(&self) -> PortResult {
            mockable::Mockable::mock::<PortResult>(self.state)
        }
        fn try_recv(&self) -> Option<PortResult> {
            mockable::Mockable::mock::<Option<PortResult>>(self.state)
        }
    }

    impl Socket for MockSocket {
        fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData> {
            Err(mockable::Mockable::mock::<TcpErrData>(self.state)) //for now only allow fail mocking
        }
        fn read_stop(&self) -> Result<(), TcpErrData> {
            mockable::Mockable::mock::<Result<(), TcpErrData>>(self.state)
        }
        fn write_future(&self, _: ~[u8]) -> future::Future<Result<(), TcpErrData>> {
            mockable::Mockable::mock::<future::Future<Result<(), TcpErrData>>>(self.state)
        }
    }

    #[test]
    fn test_connect_preexisting_socket() {
        let s: @Socket = @MockSocket {state: 1} as @Socket;
        let mut conn = NodeConnection::new(~"foo", 42);
        conn.sock = Cell::new(s);
        assert!(conn.connect().is_err());
    }

    #[test]
    fn test_connect_ip_parse_fail() {
        let conn = NodeConnection::new(~"invalid.ip.str", 42);
        assert!(conn.connect().is_err());
    }

    #[test]
    fn test_send_null_sock() {
        let conn = NodeConnection::new(~"foo", 42);
        assert!(conn.send(~[0u8], false).is_err());
    }

    #[test]
    fn test_send_write_future_err() {
        let mut conn = NodeConnection::new(~"foo", 42);
        let s: @Socket = @MockSocket {state: 1} as @Socket;
        conn.sock = Cell::new(s);
        assert!(conn.send(~[0u8], false).is_err());
    }

    #[test]
    fn test_send_write_future() {
        let mut conn = NodeConnection::new(~"foo", 42);
        let s: @Socket = @MockSocket {state: 0} as @Socket;
        conn.sock = Cell::new(s);
        assert!(conn.send(~[0u8], false).is_ok());
    }

    #[test]
    fn test_recv_null_port() {
        let conn = NodeConnection::new(~"foo", 42);
        let mut buf = ~[];
        assert!(conn.recv(&mut buf, false).is_err());
    }

    #[test]
    fn test_recv_read_err() {

        let mut conn = NodeConnection::new(~"foo", 42);
        let mut buf = ~[];
        let p: @GenericPort<PortResult> = @MockPort {state: 1} as @GenericPort<PortResult>;
        conn.port = Cell::new(p);
        assert!(conn.recv(&mut buf, false).is_err());
    }

    #[test]
    fn test_recv_read() {
        let mut conn = NodeConnection::new(~"foo", 42);
        let mut buf = ~[];
        let p: @GenericPort<PortResult> = @MockPort {state: 0} as @GenericPort<PortResult>;
        conn.port = Cell::new(p);
        assert!(conn.recv(&mut buf, false).is_ok());
    }

    #[test]
    fn test_disconnect_no_socket() {
        let conn = NodeConnection::new(~"foo", 42);
        let e = conn.disconnect();
        assert!(conn.sock.is_empty());
        assert!(e.is_ok());
    }

    #[test]
    fn test_disconnect_read_stop_err() {
        let mut conn = NodeConnection::new(~"foo", 42);
        let s: @Socket = @MockSocket {state: 1} as @Socket;
        conn.sock = Cell::new(s);
        let e = conn.disconnect();
        assert!(conn.sock.is_empty());
        assert!(e.is_err());
    }

    #[test]
    fn test_disconnect_read_stop() {
        let mut conn = NodeConnection::new(~"foo", 42);
        let s: @Socket = @MockSocket {state: 0} as @Socket;
        conn.sock = Cell::new(s);
        let e = conn.disconnect();
        assert!(conn.sock.is_empty());
        assert!(e.is_ok());
    }
}
