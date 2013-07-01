/* Copyright 2013 10gen Inc.
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

use std::comm::GenericPort;

use extra::net::ip::*;
use extra::net::tcp::*;
use extra::uv::*;
use extra::future::*;

use util::*;

pub type PortResult = Result<~[u8], TcpErrData>;

/**
 * Trait for sockets used by Connection. Used as a traitobject.
 */
pub trait Socket {
<<<<<<< HEAD
	fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData>;
	fn read_stop(&self) -> Result<(), TcpErrData>;
	fn write_future(&self, raw_write_data: ~[u8]) -> Future<Result<(), TcpErrData>>;
}

impl Socket for TcpSocket {
	fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData> {
		self.read_start()
	}
	fn read_stop(&self) -> Result<(), TcpErrData> {
		self.read_stop()
	}
	fn write_future(&self, raw_write_data: ~[u8]) -> Future<Result<(), TcpErrData>> {
		self.write_future(raw_write_data)
	}
}
/**
 * Connection interface all connectors use (ReplicaSetConnection,
 * ShardedClusterConnection, NodeConnection).
 */
pub trait Connection {
    fn new(server_ip_str : ~str, server_port : uint) -> Self;
    fn connect(&self) -> Result<(), MongoErr>;
    fn send(&self, data : ~[u8]) -> Result<(), MongoErr>;
    fn recv(&self) -> Result<~[u8], MongoErr>;
    fn disconnect(&self) -> Result<(), MongoErr>;
}

/**
 * Connection between Client and Server. Routes user requests to
 * server and hands back wrapped result for Client to parse.
 */
pub struct NodeConnection {
    priv server_ip_str : ~str,
    priv server_port : uint,
    priv server_ip : @mut Option<IpAddr>,
    priv iotask : iotask::IoTask,
    priv sock : @mut Option<@Socket>,
    //priv port : @mut Option<@Port<Result<~[u8], TcpErrData>>>,
    priv port : @mut Option<@GenericPort<PortResult>>
}

impl Connection for NodeConnection {
    pub fn new(server_ip_str : ~str, server_port : uint) -> NodeConnection {
        NodeConnection {
            server_ip_str : server_ip_str,
            server_port : server_port,
            server_ip : @mut None,
            iotask : global_loop::get(),
            sock : @mut None,
            port : @mut None,
        }
    }

    /**
     * Actually connect to the server with the initialized fields.
     * # Returns
     * Ok(()), or a MongoConnectionErr
     */
    pub fn connect(&self) -> Result<(), MongoErr> {
        // sanity check: should not connect if already connected (?)
        if !(self.sock.is_none() && self.port.is_none()) {
        return Err(MongoErr::new(
                        ~"conn::connect",
                        ~"pre-existing socket",
                        ~"cannot override existing socket"));
    }

        // parse IP addr
        let tmp_ip = match v4::try_parse_addr(self.server_ip_str) {
            Err(e) => return Err(MongoErr::new(
                                    ~"conn::connect",
                                    ~"IP parse err",
                                    e.err_msg.clone())),
            Ok(addr) => addr,
        };

        // set up the socket --- for now, just v4
        let tmp_sock = match connect(tmp_ip, self.server_port, &self.iotask) {
            Err(GenericConnectErr(ename, emsg)) => return Err(MongoErr::new(~"conn::connect", ename, emsg)),
            Err(ConnectionRefused) => return Err(MongoErr::new(~"conn::connect", ~"EHOSTNOTFOUND", ~"Invalid IP or port")),
            Ok(sock) => @sock as @Socket
        };

        // start the read port
        *(self.port) = match tmp_sock.read_start() {
            Err(e) => return Err(MongoErr::new(
                                    ~"conn::connect",
                                    e.err_name.clone(),
                                    e.err_msg.clone())),
            Ok(port) => Some(port as @GenericPort<PortResult>),
        };

        // hand initialized fields to self
        *(self.sock) = Some(tmp_sock);
        *(self.server_ip) = Some(tmp_ip);

        Ok(())
    }

    /**
     * "Fire and forget" asynchronous write to server of given data.
     */
    pub fn send(&self, data : ~[u8]) -> Result<(), MongoErr> {
        match *(self.sock) {
            None => return Err(MongoErr::new(~"connection", ~"unknown send err", ~"cannot send on null socket")),
            Some(sock) => {
                match sock.write_future(data).get() {
                    Err(e) => return Err(MongoErr::new(
                                            ~"conn::send",
                                            e.err_name.clone(),
                                            e.err_msg.clone())),
                    Ok(_) => Ok(()),
                }
            }
        }
    }

    /**
     * Pick up a response from the server.
     */
    pub fn recv(&self) -> Result<~[u8], MongoErr> {
         // sanity check and unwrap: should not send on an unconnected connection
        match *(self.port) {
            None => return Err(MongoErr::new(
                                    ~"conn::recv",
                                    ~"unknown recv err",
                                    ~"cannot receive from null port")),
            Some(port) => {
                match port.recv() {
                    Err(e) => Err(MongoErr::new(
                                    ~"conn::recv",
                                    e.err_name.clone(),
                                    e.err_msg.clone())),
                    Ok(msg) => Ok(msg),
                }
            }
        }
    }

    /**
     * Disconnect from the server.
     */
    pub fn disconnect(&self) -> Result<(), MongoErr> {
        // NO sanity check: don't really care if disconnect unconnected connection

        // nuke port first (we can keep the ip)
        *(self.port) = None;

        match *(self.sock) {
            None => (),
            Some(sock) => {
                match sock.read_stop() {
                    Err(e) => return Err(MongoErr::new(
                                            ~"conn::disconnect",
                                            e.err_name.clone(),
                                            e.err_msg.clone())),
                    Ok(_) => (),
                }
            }
        }
        *(self.sock) = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
<<<<<<< HEAD
	use super::*;
	use std::comm::GenericPort;
	use extra::net::tcp::*;
	use extra::future;
    use mockable::*;

	struct MockSocket {
		flag: bool
	}

	struct MockPort {
		state: int
	}

    impl Mockable for TcpErrData {
        fn mock(_: int) -> TcpErrData {
            TcpErrData { err_name: ~"mock error", err_msg: ~"mock" }
        }
    }

    mock!(GenericPort<PortResult> => MockPort:
        recv(&self) -> PortResult |
        try_recv(&self) -> Option<PortResult>)

	impl Socket for MockSocket {
		fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData> {
			let ret: Result<@Port<Result<~[u8], TcpErrData>>, TcpErrData> = Err(TcpErrData { err_name: ~"mock error", err_msg: ~"mocksocket" });
			ret
		}
		fn read_stop(&self) -> Result<(), TcpErrData> {
			if self.flag { return Ok(()); }
			return Err(TcpErrData { err_name: ~"mock error", err_msg: ~"mocksocket" });
		}
		fn write_future(&self, _: ~[u8]) -> future::Future<Result<(), TcpErrData>> {
            if self.flag {
                do future::spawn {
                    Ok(())
                }
            }
            else {
                do future::spawn {
                    Err(TcpErrData { err_name: ~"mock error", err_msg: ~"mocksocket" })
                }
            }
		}
	}

	#[test]
	fn test_connect_preexisting_socket() {
		let s: @Socket = @MockSocket {flag: true} as @Socket;
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		conn.sock = @mut Some(s);
		assert!(conn.connect().is_err());
	}

	#[test]
	fn test_connect_ip_parse_fail() {
		let conn = Connection::new::<NodeConnection>(~"invalid.ip.str", 42);
		assert!(conn.connect().is_err());
	}

	#[test]
	fn test_send_null_sock() {
		let conn = Connection::new::<NodeConnection>(~"foo", 42);
		assert!(conn.send(~[0u8]).is_err());
	}

	#[test]
	fn test_send_write_future_err() {
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let s: @Socket = @MockSocket {flag: false} as @Socket;
		conn.sock = @mut Some(s);
		assert!(conn.send(~[0u8]).is_err());
	}

	#[test]
	fn test_send_write_future() {
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let s: @Socket = @MockSocket {flag: true} as @Socket;
		conn.sock = @mut Some(s);
		assert!(conn.send(~[0u8]).is_ok());
	}

	#[test]
	fn test_recv_null_port() {
		let conn = Connection::new::<NodeConnection>(~"foo", 42);
		assert!(conn.recv().is_err());
	}

	#[test]
	fn test_recv_read_err() {

		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let p: @GenericPort<PortResult> = @MockPort {state: 1} as @GenericPort<PortResult>;
		conn.port = @mut Some(p);
		assert!(conn.recv().is_err());
	}

	#[test]
	fn test_recv_read() {
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let p: @GenericPort<PortResult> = @MockPort {state: 0} as @GenericPort<PortResult>;
		conn.port = @mut Some(p);
		assert!(conn.recv().is_ok());
	}

	#[test]
	fn test_disconnect_no_socket() {
		let conn = Connection::new::<NodeConnection>(~"foo", 42);
		let e = conn.disconnect();
		assert!(conn.sock.is_none());
		assert!(e.is_ok());
	}

	#[test]
	fn test_disconnect_read_stop_err() {
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let s: @Socket = @MockSocket {flag: false} as @Socket;
		conn.sock = @mut Some(s);
		let e = conn.disconnect();
		assert!(!(conn.sock.is_none()));
		assert!(e.is_err());
	}

	#[test]
	fn test_disconnect_read_stop() {
		let mut conn = Connection::new::<NodeConnection>(~"foo", 42);
		let s: @Socket = @MockSocket {flag: true} as @Socket;
		conn.sock = @mut Some(s);
		let e = conn.disconnect();
		assert!(conn.sock.is_none());
		assert!(e.is_ok());
	}
}
