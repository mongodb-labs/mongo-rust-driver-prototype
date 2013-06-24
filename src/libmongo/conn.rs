use ip = extra::net::ip;
use tcp = extra::net::tcp;
use extra::uv;

use result = std::result;

use util;

/**
 * Connection interface all connectors use (ReplicaSetConnection,
 * ShardedClusterConnection, NodeConnection).
 */
pub trait Connection {
    fn new(server_ip_str : ~str, server_port : uint) -> Self;
    fn connect(&self) -> result::Result<(), util::MongoErr>;
    fn send(&self, data : ~[u8]) -> result::Result<(), util::MongoErr>;
    fn recv(&self) -> result::Result<~[u8], util::MongoErr>;
    fn disconnect(&self) -> result::Result<(), util::MongoErr>;
}

/**
 * Connection between Client and Server. Routes user requests to
 * server and hands back wrapped result for Client to parse.
 */
pub struct NodeConnection<'self> {
    priv server_ip_str : ~str,
    priv server_port : uint,
    priv server_ip : @mut Option<ip::IpAddr>,
    priv iotask : uv::iotask::IoTask,
    priv sock : @mut Option<@tcp::TcpSocket>,
    priv port : @mut Option<@Port<result::Result<~[u8], tcp::TcpErrData>>>,
}

impl Connection for NodeConnection {
    fn new(server_ip_str : ~str, server_port : uint) -> NodeConnection {
        NodeConnection {
            server_ip_str : server_ip_str,
            server_port : server_port,
            server_ip : @mut None,
            iotask : uv::global_loop::get(),
            sock : @mut None,
            port : @mut None,
        }
    }

    /**
     * Actually connect to the server with the initialized fields.
     * # Returns
     * Ok(()), or a MongoConnectionErr
     */
    fn connect(&self) -> result::Result<(), util::MongoErr> {
        // sanity check: should not connect if already connected (?)
        assert!(self.sock.is_none() && self.port.is_none());

        // parse IP addr
        let tmp_ip = match ip::v4::try_parse_addr(self.server_ip_str) {
            result::Err(e) => return result::Err(util::MongoErr::new(~"connection", ~"IP Parse Err", e.err_msg.clone())),
            result::Ok(addr) => addr,
        };

        // set up the socket --- for now, just v4
        let tmp_sock = match tcp::connect(tmp_ip, self.server_port, &self.iotask) {
            result::Err(tcp::GenericConnectErr(ename, emsg)) => return result::Err(util::MongoErr::new(~"connection", ename, emsg)),
            result::Err(tcp::ConnectionRefused) => return result::Err(util::MongoErr::new(~"connection", ~"EHOSTNOTFOUND", ~"Invalid IP or port")),
            result::Ok(sock) => sock,
        };

        // start the read port
        *(self.port) = match tmp_sock.read_start() {
            result::Err(e) => return result::Err(util::MongoErr::new(~"connection", e.err_name.clone(), e.err_msg.clone())),
            result::Ok(port) => Some(port),
        };

        // hand initialized fields to self
        *(self.sock) = Some(@tmp_sock);
        *(self.server_ip) = Some(tmp_ip);

        result::Ok(())
    }

    /**
     * "Fire and forget" asynchronous write to server of given data.
     */
    fn send(&self, data : ~[u8]) -> result::Result<(), util::MongoErr> {
        // sanity check and unwrap: should not send on an unconnected connection
        assert!(self.sock.is_some());
        match *(self.sock) {
            None => return result::Err(util::MongoErr::new(~"connection", ~"unknown send err", ~"this code path should never be reached (null socket)")),
            Some(sock) => {
                match sock.write_future(data).get() {
                    result::Err(e) => return result::Err(util::MongoErr::new(~"connection", e.err_name.clone(), e.err_msg.clone())),
                    result::Ok(_) => result::Ok(()),
                }
            }
        }
    }

    /**
     * Pick up a response from the server.
     */
    fn recv(&self) -> result::Result<~[u8], util::MongoErr> {
         // sanity check and unwrap: should not send on an unconnected connection
        assert!(self.port.is_some());
        match *(self.port) {
            None => return result::Err(util::MongoErr::new(~"connection", ~"unknown send err", ~"this code path should never be reached (null port)")),
            Some(port) => {
                match port.recv() {
                    result::Err(e) => result::Err(util::MongoErr::new(~"connection", e.err_name.clone(), e.err_msg.clone())),
                    result::Ok(msg) => result::Ok(msg),
                }
            }
        }
    }

    /**
     * Disconnect from the server.
     */
    fn disconnect(&self) -> result::Result<(), util::MongoErr> {
        // NO sanity check: don't really care if disconnect unconnected connection

        // nuke port first (we can keep the ip)
        *(self.port) = None;

        match *(self.sock) {
            None => (),
            Some(sock) => {
                match sock.read_stop() {
                    result::Err(e) => return result::Err(util::MongoErr::new(~"connection", e.err_name.clone(), e.err_msg.clone())),
                    result::Ok(_) => (),
                }
            }
        }
        *(self.sock) = None;

        result::Ok(())
    }
}
