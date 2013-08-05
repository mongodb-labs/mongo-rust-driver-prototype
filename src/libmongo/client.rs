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
use sys = std::sys;
use std::from_str::FromStr;

use bson::encode::*;
use bson::formattable::*;

use util::*;
use msg::*;
use conn::Connection;
use conn_node::NodeConnection;
use conn_replica::ReplicaSetConnection;
use db::DB;
use coll::Collection;
use rs::RSConfig;

/**
 * User interfaces with `Client`, which processes user requests
 * and sends them through the connection.
 *
 * All communication to server goes through `Client`, i.e. `DB`,
 * `Collection`, etc. all store their associated `Client`
 */
pub struct Client {
    conn : Cell<@Connection>,
    timeout : u64,
    priv rs_conn : Cell<@ReplicaSetConnection>,
    priv cur_requestId : Cell<i32>,     // first unused requestId
    // XXX index cache?
}

impl Client {
    /**
     * Creates a new Mongo client.
     *
     * Currently can connect to single unreplicated, unsharded
     * server via `connect`, or to a replica set via `connect_to_rs`
     * (given a seed, if already initiated), or via `initiate_rs`
     * (given a configuration and single host, if not yet initiated).
     *
     * # Returns
     * empty `Client`
     */
    pub fn new() -> Client {
        Client {
            conn : Cell::new_empty(),
            timeout : MONGO_TIMEOUT_SECS,
            rs_conn : Cell::new_empty(),
            cur_requestId : Cell::new(0),
        }
    }

    /*pub fn parse_uri(uri : ~str) {
        // XXX
    }*/

    pub fn get_admin(@self) -> DB {
        DB::new(~"admin", self)
    }

    /**
     * Returns vector of database names.
     *
     * # Returns
     * vector of database names on success, `MongoErr` on any failure
     *
     * # Failure Types
     * * errors propagated from `run_command`
     * * response from server not in expected form (must contain
     *      "databases" field whose value is array of docs containing
     *      "name" fields of `UString`s)
     */
    pub fn get_dbs(@self) -> Result<~[~str], MongoErr> {
        let mut names : ~[~str] = ~[];

        // run_command from admin database
        let db = DB::new(~"admin", self);
        let resp = match db.run_command(SpecNotation(~"{ \"listDatabases\":1 }")) {
            Ok(doc) => doc,
            Err(e) => return Err(e),
        };

        // pull out database names
        let list = match resp.find(~"databases") {
            None => return Err(MongoErr::new(
                            ~"client::get_dbs",
                            ~"could not get databases",
                            ~"missing \"databases\" field in reply")),
            Some(tmp_doc) => {
                let tmp = copy *tmp_doc;
                match tmp {
                    Array(l) => l,
                    _ => return Err(MongoErr::new(
                            ~"client::get_dbs",
                            ~"could not get databases",
                            ~"\"databases\" field in reply not an Array")),
                }
            }
        };
        let fields = list.fields;
        for fields.iter().advance |&(_, @doc)| {
            match doc {
                Embedded(bson_doc) => match bson_doc.find(~"name") {
                    Some(tmp_doc) => {
                        match (copy *tmp_doc) {
                            UString(n) => names.push(n),
                            x => return Err(MongoErr::new(
                                        ~"client::get_dbs",
                                        ~"could not extract database name",
                                        fmt!("name field %? not UString", x))),

                        }
                    }
                    None => return Err(MongoErr::new(
                                ~"client::get_dbs",
                                ~"could not extract database name",
                                fmt!("no name field in %?", bson_doc))),

                },
                _ => return Err(MongoErr::new(
                                ~"client::get_dbs",
                                ~"could not extract database name",
                                fmt!("no BsonDocument in %?", doc))),
            }
        }

        Ok(names)
    }

    /**
     * Gets the specified `DB`.
     * Alternative to constructing the `DB` explicitly
     * (`DB::new(db, client)`).
     *
     * # Arguments
     * * `db` - name of `DB` to get
     *
     * # Returns
     * handle to specified database
     */
    pub fn get_db(@self, db : ~str) -> DB {
        DB::new(db, self)
    }

    /**
     * Drops the given database.
     *
     * # Arguments
     * * `db` - name of database to drop
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * anything propagated from run_command
     */
    pub fn drop_db(@self, db : &str) -> Result<(), MongoErr> {
        let db = DB::new(db.to_owned(), self);
        match db.run_command(SpecNotation(~"{ \"dropDatabase\":1 }")) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /**
     * Gets the specified `Collection`.
     * Alternative to constructing the `Collection` explicitly
     * (`Collection::new(db, collection, client)`).
     *
     * # Arguments
     * * `db` - database from which to get collection
     * * `coll` - name of `Collection` to get
     *
     * # Returns
     * handle to specified collection
     */
    pub fn get_collection(@self, db : ~str, coll : ~str) -> Collection {
        Collection::new(db, coll, self)
    }

    /*
     * Helper function for connections.
     */
    pub fn _connect_to_conn(&self, call : &str, conn : @Connection)
                -> Result<(), MongoErr> {
        // check if already connected
        if !self.conn.is_empty() {
            return Err(MongoErr::new(
                            call.to_owned(),
                            ~"already connected",
                            ~"cannot connect if already connected; please first disconnect"));
        }

        // otherwise, make connection and connect to it
        match conn.connect() {
            Ok(_) => {
                self.conn.put_back(conn);
                Ok(())
            }
            Err(e) => return Err(MongoErr::new(
                                    call.to_owned(),
                                    ~"connecting",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }

    /**
     * Connects to a single server.
     *
     * # Arguments
     * * `server_ip_str` - string containing IP address of server
     * * `server_port` - port to which to connect
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * already connected
     * * network
     */
    pub fn connect(&self, server_ip_str : &str, server_port : uint)
                -> Result<(), MongoErr> {
        let tmp = @NodeConnection::new(server_ip_str, server_port);
        tmp.set_timeout(self.timeout);
        self._connect_to_conn(  fmt!("client::connect[%s:%?]", server_ip_str, server_port),
                                tmp as @Connection)
    }

    /**
     * Connect to replica set with specified seed list.
     *
     * # Arguments
     * `seed` - seed list (vector) of ip/port pairs
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn connect_to_rs(&self, seed : &[(~str, uint)])
                -> Result<(), MongoErr> {
        let tmp = @ReplicaSetConnection::new(seed);
        tmp.set_timeout(self.timeout);
        self.rs_conn.put_back(tmp);
        self._connect_to_conn(  fmt!("client::connect_to_rs[%?]", seed),
                                tmp as @Connection)
    }

    /**
     * Initiates given configuration specified as `RSConfig`, and connects
     * to the replica set.
     *
     * # Arguments
     * * `via` - host to connect to in order to initiate the set
     * * `conf` - configuration to initiate
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn initiate_rs(@self, via : (&str, uint), conf : RSConfig)
                -> Result<(), MongoErr> {
        let (ip, port) = via;
        match self.connect(ip, port) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let conf_doc = conf.to_bson_t();
        let db = self.get_admin();
        let mut cmd_doc = BsonDocument::new();
        cmd_doc.put(~"replSetInitiate", conf_doc);
        match db.run_command(SpecObj(cmd_doc)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        self.disconnect();

        let mut seed = ~[];
        for conf.members.iter().advance |&member| {
            match parse_host(&member.host) {
                Ok(p) => seed.push(p),
                Err(e) => return Err(e),
            }
        }

        match self.connect_to_rs(seed) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /**
     * Sets read preference as specified, returning former preference.
     *
     * # Arguments
     * * `np` - new read preference
     *
     * # Returns
     * old read preference on success, MongoErr on failure
     */
    pub fn set_read_pref(&self, np : READ_PREFERENCE)
                -> Result<READ_PREFERENCE, MongoErr> {
        if self.rs_conn.is_empty() {
            return Err(MongoErr::new(
                            ~"client::set_read_pref",
                            ~"could not set read preference",
                            ~"connection not to replica set"));
        }
        let rs = self.rs_conn.take();
        // read_pref and read_pref_changed should never be empty
        let op = rs.read_pref.take();
        rs.read_pref_changed.take();
        // might as well only note updated if actually changed
        rs.read_pref_changed.put_back( if op == np { false } else { true });
        rs.read_pref.put_back(np);
        // put everything back
        self.rs_conn.put_back(rs);
        Ok(op)
    }

    /**
     * Disconnect from server.
     * Simultaneously empties connection cell.
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * network
     */
    pub fn disconnect(&self) -> Result<(), MongoErr> {
        if !self.rs_conn.is_empty() { self.rs_conn.take(); }
        if !self.conn.is_empty() { self.conn.take().disconnect() }
        // XXX currently succeeds even if not previously connected
        //      (may or may not be desired)
        else { Ok(()) }
    }

    pub fn reconnect(&self) -> Result<(), MongoErr> {
        if !self.conn.is_empty() {
            let tmp = self.conn.take();
            let result = tmp.reconnect();
            self.conn.put_back(tmp);
            result
        }
        // XXX currently succeeds even if not previously connected
        //      (may or may not be desired)
        else { Ok(()) }
    }

    /**
     * Sends message on connection; if write, checks write concern,
     * and if query, picks up OP_REPLY.
     *
     * # Arguments
     * * `msg` - bytes to send
     * * `wc` - write concern (if applicable)
     * * `read` - whether read operation; whether `Client` should
     *                      expect an `OP_REPLY` from the server
     *
     * # Returns
     * if read operation, `OP_REPLY` on success, `MongoErr` on failure;
     * if write operation, `None` on no last error, `MongoErr` on last error
     *      or network error
     */
    // TODO check_primary for replication purposes?
    pub fn _send_msg(@self, msg : ~[u8],
                            wc_pair : (~str, Option<~[WRITE_CONCERN]>),
                            read : bool)
                -> Result<Option<ServerMsg>, MongoErr> {
        // first send message, exiting if network error
        match self.send(msg, read) {
            Ok(_) => (),
            Err(e) => return Err(MongoErr::new(
                                    ~"client::_send_msg",
                                    ~"",
                                    fmt!("-->\n%s", e.to_str()))),
        }

        // handle write concern or handle query as appropriate
        if !read {
            // requested write concern
            let (db_str, wc) = wc_pair;
            let db = DB::new(db_str, self);

            match db.get_last_error(wc) {
                Ok(_) => Ok(None),
                Err(e) => Err(MongoErr::new(
                                    ~"client::_send_msg",
                                    ~"write concern error",
                                    fmt!("-->\n%s", e.to_str()))),
            }
        } else {
            // requested query
            match self._recv_msg(read) {
                Ok(m) => Ok(Some(m)),
                Err(e) => Err(MongoErr::new(
                                    ~"client::_send_msg",
                                    ~"error in response",
                                    fmt!("-->\n%s", e.to_str()))),
            }
        }
    }

    /**
     * Picks up server response.
     *
     * # Returns
     * `ServerMsg` on success, `MongoErr` on failure
     *
     * # Failure Types
     * * invalid bytestring/message returned (should never happen)
     * * server returned message with error flags
     * * network
     */
    fn _recv_msg(&self, read : bool) -> Result<ServerMsg, MongoErr> {
        /* BEGIN BLOCK to remove with new io */
        let mut bytes = ~[];
        let header_sz = 4*sys::size_of::<i32>();
        // receive message
        let m = match self.recv(&mut bytes, read) {
            Ok(_) => {
                if bytes.len() < header_sz {
                    return Err(MongoErr::new(
                                ~"client::_recv_msg",
                                ~"too few bytes in resp",
                                fmt!("expected %?, received %?",
                                        header_sz,
                                        bytes.len())));
                }
                // first get header
                let header_bytes = bytes.slice(0, header_sz);
                let h = match parse_header(header_bytes) {
                    Ok(head) => head,
                    Err(e) => return Err(e),
                };
                // now get rest of message
                let body_bytes = bytes.slice(header_sz, bytes.len());
                match parse_reply(h, body_bytes) {
                    Ok(m_tmp) => m_tmp,
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        /* BEGIN BLOCK to remove with new io */

        /* BEGIN BLOCK to add with new io */
        /*// receive header
        let header_sz = 4*sys::size_of::<i32>();
        let mut buf : ~[u8] = ~[];
        for header_sz.times { buf.push(0); }
        self.recv(buf, read);
        let header = match parse_header(buf) {
            Ok(h) => h,
            Err(e) => return Err(e),
        };

        // receive rest of message
        buf = ~[];
        for (header.len as uint-header_sz).times { buf.push(0); }
        self.recv(buf, read);
        let m = match parse_reply(header, buf) {
            Ok(m_tmp) => m_tmp,
            Err(e) => return Err(e),
        };*/
        /* END BLOCK to add with new io */

        // check if any errors in response and convert to MongoErr,
        //      else pass along
        match copy m {
            OpReply { header:_, flags:f, cursor_id:_, start:_, nret:_, docs:_ } => {
                if (f & CUR_NOT_FOUND as i32) != 0i32 {
                    return Err(MongoErr::new(
                                ~"client::_recv_msg",
                                ~"CursorNotFound",
                                ~"cursor ID not valid at server"));
                } else if (f & QUERY_FAIL as i32) != 0i32 {
                    return Err(MongoErr::new(
                                ~"client::_recv_msg",
                                ~"QueryFailure",
                                ~"tmp"));
                }
                return Ok(m)
            }
        }
    }

    /**
     * Sends on `Connection` affiliated with this `Client`.
     *
     * # Arguments
     * * `bytes` - bytes to send
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * not connected
     * * network
     */
    fn send(&self, bytes : ~[u8], read : bool) -> Result<(), MongoErr> {
        if self.conn.is_empty() {
            Err(MongoErr::new(
                    ~"client::send",
                    ~"client not connected",
                    ~"attempted to send on nonexistent connection"))
        } else {
            let tmp = self.conn.take();
            let result = tmp.send(bytes, read);
            self.conn.put_back(tmp);
            result
        }
    }

    /**
     * Receives on `Connection` affiliated with this `Client`.
     *
     * # Returns
     * bytes received over connection on success, `MongoErr` on failure
     *
     * # Failure Types
     * * not connected
     * * network
     */
    //fn recv(&self, read : bool) -> Result<~[u8], MongoErr> {
    fn recv(&self, buf : &mut ~[u8], read : bool) -> Result<uint, MongoErr> {
        if self.conn.is_empty() {
            Err(MongoErr::new(
                    ~"client::recv",
                    ~"client not connected",
                    ~"attempted to receive on nonexistent connection"))
        } else {
            let tmp = self.conn.take();
            //let result = tmp.recv(read);
            let result = tmp.recv(buf, read);
            self.conn.put_back(tmp);
            result
        }
    }

    /**
     * Returns first unused requestId.
     */
    pub fn get_requestId(&self) -> i32 { self.cur_requestId.take() }

    /**
     * Increments first unused requestId and returns former value.
     */
    pub fn inc_requestId(&self) -> i32 {
        let tmp = self.cur_requestId.take();
        self.cur_requestId.put_back(tmp+1);
        tmp
    }

    pub fn check_version(@self, ver: ~str) -> Result<(), MongoErr> {
       let admin = self.get_admin();
       match admin.run_command(SpecNotation(~"{ 'buildInfo': 1 }")) {
           Ok(doc) => match doc.find(~"version") {
               Some(&UString(ref s)) => {
                   let mut it = s.split_iter('.').zip(ver.split_iter('.'));
                   for it.advance |(vcur, varg)| {
                       let ncur = FromStr::from_str::<uint>(vcur);
                       let narg = FromStr::from_str::<uint>(varg);
                       if ncur > narg {
                           return Ok(());
                       }
                       else if ncur < narg {
                           return Err(MongoErr::new(
                                   ~"shard::check_version",
                                   fmt!("version %s is too old", *s),
                                   fmt!("please upgrade to at least version %s of MongoDB", ver)));
                       }
                   }
                   return Ok(());
               },
               _ => return Err(MongoErr::new(
                           ~"shard::check_version",
                           ~"unknown error while checking version",
                           ~"the database did not return a version field"))
           },
           Err(e) => return Err(e)
       }
    }
}
