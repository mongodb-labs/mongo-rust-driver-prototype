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

use std::*;

use bson::encode::*;

use util::*;
use msg::*;
use conn::*;
use db::DB;

/**
 * User interfaces with Client, which processes user requests
 * and sends them through the connection.
 *
 * All communication to server goes through Client, i.e. database,
 * collection, etc. all store their associated Client
 */
pub struct Client {
    conn : ~cell::Cell<NodeConnection>,
    db : ~cell::Cell<~str>,
    priv cur_requestId : ~cell::Cell<i32>,      // first unused requestId
    // XXX index cache?
}

impl Client {
    /**
     * Create a new Mongo Client.
     *
     * Currently can connect to single unreplicated, unsharded
     * server via `connect`.
     *
     * # Returns
     * empty Client
     */
    pub fn new() -> Client {
        Client {
            conn : ~cell::Cell::new_empty(),
            db : ~cell::Cell::new_empty(),
            cur_requestId : ~cell::Cell::new(0),
        }
    }

    pub fn get_admin(@self) -> DB {
        DB::new(~"admin", self)
    }

    /**
     * Return vector of database names.
     *
     * # Returns
     * vector of database names on success,
     * `MongoErr` on any failure
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
        for fields.iter().advance |&(@_, @doc)| {
            match doc {
                Embedded(bson_doc) => match bson_doc.find(~"name") {
                    Some(tmp_doc) => {
                        let tmp = copy *tmp_doc;
                        match tmp {
                            UString(n) => names = names + ~[n],
                            x => return Err(MongoErr::new(
                                        ~"client::get_dbs",
                                        ~"could not extract database name",
                                        fmt!("name field %? not UString", copy x))),

                        }
                    }
                    None => return Err(MongoErr::new(
                                ~"client::get_dbs",
                                ~"could not extract database name",
                                fmt!("no name field in %?", copy bson_doc))),

                },
                _ => return Err(MongoErr::new(
                                ~"client::get_dbs",
                                ~"could not extract database name",
                                fmt!("no BsonDocument in %?", copy doc))),
            }
        }

        Ok(names)
    }

    // probably not actually needed
    pub fn use_db(&self, db : ~str) {
        if !self.db.is_empty() {
            self.db.take();
        }
        self.db.put_back(db);
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
    pub fn drop_db(@self, db : ~str) -> Result<(), MongoErr> {
        let db = @DB::new(db, self);
        match db.run_command(SpecNotation(~"{ \"dropDatabase\":1 }")) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /**
     * Connect to a single server.
     *
     * # Arguments
     * * `server_ip_str` - string containing IP address of server
     * * `server_port` - port to which to connect
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * already connected
     * * network
     */
    pub fn connect(&self, server_ip_str : ~str, server_port : uint)
                -> Result<(), MongoErr> {
        if !self.conn.is_empty() {
            return Err(MongoErr::new(
                            ~"client::connect",
                            ~"already connected",
                            ~"cannot connect if already connected; please first disconnect"));
        }

        let tmp = NodeConnection::new(server_ip_str, server_port);
        match tmp.connect() {
            Ok(_) => {
                self.conn.put_back(tmp);
                Ok(())
            }
            Err(e) => return Err(MongoErr::new(
                                    ~"client::connect",
                                    ~"connecting",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /**
     * Disconnect from server.
     * Simultaneously empties connection cell.
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * network
     */
    pub fn disconnect(&self) -> Result<(), MongoErr> {
        if !self.conn.is_empty() { self.conn.take().disconnect() }
        // XXX currently succeeds even if not previously connected.
        else { Ok(()) }
    }

    /**
     * Sends message on connection; if write, checks write concern,
     * and if query, picks up OP_REPLY.
     *
     * # Arguments
     * * `msg` - bytes to send
     * * `wc` - write concern (if applicable)
     * * `auto_get_reply` - whether Client should expect an `OP_REPLY`
     *                      from the server
     *
     * # Returns
     * if read operation, `OP_REPLY` on success, MongoErr on failure;
     * if write operation, None on no last error, MongoErr on last error
     *      or network error
     */
    // XXX right now, public---try to move things around so doesn't need to be?
    // TODO check_primary for replication purposes?
    pub fn _send_msg(&self, msg : ~[u8], wc_pair : (~str, Option<~[WRITE_CONCERN]>), auto_get_reply : bool)
                -> Result<Option<ServerMsg>, MongoErr> {
        // first send message, exiting if network error
        match self.send(msg) {
            Ok(_) => (),
            Err(e) => return Err(MongoErr::new(
                                    ~"client::_send_msg",
                                    ~"",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }

        // if not, for instance, query, handle write concern
        if !auto_get_reply {
            let (db, wc) = wc_pair;
            match self._parse_and_send_wc(db, wc) {
                Ok(None) => return Ok(None),
                Ok(Some(_)) => (),
                Err(e) => return Err(MongoErr::new(
                                    ~"client::_send_msg",
                                    ~"write concern error",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
            }
        }

        // get response
        let response = self._recv_msg();

        // if write concern, check err field, convert to MongoErr if needed
        match response {
            Ok(m) => if auto_get_reply { return Ok(Some(m)) } else {
                match m {
                    OpReply { header:_, flags:_, cursor_id:_, start:_, nret:_, docs:d } => {
                        match d[0].find(~"err") {
                            None => Err(MongoErr::new(
                                            ~"coll::_send_msg",
                                            ~"getLastError unknown error",
                                            ~"no $err field in reply")),
                            Some(doc) => {
                                let err_doc = copy *doc;
                                match err_doc {
                                    Null => Ok(None),
                                    UString(s) => Err(MongoErr::new(
                                                        ~"coll::_send_msg",
                                                        ~"getLastError error",
                                                        copy s)),
                                    _ => Err(MongoErr::new(
                                                ~"coll::_send_msg",
                                                ~"getLastError unknown error",
                                                ~"unknown last error in reply")),
                                }
                            },
                        }
                    }
                }
            },
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::_send_msg",
                                    ~"receiving write concern",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /**
     * Parses write concern into bytes and sends to server.
     *
     * # Arguments
     * * `wc` - write concern, i.e. getLastError specifications
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid write concern specification (should never happen)
     * * network
     */
    //fn _parse_and_send_wc(&self, wc : ~str) -> Result<(), MongoErr>{
    fn _parse_and_send_wc(&self, db : ~str, wc : Option<~[WRITE_CONCERN]>) -> Result<Option<()>, MongoErr>{
        // set default write concern (to 1) if not specified
        let concern = match wc {
            None => ~[W_N(1), FSYNC(false)],
            Some(w) => w,
        };
        // parse write concern, early exiting if set to <= 0
        let mut concern_str = ~"{ \"getLastError\":1";
        for concern.iter().advance |&opt| {
            //concern_str += match opt {
            concern_str = concern_str + match opt {
                JOURNAL(j) => fmt!(", \"j\":%?", j),
                W_N(w) => {
                    if w <= 0 { return Ok(None); }
                    else { fmt!(", \"w\":%d", w) }
                }
                W_STR(w) => fmt!(", \"w\":\"%s\"", w),
                WTIMEOUT(t) => fmt!(", \"wtimeout\":%d", t),
                FSYNC(s) => fmt!(", \"fsync\":%?", s),
            };
        }
        //concern_str += " }";
        concern_str = concern_str + " }";

        // parse write concern into bytes and send off
        let concern_json = match _str_to_bson(concern_str) {
            Ok(b) => *b,
            Err(e) => return Err(MongoErr::new(
                                    ~"client::_parse_and_send_wc",
                                    ~"concern specification",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        };
        let concern_query = mk_query(
                                self.inc_requestId(),
                                db,
                                ~"$cmd",
                                NO_CUR_TIMEOUT as i32,
                                0,
                                -1,
                                concern_json,
                                None);

        match self.send(msg_to_bytes(concern_query)) {
            Ok(_) => Ok(Some(())),
            Err(e) => return Err(MongoErr::new(
                                    ~"client::_parse_and_send_wc",
                                    ~"sending write concern",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /**
     * Picks up server response.
     *
     * # Returns
     * ServerMsg on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid bytestring/message returned (should never happen)
     * * server returned message with error flags
     * * network
     */
    fn _recv_msg(&self) -> Result<ServerMsg, MongoErr> {
        let m = match self.recv() {
            Ok(bytes) => match parse_reply(bytes) {
                Ok(m_tmp) => m_tmp,
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };

        match m {
            OpReply { header:_, flags:f, cursor_id:_, start:_, nret:_, docs:_ } => {
                if (f & CUR_NOT_FOUND as i32) != 0i32 {
                    return Err(MongoErr::new(
                                ~"coll::_recv_msg",
                                ~"CursorNotFound",
                                ~"cursor ID not valid at server"));
                } else if (f & QUERY_FAIL as i32) != 0i32 {
                    return Err(MongoErr::new(
                                ~"coll::_recv_msg",
                                ~"QueryFailure",
                                ~"tmp"));
                }
                return Ok(m)
            }
        }
    }
    /**
     * Send on connection affiliated with this client.
     *
     * # Arguments
     * * `bytes` - bytes to send
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * not connected
     * * network
     */
    pub fn send(&self, bytes : ~[u8]) -> Result<(), MongoErr> {
        if self.conn.is_empty() {
            Err(MongoErr::new(
                    ~"client::send",
                    ~"client not connected",
                    ~"attempted to send on nonexistent connection"))
        } else {
            let tmp = self.conn.take();
            let result = tmp.send(bytes);
            self.conn.put_back(tmp);
            result
        }
    }

    /**
     * Receive on connection affiliated with this client.
     *
     * # Returns
     * bytes received over connection on success, MongoErr on failure
     *
     * # Failure Types
     * * not connected
     * * network
     */
    pub fn recv(&self) -> Result<~[u8], MongoErr> {
        if self.conn.is_empty() {
            Err(MongoErr::new(
                    ~"client::recv",
                    ~"client not connected",
                    ~"attempted to receive on nonexistent connection"))
        } else {
            let tmp = self.conn.take();
            let result = tmp.recv();
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
}
