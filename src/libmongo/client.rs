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

use util::*;
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
            Err(e) => Err(e)
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

        let tmp = Connection::new::<NodeConnection>(server_ip_str, server_port);
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
