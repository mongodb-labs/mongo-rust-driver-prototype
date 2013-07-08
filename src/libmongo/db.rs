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

use bson::encode::*;

use util::*;
use client::Client;
use coll::Collection;

pub struct DB {
    name : ~str,
    priv client : @Client,
}

// TODO auth (logout, auth, add_user, remove_user, change_password)
// TODO coll (drop_collection)

/**
 * Having created a `Client` and connected as desired
 * to a server or cluster, users may interact with
 * databases by creating `DB` handles to those databases.
 */
impl DB {
    /**
     * Create a new Mongo DB with given name and associated Client.
     *
     * # Arguments
     * * `name` - name of DB
     * * `client` - Client with which this DB is associated
     *
     * # Returns
     * DB (handle to database)
     */
    pub fn new(name : ~str, client : @Client) -> DB {
        DB {
            name : name,
            client : client
        }
    }

    // COLLECTION INTERACTION
    /**
     * Get names of all collections in this `DB`, returning error
     * if any fail. Names do not include `DB` name.
     *
     * # Returns
     * vector of collection names on success, `MongoErr` on failure
     *
     * # Failure Types
     * * error querying `system.indexes` collection
     * * response from server not in expected form (must contain
     *      vector of `BsonDocument`s each containing "name" fields of
     *      `UString`s)
     */
    pub fn get_collection_names(&self) -> Result<~[~str], MongoErr> {
        let mut names : ~[~str] = ~[];

        // query on namespace collection
        let coll = @Collection::new(copy self.name, fmt!("%s", SYSTEM_NAMESPACE), self.client);
        let mut cur = match coll.find(None, None, None) {
            Ok(cursor) => cursor,
            Err(e) => return Err(e),
        };

        // pull out all the names, returning error if any fail
        for cur.advance |doc| {
            match doc.find(~"name") {
                Some(val) => {
                    let tmp = copy *val;
                    match tmp {
                        UString(s) => {
                            // ignore special collections (with "$")
                            if !s.contains_char('$') {
                                let name = s.slice_from(self.name.len()+1).to_owned();
                                names = names + ~[name];
                            }
                        },
                        _ => return Err(MongoErr::new(
                                    ~"db::get_collection_names",
                                    fmt!("db %s", copy self.name),
                                    ~"got non-string collection name")),
                    }
                },
                None => return Err(MongoErr::new(
                                ~"db::get_collection_names",
                                fmt!("db %s", copy self.name),
                                ~"got no name for collection")),

            }
        }

        Ok(names)
    }
    /**
     * Get `Collection`s in this `DB`, returning error if any fail.
     *
     * # Returns
     * vector of `Collection`s on success, `MongoErr` on failure
     *
     * # Failure Types
     * * errors propagated from `get_collection_names`
     */
    pub fn get_collections(&self) -> Result<~[@Collection], MongoErr> {
        let names = match self.get_collection_names() {
            Ok(n) => n,
            Err(e) => return Err(e),
        };

        let mut coll : ~[@Collection] = ~[];
        for names.iter().advance |&n| {
            coll = coll + ~[@Collection::new(copy self.name, n, self.client)];
        }

        Ok(coll)
    }
    /**
     * Get `Collection` with given name, from this `DB`.
     *
     * # Arguments
     * * `coll` - name of collection to get
     *
     * # Returns
     * managed pointer to collecton handle
     */
    pub fn get_collection(&self, coll : ~str) -> @Collection {
        @Collection::new(copy self.name, coll, self.client)
    }

    // TODO make take options? (not strictly necessary but may be good?)
    // TODO allow other query options, e.g. SLAVE_OK, with helper function
    /**
     * Runs given command (taken as `BsonDocument` or `~str`).
     *
     * # Arguments
     * * `cmd` - command to run, taken as `SpecObj(BsonDocument)` or
     *              `SpecNotation(~str)`
     *
     * # Returns
     * `~BsonDocument` response from server on success that must be parsed
     * appropriately by caller, `MongoErr` on failure
     */
    pub fn run_command(&self, cmd : QuerySpec) -> Result<~BsonDocument, MongoErr> {
        let coll = @Collection::new(copy self.name, fmt!("%s", SYSTEM_COMMAND), self.client);

        //let ret_msg = match coll.find_one(Some(cmd), None, None, None) {
        let ret_msg = match coll.find_one(Some(copy cmd), None, Some(~[NO_CUR_TIMEOUT])) {
            Ok(msg) => msg,
            Err(e) => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error getting return value from run_command %?", cmd),
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        };

        // check if run_command succeeded
        let ok = match ret_msg.find(~"ok") {
            Some(x) => match *x {
                Double(v) => v,
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    fmt!("\"ok\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    ~"no \"ok\" field in return message!")),
        };
        match ok {
            0f64 => (),
            _ => return Ok(ret_msg)
        }

        // otherwise, extract error message
        let errmsg = match ret_msg.find(~"errmsg") {
            Some(x) => match *x {
                UString(ref s) => s,
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    fmt!("\"errmsg\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_comand %?", cmd),
                                    ~"run_command failed without msg!")),
        };

        Err(MongoErr::new(
                ~"db::run_command",
                fmt!("run_command %? failed", cmd),
                copy *errmsg))
    }

    pub fn add_user(&self, username: ~str, password: ~str) {
        let coll = @(self.get_collection(~"system.users"));
        let user = match coll.find_one(Some(SpecNotation(fmt!("{ user: %s }", username))), None, None)
            {
                Ok(u) => u,
                Err(_) => {
                    let mut doc = BsonDocument::new();
                    doc.put(~"user", UString(copy username));
                    ~doc
                }
            };
        //user.put(~"pwd", md5(fmt!("%s:mongo:%s", username, pass)));
        //TODO: get an MD5 implementation. OpenSSL bindings are available from
        //github.com/kballard/rustcrypto
        coll.save(user, None);
    }

    pub fn authenticate(&self, username: ~str, password: ~str) -> bool {
        let nonce = self.run_command(SpecNotation(~"{ getnonce: 1 }"));
        //TODO: blocked on run_command returning correct values?
        //TODO: definitely blocked on md5
        /*match self.run_command(SpecNotation(fmt!(" {
              authenticate: 1,
              username: %s,
              nonce: %x,
              key: %x } ",
              username,
              nonce,
              md5(fmt!("%x%s%x", nonce, username, md5(fmt!("%s:mongo:%s",username, pass))))))) {
           Ok(_) => return true,
           Err(_) => return false
        }
        */
        return false;
    }
}
