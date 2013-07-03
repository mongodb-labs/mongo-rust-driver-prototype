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

    /**
     * Get names of all collections in this db, returning error
     * if any fail. Names do not include db name.
     *
     * # Returns
     * vector of collection names on success, MongoErr on failure
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
                                names = names + [name];
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

    pub fn get_collection(&self, coll : ~str) -> Collection {
        Collection::new(copy self.name, coll, self.client)
    }

    // TODO make take options? (not strictly necessary but may be good?)
    // TODO allow other query options, e.g. SLAVE_OK, with helper function
    // TODO return non-unit for things like listDatabases
    pub fn run_command(&self, cmd : QuerySpec) -> Result<(), MongoErr> {
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
            _ => return Ok(())
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
}
