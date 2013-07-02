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

use bson::decode::*;
use bson::encode::*;

use util::*;
use util::special::*;
use client::Client;
use coll::Collection;

pub struct DB {
    name : ~str,
    priv client : @Client,
    cur_coll : ~cell::Cell<~str>,
}

/**
 */
impl DB {

    pub fn new(name : ~str, client : @Client) -> DB {
        DB {
            name : name,
            cur_coll : ~cell::Cell::new_empty(),
            client : client
        }
    }

    /*pub fn get_collection_names(&self) -> Collection {

    }

    pub fn get_collection(&self, coll : ~str) -> Collection {
        Collection::new(copy self.name, coll, self.client);
    }

    pub fn create_collection(&self, coll : ~str) -> result::Result<(), MongoErr> {

    }

    pub fn drop_collection(&self, coll : ~str) -> result::Result<(), MongoErr> {
        
    }

    pub fn get_admin(&self) -> DB {
        DB::new(~"admin", self.client)
    }*/

    // TODO make take options?
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
