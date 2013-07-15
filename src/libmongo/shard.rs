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

use util::*;
use client::*;
use db::*;
//use coll::*;

use bson::encode::*;

/**
 * A shard controller. An instance of this
 * wraps a Client connection to a mongos instance.
 */
pub struct ShardController {
    mongos: @Client
}

impl ShardController {

    /**
     * Enable sharding on the specified database.
     * The database must exist or this operation will fail.
     */
    pub fn enable_sharding(&self, db: ~str) -> Result<(), MongoErr> {
        match self.mongos.get_dbs() {
            Ok(strs) => if !(strs.contains(&db)) {
                return Err(MongoErr::new(
                    ~"shard::enable_sharding",
                    fmt!("db %s not found", db),
                    ~"sharding can only be enabled on an existing db"))
            },
            Err(e) => return Err(e)
        }

        let d = DB::new(copy db, copy self.mongos);
        match d.run_command(SpecNotation(fmt!("{ \"enableSharding\": %s }", db))) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => return Ok(()),
                Int32(1i32) => return Ok(()),
                Int64(1i64) => return Ok(()),
                _ => return Err(MongoErr::new(
                    ~"shard::enable_sharding",
                    fmt!("error enabling sharding on %s", db),
                    ~"the server returned ok: 0"))
            },
            Err(e) => return Err(e)
        };
    }

    pub fn add_shard(&self, hostname: ~str) -> Result<(), MongoErr> {
        let admin = self.mongos.get_admin();
        match admin.run_command(SpecNotation(fmt!("{ \"addShard\": %s }", copy hostname))) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => return Ok(()),
                Int32(1i32) => return Ok(()),
                Int64(1i64) => return Ok(()),
                _ => return Err(MongoErr::new(
                    ~"shard::add_shard",
                    fmt!("error adding shard at %s", hostname),
                    ~"the server returned ok: 0"))
            },
            Err(e) => return Err(e)
        };
    }
}
