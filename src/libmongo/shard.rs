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
pub struct MongosClient {
    mongos: @Client
}

impl MongosClient {

    /**
     * Create a new MongosClient.
     * Will fail if the given Client is not connected
     * to a mongos instance.
     */
    pub fn new(client: @Client) -> MongosClient {
        //check that client points to a mongos; fail if it doesn't
        //since a new method should not return a result (I think?)
        let admin = client.get_admin();
        match admin.run_command(
            SpecNotation(~"{ 'ismaster': 1 }")) {
                Ok(res) => match res.find(~"msg") {
                    Some(&UString(~"isdbgrid")) => (),
                    _ => fail!("MongosClient can only connect to a mongos instance")
                },
                _ => fail!("MongosClient can only connect to a mongos instance")
            };
        MongosClient { mongos: client }
    }

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

        let admin = self.mongos.get_admin();
        match admin.run_command(SpecNotation(fmt!("{ 'enableSharding': '%s' }", db))) {
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

    ///Return a list of all shards on the current cluster.
    pub fn list_shards(&self) -> Result<~BsonDocument, MongoErr> {
        let admin = self.mongos.get_admin();
        admin.run_command(SpecNotation(~"{ 'listShards': 1 }"))
    }

    /**
     * Allow this shard controller to manage a new shard.
     * Hostname can be in a variety of formats:
     * * <hostname>
     * * <hostname>:<port>
     * * <replset>/<hostname>
     * * <replset>/<hostname>:port
     */
    pub fn add_shard(&self, hostname: ~str) -> Result<(), MongoErr> {
        let admin = self.mongos.get_admin();
        match admin.run_command(SpecNotation(fmt!("{ 'addShard': '%s' }", hostname))) {
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

    /**
     * Begins removing a shard from this cluster.
     * If called while a shard is being removed, will instead return
     * a document describing the current removal status.
     */
    pub fn remove_shard(&self, shardname: ~str) -> Result<~BsonDocument, MongoErr> {
        let db = self.mongos.get_admin();
        db.run_command(SpecNotation(fmt!("{ 'removeShard': '%s' }", shardname)))
    }

     ///Enable sharding on the specified collection.
     pub fn shard_collection(&self, db: ~str, coll: ~str, key: QuerySpec, unique: bool) -> Result<(), MongoErr> {
        let d = DB::new(db.clone(), self.mongos);
        match d.run_command(SpecNotation(
            fmt!("{ 'shardCollection': '%s.%s', 'key': %s, 'unique': '%s' }",
                db, coll, match key {
                    SpecObj(doc) => doc.to_str(),
                    SpecNotation(ref s) => s.clone()
                }, unique.to_str()))) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => return Ok(()),
                Int32(1i32) => return Ok(()),
                Int64(1i64) => return Ok(()),
                _ => return Err(MongoErr::new(
                    ~"shard::shard_collection",
                    fmt!("error sharding collection %s.%s", db, coll),
                    ~"the server returned ok: 0"))
            },
            Err(e) => return Err(e)
        };
     }

     /**
      * Display the status of the current cluster.
      * Equivalent to running sh.status() in shell.
      */
     pub fn status(&self) -> Result<~str, MongoErr> {
         //TODO verbose mode
        let mut out = ~"";
        let config = DB::new(~"config", self.mongos);
        let version = match config.get_collection(~"version").find_one(None, None, None) {
            Ok(doc) => doc,
            Err(e) => return Err(e)
        };
        out.push_str(~"--- Sharding Status ---\n");
        out.push_str(fmt!("  sharding version: %s\n", version.to_str()));
        out.push_str(~"  shards:\n");
        match config.get_collection(~"shards").find(None, None, None) {
            Ok(ref mut c) => {
                for c.advance |sh| {
                    out.push_str(fmt!("\t%s\n", sh.to_str()));
                }
            },
            Err(e) => return Err(e)
        };
        out.push_str(~"  databases:\n");
        match config.get_collection(~"databases").find(None,
            None, None) {
            Ok(ref mut c) => {
                for c.advance |d| {
                    out.push_str(fmt!("\t%s\n", d.to_str()));
                    //TODO chunk info
                }
            }
            Err(e) => return Err(e)
        }
        //TODO finish this
        Ok(out)
     }

    //TODO: using cfg! instead of #[cfg] will clean up a lot of mess,
    //though the check will be done at runtime. It will still be
    //much faster than client::check_version.

    ///Add a tag to the given shard.
    ///Requires MongoDB 2.2 or higher.
    #[cfg(not(major=1), not(major=0))]
    pub fn add_shard_tag(&self, shard: ~str, tag: ~str) -> Result<(), MongoErr> {
        //let ch = self.mongos.check_version(~"2.2.0");
        //if ch.is_err() { return ch; }
        let config = DB::new(~"config", self.mongos);
        match config.get_collection(~"shards").find_one(
           Some(SpecNotation(fmt!("{ '_id': '%s' }", shard))), None, None) {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        match config.get_collection(~"shards").update(
            SpecNotation(fmt!("{ '_id': '%s' }", shard)),
            SpecNotation(fmt!("{ '$addToSet': { 'tags': '%s' } }", tag)),
            None, None, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    #[cfg(major=1)]
    #[cfg(major=0)]
    pub fn add_shard_tag(&self, _shard: ~str, _tag: ~str) -> Result<(), MongoErr> {
        Err(MongoErr::new(
            ~"shard::add_shard_tag",
            ~"this function requires MongoDB 2.2 or higher",
            ~"please upgrade at mongodb.org"))
    }

     ///Remove a tag from the given shard.
     ///Requires MongoDB 2.2 or higher.
    #[cfg(not(major=1), not(major=0), not(major=2,minor=1), not(major=2,minor=0))]
     pub fn remove_shard_tag(&self, shard: ~str, tag: ~str) -> Result<(), MongoErr> {
         //let ch = self.mongos.check_version(~"2.2.0");
         //if ch.is_err() { return ch; }
        let config = DB::new(~"config", self.mongos);
        match config.get_collection(~"shards").find_one(
           Some(SpecNotation(fmt!("{ '_id': '%s' }", shard))), None, None) {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        match config.get_collection(~"shards").update(
            SpecNotation(fmt!("{ '_id': '%s' }", shard)),
            SpecNotation(fmt!("{ '$pull': { 'tags': '%s' } }", tag)),
            None, None, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
     }

    #[cfg(major=1)]
    #[cfg(major=0)]
    pub fn remove_shard_tag(&self, _shard: ~str, _tag: ~str) -> Result<(), MongoErr> {
        Err(MongoErr::new(
            ~"shard::remove_shard_tag",
            ~"this function requires MongoDB 2.2 or higher",
            ~"please upgrade at mongodb.org"))
    }
}
