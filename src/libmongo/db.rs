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

use std::int::range;
use std::libc::c_int;
use std::ptr::to_unsafe_ptr;
use std::to_bytes::*;

use bson::encode::*;
use bson::formattable::*;

use util::*;
use client::Client;
use coll::Collection;

static L_END: bool = true;

#[link_args = "-lmd5"]
extern {
    fn md5_init(pms: *MD5State);
    fn md5_append(pms: *MD5State, data: *const u8, nbytes: c_int);
    fn md5_finish(pms: *MD5State, digest: *[u8,..16]);
}

priv struct MD5State {
    count: [u32,..2],
    abcd: [u32,..4],
    buf: [u8,..64]
}

pub struct DB {
    name : ~str,
    priv client : @Client,
}

impl MD5State {
    fn new(len: u64) -> MD5State {
        let mut c: [u32,..2] = [0u32,0];
        let l = len.to_bytes(L_END);
        c[0] |= l[0] as u32;
        c[0] |= (l[1] << 8) as u32;
        c[0] |= (l[2] << 16) as u32;
        c[0] |= (l[3] << 24) as u32;
        c[1] |= l[4] as u32;
        c[1] |= (l[5] << 8) as u32;
        c[1] |= (l[6] << 16) as u32;
        c[1] |= (l[7] << 24) as u32;

        MD5State {
            count: c,
            abcd: [0u32,0,0,0],
            buf: [
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0
                ]
        }
    }
}

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
 
    ///Add a new database user with the given username and password.
    ///If the system.users collection becomes unavailable, this will fail.
    pub fn add_user(&self, username: ~str, password: ~str, roles: ~[~str]) -> Result<(), MongoErr>{
        let coll = @(self.get_collection(~"system.users"));
        let mut user = match coll.find_one(Some(SpecNotation(fmt!("{ user: %s }", username))), None, None)
            {
                Ok(u) => u,
                Err(_) => {
                    let mut doc = BsonDocument::new();
                    doc.put(~"user", UString(copy username));
                    ~doc
                }
            };
        user.put(~"pwd", UString(md5(fmt!("%s:mongo:%s", username, password))));
        user.put(~"roles", roles.to_bson_t());
        coll.save(user, None)
    }

    ///Become authenticated as the given username with the given password.
    pub fn authenticate(&self, username: ~str, password: ~str) -> Result<(), MongoErr> {
        let nonce = match self.run_command(SpecNotation(~"{ \"getnonce\": 1 }")) {
            Ok(doc) => match *doc.find(~"nonce").unwrap() { //this unwrap should always succeed
                UString(ref s) => copy *s,
                _ => return Err(MongoErr::new(
                    ~"db::authenticate",
                    ~"error while getting nonce",
                    fmt!("an invalid nonce (%?) was returned by the server", *doc.find(~"nonce").unwrap())))
            },
            Err(e) => return Err(e)
        };
        match self.run_command(SpecNotation(fmt!(" {
              \"authenticate\": 1,
              \"user\": \"%s\",
              \"nonce\": \"%s\",
              \"key\": \"%s\" } ",
              username,
              nonce,
              md5(fmt!("%s%s%s", nonce, username, md5(fmt!("%s:mongo:%s",username, password))))))) {
           Ok(_) => return Ok(()),
           Err(e) => return Err(e)
        }
    }

    ///Log out of the current user.
    ///Closing a connection will also log out.
    pub fn logout(&self) -> Result<(), MongoErr> {
        match self.run_command(SpecNotation(~"{ \"logout\": 1 }")) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => return Ok(()),
                Int32(1i32) => return Ok(()),
                Int64(1i64) => return Ok(()),
                _ => return Err(MongoErr::new(
                    ~"db::logout",
                    ~"error while logging out",
                    ~"the server returned ok: 0"))
            },
            Err(e) => return Err(e)
        };
    }

    ///Get the profiling level of the database.
    pub fn get_profiling_level(&self) -> Result<BsonDocument, MongoErr> {
        self.run_command(SpecNotation(~"{ \"profile\": -1 }"))
    }

    ///Set the profiling level of the database.
    pub fn set_profiling_level(&self, level: &str) -> Result<BsonDocument, MongoErr> {
        self.run_command(SpecNotation(fmt!("{ \"profile\": \"%s\" }", level)))
    }
}

priv fn md5(msg: &str) -> ~str {
    let msg_bytes = msg.to_bytes(L_END);
    let m = MD5State::new(msg_bytes.len() as u64);
    let digest: [u8,..16] = [
        0,0,0,0,
        0,0,0,0,
        0,0,0,0,
        0,0,0,0
    ];

    unsafe {
        md5_init(to_unsafe_ptr(&m));
        md5_append(to_unsafe_ptr(&m), to_unsafe_ptr(&(msg_bytes[0])), msg_bytes.len() as i32);
        md5_finish(to_unsafe_ptr(&m), to_unsafe_ptr(&digest));
    }

    let mut result: ~str = ~"";
    for range(0, 16) |i| {
        let mut byte = fmt!("%x", digest[i] as uint);
        if byte.len() == 1 {
            byte = (~"0").append(byte);
        }
        result.push_str(byte);
    }
    result
}

#[cfg(test)]
#[test]
fn md5_test() {
    assert_eq!(md5(~"hello"), ~"5d41402abc4b2a76b9719d911017c592");
    assert_eq!(md5(~"asdfasdfasdf"), ~"a95c530a7af5f492a74499e70578d150");
}
