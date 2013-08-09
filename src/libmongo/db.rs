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
use bson::formattable::*;

use util::*;
use tools::md5::*;
use client::Client;
use coll::Collection;

static L_END: bool = true;

pub struct DB {
    name : ~str,
    priv client : @Client,
}

/**
 * Having created a `Client` and connected as desired
 * to a server or cluster, users may interact with
 * databases by creating `DB` handles to those databases.
 */
impl DB {
    /**
     * Creates a new Mongo DB with given name and associated Client.
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
     * Gets names of all collections in this `DB`, returning error
     * if any fail. Names do not include `DB` name, i.e. are not
     * full namespaces.
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
        let coll = Collection::new(self.name.clone(), SYSTEM_NAMESPACE.to_owned(), self.client);
        let mut cur = match coll.find(None, None, None) {
            Ok(cursor) => cursor,
            Err(e) => return Err(e),
        };

        // pull out all the names, returning error if any fail
        for cur.advance |doc| {
            match doc.find(~"name") {
                Some(val) => {
                    match val {
                        &UString(ref s) => {
                            // ignore special collections (with "$")
                            if !s.contains_char('$') {
                                names.push(s.slice_from(self.name.len()+1).to_owned());
                            }
                        },
                        _ => return Err(MongoErr::new(
                                    ~"db::get_collection_names",
                                    fmt!("db %s", self.name),
                                    ~"got non-string collection name")),
                    }
                },
                None => return Err(MongoErr::new(
                                ~"db::get_collection_names",
                                fmt!("db %s", self.name),
                                ~"got no name for collection")),

            }
        }

        Ok(names)
    }
    /**
     * Gets `Collection`s in this `DB`, returning error if any fail.
     *
     * # Returns
     * vector of `Collection`s on success, `MongoErr` on failure
     *
     * # Failure Types
     * * errors propagated from `get_collection_names`
     */
    pub fn get_collections(&self) -> Result<~[Collection], MongoErr> {
        let names = match self.get_collection_names() {
            Ok(n) => n,
            Err(e) => return Err(e),
        };

        let mut coll : ~[Collection] = ~[];
        for names.iter().advance |&n| {
            coll.push(Collection::new(self.name.clone(), n, self.client));
        }

        Ok(coll)
    }
    /**
     * Creates collection with given options.
     *
     * # Arguments
     * * `coll` - name of collection to create
     * * `flag_array` - collection creation flags
     * * `option_array` - collection creation options
     *
     * # Returns
     * handle to collection on success, `MongoErr` on failure
     */
    pub fn create_collection(   &self,
                                coll : ~str,
                                flag_array : Option<~[COLLECTION_FLAG]>,
                                option_array : Option<~[COLLECTION_OPTION]>)
            -> Result<Collection, MongoErr> {
        let flags = process_flags!(flag_array);
        let cmd = fmt!( "{ \"create\":\"%s\", %s }",
                        coll,
                        self.process_create_ops(flags, option_array));
        let old_pref = self.client.set_read_pref(PRIMARY_ONLY);
        let result = match self.run_command(SpecNotation(cmd)) {
            Ok(_) => Ok(Collection::new(self.name.clone(), coll, self.client)),
            Err(e) => Err(e),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }
    priv fn process_create_ops(&self, flags : i32, options : Option<~[COLLECTION_OPTION]>)
            -> ~str {
        let mut opts_str = ~"";
        opts_str.push_str(fmt!( "\"autoIndexId\":%? ",
                                (flags & AUTOINDEX_ID as i32) != 0i32));

        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    opts_str.push_str(match opt {
                        CAPPED(sz) => fmt!(", \"capped\":true, \"size\":%?", sz),
                        SIZE(sz) => fmt!(", \"size\":%?", sz),
                        MAX_DOCS(k) => fmt!(", \"max\":%?", k),
                    })
                }
            }
        }

        opts_str
    }
    /**
     * Gets handle to collection with given name, from this `DB`.
     *
     * # Arguments
     * * `coll` - name of `Collection` to get
     *
     * # Returns
     * handle to collection
     */
    pub fn get_collection(&self, coll : ~str) -> Collection {
        Collection::new(self.name.clone(), coll, self.client)
    }
    /**
     * Drops given collection from database associated with this `DB`.
     *
     * # Arguments
     * * `coll` - name of collection to drop
     *
     * # Returns
     * () on success, `MongoErr` on failure
     */
    pub fn drop_collection(&self, coll : &str) -> Result<(), MongoErr> {
        let old_pref = self.client.set_read_pref(PRIMARY_ONLY);
        let result = match self.run_command(SpecNotation(fmt!("{ \"drop\":\"%s\" }", coll))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
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
        let coll = Collection::new(self.name.clone(), SYSTEM_COMMAND.to_owned(), self.client);

        //let ret_msg = match coll.find_one(Some(cmd), None, None, None) {
        let ret_msg = match coll.find_one(Some(cmd.clone()), None, Some(~[NO_CUR_TIMEOUT])) {
            Ok(msg) => msg,
            Err(e) => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error getting return value from run_command %s", cmd.to_str()),
                                    fmt!("-->\n%s", e.to_str()))),
        };

        // check if run_command succeeded
        let ok = match ret_msg.find(~"ok") {
            Some(x) => match *x {
                Double(v) => v,
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %s", cmd.to_str()),
                                    fmt!("\"ok\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %s", cmd.to_str()),
                                    ~"no \"ok\" field in return message!")),
        };
        match ok {
            0f64 => (),
            _ => return Ok(ret_msg)
        }

        // otherwise, extract error message
        let errmsg = match ret_msg.find(~"errmsg") {
            Some(x) => match *x {
                UString(ref s) => s.to_owned(),
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %s", cmd.to_str()),
                                    fmt!("\"errmsg\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_comand %s", cmd.to_str()),
                                    ~"run_command failed without msg!")),
        };

        Err(MongoErr::new(
                ~"db::run_command",
                fmt!("run_command %s failed", cmd.to_str()),
                errmsg))
    }

    /**
     * Parses write concern into bytes and sends to server.
     *
     * # Arguments
     * * `wc` - write concern, i.e. getLastError specifications
     *
     * # Returns
     * `Option<~BsonDocument>` with full response on success (or None
     * if write concern was 0), `MongoErr` on failure
     *
     * # Failure Types
     * * invalid write concern specification (should never happen)
     * * network
     * * getLastError error, e.g. duplicate ```_id```s
     */
    pub fn get_last_error(&self, wc : Option<~[WRITE_CONCERN]>)
                -> Result<Option<~BsonDocument>, MongoErr>{
        // set default write concern (to 1) if not specified
        let concern = match wc {
            None => ~[W_N(1), FSYNC(false)],
            Some(w) => w,
        };

        let mut concern_doc = BsonDocument::new();
        concern_doc.put(~"getLastError", Bool(true));

        // parse write concern, early exiting if set to <= 0
        for concern.iter().advance |&opt| {
            match opt {
                JOURNAL(j) => concern_doc.put(~"j", Bool(j)),
                W_N(w) => {
                    if w <= 0 { return Ok(None); }
                    else { concern_doc.put(~"w", Int32(w as i32)) }
                }
                W_STR(w) => concern_doc.put(~"w", UString(w)),
                W_TAGSET(ts) => concern_doc.union(ts.to_bson_t()),
                WTIMEOUT(t) => concern_doc.put(~"wtimeout", Int32(t as i32)),
                FSYNC(s) => concern_doc.put(~"fsync", Bool(s)),
            }
        }

        // run_command and get entire doc
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));
        let err_doc_tmp = match self.run_command(SpecObj(concern_doc)) {
            Ok(doc) => doc,
            Err(e) => return Err(MongoErr::new(
                                    ~"db::get_last_error",
                                    ~"run_command error",
                                    fmt!("-->\n%s", e.to_str()))),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }

        // error field name possibitilies
        let err_field = ~[  err_doc_tmp.find(~"err"),
                            err_doc_tmp.find(~"$err")];

        // search for error field
        let mut err_found = false;
        let mut err_doc = Int32(1); // [invalid err_doc]
        for err_field.iter().advance |&err_result| {
            match err_result {
                None => (),
                Some(doc) => {
                    err_found = true;
                    err_doc = doc.clone();
                }
            }
        };

        if !err_found {
            return Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError unexpected format",
                            ~"no $err field in reply"));
        }

        // unwrap error message
        match err_doc {
            Null => Ok(Some(err_doc_tmp.clone())),
            UString(s) => Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError error",
                            s)),
            _ => Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError unexpected format",
                            ~"unknown last error in reply")),
        }
    }

    ///Enable sharding on this database.
    pub fn enable_sharding(&self) -> Result<(), MongoErr> {
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));   // XXX check
        let result = match self.run_command(SpecNotation(fmt!("{ \"enableSharding\": %s }", self.name))) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => Ok(()),
                Int32(1i32) => Ok(()),
                Int64(1i64) => Ok(()),
                _ => Err(MongoErr::new(
                    ~"db::logout",
                    ~"error while logging out",
                    ~"the server returned ok: 0")),
            },
            Err(e) => Err(e),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }

    ///Add a new database user with the given username and password.
    ///If the system.users collection becomes unavailable, this will fail.
    pub fn add_user(&self, username: ~str, password: ~str, roles: ~[~str]) -> Result<(), MongoErr>{
        let coll = self.get_collection(SYSTEM_USERS.to_owned());
        let mut user = match coll.find_one(Some(SpecNotation(fmt!("{ \"user\": \"%s\" }", username))), None, None)
            {
                Ok(u) => u,
                Err(_) => {
                    let mut doc = BsonDocument::new();
                    doc.put(~"user", UString(username.clone()));
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
                UString(ref s) => s.to_owned(),
                _ => return Err(MongoErr::new(
                    ~"db::authenticate",
                    ~"error while getting nonce",
                    fmt!("an invalid nonce (%?) was returned by the server", *doc.find(~"nonce").unwrap())))
            },
            Err(e) => return Err(e)
        };
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));
        let result = match self.run_command(SpecNotation(fmt!(" {
              \"authenticate\": 1,
              \"user\": \"%s\",
              \"nonce\": \"%s\",
              \"key\": \"%s\" } ",
              username,
              nonce,
              md5(fmt!("%s%s%s", nonce, username, md5(fmt!("%s:mongo:%s",username, password))))))) {
           Ok(_) => Ok(()),
           Err(e) => Err(e)
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }

    ///Log out of the current user.
    ///Closing a connection will also log out.
    pub fn logout(&self) -> Result<(), MongoErr> {
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));
        let result = match self.run_command(SpecNotation(~"{ \"logout\": 1 }")) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => Ok(()),
                Int32(1i32) => Ok(()),
                Int64(1i64) => Ok(()),
                _ => Err(MongoErr::new(
                    ~"db::logout",
                    ~"error while logging out",
                    ~"the server returned ok: 0")),
            },
            Err(e) => Err(e),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }

    ///Get the profiling level of the database.
    // XXX return type; potential for change
    pub fn get_profiling_level(&self) -> Result<(int, Option<int>), MongoErr> {
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));
        let result = match self.run_command(SpecNotation(~"{ 'profile':-1 }")) {
            Ok(d) => {
                let mut err = None;
                let mut level = None;
                let mut thresh = None;
                match d.find(~"was") {
                    Some(&Double(f)) => level = Some(f as int),
                    _ => err = Some(MongoErr::new(
                        ~"db::get_profiling_level",
                        ~"could not get profiling level",
                        ~"an invalid profiling level was returned"))
                }
                match d.find(~"slowms") {
                    None => (),
                    Some(&Double(ms)) => thresh = Some(ms as int),
                    _ => err = Some(MongoErr::new(
                        ~"db::get_profiling_level",
                        ~"could not get profiling threshold",
                        ~"an invalid profiling threshold was returned"))
                };

                if err.is_none() { Ok((level.unwrap(), thresh)) }
                else { Err(err.unwrap()) }
            }
            Err(e) => Err(e),
        };
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }

    ///Set the profiling level of the database.
    // XXX argument types; potential for change
    pub fn set_profiling_level(&self, level: int)
                -> Result<~BsonDocument, MongoErr> {
        let old_pref = self.client.set_read_pref(PRIMARY_PREF(None));
        let result = self.run_command(SpecNotation(fmt!("{ \"profile\": %d }", level)));
        match old_pref {
            Ok(p) => { self.client.set_read_pref(p); }
            Err(_) => (),
        }
        result
    }
}
