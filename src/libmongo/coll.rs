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
use msg::*;
use client::Client;
use cursor::Cursor;
use db::DB;

// macro for compressing options array into single i32 flag
// may need to remove if each CRUD op responsible for own options parsing
macro_rules! process_flags(
    ($options:ident) => (
        match $options {
            None => 0i32,
            Some(opt_array) => {
                let mut tmp = 0i32;
                for opt_array.iter().advance |&f| { tmp |= f as i32; }
                tmp
            }
        }
    );
)

pub enum MongoIndex {
    MongoIndexName(~str),
    MongoIndexFields(~[INDEX_FIELD]),
}

impl MongoIndex {
    // XXX
    fn process_index_opts(flags : i32, options : Option<~[INDEX_OPTION]>) -> (Option<~str>, ~[~str]) {
        let mut opts_str = ~[];

        // flags
        /*if (flags & BACKGROUND as i32) != 0i32 { opts_str += [~"\"background\":true"]; }
        if (flags & UNIQUE as i32) != 0i32 { opts_str += [~"\"unique\":true"]; }
        if (flags & DROP_DUPS as i32) != 0i32 { opts_str += [~"\"dropDups\":true"]; }
        if (flags & SPARSE as i32) != 0i32 { opts_str += [~"\"spare\":true"]; } */
        if (flags & BACKGROUND as i32) != 0i32 { opts_str = opts_str + [~"\"background\":true"]; }
        if (flags & UNIQUE as i32) != 0i32 { opts_str = opts_str + [~"\"unique\":true"]; }
        if (flags & DROP_DUPS as i32) != 0i32 { opts_str = opts_str + [~"\"dropDups\":true"]; }
        if (flags & SPARSE as i32) != 0i32 { opts_str = opts_str + [~"\"spare\":true"]; }

        // options
        let mut name = None;
        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    //opts_str += match opt {
                    opts_str = opts_str + match opt {
                        INDEX_NAME(n) => {
                            name = Some(copy n);
                            [fmt!("\"name\":\"%s\"", n)]
                        }
                        EXPIRE_AFTER_SEC(exp) => [fmt!("\"expireAfterSeconds\":%d", exp).to_owned()],
                        //VERS(int),
                        //WEIGHTS(BsonDocument),
                        //DEFAULT_LANG(~str),
                        //OVERRIDE_LANG(~str),
                    };
                }
            }
        };

        (name, opts_str)
    }
    fn process_index_fields(index_arr : ~[INDEX_FIELD], get_name : bool) -> (~str, ~[~str]) {
        let mut name = ~"";
        let mut index_str = ~[];
        for index_arr.iter().advance |&field| {
            match field {
                NORMAL(arr) => {
                    for arr.iter().advance |&(key, order)| {
                        /*index_str += [fmt!("\"%s\":%d", copy key, order as int)];
                        if get_name { name += fmt!("%s_%d", copy key, order as int); } */
                        index_str = index_str + [fmt!("\"%s\":%d", copy key, order as int)];
                        if get_name { name = name + fmt!("%s_%d", copy key, order as int); }
                    }
                }
                //HASHED(key) => index_str += [fmt!("\"%s\":\"hashed\"", copy key)],
                HASHED(key) => index_str = index_str + [fmt!("\"%s\":\"hashed\"", copy key)],
                GEOSPATIAL(key, geotype) => {
                    let typ = match geotype {
                        SPHERICAL => ~"2dsphere",
                        FLAT => ~"2d",
                    };
                    //index_str += [fmt!("\"%s\":\"%s\"", copy key, typ)];
                    index_str = index_str + [fmt!("\"%s\":\"%s\"", copy key, typ)];
                }
            }
        }

        (name, index_str)
    }

    /**
     * From either `~str` or full specification of index, get name.
     */
    pub fn get_name(&self) -> ~str {
        let tmp = copy *self;
        match tmp {
            MongoIndexName(s) => s,
            MongoIndexFields(arr) => {
                let (name, _) = MongoIndex::process_index_fields(arr, true);
                name
            }
        }
    }
}

pub struct Collection {
    db : ~str,          // XXX should be private? if yes, refactor cursor
    name : ~str,        // XXX should be private? if yes, refactor cursor
    client : @Client,   // XXX should be private? if yes, refactor cursor
}

// TODO: checking arguments for validity?

impl Collection {
    /**
     * Collection constructor for Client, etc. use.
     */
    pub fn new(db : ~str, name : ~str, client : @Client) -> Collection {
        Collection { db : db, name : name, client : client }
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
    pub fn _send_msg(&self, msg : ~[u8], wc : Option<~[WRITE_CONCERN]>, auto_get_reply : bool)
                -> Result<Option<ServerMsg>, MongoErr> {
        // first send message, exiting if network error
        match self.client.send(msg) {
            Ok(_) => (),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::_send_msg",
                                    ~"",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }

        // if not, for instance, query, handle write concern
        if !auto_get_reply {
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
            match self._send_wc(concern_str) {
                Ok(_) => (),
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::_send_msg",
                                        ~"sending write concern",
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
     * () on success, MongoErr on failuer
     *
     * # Failure Types
     * * invalid write concern specification (should never happen)
     * * network
     */
    fn _send_wc(&self, wc : ~str) -> Result<(), MongoErr>{
        let concern_json = match _str_to_bson(wc) {
            Ok(b) => *b,
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::_send_wc",
                                    ~"concern specification",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        };
        let concern_query = mk_query(
                                self.client.inc_requestId(),
                                copy self.db,
                                ~"$cmd",
                                NO_CUR_TIMEOUT as i32,
                                0,
                                -1,
                                concern_json,
                                None);

        match self.client.send(msg_to_bytes(concern_query)) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::_send_wc",
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
        let m = match self.client.recv() {
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
     * CRUD ops.
     * Different methods rather than enum of arguments
     * since complexity not decreased with enum (for
     * both users and developers), and CRUD oeprations
     * assumed reasonably stable.
     */

    /// INSERT OPS
    // TODO possibly combine anyway?
    /**
     * Insert given document with given writeconcern into Collection.
     *
     * # Arguments
     * * `doc`- BsonFormattable to input
     * * `wc` - write concern with which to insert (None for default of 1,
     *          Some for finer specification)
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid document to insert
     * * network
     */
    pub fn insert<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let bson_doc = ~[match doc.to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                    ~"coll::insert",
                                    ~"unknown BsonDocument/Document error",
                                    ~"BsonFormattable not actually BSON formattable")),
            }];
        let msg = mk_insert(
                        self.client.inc_requestId(),
                        copy self.db,
                        copy self.name,
                        0i32,
                        bson_doc);

        match self._send_msg(msg_to_bytes(msg), wc, false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::insert",
                                    ~"sending insert",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }
    /**
     * Insert given batch of documents with given writeconcern
     * into Collection.
     *
     * # Arguments
     * * `docs`- array of BsonFormattable to input
     * * `flag_array` - `CONT_ON_ERR`
     * * `option_array` - [none yet]
     * * `wc` - write concern with which to insert (None for default of 1,
     *          Some for finer specification)
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid document to insert (e.g. not proper format or
     *      duplicate `_id`)
     * * network
     */
    pub fn insert_batch<U : BsonFormattable>(&self, docs : ~[U],
                                                    flag_array : Option<~[INSERT_FLAG]>,
                                                    option_array : Option<~[INSERT_OPTION]>,
                                                    wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let mut bson_docs : ~[BsonDocument] = ~[];
        for docs.iter().advance |&d| {
            //bson_docs += [match d.to_bson_t() {
            bson_docs = bson_docs + [match d.to_bson_t() {
                    Embedded(bson) => *bson,
                    _ => return Err(MongoErr::new(
                                    ~"coll::insert_batch",
                                    ~"some BsonDocument/Document error",
                                    ~"no idea")),
                }];
        }
        let flags = process_flags!(flag_array);
        let _ = option_array;
        let msg = mk_insert(
                        self.client.inc_requestId(),
                        copy self.db,
                        copy self.name,
                        flags,
                        bson_docs);

        match self._send_msg(msg_to_bytes(msg), wc, false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::insert_batch",
                                    ~"sending batch insert",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }
    // TODO check
    pub fn save<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let bson_doc = match doc.to_bson_t() {
            Embedded(bson) => *bson,
            _ => return Err(MongoErr::new(
                                ~"coll::save",
                                ~"unknown BsonDocument/Document error",
                                ~"BsonFormattable not actually BSON formattable")),
        };
        let maybe_id = copy bson_doc.find(~"id");
        match maybe_id {
            None => self.insert(doc, wc),
            Some(id) => {
                let new_id = copy *id;
                let mut query = BsonDocument::new();
                query.append(~"_id", new_id);
                self.update(SpecObj(query), SpecObj(copy bson_doc), Some(~[UPSERT]), None, wc)
            },
        }
    }

    /// UPDATE OPS
    /**
     * Update documents satisfying given query with given update
     * specification and write concern.
     *
     * # Arguments
     * * `query` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to update
     * * `update_spec` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying update to documents
     * * `flag_array` - `UPSERT`, `MULTI`
     * * `option_array` - [nothing yet]
     * * `wc` - write concern with which to update documents
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid query or update specification
     * * getLastError
     * * network
     */
    pub fn update(&self,    query : QuerySpec,
                            update_spec : QuerySpec,
                            flag_array : Option<~[UPDATE_FLAG]>,
                            option_array : Option<~[UPDATE_OPTION]>,
                            wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let flags = process_flags!(flag_array);
        let _ = option_array;
        let q = match query {
            SpecObj(bson_doc) => bson_doc,
            SpecNotation(s) => match _str_to_bson(s) {
                Ok(b) => *b,
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::update",
                                        ~"query specification",
                                        fmt!("-->\n%s", MongoErr::to_str(e)))),
            },
        };
        let up = match update_spec {
            SpecObj(bson_doc) => bson_doc,
            SpecNotation(s) => match _str_to_bson(s) {
                Ok(b) => *b,
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::update",
                                        ~"update specification",
                                        fmt!("-->\n%s", MongoErr::to_str(e)))),
            },
        };
        let msg = mk_update(
                        self.client.inc_requestId(),
                        copy self.db,
                        copy self.name,
                        flags,
                        q,
                        up);

        match self._send_msg(msg_to_bytes(msg), wc, false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::update",
                                    ~"sending update",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /// FIND OPS
    // TODO make more general
    priv fn process_find_opts(&self, options : Option<~[QUERY_OPTION]>) -> (i32, i32) {
        let (x, y) = (0i32, 0i32);
        let mut nskip = x; let mut nret = y;
        match options {
            None => (),
            Some(opts) => {
                for opts.iter().advance |&opt| {
                    match opt {
                        NRET(n) => nret = n as i32,
                        NSKIP(n) => nskip = n as i32,
                    }
                }
            }
        }
        (nskip, nret)
    }
    /**
     * Returns Cursor over given projection from queried documents.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `proj` -  optioal `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying projection from queried documents
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     *
     * # Returns
     * initialized (unqueried) Cursor on success, MongoErr on failure
     */
    pub fn find(@self,  query : Option<QuerySpec>,
                        proj : Option<QuerySpec>,
                        flag_array : Option<~[QUERY_FLAG]>/*,
                        option_array : Option<~[QUERY_OPTION]>*/)
                -> Result<Cursor, MongoErr> {
        // construct query (wrapped as { $query : {...} } for ease of query modification)
        let q_field = match query {
            None => BsonDocument::new(),                // empty Bson
            Some(SpecObj(bson_doc)) => bson_doc,
            Some(SpecNotation(s)) => match _str_to_bson(s) {
                Ok(b) => *b,
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::find",
                                        ~"query specification",
                                        fmt!("-->\n%s", MongoErr::to_str(e)))),
            },
        };
        let mut q = BsonDocument::new();
        q.put(~"$query", Embedded(~q_field));

        // construct projection
        let p = match proj {
            None => None,
            Some(SpecObj(bson_doc)) => Some(bson_doc),
            Some(SpecNotation(s)) => match _str_to_bson(s) {
                Ok(b) => Some(*b),
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::find",
                                        ~"projection specification",
                                        fmt!("-->\n%s", MongoErr::to_str(e)))),
            },
        };

        // get flags
        let flags = process_flags!(flag_array);

        // get skip and limit if applicable
//        let (nskip, nret) = self.process_find_opts(option_array);

        // construct cursor and return
//        Ok(Cursor::new(q, p, @self, flags, nskip, nret))
        Ok(Cursor::new(q, p, self, flags))
    }
    /**
     * Returns pointer to first Bson from queried documents.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `proj` -  optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying projection from queried documents
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     *
     * # Returns
     * ~BsonDocument of first result on success, MongoErr on failure
     */
    //pub fn find_one(@self, query : Option<QuerySpec>, proj : Option<QuerySpec>, flag_array : Option<~[QUERY_FLAG]>, option_array : Option<~[QUERY_OPTION]>)
    pub fn find_one(@self, query : Option<QuerySpec>, proj : Option<QuerySpec>, flag_array : Option<~[QUERY_FLAG]>)
                -> Result<~BsonDocument, MongoErr> {
        /*let options = match option_array {
            None => Some(~[NRET(1)]),
            Some(opt) => Some(opt + [NRET(1)]),
        };

        let mut cur = self.find(query, proj, flag_array, options); */
        let mut cur = self.find(query, proj, flag_array);
        match cur {
            Ok(ref mut cursor) => {
                cursor.limit(-1);
                match cursor.next() {
                    Some(doc) => Ok(doc),
                    None => Err(MongoErr::new(
                                        ~"coll::find_one",
                                        ~"empty collection",
                                        ~"no documents in collection")),
                }
            },
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::find_one",
                                    ~"",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /// DELETE OPS
    priv fn process_delete_opts(&self, options : Option<~[DELETE_OPTION]>) -> i32 {
        let _ = options;
        0i32
    }
    /**
     * Remove specified documents from collection.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     * * `option_array` - [nothing yet]
     * * `wc` - write concern with which to perform remove
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn remove(&self, query : Option<QuerySpec>, flag_array : Option<~[DELETE_FLAG]>, option_array : Option<~[DELETE_OPTION]>, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let q = match query {
            None => BsonDocument::new(),
            Some(SpecObj(bson_doc)) => bson_doc,
            Some(SpecNotation(s)) => match _str_to_bson(s) {
                Ok(b) => *b,
                Err(e) => return Err(MongoErr::new(
                                        ~"coll::remove",
                                        ~"query specification",
                                        fmt!("-->\n%s", MongoErr::to_str(e)))),
            },
        };
        let flags = process_flags!(flag_array);
        let _ = self.process_delete_opts(option_array);
        let msg = mk_delete(self.client.inc_requestId(), copy self.db, copy self.name, flags, q);

        match self._send_msg(msg_to_bytes(msg), wc, false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::remove",
                                    ~"sending remove",
                                    fmt!("-->\n%s", MongoErr::to_str(e)))),
        }
    }

    /// INDICES (or "Indexes")
    /**
     * Create index by specifying a vector of the different elements
     * that can form an index (e.g. (field,order) pairs, geographical
     * options, etc.)
     *
     * # Arguments
     * * `index_arr` - vector of index elements
     *                  (NORMAL(vector of (field, order) pairs),
     *                  HASHED(field),
     *                  GEOSPATIAL(field, type))
     * * `flag_array` - optional vector of index-creating flags:
     *                  BACKGROUND,
     *                  UNIQUE,
     *                  DROP_DUPS,
     *                  SPARSE
     * * `option_array` - optional vector of index-creating options:
     *                  INDEX_NAME(name),
     *                  EXPIRE_AFTER_SEC(nsecs)
     *
     * # Returns
     * name of index as MongoIndexName (in enum MongoIndex) on success,
     * MongoErr on failure
     */
    pub fn create_index(&self,  index_arr : ~[INDEX_FIELD],
                                flag_array : Option<~[INDEX_FLAG]>,
                                option_array : Option<~[INDEX_OPTION]>)
                -> Result<MongoIndex, MongoErr> {
        let coll = @Collection::new(copy self.db, fmt!("%s", SYSTEM_INDEX), self.client);

        let flags = process_flags!(flag_array);
        let (x, y) = MongoIndex::process_index_opts(flags, option_array);
        let mut maybe_name = x; let mut opts = y;
        let (default_name, index) = MongoIndex::process_index_fields(index_arr, maybe_name.is_none());
        if maybe_name.is_none() {
            opts = opts + [fmt!("\"name\":\"%s\"", default_name)];
            maybe_name = Some(default_name);
        }

        let index_str = fmt!("{ \"key\":{ %s }, \"ns\":\"%s.%s\", %s }",
                            index.connect(", "),
                            copy self.db,
                            copy self.name,
                            opts.connect(", "));
        match coll.insert(index_str, None) {
            Ok(_) => Ok(MongoIndexName(maybe_name.unwrap())),
            Err(e) => Err(e),
        }
    }
    // TODO index cache? XXX presently just does a create_index...
    pub fn ensure_index(&self,  index_arr : ~[INDEX_FIELD],
                                flag_array : Option<~[INDEX_FLAG]>,
                                option_array : Option<~[INDEX_OPTION]>)
                -> Result<MongoIndex, MongoErr> {
        self.create_index(index_arr, flag_array, option_array)
    }
    /**
     * Drops specified index.
     *
     * # Arguments
     * * `index` - MongoIndex to drop specified either by explicit name
     *              or fields
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn drop_index(&self, index : MongoIndex) -> Result<(), MongoErr> {
        let db = DB::new(copy self.db, self.client);
        db.run_command(SpecNotation(
            fmt!("{ \"deleteIndexes\":\"%s\", \"index\":\"%s\" }",
                copy self.name,
                index.get_name())))
    }
}
