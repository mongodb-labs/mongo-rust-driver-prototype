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
//01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567

use bson::encode::*;
use bson::formattable::*;

use util::*;
use util::special::*;
use msg::*;
use client::Client;
use cursor::Cursor;

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
     * Returns OP_REPLY on successful query, None on no last error,
     * MongoErr otherwise.
     */
    // XXX right now, public---try to move refresh in here so that can be private?
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
    pub fn insert<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let bson_doc = ~[match doc.to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                    ~"coll::insert",
                                    ~"some BsonDocument/Document error",
                                    ~"no idea")),
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
    /*pub fn save<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
    }*/

    /// UPDATE OPS
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
                cursor.limit(1);
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
    pub fn remove<U : BsonFormattable>(&self, query : Option<U>, flag_array : Option<~[DELETE_FLAG]>, option_array : Option<~[DELETE_OPTION]>, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let q = match query {
            None => BsonDocument::new(),
            Some(doc) => match doc.to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                    ~"coll::remove",
                                    ~"some BsonDocument/Document error",
                                    ~"no idea")),
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
    // XXX
    priv fn process_index_opts(&self, flags : i32, options : Option<~[INDEX_OPTION]>) -> (bool, ~[~str]) {
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
        let mut has_name = false;
        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    //opts_str += match opt {
                    opts_str = opts_str + match opt {
                        INDEX_NAME(n) => {
                            has_name = true;
                            [fmt!("\"name\":%s", n)]
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

        (has_name, opts_str)
    }
    priv fn process_index_fields(&self, index_arr : ~[INDEX_FIELD], get_name : bool) -> (~str, ~[~str]) {
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
    pub fn create_index(&self, index_arr : ~[INDEX_FIELD], flag_array : Option<~[INDEX_FLAG]>, option_array : Option<~[INDEX_OPTION]>) -> Result<(), MongoErr> {
        let coll = @Collection::new(copy self.db, fmt!("%s", SYSTEM_INDEX), self.client);

        let flags = process_flags!(flag_array);
        let (x, y) = self.process_index_opts(flags, option_array);
        let has_name = x; let mut opts = y;
        let (default_name, index) = self.process_index_fields(index_arr, !has_name);
        if !has_name { opts = opts + [fmt!("\"name\":\"%s\"", default_name)]; }

        let index_str = fmt!("{ \"key\":{ %s }, \"ns\":\"%s.%s\", %s }",
                            index.connect(", "),
                            copy self.db,
                            copy self.name,
                            opts.connect(", "));
        coll.insert(index_str, None)
    }

    // TODO index cache
    pub fn ensure_index(&self, index_arr : ~[INDEX_FIELD], flag_array : Option<~[INDEX_FLAG]>, option_array : Option<~[INDEX_OPTION]>) -> Result<(), MongoErr> {
        self.create_index(index_arr, flag_array, option_array)
    }
}

// Start building tests
#[cfg(test)]
mod tests {
    use bson::encode::*;
    use bson::decode::*;
    use bson::formattable::*;
    use bson::json_parse::*;

    use util::*;
    use client::Client;
    use db::DB;
    use super::*;

    // insert good document with several fields
    /*#[test]
    fn test_good_insert() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"good_insert_one", client);

        // clear out collection to start from scratch
        coll.remove::<~str>(None, None, None, None);

        // create and insert document
        let ins = ~"{ \"_id\":0, \"a\":0, \"msg\":\"first insert!\" }";
        let ins_doc = BsonDocument::from_formattable(copy ins);
        coll.insert::<~str>(ins, None);

        // try to extract it and compare
        match coll.find_one(None, None, None, None) {
            Ok(ret_doc) => assert!(ret_doc == ins_doc),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    }*/

    // insert small batch of good documents with several fields
    /*#[test]
    fn test_good_insert_batch_small() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"good_insert_batch_small", client);

        // clear out collection to start from scratch
        coll.remove::<~str>(None, None, None, None);

        // create and insert batch
        let mut ins_strs = ~[];
        let mut ins_docs = ~[];
        let mut i = 0;
        let n = 5;
        for n.times {
            let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", i, i/2, i);
            let ins_doc = BsonDocument::from_formattable(copy ins_str);
            ins_strs += [ins_str];
            ins_docs += [ins_doc];
            i += 1;
        }
        coll.insert_batch(ins_strs, None, None, None);

        // try to find all of them and compare all of them
        match coll.find(None, None, None) {
            Ok(c) => {
                let mut cursor = c;
                let mut j = 0;
                for cursor.advance |ret_doc| {
                    if j >= n { fail!("more docs returned than inserted"); }
                    assert!(ret_doc == ins_docs[j]);
                    j += 1;
                }
                if j < n { fail!("fewer docs returned than inserted"); }
            }
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    }*/

    // insert big batch of good documents with several fields
    // XXX succeeds with <= 101, fails with >= 102
    #[test]
    fn test_good_insert_batch_big() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

        // clear out collection to start from scratch
        coll.remove::<~str>(None, None, None, None);

        // create and insert batch
        let mut ins_strs : ~[~str] = ~[];
        let mut ins_docs : ~[BsonDocument] = ~[];
        let mut i = 0;
        let n = 105;
        for n.times {
            let ins_str = fmt!("{ \"a\":%d, \"b\":\"ins %d\" }", i/2, i);
            //let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", i, i/2, i);
            //let ins_doc = BsonDocument::from_formattable(copy ins_str);
            let ins_doc = match (copy ins_str).to_bson_t() {
                    Embedded(bson) => *bson,
                    _ => fail!("what happened"),
                };
            //ins_strs += [ins_str];
            //ins_docs += [ins_doc];
            //i += 1;
            ins_strs = ins_strs + [ins_str];
            ins_docs = ins_docs + [ins_doc];
            i = i + 1;
        }
        coll.insert_batch(ins_strs, None, None, None);

        // try to find all of them and compare all of them
        match coll.find(None, None, None) {
            Ok(c) => {
                let mut cursor = c;
                //let mut j = 0;
                for cursor.advance |ret_doc| {
                    //if j >= n { fail!("more docs returned than inserted"); }
                    //if ret_doc != ins_docs[j] {
                    println(fmt!("\n%?", ret_doc));
                    //    println(fmt!("\n%?\n%?", ret_doc, ins_docs[j]));
                    //}
                    //assert!(ret_doc == ins_docs[j]);
                    //j += 1;
                }
                match cursor.iter_err {
                    Some(e) => println(fmt!("\n%?", MongoErr::to_str(e))),
                    None => (),
                }
                //if j < n { fail!("fewer docs (%?) returned than inserted (%?)", j, n); }
            }
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    }

    //      bad document with several fields
    //      batch with bad documents with several fields; no cont on err
    /*#[test]
    fn test_bad_insert_batch_no_cont() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"bad_insert_batch_no_cont", client);

        // clear out collection to start from scratch
        coll.remove::<~str>(None, None, None, None);

        // create and insert batch
        let mut ins_strs = ~[];
        let mut ins_docs = ~[];
        let mut i = 1;
        let n = 20;
        for n.times {
            let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i);
            let ins_doc = BsonDocument::from_formattable(copy ins_str);
            ins_strs += [ins_str];
            ins_docs += [ins_doc];
            i += 1;
        }
        coll.insert_batch(ins_strs, None, None, None);

        // try to find all of them and compare all of them
        match coll.find(None, None, None) {
            Ok(c) => {
                let mut cursor = c;
                let mut j = 0;
                for cursor.advance |ret_doc| {
                    if j >= 3 { fail!("more docs returned (%d) than successfully inserted (3)", j+1); }
                    assert!(ret_doc == ins_docs[j]);
                    j += 1;
                }
                if j < 3 { fail!("fewer docs returned (%d) than successfully inserted (3)", j); }
            }
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    } */
    //      batch with bad documents with several fields; cont on err
    /*#[test]
    fn test_bad_insert_batch_cont() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"bad_insert_batch_cont", client);

        // clear out collection to start from scratch
        coll.remove::<~str>(None, None, None, None);

        // create and insert batch
        let mut ins_strs = ~[];
        let mut ins_docs = ~[];
        let mut i = 1;
        let n = 20;
        for n.times {
            let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i);
            let ins_doc = BsonDocument::from_formattable(copy ins_str);
            ins_strs += [ins_str];
            ins_docs += [ins_doc];
            i += 1;
        }
        coll.insert_batch(ins_strs, Some(~[CONT_ON_ERR]), None, None);

        // try to find all of them and compare all of them
        match coll.find(None, None, None) {
            Ok(c) => {
                let mut cursor = c;
                let mut j = 0;
                let valid_inds = [0, 1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19];
                for cursor.advance |ret_doc| {
                    if j >= 14 { fail!("more docs returned (%d) than successfully inserted (14)", j+1); }
                    assert!(ret_doc == ins_docs[valid_inds[j]]);
                    j += 1;
                }
                if j < 14 { fail!("fewer docs returned (%d) than successfully inserted (14)", j); }
            }
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    } */

    // update
    /*#[test]
    fn test_good_update() {
        test_good_insert_batch_big();

        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

        match coll.update(SpecNotation(~"{ \"a\":2 }"), SpecNotation(~"{ \"$set\": { \"a\":3 }}"), Some(~[MULTI]), None, None) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        // TODO missing some...

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    } */

    // indices
    /*#[test]
    fn test_create_index() {
        test_good_insert_batch_big();

        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

        match coll.create_index(~[NORMAL(~[(~"b", ASC)])], None, None) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    } */

    // run_command/dropDatabase
    /*#[test]
    fn test_dropDatabase() {
        let client = @Client::new();
        match client.connect(~"127.0.0.1", 27017 as uint) {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.drop_db(~"rust") {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }

        match client.disconnect() {
            Ok(_) => (),
            Err(e) => fail!("%s", MongoErr::to_str(e)),
        }
    }*/
}
