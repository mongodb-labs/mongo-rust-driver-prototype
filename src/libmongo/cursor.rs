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

use std::cmp::min;
use extra::deque::Deque;

use bson::decode::*;
use bson::encode::*;
use bson::json_parse::*;

use util::*;
use coll::Collection;
use msg;

//TODO temporary
//pub struct Collection;

///Structure representing a cursor
pub struct Cursor {
    priv id : Option<i64>,                  // id on server (None if cursor not yet queried, 0 if closed)
    priv collection : @Collection,          // collection associated with cursor
    flags : i32,                            // QUERY_FLAGs
    batch_size : i32,                       // size of batch in cursor fetch, may be modified
    query_spec : BsonDocument,              // query, may be modified
    open : bool,                            // is cursor open?
    iter_err : Option<MongoErr>,            // last error from iteration (stored in cursor)
    priv retrieved : i32,                   // number retrieved by cursor already
    priv proj_spec : Option<BsonDocument>,  // projection, does not appear to be resettable
    priv skip : i32,                        // number for cursor to skip, must be specified before first "next"
    priv limit : i32,                       // max number for cursor to return, must be specified before first "next"
    priv data : ~[BsonDocument],            // docs stored in cursor
    priv i : i32,                           // maybe i64 just in case? index within data currently held
}

///Iterator implementation, opens access to powerful functions like collect, advance, map, etc.
impl Iterator<BsonDocument> for Cursor {
    pub fn next(&mut self) -> Option<BsonDocument> {
        //if self.refresh().unwrap() == 0 || !self.open {
        //if self.collection.refresh(@self) == 0 || !self.open {
        if self.refresh() == 0 {
            return None;
        }
        //Some(self.data.pop_front())
        //self.i += 1;
        self.i = self.i + 1;
        Some(copy self.data[self.i-1])  // TODO move out of vector rather than copy out of vector
    }
}
macro_rules! query_add (
   ($obj:ident, $field:expr, $cb:ident) => {
        match $obj {
            SpecObj(doc) => {
                let mut t = BsonDocument::new();
                t.put($field, Embedded(~doc));
                self.add_query_spec(&t);
                Ok(~"added to query spec")
            }
            SpecNotation(ref s) => {
                let obj = ObjParser::from_string::<Document, ExtendedJsonParser<~[char]>>(copy *s);
                if obj.is_ok() {
                    match obj.unwrap() {
                        Embedded(ref map) => return self.$cb(SpecObj(BsonDocument::from_map(copy map.fields))),
                        _ => fail!()
                    }
                } else {
                    return Err(MongoErr::new(
                                ~"cursor::query_add!",
                                ~"query-adding macro expansion",
                                ~"could not parse json object"));
                }
            }
        }
   }
)

///Cursor API
impl Cursor {
    /**
     * Initialize cursor with query, projection, collection, flags, and skip and limit,
     * but don't query yet (i.e. constructed cursors are empty).
     */
    //pub fn new(query : BsonDocument, proj : BsonDocument, collection : @Collection, flags : i32, nskip : i32, nlimit : i32) -> Cursor {
    pub fn new(query : BsonDocument, proj : Option<BsonDocument>, collection : @Collection, flags : i32) -> Cursor {
        Cursor {
            id: None,
            collection: collection,
            flags: flags,
            batch_size: 0,
            query_spec: query,
            open: true,
            iter_err: None,
            retrieved: 0,
            proj_spec: proj,
            skip: 0,
            limit: 0,
            data: ~[],
            i: 0,
        }
    }

    /**
     * Actual function used to refresh cursor and iterate.
     */
    fn refresh(&mut self) -> i32 {
        // clear out error
        self.iter_err = None;

        // if cursor's never been queried, query and fill data up
        if self.id.is_none() {
            let msg = msg::mk_query(
                            self.collection.client.inc_requestId(),
                            copy self.collection.db,
                            copy self.collection.name,
                            self.flags,
                            self.skip,
                            self.batch_size,
                            copy self.query_spec,
                            copy self.proj_spec);
println(fmt!("\nquery:%?", msg));
            match self.collection._send_msg(msg::msg_to_bytes(msg), None, true) {
                Ok(reply) => match reply {
                    Some(r) => match r {
                        // XXX check if need start
                        msg::OpReply { header:_, flags:_, cursor_id:id, start:_, nret:n, docs:d } => {
println(fmt!("\n%?", copy d));
                            self.id = Some(id);
                            self.retrieved = n;
                            self.data = d;
                            self.i = 0;

                            return n;
                        }
                    },
                    None => {
                        self.iter_err = Some(MongoErr::new(
                                        ~"cursor::refresh",
                                        ~"no reply",
                                        ~"received no reply from initial query"));
                        return 0;
                    }
                },
                Err(e) => {
                    self.iter_err = Some(MongoErr::new(
                                        ~"cursor::refresh",
                                        ~"sending query",
                                        fmt!("-->\n%s", MongoErr::to_str(e))));
                    return 0;
                }
            }

        }

        // otherwise, queried before; see if need to get_more
        if self.has_next() {
            // has_next within cursor, so don't get_more
            return (self.data.len() as i32) - self.i;
        }

        // otherwise, no more within cursor, so see if can get_more
        let cur_id = self.id.unwrap();

        if cur_id == 0 {
            // exhausted cursor; return
            self.iter_err = Some(MongoErr::new(
                                    ~"cursor::refresh",
                                    ~"querying on closed cursor",
                                    ~"cannot query on closed cursor"));
            return 0;
        }

        // otherwise, check if allowed to get more
        if self.retrieved >= self.limit {
            self.iter_err = Some(MongoErr::new(
                                    ~"cursor::refresh",
                                    fmt!("cursor limit %? reached", self.limit),
                                    ~"cannot retrieve beyond limit"));
            return 0;
        }

        // otherwise, get_more
        let msg = msg::mk_get_more(
                            self.collection.client.inc_requestId(),
                            copy self.collection.db,
                            copy self.collection.name,
                            self.batch_size,
                            cur_id);
        match self.collection._send_msg(msg::msg_to_bytes(msg), None, true) {
            Ok(reply) => match reply {
                Some(r) => match r {
                    // TODO check how start used
                    msg::OpReply { header:_, flags:_, cursor_id:id, start:_, nret:n, docs:d } => {
                        // send a kill cursors if needed---TODO batch
                        if id == 0 {
                            let kill_msg = msg::mk_kill_cursor(
                                                self.collection.client.inc_requestId(),
                                                1i32,
                                                ~[cur_id]);
                            match self.collection._send_msg(msg::msg_to_bytes(kill_msg), None, false) {
                                Ok(reply) => match reply {
                                    Some(r) => self.iter_err = Some(MongoErr::new(
                                                ~"cursor::refresh",
                                                ~"unknown error",
                                                fmt!("received unexpected response %? from server", r))),
                                    None => (),
                                },
                                Err(e) => self.iter_err = Some(e),
                            }
                        }

                        // also update this cursor's fields
                        self.id = Some(id);
                        self.retrieved = self.retrieved + n;
                        self.data = d;
                        self.i = 0;

                        return n;
                    }
                },
                None => self.iter_err = Some(MongoErr::new(
                                ~"cursor::refresh",
                                ~"cursor could not refresh",
                                ~"no get_more received from server")),
            },
            Err(e) => self.iter_err = Some(e),
        }

        return 0;
    }

    /// CURSOR OPTIONS (must be specified pre-querying)
    pub fn skip(&mut self, skip: i32) -> Result<(), MongoErr> {
        if self.id.is_some() {
            return Err(MongoErr::new(
                        ~"cursor::skip",
                        ~"skipping in already queried cursor",
                        ~"must specify skip before querying cursor"));
        }

        self.skip = skip;
        Ok(())
    }

    pub fn limit(&mut self, limit: i32) -> Result<(), MongoErr> {
        if self.id.is_some() {
            return Err(MongoErr::new(
                        ~"cursor::limit",
                        ~"limiting already queried cursor",
                        ~"must specify limit before querying cursor"));
        }

        self.limit = limit;
        self.batch_size = limit;
        Ok(())
    }

    /// QUERY MODIFICATIONS
    pub fn explain(&mut self) {
        let mut doc = BsonDocument::new();
        doc.put(~"$explain", Bool(true));
        self.add_query_spec(&doc);
        self.next();
    }

    // TODO make take proper index arguments
    pub fn hint(&mut self, index: QuerySpec) -> Result<(), MongoErr> {
        let result : Result<~str, ~str> = query_add!(index, ~"$hint", hint);
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(MongoErr::new(
                            ~"cursor::hint",
                            ~"error adding index hint",
                            e))
        }
    }

    pub fn sort(&mut self, orderby: QuerySpec) -> Result<(), MongoErr> {
        let result : Result<~str, ~str> = query_add!(orderby, ~"$orderby", sort);
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(MongoErr::new(
                            ~"cursor::hint",
                            ~"error adding sort option",
                            e))
        }
    }

    pub fn add_flag(&mut self, flag : QUERY_FLAG) {
        self.flags |= (flag as i32);
    }

    pub fn remove_flag(&mut self, flag : QUERY_FLAG) {
        self.flags &= !(flag as i32);
    }

    /// OTHER USEFUL FUNCTIONS
    pub fn has_next(&self) -> bool {
        //!self.data.is_empty()
        // return true even if right at end, due to how i works
        self.i <= self.data.len() as i32
    }
    pub fn close(&mut self) {
        //self.collection.db.connection.close_cursor(self.id);
        self.open = false
    }
    fn add_query_spec(&mut self, doc: &BsonDocument) {
        for doc.fields.iter().advance |&(@k, @v)| {
            self.query_spec.put(k,v);
        }
    }
}

#[cfg(test)]
mod tests {
    extern mod bson;
    extern mod extra;

    use super::*;
    use bson::encode::*;
    use util::*;
    //use coll::*;

/*    #[test]
    fn test_add_index_obj() {
        let mut doc = BsonDocument::new();
        doc.put(~"foo", Double(1f64));
        let mut cursor = Cursor::new(BsonDocument::new(), None, 0i64, 0i32, 10i32, ~[]);
        cursor.hint(SpecObj(doc));

        let mut spec = BsonDocument::new();
        let mut speci = BsonDocument::new();
        speci.put(~"foo", Double(1f64));
        spec.put(~"$hint", Embedded(~speci));

        assert_eq!(cursor.query_spec, spec);
    }
    #[test]
    fn test_add_index_str() {
        let hint = ~"{\"foo\": 1}";
        let mut cursor = Cursor::new(BsonDocument::new(), None, 0i64, 0i32, 10i32, ~[]);
        cursor.hint(SpecNotation(hint));

        let mut spec = BsonDocument::new();
        let mut speci = BsonDocument::new();
        speci.put(~"foo", Double(1f64));
        spec.put(~"$hint", Embedded(~speci));

        assert_eq!(cursor.query_spec, spec);
    }    */
}
