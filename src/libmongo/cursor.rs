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

use std::num::*;

use bson::encode::*;

use util::*;
use msg::*;
use coll::Collection;
use coll::MongoIndex;

///Structure representing a cursor
pub struct Cursor {
    priv id : Option<i64>,                  // id on server (None->not yet queried, 0->closed)
    priv collection : @Collection,          // XXX collection associated with cursor?
    flags : i32,                            // QUERY_FLAGs
    batch_size : i32,                       // size of batch in cursor fetch, may be modified
    query_spec : BsonDocument,              // query, may be modified
    open : bool,                            // is cursor open?
    iter_err : Option<MongoErr>,            // last error from iteration (stored in cursor)
    priv retrieved : i32,                   // number retrieved by cursor already
    priv proj_spec : Option<BsonDocument>,  // projection, does not appear to be resettable
    priv skip : i32,                        // number to skip, specify before first "next"
    priv limit : i32,                       // max to return, specify before first "next"
    priv data : ~[~BsonDocument],           // docs stored in cursor
    priv i : i32,                           // i64? index within data currently held
}

///Iterator implementation, opens access to powerful functions like collect, advance, map, etc.
impl Iterator<~BsonDocument> for Cursor {
    /**
     * Returns pointer to next BsonDocument.
     *
     * Pointers passed to avoid excessive copying. Any errors
     * are stored in Cursor's iter_err field.
     *
     * # Returns
     * `Some(~BsonDocument)` if there are more BsonDocuments,
     * `None` otherwise
     */
    pub fn next(&mut self) -> Option<~BsonDocument> {
        if self.refresh() == 0 {
            return None;
        }
        self.i = self.i + 1;
        Some(copy self.data[self.i-1])
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
                        Embedded(ref map) =>
                            return self.$cb(SpecObj(BsonDocument::from_map(copy map.fields))),
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
     * Initialize cursor with query, projection, collection, flags,
     * and skip and limit, but don't query yet (i.e. constructed
     * cursors are empty).
     *
     * # Arguments
     * * `query` - query associated with this Cursor
     * * `proj` - projection of query associated with this Cursor
     * * `collection` - collection associated with this Cursor
     * * `flags` -  `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *              `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *              `PARTIAL`
     *
     * # Returns
     * Cursor
     */
    //pub fn new(query : BsonDocument, proj : BsonDocument, collection : @Collection, flags : i32, nskip : i32, nlimit : i32) -> Cursor {
    pub fn new(     query : BsonDocument,
                    proj : Option<BsonDocument>,
                    collection : @Collection,
                    flags : i32) -> Cursor {
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
     * Actual function used to refresh Cursor and iterate.
     * Any errors go into iter_eff field of Cursor.
     *
     * # Returns
     * amount left in what's currently held by Cursor
     */
    fn refresh(&mut self) -> i32 {
        // clear out error
        self.iter_err = None;

        // if cursor's never been queried, query and fill data up
        if self.id.is_none() {
            let msg = mk_query(
                            self.collection.client.inc_requestId(),
                            copy self.collection.db,
                            copy self.collection.name,
                            self.flags,
                            self.skip,
                            self.batch_size,
                            copy self.query_spec,
                            copy self.proj_spec);
            match self.collection._send_msg(msg_to_bytes(msg), None, true) {
                Ok(reply) => match reply {
                    Some(r) => match r {
                        // XXX check if need start
                        OpReply { header:_, flags:_, cursor_id:id, start:_, nret:n, docs:d } => {
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
        let msg = mk_get_more(
                            self.collection.client.inc_requestId(),
                            copy self.collection.db,
                            copy self.collection.name,
                            self.batch_size,
                            cur_id);
        match self.collection._send_msg(msg_to_bytes(msg), None, true) {
            Ok(reply) => match reply {
                Some(r) => match r {
                    // TODO check re: start
                    OpReply { header:_, flags:_, cursor_id:id, start:_, nret:n, docs:d } => {
                        // send a kill cursors if needed---TODO batch
                        if id == 0 {
                            let kill_msg = mk_kill_cursor(
                                                self.collection.client.inc_requestId(),
                                                1i32,
                                                ~[cur_id]);
                            match self.collection._send_msg(msg_to_bytes(kill_msg), None, false) {
                                Ok(reply) => match reply {
                                    Some(r) => self.iter_err = Some(MongoErr::new(
                                                ~"cursor::refresh",
                                                ~"unknown error",
                                                fmt!("received unexpected response %? from server",
                                                    r))),
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
    /**
     * Skip specified amount before starting to iterate.
     *
     * # Arguments
     * * `skip` - amount to skip
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * Cursor already iterated over
     */
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

    /**
     * Limit amount to return from Cursor.
     *
     * # Arguments
     * * `limit` - total amount to return
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * Cursor already iterated over
     */
    pub fn limit(&mut self, limit: i32) -> Result<(), MongoErr> {
        if self.id.is_some() {
            return Err(MongoErr::new(
                        ~"cursor::limit",
                        ~"limiting already queried cursor",
                        ~"must specify limit before querying cursor"));
        }

        self.limit = limit;

        // also fix batch_size if needed
        if self.batch_size == 0 || self.batch_size > abs(limit) {
            self.batch_size = limit;
        }
        Ok(())
    }

    /// QUERY MODIFICATIONS
    /**
     * Explain the query.
     * Copies the cursor and runs the query to gather information.
     *
     * # Returns
     * ~BsonDocument explaining query on success, MongoErr on failure
     */
    // TODO convert ~BsonDocument to more user-friendly form
    pub fn explain(&mut self) -> Result<~BsonDocument, MongoErr> {
        let mut query = copy self.query_spec;
        query.append(~"$explain", Double(1f64));
        let mut tmp_cur = Cursor::new(query, copy self.proj_spec, self.collection, self.flags);
        tmp_cur.limit(-1);
        match tmp_cur.next() {
            Some(exp) => Ok(exp),
            None => Err(MongoErr::new(
                            ~"cursor::explain",
                            ~"no explanation",
                            ~"no explanation returned by cursor")),
        }
    }

    /**
     * Hints an index (name or fields+order) to use while querying.
     *
     * # Arguments
     * * `index` -  `MongoIndexName(name)` of index to use (if named),
     *              `MongoIndexFields(~[INDEX_FIELD])` to fully specify
     *                  index from scratch
     */
    pub fn hint(&mut self, index : MongoIndex) {
        self.query_spec.append(~"$hint", UString(index.get_name()));
    }

    /**
     * Sorts results from cursor given fields and their direction.
     *
     * # Arguments
     * * `orderby` - `NORMAL(~[(field, direction)])` where `field`s are
     *                  `~str` and `direction` are `ASC` or `DESC`
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * invalid sorting specification (`orderby`)
     */
    pub fn sort(&mut self, orderby : INDEX_FIELD) -> Result<(), MongoErr> {
        let mut spec = BsonDocument::new();
        match orderby {
            NORMAL(fields) => {
                for fields.iter().advance |&(k,v)| {
                    spec.append(k, Int32(v as i32));
                }
            },
            _ => return Err(MongoErr::new(
                                ~"cursor::sort",
                                ~"invalid orderby specification",
                                ~"only fields and their orders allowed")),
        }
        self.query_spec.append(~"$orderby", Embedded(~spec));
        Ok(())
    }

    /**
     * Adds flags to Cursor.
     *
     * # Arguments
     * * `flags` - array of `QUERY_FLAGS` (specified above), each
     *              of which to add
     */
    pub fn add_flags(&mut self, flags : ~[QUERY_FLAG]) {
        for flags.iter().advance |&f| {
            self.flags |= (f as i32);
        }
    }

    /**
     * Removes flags from Cursor.
     *
     * # Arguments
     * * `flags` - array of `QUERY_FLAGS` (specified above), each
     *              of which to remove
     */
    pub fn remove_flags(&mut self, flags : ~[QUERY_FLAG]) {
        for flags.iter().advance |&f| {
            self.flags &= !(f as i32);
        }
    }

    /**
     * Modify size of next batch to fetch on Cursor refresh.
     *
     * # Arguments
     * * `sz` - size of next batch to fetch on Cursor refresh (`QUERY`
     *          or `GET_MORE`)
     */
    pub fn batch_size(&mut self, sz : i32) {
        self.batch_size = sz;
    }

    /// OTHER USEFUL FUNCTIONS
    /**
     * Returns whether Cursor has a next `~BsonDocument`.
     * Considers the last element of a Cursor to be `None`, hence
     * returns `true` at edge case when Cursor exhausted naturally.
     */
    pub fn has_next(&self) -> bool {
        //!self.data.is_empty()
        // return true even if right at end (normal exhaustion of cursor)
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
