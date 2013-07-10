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

use mongo::db::*;
use mongo::client::*;
use mongo::util::*;

use fill_coll::*;

#[test]
fn test_capped_coll() {
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e))
    }

    let db = DB::new(~"rust_capped_db", client);
    db.drop_collection(~"capped");

    let coll = match db.create_collection(~"capped", None, Some(~[CAPPED(100000), MAX_DOCS(5)])) {
        Ok(c) => c,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    // should fail
    match db.create_collection(~"capped", None, Some(~[CAPPED(100000), MAX_DOCS(5)])) {
        Ok(_) => fail!("duplicate capped collection creation succeeded"),
        Err(_) => (),
    }

    coll.insert(~"{ \"a\":1 }", None);
    let n = 5;
    let (coll, _, ins_docs) = fill_coll(~"rust_capped_db", ~"capped", client, n);
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            cursor.sort(NORMAL(~[(~"$natural", DESC)]));
            for cursor.advance |ret_doc| {
                if j >= n { fail!("more docs returned than inserted"); }
                debug!(fmt!("\n%?", *ret_doc));
                assert!(*ret_doc == ins_docs[n-j-1]);
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
}
