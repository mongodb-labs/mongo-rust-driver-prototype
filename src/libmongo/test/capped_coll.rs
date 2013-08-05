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
use mongo::coll::*;
use mongo::index::*;

use fill_coll::*;

#[test]
fn test_capped_coll() {
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str())
    }

    let db = DB::new(~"rust", client);
    db.drop_collection(~"capped");

    let n = 100;
    match db.create_collection(~"capped", None, Some(~[CAPPED(100000), MAX_DOCS(n)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    // should fail
    match db.create_collection(~"capped", None, Some(~[CAPPED(100000), MAX_DOCS(n)])) {
        Ok(_) => fail!("duplicate capped collection creation succeeded"),
        Err(_) => (),
    }

    let (coll, _, ins_docs) = fill_coll(~"rust", ~"capped", client, n);

    let n_tmp = 200;
    let (_, ins_strs_tmp, ins_docs_tmp) = fill_coll(~"rust", ~"capped_tmp", client, n_tmp);
    coll.insert(copy ins_strs_tmp[1], None);

    // regular cursor
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            cursor.sort(NORMAL(~[(~"$natural", DESC)]));
            for cursor.advance |ret_doc| {
                if j >= n { fail!("more docs than in capped collection"); }
                if j == 0 { assert!(*ret_doc == ins_docs_tmp[1]); }
                else { assert!(*ret_doc == ins_docs[n-j]); }
                j += 1;
            }
            if j < n { fail!("fewer docs than in capped collection"); }
        }
        Err(e) => fail!("%s", e.to_str()),
    }

    // tailable cursor now
    let cur_maybe = coll.find(None, None, None);
    match cur_maybe {
        Ok(c) => {
            let mut cursor = c;
            cursor.add_flags(~[CUR_TAILABLE, AWAIT_DATA]);
            do spawn {
                let mut j = 0;
                for n_tmp.times {
                    let client_tmp = @Client::new();
                    client_tmp.connect(~"127.0.0.1", MONGO_DEFAULT_PORT);
                    let coll_tmp = Collection::new(~"rust", ~"capped", client_tmp);
                    coll_tmp.insert(copy ins_strs_tmp[j], None);
                    j += 1;
                }
            }

            let mut j = 0;
            for (n+n_tmp).times {
                let mut tmp = cursor.next();
                while tmp.is_none() && !cursor.is_dead() {
                    tmp = cursor.next();
                }

                let doc = tmp.unwrap();
                if j < n-1 { assert!(*doc == ins_docs[j+1]); }
                else if j < n { assert!(*doc == ins_docs_tmp[1]); }
                else { assert!(*doc == ins_docs_tmp[j-n]); }

                if cursor.is_dead() { break; }
                j += 1;
            }
        }
        Err(e) => fail!("%s", e.to_str()),
    }

    // regular cursor again to check collection contains only 5 documents
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            for cursor.advance |ret_doc| {
                if j >= n { fail!("more docs than in capped collection"); }
                assert!(*ret_doc == ins_docs_tmp[j+n]);
                j += 1;
            }
            if j < n { fail!("fewer docs than in capped collection"); }
        }
        Err(e) => fail!("%s", e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
