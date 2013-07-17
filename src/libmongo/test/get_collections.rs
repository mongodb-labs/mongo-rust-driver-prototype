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

use mongo::client::*;
use mongo::db::*;
use mongo::util::*;

use fill_coll::*;

#[test]
fn test_get_collections() {
    // get collections
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let db_str = ~"rust_get_colls";
    let n = 15;
    let colls = [~"coll0", ~"coll1", ~"coll2"];
    for colls.iter().advance |&name| {
        fill_coll(db_str.clone(), name, client, n);
    }

    let db = DB::new(db_str, client);
    match db.get_collection_names() {
        Ok(names) => {
            let mut i = 0;
            for names.iter().advance |&n| {
                if i == 0 { assert!(n == ~"system.indexes"); }
                else { assert!(n == colls[i-1]); }
                i += 1;
            }
            assert!(i-1 == colls.len());
        },
        Err(e) => println(fmt!("%s", e.to_str())),
    };

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
