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
use mongo::coll::*;
use mongo::util::*;

use fill_coll::*;

#[test]
fn test_indices() {
    // indices
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let n = 105;
    let (coll, _, _) = fill_coll(~"rust", ~"test_indices", client, n);

    match coll.create_index(~[NORMAL(~[(~"b", ASC)])], None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match coll.create_index(~[NORMAL(~[(~"a", ASC)])], None, Some(~[INDEX_NAME(~"fubar")])) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let mut cursor = match coll.find(None, None, None) {
        Ok(cur) => cur,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    cursor.hint(MongoIndexName(~"fubar"));
    println(fmt!("%?", cursor.explain()));

    match coll.drop_index(MongoIndexFields(~[NORMAL(~[(~"b", ASC)])])) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match coll.drop_index(MongoIndexName(~"fubar")) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
