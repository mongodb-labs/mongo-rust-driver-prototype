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
use mongo::util::*;
use mongo::index::*;

use fill_coll::*;

#[test]
fn test_indices() {
    // indices
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let n = 105;
    let (coll, _, _) = fill_coll(~"rust", ~"test_indices", client, n);

    match coll.create_index(~[NORMAL(~[(~"b", ASC)])], None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.create_index(~[NORMAL(~[(~"a", ASC)])], None, Some(~[INDEX_NAME(~"fubar")])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.create_index(~[GEOHAYSTACK(~"loc", ~"a", 5)], None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.create_index(~[GEOHAYSTACK(~"loc", ~"b", 5)], None, Some(~[INDEX_NAME(~"geo")])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let mut cursor = match coll.find(None, None, None) {
        Ok(cur) => cur,
        Err(e) => fail!("%s", e.to_str()),
    };

    cursor.hint(MongoIndexName(~"fubar"));
    let explain = cursor.explain().unwrap();
    assert!(explain.contains_key(~"millis"));
    assert!(explain.contains_key(~"cursor"));
    assert!(explain.contains_key(~"nscanned"));
    assert!(explain.contains_key(~"indexOnly"));
    assert!(explain.contains_key(~"nYields"));
    assert!(explain.contains_key(~"nscannedObjects"));

    match coll.drop_index(MongoIndexFields(~[NORMAL(~[(~"b", ASC)])])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.drop_index(MongoIndexName(~"fubar")) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.drop_index(MongoIndexFields(~[GEOHAYSTACK(~"loc", ~"b", 5)])) {
        Ok(_) => fail!("dropped nonexistent index"),
        Err(_) => (),
    }

    match coll.drop_index(MongoIndexFields(~[GEOHAYSTACK(~"loc", ~"a", 5)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match coll.drop_index(MongoIndexName(~"geo")) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
