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
fn test_skip() {
    // limit
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let n = 20;
    let (coll, _, ins_docs) = fill_coll(~"rust", ~"limit", client, n);

    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    let lim = 10;
    match cur.limit(lim) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match cur.next() {
        None => fail!("could not find any documents"),
        Some(_) => (),
    }

    match cur.limit(lim) {
        Ok(_) => fail!("should not be able to limit after next()"),
        Err(_) => (),
    }

    let mut i = 1;
    for cur.advance |doc| {
        i += 1;
    }
    assert!(i as i32 == lim);

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
