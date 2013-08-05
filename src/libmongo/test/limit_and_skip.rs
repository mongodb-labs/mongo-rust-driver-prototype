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
fn test_limit_and_skip() {
    // limit
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let n = 400;
    let (coll, _, ins_docs) = fill_coll(~"rust", ~"limit_and_skip", client, n);

    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", e.to_str()),
    };

    let skip = 6;
    let lim = 378;
    match cur.cursor_limit(lim) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
    match cur.cursor_skip(skip) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match cur.sort(NORMAL(~[(~"_id", ASC)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match cur.next() {
        None => fail!("could not find any documents"),
        Some(_) => (),
    }

    match cur.cursor_limit(lim) {
        Ok(_) => fail!("should not be able to limit after next()"),
        Err(_) => (),
    }
    match cur.cursor_skip(skip) {
        Ok(_) => fail!("should not be able to skip after next()"),
        Err(_) => (),
    }

    let mut i = 1;
    for cur.advance |doc| {
        assert!(*doc == ins_docs[i+skip]);
        i += 1;
    }
    assert!(i as i32 == lim);

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
