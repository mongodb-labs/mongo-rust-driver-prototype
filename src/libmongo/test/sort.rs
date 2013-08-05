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
fn test_sort() {
    // sort
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let n = 105;
    let (coll, _, ins_docs) = fill_coll(~"rust", ~"test_sort", client, n);

    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", e.to_str()),
    };

    match cur.sort(NORMAL(~[(~"insert no", DESC)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let mut i = 0;
    for cur.advance |doc| {
        debug!(fmt!("\n%?", doc));
        assert!(*doc == ins_docs[n-i-1]);
        i += 1;
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
