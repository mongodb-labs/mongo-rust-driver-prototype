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

#[test]
fn test_sort() {
    // sort
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    match cur.sort(NORMAL(~[(~"b", DESC)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    for cur.advance |doc| {
        println(fmt!("\n%?", doc));
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
