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

use fill_coll::*;

#[test]
fn test_update() {
    // update
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let n = 105;
    let (coll, _, ins_docs) = fill_coll(~"rust", ~"test_update", client, n);

    match coll.update(SpecNotation(~"{ \"a\":2 }"), SpecNotation(~"{ \"$set\": { \"a\":3 }}"), Some(~[MULTI]), None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    // TODO missing some... (actual check)

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
