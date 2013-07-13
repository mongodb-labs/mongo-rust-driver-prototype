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

use bson::encode::*;

use fill_coll::*;

#[test]
fn test_update() {
    // update
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let n = 105;
    let (coll, _, _) = fill_coll(~"rust", ~"test_update", client, n);

    match coll.update(SpecNotation(~"{ \"a\":2 }"), SpecNotation(~"{ \"$set\": { \"a\":3 }}"), Some(~[MULTI]), None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let mut tot = 0;
    let mut a3 = 0;
    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", e.to_str()),
    };

    for cur.advance |doc| {
        tot += 1;
        match doc.find(~"a") {
            Some(doc) => {
                let tmp_doc = copy *doc;
                match tmp_doc {
                    Double(val) => {
                        match val {
                            2f64 => fail!("a not updated correctly, still found 2"),
                            3f64 => a3 += 1,
                            _ => (),
                        }
                    }
                    _ => fail!("found %? as value for a", copy *doc),
                }
            }
            None => fail!("no field a found"),
        }
    }

    assert!(tot == n);
    assert!(a3 == 4);

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
