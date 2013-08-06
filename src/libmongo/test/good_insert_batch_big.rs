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
fn test_good_insert_batch_big() {
    // good batch_insert, big
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let n = 105;
    let (coll, _, _) = fill_coll(~"rust", ~"good_insert_batch_big", client, n);

    // try to find all of them and compare all of them
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            for cursor.advance |_| {
                if j >= n { fail!("more docs returned than inserted"); }
                j += 1;
            }
            match cursor.iter_err {
                Some(e) => fail!("\n%?", e.to_str()),
                None => (),
            }
            if j < n { fail!("fewer docs (%?) returned than inserted (%?)", j, n); }
        }
        Err(e) => fail!("%s", e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
