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
fn test_drop_db() {
    // run_command/dropDatabase
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let db = ~"rust_drop";
    fill_coll(db.clone(), ~"tmp", client, 10);

    let all_dbs = match client.get_dbs() {
        Ok(arr) => arr,
        Err(e) => fail!("%s", e.to_str()),
    };

    match client.drop_db(db.clone()) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    };
    // oddly, should succeed (double drop of db ok)
    match client.drop_db(db.clone()) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    };

    let cur_dbs = match client.get_dbs() {
        Ok(arr) => arr,
        Err(e) => fail!("%s", e.to_str()),
    };

    assert!(cur_dbs.len() == all_dbs.len()-1);

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
