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

use mongo::db::*;
use mongo::client::*;
use mongo::util::*;

#[test]
fn test_authenticate() {
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    // drop users first; this ensures the user will be present
    let db = DB::new(~"rust_auth", client);
    db.get_collection(~"system.users").remove(None, None, None, None);
    match db.add_user(~"testuser2", ~"testpassword", ~[]) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str())
    };

    match db.authenticate(~"testuser2", ~"testpassword") {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str())
    }
}
