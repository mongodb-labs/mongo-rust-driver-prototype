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
use mongo::db::*;
use mongo::util::*;

#[test]
fn test_get_collections() {
    // get collections
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let db = DB::new(~"rust", client);
    match db.get_collection_names() {
        Ok(names) => {
            println("\n");
            for names.iter().advance |&n| { println(fmt!("%s", n)); }
        },
        Err(e) => println(fmt!("\nERRRRROOOOOOORRRRRRRR%s", MongoErr::to_str(e))),
    };

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
