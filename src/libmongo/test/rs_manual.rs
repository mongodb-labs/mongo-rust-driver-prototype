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
use std::io::*;
use extra::time::*;

use mongo::client::*;
use mongo::coll::*;
use mongo::rs::*;

#[test]
fn test_rs_manual() {
    let reader = stdin();

    let client = @Client::new();
    let seed = [(~"127.0.0.1", 27018),
                (~"127.0.0.1", 27019),
                (~"127.0.0.1", 27020),
                (~"127.0.0.1", 27021),
                (~"127.0.0.1", 27022)];
    println(fmt!("connecting"));
    match client.connect_to_rs(seed) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let rs = RS::new(client);
    println(fmt!("%s\n________________",
        match rs.get_status() {
            Ok(s) => s.fields.to_str(),
            Err(e) => e.to_str(),
    }));

    println("waiting for user to bring up/down desired server(s)");
    let _ = reader.read_line();
    println("continuing; insert");

    let t = precise_time_s();
    loop {
        match Collection::new(~"rust", ~"server_die_test", client).insert(
                fmt!("{ 'ins':'%?' }", t), None) {
            Ok(_) => {
                println(fmt!("inserted %? successfully", t));
                break;
            }
            Err(e) => println(e.to_str()),
        }
    }
    match Collection::new(~"rust", ~"server_die_test", client).find(None, None, None) {
        Ok(ref mut c) => for c.advance |doc| { println(fmt!("%?", doc)); },
        Err(e) => println(fmt!("%?", e)),
    }

    println("waiting for user to bring up/down desired server(s)");
    let _ = reader.read_line();
    println("continuing");

    match Collection::new(~"rust", ~"server_die_test", client).find(None, None, None) {
        Ok(ref mut c) => for c.advance |doc| { println(fmt!("%?", doc)); },
        Err(e) => println(fmt!("%?", e)),
    }

    println("disconnecting");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => println(fmt!("%s", e.to_str())),
    }

    println("done");
}
