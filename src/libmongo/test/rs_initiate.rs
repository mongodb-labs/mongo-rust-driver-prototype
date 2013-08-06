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
//use std::io::*;

use mongo::client::*;
use mongo::util::*;
use mongo::coll::*;
use mongo::rs::*;

#[test]
fn test_rs_initiate() {
    let ports = ~["37018", "37019", "37020", "37021", "37022"];

    let client = @Client::new();

    // set up hosts
    let host0 = RSMember::new(fmt!("localhost:%s", ports[0]),
                    ~[  TAGS(TagSet::new([  ("host_0", "0"),
                                            ("host_1", "0")]))]);
    let host1 = RSMember::new(fmt!("localhost:%s", ports[1]),
                    ~[  PRIORITY(10f),
                        TAGS(TagSet::new([  ("host_0", "0"),
                                            ("host_1", "1")]))]);
    let host2 = RSMember::new(fmt!("localhost:%s", ports[2]),
                    ~[  PRIORITY(100f),
                        TAGS(TagSet::new([  ("host_0", "1"),
                                            ("host_1", "0")]))]);
    let host3 = RSMember::new(fmt!("localhost:%s", ports[3]),
                    ~[  PRIORITY(2f),
                        TAGS(TagSet::new([  ("host_0", "1"),
                                            ("host_1", "1")]))]);
    let host4 = RSMember::new(fmt!("localhost:%s", ports[4]),
                    ~[ARB_ONLY(true)]);
    let hosts = [host0, host1, host2, host3, host4];
    let conf = RSConfig::new(Some(~"rs1"), hosts.to_owned(), None);

    let mut is_err = ~"";

    // initiate configuration
    match client.initiate_rs(("127.0.0.1", 37020), conf) {
        Ok(_) => (),
        Err(e) => {
            is_err = ~"failed to initiate";
            print(e.to_str());
        }
    };

    // attempt an insert
    let coll = Collection::new(~"test", ~"insert", client);
    // try multiple times to account for initiation/electing primary
    let mut i = 0;
    loop {
        match coll.insert(~"{ 'fst':'first insert' }", None) {
            Ok(_) => break,
            Err(e) => {
                println(e.to_str());
                if i >= 20 {
                    is_err = ~"failed to insert";
                    println(fmt!("looped %d times; exiting", i));
                    break;
                }
            }
        }
        i += 1;
    }
    // attempt a find, to check
    match coll.find(None, None, None) {
        Ok(ref mut c) => for c.advance |next| {
            println(next.to_str());
        },
        Err(e) => {
            is_err = ~"failed to find";
            println(e.to_str());
        }
    }

    /*let reader = stdin();
    let _ = reader.read_line();*/

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => print(fmt!("%s", e.to_str())),
    }

    if is_err.len() > 0 { fail!(is_err); }
}
