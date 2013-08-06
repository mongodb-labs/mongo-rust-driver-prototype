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
use extra::time::*;

use mongo::client::*;
use mongo::rs::*;

// For replica set containing port 37018.

#[test]
fn test_rs_conn_manual() {
    // replica set reconfiguration
    let client = @Client::new();
    match client.connect_to_rs([(~"127.0.0.1", 37018)]) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
    let rs = RS::new(client);

    let conf = match rs.get_config() {
        Ok(c) => c,
        Err(e) => fail!("%s", e.to_str()),
    };

    let mut conf_cpy = conf.clone();
    let t = precise_time_s();
    {
        let tags = conf_cpy.members[2].get_mut_tags();
        tags.set(~"tag_test", fmt!("%?", t));
    }
    // below should err, but presently hard to check if "correct err"
    rs.reconfig(conf_cpy, false);
    match rs.get_config() {
        Ok(c) => {
            // check here instead
            let tags = c.members[2].get_tags();
            assert!(tags.is_some());
            let val = tags.unwrap().get_ref(~"tag_test");
            assert!(val.is_some() && *(val.unwrap()) == fmt!("%?", t));
        }
        Err(e) => println(e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%?", e.to_str()),
    }
}
