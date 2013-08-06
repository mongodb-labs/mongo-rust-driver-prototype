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
    println(fmt!("conf: %?", conf));
    // cycle priorities
    {
        let n = conf_cpy.members.len();
        let tmp = 1f;

        let mut prev;
        {
            prev = match conf_cpy.members[n-1].get_priority() {
                None => tmp,
                Some(p) => *p,
            };
        }

        {
            for conf_cpy.members.mut_iter().advance |member| {
                let tmp_val;
                {
                    tmp_val = match member.get_priority() {
                        None => tmp,
                        Some(p) => *p,
                    };
                }
                {
                    let tmp = member.get_mut_priority();
                    *tmp = prev;
                }
                prev = tmp_val;
            }
        }
    }
    rs.reconfig(conf_cpy, false);   // should err, but presently hard to check
    let mut i = 0;
    loop {
        match rs.get_config() {
            Ok(c) => {
                println(fmt!("conf: %?", c));  // check here instead
                break;
            }
            Err(e) => println(e.to_str()),
        }
        if i >= 20 {
            println(fmt!("looped %?; exiting", i));
            break;
        }

        i += 1;
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%?", e.to_str()),
    }
}
