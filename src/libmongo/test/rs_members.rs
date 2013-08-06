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

// for replica set with seed containing all members (37018-37022)

#[test]
fn test_rs_members() {
    let client = @Client::new();
    client.connect_to_rs([  (~"127.0.0.1", 37018),
                            (~"127.0.0.1", 37019),
                            (~"127.0.0.1", 37020),
                            (~"127.0.0.1", 37021),
                            (~"127.0.0.1", 37022)]);

    let rs = RS::new(client);
    let mut conf = None;

    // get configuration to get host list (which *should* be same as seed list)
    match rs.get_config() {
        Ok(c) => {
            conf = Some(c.clone());
            debug!("%?", c);
        }
        Err(e) => debug!(e.to_str()),
    }

    // remove about half of the hosts;
    // expect EOF and possibly ("cannot find self in config")
    let hosts = conf.unwrap().members;
    let mut i = 0;
    let n = (hosts.len()+1)/2;
    for hosts.iter().advance |&member| {
        match rs.remove(member.host) {
            Ok(_) => (),
            Err(e) => debug!(e.to_str()),
        }
        i += 1;
        if i >= n { break; }
    }

    // get config to check if all but one removed
    match rs.get_config() {
        Ok(c) => {
            println(fmt!("%?", c));
            if c.members.len() != hosts.len()-n
                    && c.members.len() != hosts.len()-n+1 {
                    // (in case attempted remove of self)
                fail!("expected %? members in conf, found %?",
                                    hosts.len()-n,
                                    c.members.len());
            }
        }
        Err(e) => debug!(e.to_str()),
    }

    // add all back; expect one error ("duplicate hosts in config")
    for hosts.iter().advance |&member| {
        match rs.add(member) {
            Ok(_) => (),
            Err(e) => debug!(e.to_str()),
        }
    }

    // get config again to check if all there again
    match rs.get_config() {
        Ok(c) => {
            println(fmt!("%?", c));
            if c.members.len() != hosts.len() {
                debug!("expected %? members in conf, found %?",
                                    hosts.len(),
                                    c.members.len());
            }
        }
        Err(e) => debug!(e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
