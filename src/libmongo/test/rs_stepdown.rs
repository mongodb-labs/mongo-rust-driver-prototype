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

use bson::encode::*;

use mongo::client::*;
use mongo::util::*;
use mongo::coll::*;
use mongo::rs::*;

#[test]
fn test_rs_stepdown() {
    let mut i = 0;

    let client = @Client::new();
    let seed = [(~"127.0.0.1", 27018),
                (~"127.0.0.1", 27019),
                (~"127.0.0.1", 27020)];
    debug!("connecting");
    match client.connect_to_rs(seed) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let rs = RS::new(client);
    debug!(fmt!("___status (%?)___", i));
    debug!(fmt!("%s\n________________",
        match rs.get_status() {
            Ok(s) => s.fields.to_str(),
            Err(e) => e.to_str(),
    }));
    i += 1;

    debug!("switching primary");
    let time = MONGO_TIMEOUT_SECS + 10;
    // freeze everything...
    match rs.get_config() {
        Ok(conf) => {
            let mut hosts = ~[];
            for conf.members.iter().advance |&member| {
                hosts.push(member.host);
            }
            for hosts.iter().advance |&host| {
                rs.node_freeze(host, time as uint);
            }
        }
        Err(e) => debug!(e.to_str()),
    }
    // ... and step down
    rs.step_down((time+MONGO_TIMEOUT_SECS) as uint);
    debug!("end switching primary");
    // now everything should be a secondary

    debug!(fmt!("___status (%?)___", i));
    debug!(fmt!("%s\n________________",
        match rs.get_status() {
            Ok(s) => s.fields.to_str(),
            Err(e) => e.to_str(),
    }));
    i += 1;

    client.reconnect();

    debug!(fmt!("___status (%?)___", i));
    debug!(fmt!("%s\n________________",
        match rs.get_status() {
            Ok(s) => s.fields.to_str(),
            Err(e) => e.to_str(),
    }));
    i += 1;

    let t = precise_time_s();
    loop {
        match Collection::new(~"rust", ~"stepdown_test", client).insert(
                fmt!("{ 'ins':%? }", t), None) {
            Ok(_) => break,
            Err(e) => debug!(e.to_str()),
        }

        i += 1;
        if i >= 10 { fail!("insert should have succeeded by now"); }
    }
    match Collection::new(~"rust", ~"stepdown_test", client).find(None, None, None) {
        Ok(ref mut c) => {
            let mut val = None;
            for c.advance |doc| {
                debug!(fmt!("%?", doc.to_str()));
                val = match doc.find(~"ins") {
                    None => None,
                    Some(ptr) => {
                        match ptr {
                            &Double(ref value) => Some(*value),
                            _ => None,
                        }
                    }
                };
            }
            let x = val.unwrap();
            if x as f32 != t as f32 {   // possible round-off err
                fail!("expected %?, found %?", t, x);
            }
        }
        Err(e) => println(fmt!("%?", e)),
    }

    debug!("requesting status until good");
    let mut done = false;
    while !done {
        debug!(fmt!("___status (%?)___", i));
        debug!(fmt!("%s\n________________",
            match rs.get_status() {
                Ok(s) => {done = true; s.fields.to_str()}
                Err(e) => e.to_str(),
        }));
        i += 1;
    }

    debug!("disconnecting");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    println("done");
}
