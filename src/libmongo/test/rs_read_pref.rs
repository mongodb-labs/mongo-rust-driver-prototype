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
use mongo::coll::*;
use mongo::util::*;
use mongo::db::*;

use bson::encode::*;

#[test]
fn test_rs_read_pref() {
    // replica set connection
    let client = @Client::new();
    match client.connect_to_rs([(~"127.0.0.1", 27018)]) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let coll = Collection::new(~"rust", ~"read_pref", client);

    // profile primary
    match DB::new(~"rust", client).set_profiling_level(2) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    }

    // create and insert document
    let t = precise_time_s();
    debug!("%?", t);
    let ins = fmt!("{ 'a':0, 'ins':%? }", t);
    coll.insert(ins, None);

    // change read preference
    client.set_read_pref(SECONDARY_ONLY(None));
    //client.set_read_pref(
    //        PRIMARY_PREF(Some(~[TagSet::new(~[(~"use", ~"reporting")])])));
    // ---another sample change to read preference

    // try to extract it and compare
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => {
            let mut val = None;
            for cur.advance |doc| {
                debug!("%?", doc.to_str());
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
            if x as f32 != t as f32 {   // possible round-off error
                fail!("expected %?, found %?", t, x);
            }
        }
        Err(e) => debug!(e.to_str()),
    }

    client.set_read_pref(PRIMARY_ONLY);

    let prof = Collection::new(~"rust", ~"system.profile", client);
    match prof.find(    Some(SpecNotation(~"{   'op':'query',
                                                'ns':'rust.read_pref' }")),
                        None, None) {
        Ok(ref mut cur) => {
            for cur.advance |_| {
                fail!("found a query from the primary on the read_pref
                        collection, which should only be queried
                        via secondaries");
            }
        }
        Err(e) => debug!(e.to_str()),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
