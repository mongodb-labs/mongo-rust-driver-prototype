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

extern mod extra;
extern mod mongo;
extern mod bson;

use std::run::*;
use std::os::*;
use extra::time::*;

use mongo::client::*;
use mongo::util::*;
use mongo::rs::*;
use mongo::coll::*;

fn main() {
    let ports : ~[uint] = ~[37118, 37119, 37120, 37121, 37122];
    let mut do_init;

    // see if need to initiate, based on cmd-line args
    let args = args();
    let spec = "make_rs=";
    let msg = fmt!(
"[%s] cmd-line args:
    %s[0,1]     whether or not to initiate replica set on ports %?
                    (0 if already initiated, 1 to initiate here)",
                args[0], spec, ports);
    if args.len() <= 1 || !args[1].starts_with(spec) {
        println(msg);
        return;
    } else {
        do_init = args[1].char_at(spec.len()) == '1';
        let no_init = args[1].char_at(spec.len()) == '0';
        if !do_init && !no_init {
            println(msg);
            println(fmt!("\t\texpected 0 or 1, found %?",
                    args[1].char_at(spec.len())));
            return;
        }
    }

    let pause = 5f;
    let mut t;
    let mut procs = ~[];

    if do_init {
        println("starting up mongods");
        // set up servers
        let base_dir_str = "./rs_demo_dir";
        let base_dir = GenericPath::from_str::<PosixPath>(base_dir_str);
        remove_dir_recursive(&base_dir);
        make_dir(&base_dir, 0x1ff);
        for ports.iter().advance |&po| {
            let s = fmt!("%s/%?", base_dir_str, po);
            let p = GenericPath::from_str::<PosixPath>(s.as_slice());
            remove_dir(&p);
            make_dir(&p, 0x1ff);
            procs.push(Process::new("mongod",
                                    [~"--port", fmt!("%?", po),
                                     ~"--dbpath", s,
                                     ~"--replSet", ~"rs_demo",
                                     ~"--smallfiles",
                                     ~"--oplogSize", ~"128"],
                                    ProcessOptions::new()));
        }

        // give some time to set up
        t = precise_time_s();
        loop { if precise_time_s() - t >= pause { break; } }
    }

    let client = @Client::new();

    // set up hosts
    let host0 = RSMember::new(fmt!("localhost:%?", ports[0]),
                    ~[  TAGS(TagSet::new([  ("host_0", "0"),
                                            ("host_1", "0")]))]);
    let host1 = RSMember::new(fmt!("localhost:%?", ports[1]),
                    ~[  PRIORITY(2f),
                        TAGS(TagSet::new([  ("host_0", "0"),
                                            ("host_1", "1")]))]);
    let host2 = RSMember::new(fmt!("localhost:%?", ports[2]),
                    ~[  PRIORITY(4f),
                        TAGS(TagSet::new([  ("host_0", "1"),
                                            ("host_1", "0")]))]);
    let host3 = RSMember::new(fmt!("localhost:%?", ports[3]),
                    ~[  PRIORITY(8f),
                        TAGS(TagSet::new([  ("host_0", "1"),
                                            ("host_1", "1")]))]);
    let hosts = [host0, host1, host2, host3];
    let conf = RSConfig::new(Some(~"rs_demo"), hosts.to_owned(), None);

    if do_init {
        // initiate configuration
        println("initiating and connecting");
        match client.initiate_rs(("127.0.0.1", ports[0]), conf) {
            Ok(_) => (),
            Err(e) => fail!("%s", e.to_str()),
        };

        // to demonstrate, we disconnect, then reconnect
        //      to the now-initialized replica set
        client.disconnect();
    }

    let seed = [(~"127.0.0.1", ports[0]),
                (~"127.0.0.1", ports[1])];  // just use first two ports as seed
    match client.connect_to_rs(seed) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let rs = RS::new(client);

    // check the status
    print("getting status: ");
    match rs.get_status() {
        Ok(doc) => println(doc.to_str()),
        Err(e) => println(e.to_str()),
    }

    // add an arbiter
    if do_init {
        println("adding arbiter");
        let host4 = RSMember::new(fmt!("localhost:%?", ports[4]),
                        ~[ARB_ONLY(true)]);
        match rs.add(host4) {
            Ok(_) => (),
            Err(e) => println(e.to_str()),
        };
    }

    // check configuration
    print("getting conf: ");
    let mut conf = None;
    match rs.get_config() {
        Ok(c) => {
            println(fmt!("%?", c));
            conf = Some(c);
        }
        Err(e) => println(e.to_str()),
    }

    // drop collection to start afresh, then insert and read some things
    t = precise_time_s();
    println(fmt!("inserting %?", t));
    match client.drop_db("rs_demo_db") {
        Ok(_) => (),
        Err(e) => println(e.to_str()),
    };
    let coll = Collection::new(~"rs_demo_db", ~"foo", client);
    coll.insert(fmt!("{ 'time': '%?' }", t), None);
    println("reading it back");
    match coll.find(None, None, None) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // change up tags
    println("changing tags");
    let t = precise_time_s();
    let tag_val = fmt!("%?", t);
    match conf {
        None => println("could not change tags; could not get conf earlier"),
        Some(c) => {
            let mut tmp = c;
            {
                let tags = tmp.members[0].get_mut_tags();
                tags.set(~"which", tag_val.clone());
            }
            {
                let tags = tmp.members[3].get_mut_tags();
                tags.set(~"which", tag_val.clone());
            }
            rs.reconfig(tmp, false);
        }
    }

    // get config to check
    println("getting conf to check: ");
    match rs.get_config() {
        Ok(c) => println(fmt!("%?", c)),
        Err(e) => println(e.to_str()),
    }

    // change read preference
    println("changing read preference");
    let mut old_pref = None;
    match client.set_read_pref(
            SECONDARY_ONLY(Some(~[TagSet::new([("which", tag_val.as_slice())])]))) {
        Ok(p) => old_pref = Some(p),
        Err(e) => println(e.to_str()),
    }
    let _ = old_pref;

    // read again
    println("reading again (SLAVE_OK, should get previous insert)");
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }
    println("reading again (not SLAVE_OK, should get nothing (no err either))");
    match coll.find(None, None, None) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // tell primary to step down for more than the timeout,
    //      and freeze the other nodes; set becomes read-only
    println("making read-only");
    for hosts.iter().advance |&host| {
        rs.node_freeze(host.host, 2*MONGO_TIMEOUT_SECS);
    }
    rs.step_down(2*MONGO_TIMEOUT_SECS);
    match coll.insert(~"{ 'msg':'I should not be here' }", None) {
        Ok(_) => println("insert succeeded when should not have"),
        Err(e) => println(fmt!("expect timeout err; got %s", e.to_str())),
    }
    println("trying to read (should succeed)");
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // disconnect
    println("disconnecting");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => println(e.to_str()),
    }

    // reconnect using uri
    println("connecting using uri");
    match client.connect_with_uri(fmt!("mongodb://localhost:37118,localhost:37120/?journal=true&readPreference=secondary&readPreferenceTags=which:%?", t)) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    // read
    println("reading again (SLAVE_OK, should get previous insert)");
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }
    println("reading again (not SLAVE_OK, should get nothing (no err either))");
    match coll.find(None, None, None) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // disconnecting
    println("disconnecting");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => println(e.to_str()),
    }

    // destroy mongods if started
    if do_init {
        for procs.mut_iter().advance |proc| {
            proc.destroy();
        }
    }
}
