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
    let ports : ~[uint] = ~[37018, 37019, 37020, 37021, 37022];
    let mut do_init;

    // see if need to initiate, based on cmd-line args
    let args = args();
    let opt = "make_rs=";
    if args.len() <= 1 || !args[1].starts_with(opt) {
        println(fmt!("[%s] cmd-line args:\n\t
            %s[0,1]\twhether or not to initiate replica set on ports %?
                (0 if already initiated, 1 to initiate here)",
                args[0], opt, ports));
        return;
    } else {
        do_init = args[1].char_at(opt.len()) == '1';
        let no_init = args[1].char_at(opt.len()) == '0';
        if !do_init && !no_init {
            println(fmt!("[%s] cmd-line args:\n\t
                %s[0,1]\twhether or not to initiate replica set on ports %?
                    (0 if already initiated, 1 to initiate here)",
                    args[0], opt, ports));
            println(fmt!("\t\texpected 0 or 1, found %?",
                    args[1].char_at(opt.len())));
            return;
        }
    }

    let pause = 5f;
    let mut t;

    // set up servers
    let mut procs = ~[];
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
        println("initiate and connect");
        match client.initiate_rs(("127.0.0.1", ports[0]), conf) {
            Ok(_) => (),
            Err(e) => fail!("%s", e.to_str()),
        };
        t = precise_time_s();
        loop { if precise_time_s() - t > pause { break; } }

        // to demonstrate, we disconnect, then reconnect
        //      to the now-initialized replica set
        println("disconnect and reconnect via seed");
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
    println("get status");
    match rs.get_status() {
        Ok(doc) => println(doc.to_str()),
        Err(e) => println(e.to_str()),
    }

    // add an arbiter
    if do_init {
        println("add arbiter");
        let host4 = RSMember::new(fmt!("localhost:%?", ports[4]),
                        ~[ARB_ONLY(true)]);
        match rs.add(host4) {
            Ok(_) => (),
            Err(e) => println(e.to_str()),
        };
        t = precise_time_s();
        loop { if precise_time_s() - t > pause { break; } }
    }

    // check configuration
    println("get conf");
    let mut conf = None;
    match rs.get_config() {
        Ok(c) => {
            println(fmt!("%?", c));
            conf = Some(c);
        }
        Err(e) => println(e.to_str()),
    }

    // drop collection to start afresh, then insert and read some things
    println("some CRUD ops");
    client.drop_db("rs_demo_db");
    let coll = Collection::new(~"rs_demo_db", ~"foo", client);
    coll.insert(fmt!("{ 'time': '%?' }", precise_time_s()), None);
    match coll.find(None, None, None) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }
    t = precise_time_s();
    loop { if precise_time_s() - t > 2f*pause { break; } }

    // change up tags
    println("change tags");
    match conf {
        None => println("could not change tags; could not get conf earlier"),
        Some(c) => {
            let mut tmp = c;
            {
                let tags = tmp.members[0].get_mut_tags();
                tags.set(~"which", ~"this");
            }
            {
                let tags = tmp.members[3].get_mut_tags();
                tags.set(~"which", ~"this");
            }
            rs.reconfig(tmp, false);
        }
    }
    t = precise_time_s();
    loop { if precise_time_s() - t > pause { break; } }

    // get config to check
    match rs.get_config() {
        Ok(c) => println(fmt!("%?", c)),
        Err(e) => println(e.to_str()),
    }

    // change read preference
    println("change read preference");
    let mut old_pref = None;
    match client.set_read_pref(
            SECONDARY_ONLY(Some(~[TagSet::new([("which", "this")])]))) {
        Ok(p) => old_pref = Some(p),
        Err(e) => println(e.to_str()),
    }
    let _ = old_pref;
    t = precise_time_s();
    loop { if precise_time_s() - t > pause { break; } }

    // read again
    println("read again");
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // tell primary to step down for more than the timeout,
    //      and freeze the other nodes; set becomes read-only
    println("make read-only");
    for hosts.iter().advance |&host| {
        rs.node_freeze(host.host, 2*MONGO_TIMEOUT_SECS);
    }
    rs.step_down(2*MONGO_TIMEOUT_SECS);
    match coll.insert(~"{ 'msg':'I should not be here' }", None) {
        Ok(_) => println("insert succeeded when should not have"),
        Err(e) => println(fmt!("expect timeout err; got %s", e.to_str())),
    }
    match coll.find(None, None, Some(~[SLAVE_OK])) {
        Ok(ref mut cur) => for cur.advance |doc| { println(doc.to_str()); },
        Err(e) => println(e.to_str()),
    }

    // disconnect
    println("disconnect");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => println(e.to_str()),
    }

    // close mongods if started them
    if do_init {
        println("kill mongods");
        for procs.mut_iter().advance |proc| {
            proc.destroy();
        }
    }
}
