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

extern mod mongo;
extern mod bson;

use std::rand::*;
use std::rand::RngUtil;

use people::*;

use mongo::client::*;
use mongo::util::*;
use mongo::coll::*;
use mongo::db::*;
use mongo::index::*;

use bson::encode::*;
use bson::formattable::*;

mod people;

fn main() {
    println("connect");
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    }
//
    println("printing dbs");
    let mut dbs = match client.get_dbs() {
        Ok(arr) => arr,
        Err(e) => fail!(e.to_str()),
    };
    for dbs.iter().advance |&db| { println(db); }
    println("");
//
    println("inserting batch of Person structs");
    let mut coll = Collection::new(~"rust_demo", ~"people", client);
    //let mut coll = client.get_collection(~"rust_demo", ~"people");    // equivalent

    let mob = Person::make_mob(500);
    match coll.insert_batch(mob, None, None, None) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    }
//
    println("finding_one Person struct from collection");
    let doc = match coll.find_one(None, None, None) {
        Ok(bson) => Embedded(bson),
        Err(e) => fail!(e.to_str()),
    };
    let fst = match BsonFormattable::from_bson_t::<Person>(&doc) {
        Ok(p) => p,
        Err(e) => fail!(e.to_str()),
    };
    println(fmt!("%?\n", fst));
//
    println("reinserting found_one Person struct");
    match coll.insert(fst, None) {
        Ok(_) => fail!("duplicate insertion succeeded"),
        Err(e) => println(fmt!("[correctly] failed with error %s\n", e.to_str())),
    }
//
    println("ensuring indices");
    coll.ensure_index(~[NORMAL(~[(~"val", DESC), (~"id_str", ASC)])], None, None);
    coll.ensure_index(~[GEOSPATIAL(~"addr", FLAT)], None, Some(~[INDEX_NAME(~"loc")]));
    let inds = match coll.get_indexes() {
        Ok(i) => i,
        Err(e) => fail!(e.to_str()),
    };
    for inds.iter().advance |&i| { println(fmt!("%?\n", i)); }
    println("");
//
    println("creating, sorting on, and explaining cursor");
    let mut cursor = match coll.find(   Some(SpecNotation(~"{ \"val\": { \"$gt\":10 } }")),
                                        Some(SpecNotation(~"{ \"_id\":0 }")), None) {
        Ok(c) => c,
        Err(e) => fail!(e.to_str()),
    };
    cursor.sort(NORMAL(~[(~"val", DESC), (~"id_str", ASC)]));
    println(fmt!("%?\n", cursor.explain().unwrap().fields.to_str()));
//
    println("printing query results from cursor");
    for cursor.advance |p| {
        println(fmt!("%?\n", BsonFormattable::from_bson_t::<Person>(&Embedded(p))));
    }
    println("");
//
    println("creating capped collection");
    let db = DB::new(~"rust_demo", client);
    //let db = client.get_db(~"rust_demo");                             // equivalent
    match db.create_collection(~"capped", None, Some(~[CAPPED(100000), MAX_DOCS(20)])) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    };
//
    println("printing collection names");
    match db.get_collection_names() {
        Ok(c) => {
            for c.iter().advance |&coll| { println(coll); }
        }
        Err(e) => fail!(e.to_str()),
    }
    println("");
//
    println("spawning task to populate capped collection while main iterates across them");
    let n = 50;
    coll = Collection::new(~"rust_demo", ~"capped", client);
    cursor = match coll.find(None, None, None) {
        Ok(c) => c,
        Err(e) => fail!(e.to_str()),
    };
    cursor.add_flags(~[CUR_TAILABLE, AWAIT_DATA]);
    coll.insert(Person::new(None, -1, ~"first insert", &mut rng()), None);
    do spawn || {
        let batch = Person::make_mob(n);
        let tmp_client = @Client::new();
        tmp_client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT);

        let coll = Collection::new(~"rust_demo", ~"capped", tmp_client);
        for batch.iter().advance |&p| {
            match coll.insert(p.clone(), None) {
                Ok(_) => println(fmt!("    inserted %?", p)),
                Err(e) => println(fmt!("%s", e.to_str())),
            };
        }
        tmp_client.disconnect();
    }
// //
    for (n+1).times {
        let mut p = cursor.next();
        while p.is_none() && !cursor.is_dead() { p = cursor.next(); }
        if cursor.is_dead() { break; }
        println(fmt!("read %?", BsonFormattable::from_bson_t::<Person>(&Embedded(p.unwrap()))));
    }
    println("");
//
    println("drop db and print dbs to confirm");
    client.drop_db(~"rust_demo");

    dbs = match client.get_dbs() {
        Ok(arr) => arr,
        Err(e) => fail!(e.to_str()),
    };
    for dbs.iter().advance |&db| { println(db); }
//
    println("disconnect");
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    }
}
