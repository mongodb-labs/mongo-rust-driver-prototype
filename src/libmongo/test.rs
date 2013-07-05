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

use bson::encode::*;
use bson::formattable::*;

use mongo::util::*;
use mongo::client::*;
use mongo::coll::*;
use mongo::db::*;

fn main() {
    test_good_insert_single();

    test_good_insert_batch_small();

    test_good_insert_batch_big();

    test_bad_insert_no_cont();

    test_bad_insert_cont();

    test_indices();

    test_get_collections();

    test_sort();

    test_drop_db();
}

fn test_good_insert_single() {
    // good single insert
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_one", client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert document
    let ins = ~"{ \"_id\":0, \"a\":0, \"msg\":\"first insert!\" }";
    let ins_doc = match (copy ins).to_bson_t() {
            Embedded(bson) => *bson,
            _ => fail!("what happened"),
        };
    coll.insert::<~str>(ins, None);

    // try to extract it and compare
    match coll.find_one(None, None, None) {
        Ok(ret_doc) => assert!(*ret_doc == ins_doc),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_good_insert_batch_small() {
    // good batch insert
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_small", client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert batch
    let mut ins_strs = ~[];
    let mut ins_docs = ~[];
    let mut i = 0;
    let n = 5;
    for n.times {
        let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", i, i/2, i);
        let ins_doc = match (copy ins_str).to_bson_t() {
                Embedded(bson) => *bson,
                _ => fail!("what happened"),
            };
        ins_strs = ins_strs + [ins_str];
        ins_docs = ins_docs + [ins_doc];
        i += 1;
    }
    coll.insert_batch(ins_strs, None, None, None);

    // try to find all of them and compare all of them
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            for cursor.advance |ret_doc| {
                if j >= n { fail!("more docs returned than inserted"); }
                println(fmt!("\n%?", *ret_doc));
                assert!(*ret_doc == ins_docs[j]);
                j += 1;
            }
            if j < n { fail!("fewer docs returned than inserted"); }
        }
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    println("\nfinding one...\n");
    match coll.find_one(None, None, None) {
        Ok(c) => println(fmt!("boop\n%?", c)),
        Err(e) => println(fmt!("beep\n%?", e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_good_insert_batch_big() {
    // good batch_insert, big
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert batch
    let mut ins_strs : ~[~str] = ~[];
    let mut ins_docs : ~[BsonDocument] = ~[];
    let mut i = 0;
    let n = 105;
    for n.times {
        let ins_str = fmt!("{ \"a\":%d, \"b\":\"ins %d\" }", i/2, i);
        //let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", i, i/2, i);
        let ins_doc = match (copy ins_str).to_bson_t() {
                Embedded(bson) => *bson,
                _ => fail!("what happened"),
            };
        //ins_strs += [ins_str];
        //ins_docs += [ins_doc];
        //i += 1;
        ins_strs = ins_strs + [ins_str];
        ins_docs = ins_docs + [ins_doc];
        i = i + 1;
    }
    coll.insert_batch(ins_strs, None, None, None);

    // try to find all of them and compare all of them
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            //let mut j = 0;
            for cursor.advance |ret_doc| {
                //if j >= n { fail!("more docs returned than inserted"); }
                //if *ret_doc != ins_docs[j] {
                println(fmt!("\n%?", *ret_doc));
                //    println(fmt!("\n%?\n%?", ret_doc, ins_docs[j]));
                //}
                //assert!(*ret_doc == ins_docs[j]);
                //j += 1;
            }
            match cursor.iter_err {
                Some(e) => println(fmt!("\n%?", MongoErr::to_str(e))),
                None => (),
            }
            //if j < n { fail!("fewer docs (%?) returned than inserted (%?)", j, n); }
        }
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_bad_insert_no_cont() {
    // batch with bad documents with several fields; no cont on err
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"bad_insert_batch_no_cont", client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert batch
    let mut ins_strs = ~[];
    let mut ins_docs = ~[];
    let mut i = 1;
    let n = 20;
    for n.times {
        let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i);
        let ins_doc = match (copy ins_str).to_bson_t() {
                Embedded(bson) => *bson,
                _ => fail!("what happened"),
            };
        ins_strs = ins_strs + [ins_str];
        ins_docs = ins_docs + [ins_doc];
        i = i + 1;
    }
    match coll.insert_batch(ins_strs, None, None, None) {
        Ok(_) => (),
        Err(e) => println(fmt!("\nerror:%s", MongoErr::to_str(e))),
    };

    // try to find all of them and compare all of them
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            for cursor.advance |ret_doc| {
                if j >= 3 { fail!("more docs returned (%d) than successfully inserted (3)", j+1); }
                assert!(*ret_doc == ins_docs[j]);
                j = j + 1;
            }
            if j < 3 { fail!("fewer docs returned (%d) than successfully inserted (3)", j); }
        }
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_bad_insert_cont() {
    // batch with bad documents with several fields; cont on err
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"bad_insert_batch_cont", client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert batch
    let mut ins_strs = ~[];
    let mut ins_docs = ~[];
    let mut i = 1;
    let n = 20;
    for n.times {
        let ins_str = fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i);
        let ins_doc = match (copy ins_str).to_bson_t() {
                Embedded(bson) => *bson,
                _ => fail!("what happened"),
            };
        ins_strs = ins_strs + [ins_str];
        ins_docs = ins_docs + [ins_doc];
        i = i + 1;
    }
    coll.insert_batch(ins_strs, Some(~[CONT_ON_ERR]), None, None);

    // try to find all of them and compare all of them
    match coll.find(None, None, None) {
        Ok(c) => {
            let mut cursor = c;
            let mut j = 0;
            let valid_inds = [0, 1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19];
            for cursor.advance |ret_doc| {
                if j >= 14 { fail!("more docs returned (%d) than successfully inserted (14)", j+1); }
                assert!(*ret_doc == ins_docs[valid_inds[j]]);
                j = j + 1;
            }
            if j < 14 { fail!("fewer docs returned (%d) than successfully inserted (14)", j); }
        }
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    coll.remove(Some(SpecNotation(~"{ \"a\":1 }")), None, None, None);

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_update() {
    // update
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

    match coll.update(SpecNotation(~"{ \"a\":2 }"), SpecNotation(~"{ \"$set\": { \"a\":3 }}"), Some(~[MULTI]), None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    // TODO missing some... (actual check)

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_indices() {
    // indices
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

    match coll.create_index(~[NORMAL(~[(~"b", ASC)])], None, None) {
    //match coll.drop_index(MongoIndexFields(~[NORMAL(~[(~"b", ASC)])])) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match coll.create_index(~[NORMAL(~[(~"a", ASC)])], None, Some(~[INDEX_NAME(~"fubar")])) {
    //match coll.drop_index(MongoIndexName(~"fubar")) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let mut cursor = match coll.find(None, None, None) {
        Ok(cur) => cur,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    cursor.hint(MongoIndexName(~"fubar"));
    println(fmt!("%?", cursor.explain()));

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_get_collections() {
    // get collections
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let db = DB::new(~"rust", client);
    match db.get_collection_names() {
        Ok(names) => {
            println("\n");
            for names.iter().advance |&n| { println(fmt!("%s", n)); }
        },
        Err(e) => println(fmt!("\nERRRRROOOOOOORRRRRRRR%s", MongoErr::to_str(e))),
    };

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_sort() {
    // sort
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    let coll = @Collection::new(~"rust", ~"good_insert_batch_big", client);

    let mut cur = match coll.find(None, None, None) {
        Ok(cursor) => cursor,
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    };

    match cur.sort(NORMAL(~[(~"b", DESC)])) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    for cur.advance |doc| {
        println(fmt!("\n%?", doc));
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}

fn test_drop_db() {
    // run_command/dropDatabase
    let client = @Client::new();
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.drop_db(~"rust") {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }

    match client.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", MongoErr::to_str(e)),
    }
}
