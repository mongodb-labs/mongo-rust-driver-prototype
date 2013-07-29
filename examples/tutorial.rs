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

extern mod bson;
extern mod mongo;

use mongo::client::*;
use mongo::util::*;     // access to option flags and specifications, etc.
use mongo::db::*;
use mongo::coll::*;
use mongo::cursor::*;
use mongo::index::*;

fn main() {
    let client = @Client::new();

    // To connect to an unreplicated, unsharded server running on localhost, port 27017 (```MONGO_DEFAULT_PORT```), we use the ```connect``` method:
    match client.connect(~"127.0.0.1", 27017 as uint) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
            // if cannot connect, nothing to do; display error message
    }

    // Now we may create handles to databases and collections on the server. We start with collections to demonstrate CRUD operations.
    // create handles to the collections "foo_coll" and "bar_coll" in the
    //      database "foo_db" (any may already exist; if not, it will be
    //      created on the first insert)
    let foo = Collection::new(~"foo_db", ~"foo_coll", client);
    let bar = Collection::new(~"foo_db", ~"bar_coll", client);
    // Equivalently, we may create collection handles direction from the ```Client```:
    //let foo = client.get_collection(~"foo_db", ~"foo_coll");

    // ##### CRUD Operations
    // We input JSON as strings formatted for JSON and manipulate them (in fact, we can insert anything implementing the ```BsonFormattable``` trait [see BSON section below] as long as its ```to_bson_t``` conversion returns an ```Embedded(~BsonDocument)```, i.e. it is represented as a BSON):
    // insert a document into bar_coll
    let ins = ~"{ \"_id\":0, \"a\":0, \"msg\":\"first insert!\" }";
    bar.insert(ins, None);
        // no write concern specified---use default

    // insert a big batch of documents into foo_coll
    let mut ins_batch : ~[~str] = ~[];
    let n = 200;
    let mut i = 0;
    for n.times {
        ins_batch.push(fmt!("{ \"a\":%d, \"b\":\"ins %d\" }", i/2, i));
        i += 1;
    }
    foo.insert_batch(ins_batch, None, None, None);
        // no write concern specified---use default; no special options

    // read one back (no specific query or query options/flags)
    match foo.find_one(None, None, None) {
        Ok(ret_doc) => println(fmt!("%?\n", *ret_doc)),
        Err(e) => fail!("%s", e.to_str()), // should not happen
    }

    // In general, to specify options, we put the appropriate options into a vector and wrap them in ```Some```; for the default options we use ```None```. For specific options, see ```util.rs```. Nearly every method returns a ```Result```; operations usually return a ```()``` (for writes) or some variant on ```~BsonDocument``` or ```Cursor``` (for reads) if successful, and a ```MongoErr``` if unsuccessful due to improper arguments, network errors, etc.
    // insert a big batch of documents with duplicated _ids
    ins_batch = ~[];
    for 5.times {
        ins_batch.push(fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i));
        i += 1;
    }

    // run with only one of the below uncommented
    // ***error returned***
    match foo.insert_batch(ins_batch, None, None, None) {
        Ok(_) => fail!("bad insert succeeded"),          // should not happen
        Err(e) => println(fmt!("%s\n", e.to_str())),
    }
    // ***no error returned since duplicated _ids skipped (CONT_ON_ERR specified)***
    /*match foo.insert_batch(ins_batch, Some(~[CONT_ON_ERR]), None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }*/

    // create an ascending index on the "b" field named "fubar"
    match foo.create_index(~[NORMAL(~[(~"b", ASC)])], None, Some(~[INDEX_NAME(~"fubar")])) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }

    // ##### Cursor and Query-related Operations
    // To specify queries and projections, we must input them either as ```BsonDocument```s or as properly formatted JSON strings using ```SpecObj```s or ```SpecNotation```s. In general, the order of arguments for CRUD operations is (as applicable) query, projection or operation-dependent specification (e.g. update document for ```update```), optional array of option flags, optional array of user-specified options (e.g. *number* to skip), and write concern.
    // interact with a cursor projected on "b" and using indices and options
    match foo.find(None, Some(SpecNotation(~"{ \"b\":1 }")), None) {
        Ok(c) => {
            let mut cursor = c;

            // hint the index "fubar" for the cursor
            cursor.hint(MongoIndexName(~"fubar"));

            // explain the cursor
            println(fmt!("%?\n", cursor.explain().unwrap().fields.to_str()));

            // sort on the cursor on the "a" field, ascending
            cursor.sort(NORMAL(~[(~"a", ASC)]));

            // iterate on the cursor---no query specified so over whole collection
            for cursor.advance |doc| {
                println(fmt!("%?\n", *doc));
            }
        }
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }

    // drop the index by name (if it were not given a name, specifying by
    //      field would have the same effect as providing the default
    //      constructed name)
    match foo.drop_index(MongoIndexName(~"fubar")) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }

    // remove every element where "a" is 1
    match foo.remove(Some(SpecNotation(~"{ \"a\":1 }")), None, None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }

    // upsert every element where "a" is 2 to be 3
    match foo.update(   SpecNotation(~"{ \"a\":2 }"),
                        SpecNotation(~"{ \"$set\": { \"a\":3 } }"),
                        Some(~[MULTI, UPSERT]), None, None) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),     // should not happen
    }

    // ##### Database Operations
    // Now we create a database handle.
    let db = DB::new(~"foo_db", client);

    // list the collections in the database
    match db.get_collection_names() {
        Ok(names) => {
            // should output
            //      system.indexes
            //      bar_coll
            //      foo_coll
            for names.iter().advance |&n| { println(fmt!("%s", n)); }
        }
        Err(e) => println(fmt!("%s\n", e.to_str())), // should not happen
    }

    // perform a run_command, but the result (if successful, a ~BsonDocument)
    //      must be parsed appropriately
    println(fmt!("%?\n", db.run_command(SpecNotation(~"{ \"count\":1 }"))));

    // drop the database
    match client.drop_db(~"foo_db") {
        Ok(_) => (),
        Err(e) => println(fmt!("%s\n", e.to_str())), // should not happen
    }

    // Finally, we should disconnect the client. It can be reconnected to another server after disconnection.
    match client.disconnect() {
        Ok(_) => (),
        Err(e) => println(fmt!("%s\n", e.to_str())), // should not happen
    }
}
