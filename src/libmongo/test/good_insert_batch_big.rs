use mongo::client::*;
use mongo::db::*;
use mongo::coll::*;
use mongo::util::*;

use bson::formattable::*;
use bson::encode::*;
#[test]
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
        ins_strs = ins_strs + ~[ins_str];
        ins_docs = ins_docs + ~[ins_doc];
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
