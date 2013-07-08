use mongo::client::*;
use mongo::coll::*;
use mongo::util::*;

use bson::formattable::*;
use bson::encode::*;
#[test]
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
        ins_strs = ins_strs + ~[ins_str];
        ins_docs = ins_docs + ~[ins_doc];
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
