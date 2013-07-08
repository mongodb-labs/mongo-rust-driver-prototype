use mongo::client::*;
use mongo::coll::*;
use mongo::util::*;

use bson::formattable::*;
use bson::encode::*;
#[test]
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
