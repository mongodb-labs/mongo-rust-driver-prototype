use mongo::client::*;
use mongo::coll::*;
use mongo::util::*;

#[test]
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
