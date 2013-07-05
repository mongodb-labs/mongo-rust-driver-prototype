#[test]
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
