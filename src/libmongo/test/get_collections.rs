use mongo::client::*;
use mongo::db::*;
use mongo::coll::*;
use mongo::util::*;

use bson::formattable::*;
use bson::encode::*;
#[test]
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
