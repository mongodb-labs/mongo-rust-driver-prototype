use bson::Document;
use bson::Bson::I64;

use mongodb::client::MongoClient;
use mongodb::client::cursor::Cursor;
use mongodb::client::wire_protocol::flags::OpQueryFlags;

#[test]
fn cursor_features() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("cursor_test");

    db.drop_database().ok().expect("Failed to drop database.");

    let docs : Vec<_> = (0..10).map(|i| {
        doc! {
            "foo" => I64(i)
        }
    }).collect();

    assert!(coll.insert_many(docs, false, None).is_ok());

    let doc = Document::new();
    let flags = OpQueryFlags::no_flags();

    let result = Cursor::query_with_batch_size(&client, "test.cursor_test".to_owned(),
                                               3, flags,
                                               0, 0, doc, None, false);

    let mut cursor = match result {
        Ok(c) => c,
        Err(s) => panic!("{}", s)
    };

    let batch = cursor.next_batch();

    assert_eq!(batch.len(), 3 as usize);

    for i in 0..batch.len() {
        match batch[i].get("foo") {
            Some(&I64(j)) => assert_eq!(i as i64, j),
            _ => panic!("Wrong value returned from Cursor#next_batch")
        };
    }

    let bson = match cursor.next() {
        Some(b) => b,
        None => panic!("Nothing returned from Cursor#next")
    };

    match bson.get("foo") {
        Some(&I64(3)) => (),
        _ => panic!("Wrong value returned from Cursor#next")
    };

    assert!(cursor.has_next());
    let vec = cursor.next_n(20);

    assert_eq!(vec.len(), 6 as usize);
    assert!(!cursor.has_next());

    for i in 0..vec.len() {
        match vec[i].get("foo") {
            Some(&I64(j)) => assert_eq!(4 + i as i64 , j),
            _ => panic!("Wrong value returned from Cursor#next_batch")
        };
    }
}
