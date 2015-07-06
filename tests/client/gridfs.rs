use bson::Bson;

use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::gridfs::{Store, ThreadedStore};

use rand::{thread_rng, Rng};
use std::io::{self, Read, Write};

#[test]
fn put_get() {
    let client = Client::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("grid_put");
    let fs = Store::with_db(db.clone());

    let name = "grid_put_file";
    
    let mut src = [0u8; 12800];
    thread_rng().fill_bytes(&mut src);
    let mut grid_file = match fs.create(name.to_owned()) {
        Ok(file) => file,
        Err(err) => panic!(err),
    };

    let id = grid_file.id();
    
    match grid_file.write(&mut src) {
        Ok(_) => (),
        Err(err) => panic!(err),
    }

    grid_file.close();

    // Check
    let mut cursor = match fs.find(Some(doc!{"filename" => name}), None) {
        Ok(cursor) => cursor,
        Err(err) => panic!(err),
    };

    match cursor.next() {
        Some(Ok(doc)) => {
            match doc.get("length") {
                Some(&Bson::I64(len)) => assert_eq!(len as usize, src.len()),
                _ => panic!("Expected i64 'length'"),
            }
        },
        _ => panic!("Expected to retrieve file from cursor."),
    }

    let coll = db.collection("fs.chunks");
    let mut cursor = match coll.find(Some(doc!{"files_id" => (id.clone())}), None) {
        Ok(cursor) => cursor,
        Err(err) => panic!(err),
    };

    match cursor.next() {
        Some(Ok(doc)) => {
            match doc.get("data") {
                Some(&Bson::Binary(_, ref data)) => {
                    for i in 0..12800 {
                        assert_eq!(src[i], data[i]);
                    }
                },
                _ => panic!("Failed serialization of data."),
            }
        },
        _ => panic!(""),
    }
    
    // Get
    let mut dest = [0u8; 12800];
    let mut read_file = match fs.open(name.to_owned()) {
        Ok(file) => file,
        Err(err) => panic!(err),
    };
    
    match read_file.read(&mut dest) {
        Ok(_) => (),
        Err(err) => panic!(err),
    }

    read_file.close();

    for i in 0..12800 {
        assert_eq!(src[i], dest[i]);
    }
}
