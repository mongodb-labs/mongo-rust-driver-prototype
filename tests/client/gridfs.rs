use bson::Bson;

use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::FindOptions;
use mongodb::db::ThreadedDatabase;
use mongodb::gridfs::{Store, ThreadedStore};
use mongodb::gridfs::file::DEFAULT_CHUNK_SIZE;

use rand::{thread_rng, Rng};
use std::io::{Read, Write};

#[test]
fn put_get() {
    let client = Client::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("grid_put");
    let fs = Store::with_db(db.clone());

    let files = db.collection("fs.files");
    let chunks = db.collection("fs.chunks");
    files.drop().ok().expect("Failed to drop files collection.");
    chunks.drop().ok().expect("Failed to drop chunks collection.");

    let name = "grid_put_file";

    let src_len = (DEFAULT_CHUNK_SIZE as f32 * 2.5) as usize;
    let mut src = Vec::with_capacity(src_len);
    unsafe { src.set_len(src_len) };
    thread_rng().fill_bytes(&mut src);

    let mut grid_file = match fs.create(name.to_owned()) {
        Ok(file) => file,
        Err(err) => panic!(err),
    };

    let id = grid_file.id.clone();

    match grid_file.write(&mut src) {
        Ok(_) => (),
        Err(err) => panic!(err),
    }

    match grid_file.close() {
        Ok(_) => (),
        Err(err) => panic!(err),
    }

    // Check file
    let mut cursor = match fs.find(Some(doc!{"filename" => name}), None) {
        Ok(cursor) => cursor,
        Err(err) => panic!(err),
    };

    match cursor.next() {
        Some(file) => assert_eq!(file.len() as usize, src_len),
        _ => panic!("Expected to retrieve file from cursor."),
    }

    let fschunks = db.collection("fs.chunks");
    let mut opts = FindOptions::new();
    opts.sort = Some(doc!{ "n" => 1});

    let mut cursor = match fschunks.find(Some(doc!{"files_id" => (id.clone())}), Some(opts)) {
        Ok(cursor) => cursor,
        Err(err) => panic!(err),
    };

    let chunks = cursor.next_batch().ok().expect("Failed to get next batch");
    assert_eq!(3, chunks.len());

    for i in 0..3 {
        if let Some(&Bson::I32(ref n)) = chunks[i].get("n") {
            assert_eq!(i as i32, *n);
        }
        
        if let Some(&Bson::Binary(_, ref data)) = chunks[i].get("data") {
            for j in 0..data.len() {
                assert_eq!(src[j + i*DEFAULT_CHUNK_SIZE as usize], data[j]);
            }
        }
    }

    // Get
    let mut dest = Vec::with_capacity(src_len);
    unsafe { dest.set_len(src_len) };
    let mut read_file = match fs.open(name.to_owned()) {
        Ok(file) => file,
        Err(err) => panic!(err),
    };

    let n = match read_file.read(&mut dest) {
        Ok(n) => n,
        Err(err) => panic!(err),
    };

    assert_eq!(src_len, n);

    match read_file.close() {
        Ok(_) => (),
        Err(err) => panic!(err),
    }

    for i in 0..src_len {
        assert_eq!(src[i], dest[i]);
    }
}
