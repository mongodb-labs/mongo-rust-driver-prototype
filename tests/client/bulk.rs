use bson::{Bson, Document};
use mongodb::client::coll::options::WriteModel;
use mongodb::client::MongoClient;

#[test]
fn bulk_ordered_insert_only() {
    let client = MongoClient::new("localhost", 27017).unwrap();
    let db = client.db("test");
    let coll = db.collection("bulk_ordered_insert_only");

    coll.drop().unwrap();

    let models = (1..5).map(|i| WriteModel::InsertOne { document: doc! {
        "_id" => (i),
        "x" => (i * 11)
    }}).collect();

    coll.bulk_write(models, true);

    let cursor : Vec<_> = coll.find(None, None).unwrap().collect();

    assert_eq!(cursor.len(), 4);

    for (i, result) in cursor.into_iter().enumerate() {
        let doc = result.unwrap();
        let expected_id = i + 1;

        match doc.get("_id") {
            Some(&Bson::I32(j)) => assert_eq!(expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc)
        }

        match doc.get("x") {
            Some(&Bson::I32(j)) => assert_eq!(11 * expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc)
        }
    }
}

#[test]
fn bulk_unordered_insert_only() {
    let client = MongoClient::new("localhost", 27017).unwrap();
    let db = client.db("test");
    let coll = db.collection("bulk_unordered_insert_only");

    coll.drop().unwrap();

    let models = (1..5).map(|i| WriteModel::InsertOne { document: doc! {
        "_id" => (i),
        "x" => (i * 11)
    }}).collect();

    coll.bulk_write(models, false);

    let cursor : Vec<_> = coll.find(None, None).unwrap().collect();

    assert_eq!(cursor.len(), 4);

    for (i, result) in cursor.into_iter().enumerate() {
        let doc = result.unwrap();
        let i = i + 1;

        match doc.get("_id") {
            Some(&Bson::I32(j)) => assert_eq!(i as i32, j),
            _ => panic!("Invalid id: {:?}", doc)
        }

        match doc.get("x") {
            Some(&Bson::I32(j)) => assert_eq!(11 * i as i32, j),
            _ => panic!("Invalid id: {:?}", doc)
        }
    }
}
