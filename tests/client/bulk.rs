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
fn bulk_ordered_mix() {
    let models = vec![
        WriteModel::InsertOne { document: doc! {
            "_id" => (1),
            "x" => (11)
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (2),
            "x" => (22)
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (3),
            "x" => (33)
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (4),
            "x" => (44)
        }},
        WriteModel::ReplaceOne {
            filter: doc! { "_id" => (3) },
            replacement: doc! { "x" => (37) },
            upsert: true,
        },
        WriteModel::UpdateMany {
            filter: doc! { "_id" => { "$lt" => (3) } },
            update: doc! { "$inc" => { "x" => (1) } },
            upsert: false,
        },
        WriteModel::DeleteOne { filter: doc! {
            "_id" => (4)
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (5),
            "x" => (55)
        }},
        WriteModel::UpdateOne {
            filter: doc! { "_id" => (6) },
            update: doc! { "$set" =>  { "x" => (62) } },
            upsert: true
        },
        WriteModel::InsertOne { document: doc! {
            "_id" => (101),
            "x" => ("dalmations")
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (102),
            "x" => ("strawberries")
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (103),
            "x" => ("blueberries")
        }},
        WriteModel::InsertOne { document: doc! {
            "_id" => (104),
            "x" => ("bananas")
        }},
        WriteModel::DeleteMany { filter: doc! {
            "_id" => { "$gte" => (103) }
        }},
    ];

    let client = MongoClient::new("localhost", 27017).unwrap();
    let db = client.db("test");
    let coll = db.collection("bulk_ordered_mix");

    coll.drop().unwrap();

    let result = coll.bulk_write(models, true);

    assert_eq!(result.inserted_count, 9);
    assert_eq!(result.inserted_ids.len() as i32, result.inserted_count);
    assert_eq!(result.matched_count, 3);
    assert_eq!(result.modified_count, 3);
    assert_eq!(result.deleted_count, 3);
    assert_eq!(result.upserted_count, 1);
    assert_eq!(result.upserted_ids.len() as i32, result.upserted_count);

    match result.inserted_ids.get(&0).unwrap() {
        &Bson::I32(1) => (),
        &Bson::I64(1) => (),
        id => panic!("Invalid inserted id at index 0: {:?}", id)
    }

    match result.inserted_ids.get(&1).unwrap() {
        &Bson::I32(2) => (),
        &Bson::I64(2) => (),
        id => panic!("Invalid inserted id at index 1: {:?}", id)
    }

    match result.inserted_ids.get(&2).unwrap() {
        &Bson::I32(3) => (),
        &Bson::I64(3) => (),
        id => panic!("Invalid inserted id at index 2: {:?}", id)
    }

    match result.inserted_ids.get(&3).unwrap() {
        &Bson::I32(4) => (),
        &Bson::I64(4) => (),
        id => panic!("Invalid inserted id at index 3: {:?}", id)
    }

    match result.inserted_ids.get(&7).unwrap() {
        &Bson::I32(5) => (),
        &Bson::I64(5) => (),
        id => panic!("Invalid inserted id at index 7: {:?}", id)
    }

    match result.inserted_ids.get(&9).unwrap() {
        &Bson::I32(101) => (),
        &Bson::I64(101) => (),
        id => panic!("Invalid inserted id at index 9: {:?}", id)
    }

    match result.inserted_ids.get(&10).unwrap() {
        &Bson::I32(102) => (),
        &Bson::I64(102) => (),
        id => panic!("Invalid inserted id at index 10: {:?}", id)
    }

    match result.inserted_ids.get(&11).unwrap() {
        &Bson::I32(103) => (),
        &Bson::I64(103) => (),
        id => panic!("Invalid inserted id at index 11: {:?}", id)
    }

    match result.inserted_ids.get(&12).unwrap() {
        &Bson::I32(104) => (),
        &Bson::I64(104) => (),
        id => panic!("Invalid inserted id at index 12: {:?}", id)
    }

    match result.upserted_ids.get(&8).unwrap() {
        &Bson::I32(6) => (),
        &Bson::I64(6) => (),
        id => panic!("Invalid inserted id at index 8: {:?}", id)
    }
}
