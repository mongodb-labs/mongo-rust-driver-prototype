use bson;
use bson::Bson;
use bson::Document;

use mongodb::client::MongoClient;

#[test]
fn find_and_insert() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("find_and_insert");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert document
    let doc = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    // let mut doc = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    coll.insert_one(doc, None).ok().expect("Failed to insert document");

    // Find document
    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let result = cursor.next().unwrap();

    // Assert expected title of document
    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };

    assert!(cursor.next().is_none());
}

#[test]
fn find_and_insert_one() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("find_and_insert");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert document
    let doc = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    // let mut doc = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    coll.insert_one(doc, None).ok().expect("Failed to insert document");

    // Find single document
    let result = coll.find_one(None, None).ok().expect("Failed to execute find command.");
    assert!(result.is_some());

    // Assert expected title of document
    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };
}

#[test]
fn list_collections() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");

    db.drop_database().ok().expect("Failed to drop database");

    // Build collections
    db.collection("list_collections1").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");
    db.collection("list_collections2").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");

    // Check for namespaces
    let result = db.list_collections().ok().expect("Failed to execute list_collections command.");;
    assert_eq!(3, result.len());

    let namespace = vec![
        "test.list_collections1",
        "test.list_collections2",
        "test.system.indexes",
    ];

    for i in 0..2 {
        assert_eq!(namespace[i], result[i].namespace);
    }
}

#[test]
fn insert_many() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("insert_many");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));

    coll.insert_many(vec![doc1, doc2], false, None).ok().expect("Failed to insert documents.");

    // Find documents
    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let results = cursor.next_n(2);
    assert_eq!(2, results.len());

    // Assert expected title of documents
    let expected_titles = vec!(
        "Jaws",
        "Back to the Future",
        );

    for i in 0..1 {
        let ref expected_title = expected_titles[i];
        match results[i].get("title") {
            Some(&Bson::String(ref title)) => assert_eq!(expected_title, title),
            _ => panic!("Expected Bson::String!"),
        };
    }
}

#[test]
fn delete_one() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("delete_one");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));

    coll.insert_many(vec![doc1.clone(), doc2.clone()], false, None)
        .ok().expect("Failed to insert documents.");

    // Delete document
    coll.delete_one(doc2.clone(), None).ok().expect("Failed to delete document.");
    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let result = cursor.next().unwrap();

    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }

    assert!(cursor.next().is_none());
}

#[test]
fn delete_many() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("delete_many");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc2.clone()], false, None)
        .ok().expect("Failed to insert documents into collection.");

    // Delete document
    coll.delete_many(doc2.clone(), None).ok().expect("Failed to delete documents.");
    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let result = cursor.next().unwrap();

    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }

    assert!(cursor.next().is_none());
}

#[test]
fn replace_one() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("replace_one");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    let doc3 = doc! {
        "title" => Bson::String("12 Angry Men".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // let mut doc3 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));
    // doc3.insert("title".to_owned(), Bson::String("12 Angry Men".to_owned()));

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], false, None)
        .ok().expect("Failed to insert documents into collection.");

    // Replace single document
    coll.replace_one(doc2.clone(), doc3.clone(), false, None).ok().expect("Failed to replace document.");
    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let results = cursor.next_n(3);
    assert_eq!(3, results.len());

    // Assert expected title of documents
    let expected_titles = vec!(
        "Jaws",
        "12 Angry Men",
        "12 Angry Men",
        );

    for i in 0..1 {
        let ref expected_title = expected_titles[i];
        match results[i].get("title") {
            Some(&Bson::String(ref title)) => assert_eq!(expected_title, title),
            _ => panic!("Expected Bson::String!"),
        };
    }
}

#[test]
fn update_one() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("update_one");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    let doc3 = doc! {
        "title" => Bson::String("12 Angry Men".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // let mut doc3 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));
    // doc3.insert("title".to_owned(), Bson::String("12 Angry Men".to_owned()));

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], false, None)
        .ok().expect("Failed to insert documents into collection.");

    // Update single document
    let update = doc! {
        "$set" => nested_doc! {
            "director" => Bson::String("Robert Zemeckis".to_owned())
        }
    };


    // let mut update = bson::Document::new();
    // let mut set = bson::Document::new();
    // set.insert("director".to_owned(), Bson::String("Robert Zemeckis".to_owned()));
    // update.insert("$set".to_owned(), Bson::Document(set));

    coll.update_one(doc2.clone(), update, false, None).ok().expect("Failed to update document.");

    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let results = cursor.next_n(3);
    assert_eq!(3, results.len());

    // Assert director attributes
    assert!(results[0].get("director").is_none());
    assert!(results[2].get("director").is_none());
    match results[1].get("director") {
        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
        _ => panic!("Expected Bson::String!"),
    }
}

#[test]
fn update_many() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("test");
    let coll = db.collection("update_many");

    db.drop_database().ok().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! {
        "title" => Bson::String("Jaws".to_owned())
    };

    let doc2 = doc! {
        "title" => Bson::String("Back to the Future".to_owned())
    };

    let doc3 = doc! {
        "title" => Bson::String("12 Angry Men".to_owned())
    };

    // let mut doc = bson::Document::new();
    // let mut doc2 = bson::Document::new();
    // let mut doc3 = bson::Document::new();
    // doc.insert("title".to_owned(), Bson::String("Jaws".to_owned()));
    // doc2.insert("title".to_owned(), Bson::String("Back to the Future".to_owned()));
    // doc3.insert("title".to_owned(), Bson::String("12 Angry Men".to_owned()));

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone(), doc2.clone()], false, None)
        .ok().expect("Failed to insert documents into collection.");

    // Update single document
    let update = doc! {
        "$set" => nested_doc! {
            "director" => Bson::String("Robert Zemeckis".to_owned())
        }
    };

    // let mut update = bson::Document::new();
    // let mut set = bson::Document::new();
    // set.insert("director".to_owned(), Bson::String("Robert Zemeckis".to_owned()));
    // update.insert("$set".to_owned(), Bson::Document(set));

    coll.update_many(doc2.clone(), update, false, None).ok().expect("Failed to update documents.");

    let mut cursor = coll.find(None, None).ok().expect("Failed to execute find command.");
    let results = cursor.next_n(4);
    assert_eq!(4, results.len());

    // Assert director attributes
    for i in 0..3 {

        // Check title
        match results[i].get("title") {
            Some(&Bson::String(ref title)) => {
                let dir_opt = results[i].get("director");

                // Only doc2 models should have a director field
                if "Back to the Future" == title {
                    match dir_opt {
                        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
                        _ => panic!("Expected Bson::String!"),
                    }
                } else {
                    assert!(dir_opt.is_none());
                }
            },
            _ => panic!("Expected Bson::String!"),
        }
    }
}
