use bson;
use mongodb::client::MongoClient;

#[test]
fn database_names() {
    let client = MongoClient::with_uri("mongodb://localhost:27018").unwrap();
    let state_results = client.database_names().ok().expect("Failed to execute database_names.");
    for name in state_results {
        if name != "local" {
            client.drop_database(&name[..]).ok().expect("Failed to drop database from server.");
        }
    }

    let base_results = client.database_names().ok().expect("Failed to execute database_names.");
    assert_eq!(1, base_results.len());
    assert_eq!("local", base_results[0]);

    // Build dbs
    let db1 = client.db("new_db");
    let db2 = client.db("new_db_2");
    db1.collection("test1").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");
    db2.collection("test2").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");

    // Check new dbs
    let results = client.database_names().ok().expect("Failed to execute database_names.");
    assert_eq!(3, results.len());
    assert!(results.contains(&"local".to_owned()));
    assert!(results.contains(&"new_db".to_owned()));
    assert!(results.contains(&"new_db_2".to_owned()));
}


#[test]
fn is_master() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let res = client.is_master().ok().expect("Failed to execute is_master.");
    assert!(res);
}
