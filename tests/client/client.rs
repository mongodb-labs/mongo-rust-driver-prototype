use bson::Bson;
use mongodb::client::MongoClient;

#[test]
fn list_databases() {
    let client = MongoClient::with_uri("mongodb://localhost:27018").unwrap();
    let mut cursor = client.list_databases().ok().expect("Failed to execute list_databases.");
    let results = cursor.next_n(3);
    assert_eq!(1, results.len());
    match results[0].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("local", name),
        _ => panic!("Expected name string!"),
    }
}

#[test]
fn database_names() {
    let client = MongoClient::with_uri("mongodb://localhost:27018").unwrap();
    let mut results = client.database_names().ok().expect("Failed to execute database_names.");
    assert_eq!(1, results.len());
    assert_eq!("local", results[0]);
}

#[test]
fn is_master() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let res = client.is_master().ok().expect("Failed to execute is_master.");
    assert!(res);
}
