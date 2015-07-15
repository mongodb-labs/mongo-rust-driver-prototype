#[test]
fn list_collections() {
    let client = Client::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("list_collections");

    db.drop_database().ok().expect("Failed to drop database");

    // Build collections
    db.collection("test").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");
    db.collection("test2").insert_one(bson::Document::new(), None)
        .ok().expect("Failed to insert placeholder document into collection");

    // Check for namespaces
    let mut cursor = db.list_collections_with_batch_size(None, 1)
        .ok().expect("Failed to execute list_collections command.");;

    let results = cursor.next_n(5);
    assert_eq!(3, results.len());

    match results[0].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("system.indexes", name),
        _ => panic!("Expected BSON string!"),
    }
    match results[1].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("test", name),
        _ => panic!("Expected BSON string!"),
    }
    match results[2].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("test2", name),
        _ => panic!("Expected BSON string!"),
    }
}
