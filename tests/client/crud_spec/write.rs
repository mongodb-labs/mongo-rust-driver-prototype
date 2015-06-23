use bson::Bson;
use json::arguments::Arguments;
use json::reader::SuiteContainer;
use json::eq;
use mongodb::client:: MongoClient;
use rustc_serialize::json::Json;

#[test]
fn insert_one() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/insertOne.json",
               "insert_one");
}
