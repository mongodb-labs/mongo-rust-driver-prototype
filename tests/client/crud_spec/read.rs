use bson::Bson;
use json::arguments::Arguments;
use json::reader::SuiteContainer;
use json::eq::{self, NumEq};
use mongodb::client:: MongoClient;
use rustc_serialize::json::Json;

#[test]
fn find() {
    run_suite!("tests/json/data/specs/source/crud/tests/read/find.json",
               "find");
}
