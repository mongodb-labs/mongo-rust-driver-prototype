use bson::Bson;
use json::arguments::Arguments;
use json::reader::SuiteContainer;
use json::eq;
use mongodb::client:: MongoClient;
use rustc_serialize::json::Json;

macro_rules! run_find_test {
    ( $db:expr, $c:expr, $f:expr, $o:expr, $t:expr ) => {
        {
            let mut cursor = $c.find($f, $o).unwrap();

            let array = match $t.result {
                Bson::Array(ref arr) => arr.clone(),
                _ => panic!("Invalid `result` of find test")
            };

            for bson in array {
                assert!(eq::bson_eq(&bson, &Bson::Document(cursor.next().unwrap())));
            }

            assert!(!cursor.has_next());

            let outcome_coll = match $t.collection {
                Some(ref coll) => coll.clone(),
                None => return
            };

            let coll = match outcome_coll.name {
                Some(ref str) => $db.collection(&str),
                None => $db.collection(&$c.name())
            };

            let mut cursor = coll.find(None, None).unwrap();

            for doc in outcome_coll.data.iter() {
                assert!(eq::bson_eq(&Bson::Document(doc.clone()),
                                    &Bson::Document(cursor.next().unwrap())));
            }

            assert!(!cursor.has_next());
         }
    };
}

macro_rules! run_suite {
    ( $f:expr, $c:expr ) => {
        {
            let json = Json::from_file($f).unwrap();
            let suite = json.get_suite().unwrap();
            let client =  MongoClient::new("localhost", 27017).unwrap();
            let db = client.db("test");
            let coll = db.collection($c);
            coll.drop().unwrap();
            coll.insert_many(suite.data, true, None).unwrap();

            for test in suite.tests {
                match test.operation {
                    Arguments::Find { filter, options } =>
                        run_find_test!(db, coll, filter, Some(options),
                                       test.outcome)
                };
            }
        }
    };
}

#[test]
fn find() {
    run_suite!("tests/json/data/find.json", "find");
}
