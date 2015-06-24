macro_rules! run_find_test {
    ( $db:expr, $coll:expr, $filter:expr, $opt:expr, $outcome:expr ) => {{
        let mut cursor = $coll.find($filter, $opt).unwrap();

        let array = match $outcome.result {
            Bson::Array(ref arr) => arr.clone(),
            _ => panic!("Invalid `result` of find test")
        };

        for bson in array {
            assert!(eq::bson_eq(&bson, &Bson::Document(cursor.next().unwrap())));
        }

        assert!(!cursor.has_next());
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_insert_many_test {
    ( $db:expr, $coll: expr, $docs:expr, $outcome:expr ) => {{
        let inserted = $coll.insert_many($docs, true, None).unwrap().inserted_ids.unwrap();
        let ids_bson = match $outcome.result {
            Bson::Document(ref doc) => doc.get("insertedIds").unwrap(),
            _ => panic!("`insert_one` test result should be a document")
        };

        let ids = match ids_bson {
            &Bson::Array(ref arr) => arr.into_iter(),
            _ => panic!("`insertedIds` test result should be an array")
        };

        let mut actual_ids = inserted.values();

        for expected_id in ids {
            assert!(eq::bson_eq(&expected_id, actual_ids.next().unwrap()));
        }

        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_insert_one_test {
    ( $db:expr, $coll: expr, $doc:expr, $outcome:expr ) => {{
        let inserted = $coll.insert_one($doc, None).unwrap().inserted_id.unwrap();
        let id = match $outcome.result {
            Bson::Document(ref doc) => doc.get("insertedId").unwrap(),
            _ => panic!("`insert_one` test result should be a document")
        };

        assert!(eq::bson_eq(&id, &inserted));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_delete_test {
    ( $db:expr, $coll: expr, $filter:expr, $outcome:expr, $many:expr ) => {{
        let count = if $many {
                        $coll.delete_many($filter, None)
                    } else {
                        $coll.delete_one($filter, None)
                    };

        let expected = count.unwrap().deleted_count;

        let actual = match $outcome.result {
            Bson::Document(ref doc) => doc.get("deletedCount").unwrap(),
            _ => panic!("`delete` test result should be a document")
        };

        assert!(actual.int_eq(expected as i64));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_update_test {
    ( $db:expr, $coll: expr, $filter:expr, $delete:expr, $upsert:expr,
      $many:expr, $outcome:expr ) => {{
          let result = if $many {
                           $coll.update_many($filter, $delete, $upsert, None)
                       } else {
                           $coll.update_one($filter, $delete, $upsert, None)
                       };

          let actual = result.unwrap();

          let (matched, modified, upserted) = match $outcome.result {
              Bson::Document(ref doc) => (
                  doc.get("matchedCount").unwrap(),
                  doc.get("modifiedCount").unwrap(),
                  doc.get("upsertedId"),
              ),
              _ => panic!("`delete` test result should be a document")
          };

          assert!(matched.int_eq(actual.matched_count as i64));
          assert!(modified.int_eq(actual.modified_count as i64));

          match (upserted, actual.upserted_id) {
              (None, None) => (),
              (Some(ref bson1), Some(ref bson2)) =>
                  assert!(eq::bson_eq(&bson1, &bson2)),
              _ => panic!("Wrong `upsertedId` returned")
          };

          check_coll!($db, $coll, $outcome.collection);
    }};
}

#[macro_export]
macro_rules! run_suite {
    ( $file:expr, $coll:expr ) => {{
        let json = Json::from_file($file).unwrap();
        let suite = json.get_suite().unwrap();
        let client =  MongoClient::new("localhost", 27017).unwrap();
        let db = client.db("test");
        let coll = db.collection($coll);

        for test in suite.tests {
            coll.drop().unwrap();
            coll.insert_many(suite.data.clone(), true, None).unwrap();

            match test.operation {
                Arguments::Delete { filter, many } =>
                    run_delete_test!(db, coll, filter, test.outcome, many),
                Arguments::Find { filter, options } =>
                    run_find_test!(db, coll, filter, Some(options), test.outcome),
                Arguments::InsertMany { documents } =>
                    run_insert_many_test!(db, coll, documents, test.outcome),
                Arguments::InsertOne { document } =>
                    run_insert_one_test!(db, coll, document, test.outcome),
                Arguments::Update { filter, update, upsert, many } =>
                    run_update_test!(db, coll, filter, update, upsert, many, test.outcome),
            };
        }
    }};
}

#[macro_export]
macro_rules! check_coll {
    ( $db:expr, $coll:expr, $coll_opt:expr ) => {{
        let outcome_coll = match $coll_opt {
            Some(ref coll) => coll.clone(),
            None => continue
        };

        let coll = match outcome_coll.name {
            Some(ref str) => $db.collection(&str),
            None => $db.collection(&$coll.name())
        };

        let mut cursor = coll.find(None, None).unwrap();

        for doc in outcome_coll.data.iter() {
            assert!(eq::bson_eq(&Bson::Document(doc.clone()),
                                &Bson::Document(cursor.next().unwrap())));
        }

        assert!(!cursor.has_next());

        println!("done one...");
    }};
}
