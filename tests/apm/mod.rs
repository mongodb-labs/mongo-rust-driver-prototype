use bson::Bson;
use mongodb::{Client, CommandResult, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use rand;

fn timed_query(_client: Client, command_result: &CommandResult) {
    let duration = match command_result {
        &CommandResult::Success { duration, .. } => duration,
        _ => panic!("Command failed!")
    };

    // Sanity check
    assert!(duration >= 1500000000);

    // Technically not guaranteed, but since the query is running locally, it shouldn't even be close
    assert!(duration < 2000000000);
}

#[test]
fn command_duration() {
    let mut client = Client::connect("localhost", 27017).ok().expect("damn it!");
    let db = client.db("test");
    let coll = db.collection("event_hooks");
    coll.drop().unwrap();

    let docs = (1..4).map(|i| doc! { "_id" => i, "x" => (rand::random::<u8>() as u32) }).collect();
    coll.insert_many(docs, false, None).unwrap();
    client.add_completion_hook(timed_query).unwrap();

    let doc = doc! {
        "$where" => (Bson::JavaScriptCode("function() { sleep(500); }".to_owned()))
    };

    coll.find(Some(doc), None).unwrap();
}
