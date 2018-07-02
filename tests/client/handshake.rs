use bson::{self, Bson};
use mongodb::{DRIVER_NAME, Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::CommandType;

#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(rename = "clientMetadata")]
    pub client: ClientMetadata,
}

#[derive(Debug, Deserialize)]
struct ClientMetadata {
    pub driver: DriverMetadata,
    pub os: OsMetadata,
}

#[derive(Debug, Deserialize)]
struct DriverMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
struct OsMetadata {
    #[serde(rename = "type")]
    pub os_type: String,
    pub architecture: String,
}

#[test]
fn metadata_sent_in_handshake() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("admin");
    skip_if_db_version_below!(db, 3, 4);

    let result = db.command(doc! { "currentOp" => 1 }, CommandType::Suppressed, None).unwrap();
    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, DRIVER_NAME);
}

