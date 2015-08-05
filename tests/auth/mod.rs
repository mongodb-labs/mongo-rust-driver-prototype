use mongodb::{Authenticator, CommandType, Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

#[test]
fn auth() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("auth");

    let _ = db.drop_all_users(None).unwrap();

    let doc = doc! { "connectionStatus" => 1};
    let out = db.command(doc.clone(), CommandType::Suppressed).unwrap();
    println!("{}", out);

    db.create_user("saghm", "such_secure_password", None).unwrap();
    db.auth("saghm", "such_secure_password").unwrap();

    let out = db.command(doc, CommandType::Suppressed).unwrap();
    println!("{}", out);
}
