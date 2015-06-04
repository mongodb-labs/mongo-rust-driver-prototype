use bson::Document;
use bson::Bson::FloatingPoint;
use mongodb::client::wire_protocol::operations::{OpQueryFlags, Message};
use std::io::Write;
use std::net::TcpStream;

// Not prefixing this with "#[test]" since it requires both a server to be
// running locally and the correct database state to pass, and we don't want
// Travis to fail.
fn wire_protocol() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            println!("Successfully connected to server");

            let doc = Document::new();
            let flags = OpQueryFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_query(1, flags, name, 0, 1, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create message!")
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let bson = match Message::read(&mut stream) {
                Ok(bson) => bson,
                Err(s) => panic!("Could not read response")
            };

            match bson.get("foo") {
                Some(&FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            }
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
