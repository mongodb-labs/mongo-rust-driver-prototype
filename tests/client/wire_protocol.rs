use bson::Document;
use bson::Bson::FloatingPoint;
use mongodb::client::wire_protocol::flags::{Flags, OpInsertFlags, OpQueryFlags};
use mongodb::client::wire_protocol::operations::Message;
use std::io::Write;
use std::net::TcpStream;

fn query() {
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

fn insert() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            println!("Successfully connected to server");

            let mut doc = Document::new();
            doc.insert("foo".to_owned(), FloatingPoint(42.0));
            let docs = vec![doc];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create message!")
            };

            match cm.write(&mut stream) {
                Ok(_) => println!("written!"),
                Err(s) => panic!("{}", s)
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
