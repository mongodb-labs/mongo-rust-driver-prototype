use bson::Document;
use bson::Bson::{FloatingPoint, I64};
use bson::Bson::String as BsonString;
use mongodb::client::cursor::Cursor;
use mongodb::client::wire_protocol::flags::{OpInsertFlags, OpQueryFlags};
use mongodb::client::wire_protocol::operations::Message;
use std::io::Write;
use std::net::TcpStream;

#[test]
fn cursor_features() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let docs : Vec<_> = (0..10).map(|i| {
                let mut doc = Document::new();
                doc.insert("foo".to_owned(), I64(i));

                doc
            }).collect();

            let flags = OpInsertFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create insert message!")
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let doc = Document::new();
            let flags = OpQueryFlags::no_flags();
            let name = "test.test";
            let result = Cursor::query_with_batch_size(&mut stream, 3, 2, flags,
                                                       name, 0, 0, doc, None);

            let mut cursor = match result {
                Ok(c) => c,
                Err(s) => panic!("{}", s)
            };

            let batch = cursor.next_batch();

            assert_eq!(batch.len(), 3 as usize);

            for i in 0..batch.len() {
                match batch[i].get("foo") {
                    Some(&I64(j)) => assert_eq!(i as i64, j),
                    _ => panic!("Wrong value returned from Cursor#next_batch")
                };
            }

            let bson = match cursor.next() {
                Some(b) => b,
                None => panic!("Nothing returned from Cursor#next")
            };

            match bson.get("foo") {
                Some(&I64(3)) => (),
                _ => panic!("Wrong value returned from Cursor#next")
            };

            let vec = cursor.next_n(20);

            assert_eq!(vec.len(), 6 as usize);

            for i in 0..vec.len() {
                match vec[i].get("foo") {
                    Some(&I64(j)) => assert_eq!(4 + i as i64 , j),
                    _ => panic!("Wrong value returned from Cursor#next_batch")
                };
            }
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
