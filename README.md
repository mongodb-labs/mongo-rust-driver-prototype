![Travis](https://travis-ci.org/mongodbinc-interns/mongo-rust-driver-prototype.svg)

MongoDB Rust Driver Prototype
=============================

This branch contains active development on a new driver written for Rust 1.0.

The API and implementation are currently subject to change at any time. You must not use this driver in production as it is still under development and is in no way supported by MongoDB Inc. We absolutely encourage you to experiment with it and provide us feedback on the API, design, and implementation. Bug reports and suggestions for improvements are welcomed, as are pull requests.

## Installation

#### Dependencies
- [Rust 1.0 with Cargo](http://rust-lang.org)

#### Importing
The 1.0 driver is currently available as a git dependency. To use the MongoDB driver in your code, add the bson and mongodb packages to your ```Cargo.toml```:
```
[dependencies.bson]
git = "https://github.com/zonyitoo/bson-rs"

[dependencies.mongodb]
git = "https://github.com/mongodbinc-interns/mongo-rust-driver-prototype"
branch = "1.0"
```

Then, import the bson and driver libraries within your code.
```rust
#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
```

## Examples

Here's a basic example of driver usage:

```rust
use bson::Bson;
use mongodb::Client;

fn main() {
   let client = Client::new("localhost", 27017);
   let db = client.db("test");
   let coll = db.collection("movies");

   let doc = doc! { "title" => "Jaws",
                    "array" => [ 1, 2, 3 ] };

   // Insert document into 'test.movies' collection
   coll.insert_one(doc.clone(), None)
       .ok().expect("Failed to insert document.");

   // Find the document and receive a cursor
   let mut cursor = coll.find(doc.clone(), None)
       .ok().expect("Failed to execute find.");

   let item = cursor.next();

   // cursor.next() returns an Option<Result<Document>>
   match item {
      Some(Ok(doc)) => match doc.get("title") {
         Some(&Bson::String(title)) => println!("{}", title),
         _ => panic!("Expected title to be a string!"),
      },
      Some(Err(_)) => panic!("Failed to get next from server!"),
      None => panic!("Server returned no results!"),
   }
}
```

## Documentation
Documentation is built using Cargo. Generated documentation using ```cargo doc``` can be found under the _target/doc/_ folder.
