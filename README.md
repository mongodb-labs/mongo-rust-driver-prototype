MongoDB Rust Driver Prototype
=============================

This branch contains active development on a new driver written for Rust 1.0.

The API and implementation are currently subject to change at any time. You must not use this driver in production as it is still under development and is in no way supported by 10gen. We absolutely encourage you to experiment with it and provide us feedback on the API, design, and implementation. Bug reports and suggestions for improvements are welcomed, as are pull requests.

## Installation

#### Dependencies
- [Rust 1.0 with Cargo](http://rust-lang.org)

#### Importing
The 1.0 driver is currently available as a local dependency. To use the MongoDB driver in your code, pull the 1.0 branch:

```
git clone -b 1.0 --single-branch https://github.com/mongodbinc-interns/mongo-rust-driver-prototype.git
```

Add the bson and mongodb packages to your ```Cargo.toml```:
```
[dependencies]
bson = "0.1.1"

[dependencies.mongodb]
path="/path/to/mongo-rust-driver-prototype"
```

Then, import the bson and driver libraries within your code.
```rust
extern crate bson;
extern crate mongodb;
```

## Examples

Here's a basic example of driver usage:

```rust
use bson;
use bson::Bson;

use mongodb::client::MongoClient;
use mongodb::client::db::Database;
use mongodb::client::coll::Collection;

fn main() {
   let client = MongoClient::new("localhost", 27017);
   let db = client.db("test");
   let coll = db.collection("movies");

   let doc = bson::Document::new();
   doc.insert("title".to_owned(), Bson::String("Jaws").to_owned());

   coll.insert_one(doc.clone(), None).ok().expect("Failed to insert document.");
   let cursor = coll.find_one(doc.clone(), None).ok().expect("Failed to execute find.");

   let item = cursor.next();
   match item.get("title") {
         Some(&Bson::String(title)) => println!("{}", title),
         None => panic!("Unexpected error!"),
   }
}
```

## Documentation
Documentation is built using Cargo. Generated documentation using ```cargo doc``` can be found under the _target/doc/_ folder.