MongoDB Rust Driver Prototype
=============================

This is a prototype version of a MongoDB driver for the Rust programming language.

## Tutorial

#### Mongo Driver
In general, aside from the BSON library imports (see below), we will need the following imports:
```rust
use mongo::client::*;
use mongo::util::*;     // access to option flags and specifications, etc.
use mongo::db::*;
use mongo::coll::*;
use mongo::cursor::*;
```

In order to connect with a Mongo server, we first create a client:
```rust
let client = Client::new();
```
To connect to an unreplicated, unsharded server running on localhost, port 27017, we use the ```connect``` method:
```rust
match client.connect(~"127.0.0.1", 27017 as uint) {
    Ok(_) => (),
    Err(e) => fail!("%s", MongoErr::to_str(e)), // if cannot connect, nothing to do; display error message
}
```
Now we may create handles to databases and collections on the server. We start with collections to demonstrate CRUD operations.
```rust
// create a handle to the collection "foo_coll" in the database "foo_db"
//      (either may already exist; if not, they will be created on the first insert)
let coll = @Collection::new(~"foo_db", ~"foo_coll", client);
```

We can insert anything implementing the BSON ```formattable``` trait (e.g. if we implement that trait for a FooStruct, we can read and write FooStructs); in the below examples we use ```~str```.
```rust
// insert a document into foo_coll
let ins = ~"{ \"_id\":0, \"a\":0, \"msg\":\"first insert!\" }";
coll.insert(ins, None);
    // no write concern specified---use default

// insert a big batch of documents into foo_coll
let mut ins_batch : ~[~str] = ~[];
let n = 200;
for n.times {
    ins_batch = ins_batch + ~[fmt!("{ \"a\":%d, \"b\":\"ins %d\" }", i/2, i)];
    i += 1;
}
coll.insert_batch(ins_strs, None, None, None);
    // no write concern specified---use default; no special options

// read one back (no specific query or query options/flags)
//      presently, ~BsonDocuments are read back, and should be converted
match ins_doc = match 
match coll.find_one(None, None, None) {
    Ok(ret_doc) => 
}

// insert a big batch of documents
```

#### BSON library
##### BSON data types
BSON-valid data items are represented in the ```Document``` type. (Valid types available from the [specification](http://bson-spec.org)).
To get a document for one of these types, you can wrap it yourself or call the ```to_bson_t``` method.
Example:
```rust
use mongo::bson::formattable::*;

let a = (1i).to_bson_t(); //Int32(1)
let b = (~"foo").to_bson_t(); //UString(~"foo")
let c = 3.14159.to_bson_t(); //Double(3.14159)
let d = extra::json::String(~"bar").to_bson_t(); //UString(~"bar")
```
```to_bson_t``` is contained in the ```BsonFormattable``` trait, so any type implementing this trait can be converted to a Document.

A complete BSON object is represented in the BsonDocument type. BsonDocument contains a size field (```i32```) and map between ```~str```s and ```Document```s.
This type exposes an API which is similar to that of a typical map.
Example:
```rust
use mongo::bson::encode::*;
use mongo::bson::formattable::*;

//Building a document {foo: "bar", baz: 5.1}
let doc = BsonDocument::new();
doc.put(~"foo", (~"bar").to_bson_t());
doc.put(~"baz", (5.1).to_bson_t());
```

In addition to constructing them directly, these types can also be built from JSON-formatted strings. The parser in ```extra::json``` will return a Json object (which implements BsonFormattable) but the fields will not necessarily be ordered properly.
The BSON library also publishes its own JSON parser, which supports [extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/) and guarantees that fields will be serialized in the order they were inserted.
Calling this JSON parser is done through the ```ObjParser``` trait's ```from_string``` method.
Example:
```rust
use mongo::bson::json_parse::*;

let json_string = ~"{\"foo\": \"bar\", \"baz\", 5}";
let parsed_doc = ObjParser::from_string<Document, ExtendedJsonParser<~[char]>>(json_string);
match parsed_doc {
    Ok(ref d) => //the string was parsed successfully; d is a Document
    Err(e) => //the string was not valid JSON and an error was encountered while parsing
}
```

##### Encoding values
```Document```s and ```BsonDocument```s can be encoded into bytes via their ```to_bson``` methods. This will produce a ```~[u8]``` meeting the specifications outlined by the [specification](http://bson-spec.org).
Through this method, standard BSON types can easily be serialized. Any type ```Foo``` can also be serialized in this way if it implements the ```BsonFormattable``` trait.
Example:
```
use mongo::bson::encode::*;
use mongo::bson::formattable::*;

struct Foo {
    ...
}

impl BsonFormattable for Foo {
    fn to_bson_t(&self) -> Document {
        //a common implementation of this might be creating a map from 
        //the names of the fields in a Foo to their values
    }

    fn from_bson_t(doc: Document) -> Foo {
        //this method is the inverse of to_bson_t,
        //in general it makes sense for the two of them to roundtrip
    }
}

//now if you call (Foo::new()).to_bson_t().to_bson(),
//you will produce a BSON representation of a Foo
```

##### Decoding values
The ```~[u8]``` representation of data is not especially useful for modifying or viewing. A ```~[u8]``` can be easily transformed into a BsonDocument for easier manipulation.
Example:
```
use mongo::bson::decode::*;

let b: ~[u8] = /*get a bson document from somewhere*/
let p = BsonParser::new(b);
let doc = p.document();
match doc {
    Ok(ref d) => //b was a valid BSON string. d is the corresponding document.
    Err(e) => //b was not a valid BSON string.
}

let c: ~[u8] = /*get another bson document*/
let doc = decode(c); //the standalone 'decode' function handles creation of the parser
```

The ```BsonFormattable``` trait also contains a method called ```from_bson_t``` as mentioned above. This static method allows a Document to be converted into the implementing type. This allows a quick path to decode a ```~[u8]``` into a ```T:BsonFormattable```.
Example:
```rust
use mongo::bson::decode::*;
use mongo::bson::formattable::*;

struct Foo {
    ...
}

impl BsonFormattable for Foo {
    ...
}

let b: ~[u8] = /*get a bson document from somewhere*/
let myfoo = BsonFormattable::from_bson_t::<Foo>(decode(b).unwrap()); //here it is assumed b was decoded successfully, though a match could be done
```


## Roadmap

- [ ] Replication set support
- [ ] Implement read preferences
- [ ] Documentation to the [API site](http://api.mongodb.org)
- [ ] Thorough test suite for CRUD functionality
To be continued...
