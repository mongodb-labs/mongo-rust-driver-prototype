% Module decode

<div class='index'>

* [Struct `BsonParser`](#struct-bsonparser) - Parser object for BSON
* [Implementation ` for BsonParser<T> where <T: Stream<u8>>`](#implementation-for-bsonparsert-where-t-streamu8)
* [Function `decode`](#function-decode) - Standalone decode binding

</div>

## Struct `BsonParser`

~~~ {.rust}
pub struct BsonParser<T> {
    stream: T,
}
~~~

Parser object for BSON. T is constrained to Stream<u8>.

## Implementation for `BsonParser<T>` where `<T: Stream<u8>>`

### Method `document`

~~~ {.rust}
fn document(&mut self) -> Result<BsonDocument, ~str>
~~~

Parse a byte stream into a BsonDocument. Returns an error string on parse failure.
Initializing a BsonParser and calling document() will fully convert a ~[u8]
into a BsonDocument if it was formatted correctly.

### Method `new`

~~~ {.rust}
fn new(stream: T) -> BsonParser<T>
~~~

Create a new parser with a given stream.

## Function `decode`

~~~ {.rust}
fn decode(b: ~[u8]) -> Result<BsonDocument, ~str>
~~~

Standalone decode binding.
This is equivalent to initializing a parser and calling document().

