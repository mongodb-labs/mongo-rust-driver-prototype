% Module json_parse

<div class='index'>

* [Struct `ExtendedJsonParser`](#struct-extendedjsonparser) - JSON parsing struct
* [Trait `ObjParser`](#trait-objparser) - This trait is for parsing non-BSON object notations such as JSON, XML, etc.
* [Implementation ` of ObjParser<Document> for ExtendedJsonParser<~[char]>`](#implementation-of-objparserdocument-for-extendedjsonparserchar) - Publicly exposes from_string.
* [Implementation ` for ExtendedJsonParser<T> where <T: Stream<char>>`](#implementation-for-extendedjsonparsert-where-t-streamchar) - Main parser implementation for JSON

</div>

## Struct `ExtendedJsonParser`

~~~ {.rust}
pub struct ExtendedJsonParser<T> {
    stream: T,
}
~~~

JSON parsing struct. T is a Stream<char>.

## Trait `ObjParser`

This trait is for parsing non-BSON object notations such as JSON, XML, etc.

### Method `from_string`

~~~ {.rust}
fn from_string(s: &str) -> Result<V, ~str>
~~~

## Implementation of `ObjParser<Document>` for `ExtendedJsonParser<~[char]>`

Publicly exposes from_string.

### Method `from_string`

~~~ {.rust}
fn from_string(s: &str) -> DocResult
~~~

## Implementation for `ExtendedJsonParser<T>` where `<T: Stream<char>>`

Main parser implementation for JSON

### Method `object`

~~~ {.rust}
fn object(&mut self) -> DocResult
~~~

Parse an object. Returns an error string on parse failure

### Method `new`

~~~ {.rust}
fn new(stream: T) -> ExtendedJsonParser<T>
~~~

Return a new JSON parser with a given stream.

