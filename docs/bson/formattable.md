% Module formattable

<div class='index'>

* [Trait `BsonFormattable`](#trait-bsonformattable) - Trait for document notations which can be represented as BSON
* [Implementation ` of BsonFormattable for f32`](#implementation-of-bsonformattable-for-f32)
* [Implementation ` of BsonFormattable for float`](#implementation-of-bsonformattable-for-float)
* [Implementation ` of BsonFormattable for i8`](#implementation-of-bsonformattable-for-i8)
* [Implementation ` of BsonFormattable for i16`](#implementation-of-bsonformattable-for-i16)
* [Implementation ` of BsonFormattable for int`](#implementation-of-bsonformattable-for-int)
* [Implementation ` of BsonFormattable for u8`](#implementation-of-bsonformattable-for-u8)
* [Implementation ` of BsonFormattable for u16`](#implementation-of-bsonformattable-for-u16)
* [Implementation ` of BsonFormattable for u32`](#implementation-of-bsonformattable-for-u32)
* [Implementation ` of BsonFormattable for uint`](#implementation-of-bsonformattable-for-uint)
* [Implementation ` of BsonFormattable for char`](#implementation-of-bsonformattable-for-char)
* [Implementation ` of BsonFormattable for f64`](#implementation-of-bsonformattable-for-f64)
* [Implementation ` of BsonFormattable for i32`](#implementation-of-bsonformattable-for-i32)
* [Implementation ` of BsonFormattable for i64`](#implementation-of-bsonformattable-for-i64)
* [Implementation ` of BsonFormattable for ~str`](#implementation-of-bsonformattable-for-str)
* [Implementation ` of BsonFormattable for ~T where <T: BsonFormattable>`](#implementation-of-bsonformattable-for-t-where-t-bsonformattable)
* [Implementation ` of BsonFormattable for @T where <T: BsonFormattable>`](#implementation-of-bsonformattable-for-@t-where-t-bsonformattable)
* [Implementation ` of BsonFormattable for json::Json`](#implementation-of-bsonformattable-for-jsonjson)
* [Implementation ` of BsonFormattable for ~[T] where <T: BsonFormattable + Copy>`](#implementation-of-bsonformattable-for-t-where-t-bsonformattable-copy)
* [Implementation ` of BsonFormattable for HashMap<~str, V> where <V: BsonFormattable>`](#implementation-of-bsonformattable-for-hashmapstr-v-where-v-bsonformattable)
* [Implementation ` of BsonFormattable for BsonDocument`](#implementation-of-bsonformattable-for-bsondocument)

</div>

## Trait `BsonFormattable`

Trait for document notations which can be represented as BSON.
This trait allows any type to be easily serialized and deserialized as BSON.
After implementing this trait on a type Foo, Foo can be converted to
a BSON formatted byte representation by calling (Foo::new()).to_bson_t().to_bson();

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

Converts an object into a Document.
Typically for a struct, an implementation of to_bson_t would convert the struct
into a HashMap-based variant of Document (usually Embedded) that would
map field names to values.

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<Self, ~str>
~~~

Converts a Document into an object of the given type.
Logically this method is the inverse of to_bson_t
and usually the two functions should roundtrip.

## Implementation of `BsonFormattable` for `f32`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<f32, ~str>
~~~

## Implementation of `BsonFormattable` for `float`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<float, ~str>
~~~

## Implementation of `BsonFormattable` for `i8`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<i8, ~str>
~~~

## Implementation of `BsonFormattable` for `i16`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<i16, ~str>
~~~

## Implementation of `BsonFormattable` for `int`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<int, ~str>
~~~

## Implementation of `BsonFormattable` for `u8`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<u8, ~str>
~~~

## Implementation of `BsonFormattable` for `u16`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<u16, ~str>
~~~

## Implementation of `BsonFormattable` for `u32`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<u32, ~str>
~~~

## Implementation of `BsonFormattable` for `uint`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<uint, ~str>
~~~

## Implementation of `BsonFormattable` for `char`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<char, ~str>
~~~

## Implementation of `BsonFormattable` for `f64`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<f64, ~str>
~~~

## Implementation of `BsonFormattable` for `i32`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<i32, ~str>
~~~

## Implementation of `BsonFormattable` for `i64`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<i64, ~str>
~~~

## Implementation of `BsonFormattable` for `~str`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<~str, ~str>
~~~

## Implementation of `BsonFormattable` for `~T` where `<T: BsonFormattable>`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<~T, ~str>
~~~

## Implementation of `BsonFormattable` for `@T` where `<T: BsonFormattable>`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<@T, ~str>
~~~

## Implementation of `BsonFormattable` for `json::Json`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<json::Json, ~str>
~~~

## Implementation of `BsonFormattable` for `~[T]` where `<T: BsonFormattable + Copy>`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<~[T], ~str>
~~~

## Implementation of `BsonFormattable` for `HashMap<~str, V>` where `<V: BsonFormattable>`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<HashMap<~str, V>, ~str>
~~~

## Implementation of `BsonFormattable` for `BsonDocument`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: Document) -> Result<BsonDocument, ~str>
~~~

