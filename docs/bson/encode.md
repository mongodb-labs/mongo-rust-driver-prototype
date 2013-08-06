% Module encode

<div class='index'>

* [Enum `Document`](#enum-document) - Algebraic data type representing the BSON AST
* [Struct `BsonDocEncoder`](#struct-bsondocencoder) - serialize::Encoder object for Bson
* [Struct `BsonDocument`](#struct-bsondocument) - The type of a complete BSON document
* [Implementation ` of ::std::cmp::Eq for Document`](#implementation-of-stdcmpeq-for-document) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for Document`](#implementation-of-stdcloneclone-for-document) - Automatically derived.
* [Implementation ` for ObjIdFactory`](#implementation-for-objidfactory)
* [Implementation ` of ::std::cmp::Eq for BsonDocument`](#implementation-of-stdcmpeq-for-bsondocument) - Automatically derived.
* [Implementation ` of Clone for BsonDocument`](#implementation-of-clone-for-bsondocument)
* [Implementation ` of Encoder for BsonDocEncoder`](#implementation-of-encoder-for-bsondocencoder) - serialize::Encoder implementation.
* [Implementation ` of Encodable<E> for BsonDocument where <E: Encoder>`](#implementation-of-encodablee-for-bsondocument-where-e-encoder) - Light wrapper around a typical Map implementation.
* [Implementation ` of Encodable<E> for Document where <E: Encoder>`](#implementation-of-encodablee-for-document-where-e-encoder) - Encodable implementation for Document.
* [Implementation ` of ToStr for BsonDocument`](#implementation-of-tostr-for-bsondocument)
* [Implementation ` for BsonDocument where <'self>`](#implementation-for-bsondocument-where-self)
* [Implementation ` for Document`](#implementation-for-document) - Methods on documents.
* [Implementation ` of ToStr for Document`](#implementation-of-tostr-for-document)

</div>

## Enum `Document`

Algebraic data type representing the BSON AST.
BsonDocument maps string keys to this type.
This can be converted back and forth from BsonDocument
by using the Embedded variant.

#### Variants


* `Double(f64)`

* `UString(~str)`

* `Embedded(~BsonDocument)`

* `Array(~BsonDocument)`

* `Binary(u8, ~[u8])`

* `ObjectId(~[u8])`

* `Bool(bool)`

* `UTCDate(i64)`

* `Null`

* `Regex(~str, ~str)`

* `JScript(~str)`

* `JScriptWithScope(~str, ~BsonDocument)`

* `Int32(i32)`

* `Timestamp(u32, u32)`

* `Int64(i64)`

* `MinKey`

* `MaxKey`

## Struct `BsonDocEncoder`

~~~ {.rust}
pub struct BsonDocEncoder {
    priv buf: ~[u8],
}
~~~

serialize::Encoder object for Bson.
After encoding has been done with an Encoder instance,
encoder.buf will contain the resulting ~[u8].

## Struct `BsonDocument`

~~~ {.rust}
pub struct BsonDocument {
    size: i32,
    fields: ~OrderedHashmap<~str, Document>,
}
~~~

The type of a complete BSON document.
Contains an ordered map of fields and values and the size of the document as i32.

## Implementation of `::std::cmp::Eq` for `Document`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &Document) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &Document) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `Document`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> Document
~~~

## Implementation for `ObjIdFactory`

### Method `new`

~~~ {.rust}
fn new() -> ObjIdFactory
~~~

Get a new ObjIdFactory.

### Method `oid`

~~~ {.rust}
fn oid(&mut self) -> Document
~~~

Generate an ObjectId.

## Implementation of `::std::cmp::Eq` for `BsonDocument`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &BsonDocument) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &BsonDocument) -> ::bool
~~~

## Implementation of `Clone` for `BsonDocument`

### Method `clone`

~~~ {.rust}
fn clone(&self) -> BsonDocument
~~~

## Implementation of `Encoder` for `BsonDocEncoder`

serialize::Encoder implementation.

### Method `emit_nil`

~~~ {.rust}
fn emit_nil(&mut self)
~~~

### Method `emit_uint`

~~~ {.rust}
fn emit_uint(&mut self, v: uint)
~~~

### Method `emit_u8`

~~~ {.rust}
fn emit_u8(&mut self, v: u8)
~~~

### Method `emit_u16`

~~~ {.rust}
fn emit_u16(&mut self, v: u16)
~~~

### Method `emit_u32`

~~~ {.rust}
fn emit_u32(&mut self, v: u32)
~~~

### Method `emit_u64`

~~~ {.rust}
fn emit_u64(&mut self, v: u64)
~~~

### Method `emit_int`

~~~ {.rust}
fn emit_int(&mut self, v: int)
~~~

### Method `emit_i64`

~~~ {.rust}
fn emit_i64(&mut self, v: i64)
~~~

### Method `emit_i32`

~~~ {.rust}
fn emit_i32(&mut self, v: i32)
~~~

### Method `emit_i16`

~~~ {.rust}
fn emit_i16(&mut self, v: i16)
~~~

### Method `emit_i8`

~~~ {.rust}
fn emit_i8(&mut self, v: i8)
~~~

### Method `emit_bool`

~~~ {.rust}
fn emit_bool(&mut self, v: bool)
~~~

### Method `emit_f64`

~~~ {.rust}
fn emit_f64(&mut self, v: f64)
~~~

### Method `emit_f32`

~~~ {.rust}
fn emit_f32(&mut self, v: f32)
~~~

### Method `emit_float`

~~~ {.rust}
fn emit_float(&mut self, v: float)
~~~

### Method `emit_str`

~~~ {.rust}
fn emit_str(&mut self, v: &str)
~~~

### Method `emit_map_elt_key`

~~~ {.rust}
fn emit_map_elt_key(&mut self, l: uint, f: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_map_elt_val`

~~~ {.rust}
fn emit_map_elt_val(&mut self, _: uint, f: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_char`

~~~ {.rust}
fn emit_char(&mut self, c: char)
~~~

### Method `emit_struct`

~~~ {.rust}
fn emit_struct(&mut self, _: &str, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_enum`

~~~ {.rust}
fn emit_enum(&mut self, _: &str, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_enum_variant`

~~~ {.rust}
fn emit_enum_variant(&mut self, _: &str, _: uint, _: uint,
                     _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_enum_variant_arg`

~~~ {.rust}
fn emit_enum_variant_arg(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_enum_struct_variant`

~~~ {.rust}
fn emit_enum_struct_variant(&mut self, _: &str, _: uint, _: uint,
                            _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_enum_struct_variant_field`

~~~ {.rust}
fn emit_enum_struct_variant_field(&mut self, _: &str, _: uint,
                                  _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_struct_field`

~~~ {.rust}
fn emit_struct_field(&mut self, _: &str, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_tuple`

~~~ {.rust}
fn emit_tuple(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_tuple_arg`

~~~ {.rust}
fn emit_tuple_arg(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_tuple_struct`

~~~ {.rust}
fn emit_tuple_struct(&mut self, _: &str, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_tuple_struct_arg`

~~~ {.rust}
fn emit_tuple_struct_arg(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_option`

~~~ {.rust}
fn emit_option(&mut self, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_option_none`

~~~ {.rust}
fn emit_option_none(&mut self)
~~~

### Method `emit_option_some`

~~~ {.rust}
fn emit_option_some(&mut self, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_seq`

~~~ {.rust}
fn emit_seq(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_seq_elt`

~~~ {.rust}
fn emit_seq_elt(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

### Method `emit_map`

~~~ {.rust}
fn emit_map(&mut self, _: uint, _: &fn(&mut BsonDocEncoder))
~~~

## Implementation of `Encodable<E>` for `BsonDocument` where `<E: Encoder>`

Light wrapper around a typical Map implementation.

### Method `encode`

~~~ {.rust}
fn encode(&self, encoder: &mut E)
~~~

## Implementation of `Encodable<E>` for `Document` where `<E: Encoder>`

Encodable implementation for Document.

### Method `encode`

~~~ {.rust}
fn encode(&self, encoder: &mut E)
~~~

After encode is run, the field 'buf' in the Encoder object will contain the encoded value.
See bson_types.rs:203

## Implementation of `ToStr` for `BsonDocument`

### Method `to_str`

~~~ {.rust}
fn to_str(&self) -> ~str
~~~

## Implementation for `BsonDocument` where `<'self>`

### Method `to_bson`

~~~ {.rust}
fn to_bson(&self) -> ~[u8]
~~~

Convert this document to its binary BSON representation.

### Method `iter`

~~~ {.rust}
fn iter(&'self self) -> VecIterator<'self, (@~str, @Document)>
~~~

Get a forwards iterator for this document.

### Method `rev_iter`

~~~ {.rust}
fn rev_iter(&'self self) -> VecRevIterator<'self, (@~str, @Document)>
~~~

Get a reverse iterator for this document.

### Method `contains_key`

~~~ {.rust}
fn contains_key(&self, key: ~str) -> bool
~~~

Check if this document contains the given key.

### Method `find`

~~~ {.rust}
fn find<'a>(&'a self, key: ~str) -> Option<&'a Document>
~~~

Find the value for the given key, if it exists.

### Method `put`

~~~ {.rust}
fn put(&mut self, key: ~str, val: Document)
~~~

Adds a key/value pair and updates size appropriately. Returns nothing.

### Method `put_all`

~~~ {.rust}
fn put_all(&mut self, pairs: ~[(~str, Document)])
~~~

Adds a list of key/value pairs and updates size. Returns nothing.

### Method `union`

~~~ {.rust}
fn union(&mut self, other: Document)
~~~

If the provided document is Embedded, puts all of its (key,val) pairs
into self. Otherwise, does nothing.

### Method `new`

~~~ {.rust}
fn new() -> BsonDocument
~~~

Returns a new BsonDocument struct.
The default size is 5: 4 for the size integer and 1 for the terminating 0x0.

### Method `fields_match`

~~~ {.rust}
fn fields_match(&self, other: &BsonDocument) -> bool
~~~

Compare two BsonDocuments to decide if they have the same fields.
Returns true if every field except the _id field is matching.
The _id field and the size are ignored.
Two documents are considered to have matching fields even if
their fields are not in the same order.

## Implementation for `Document`

Methods on documents.

### Method `to_bson`

~~~ {.rust}
fn to_bson(&self) -> ~[u8]
~~~

Allows any document to be converted to its BSON-serialized representation.

## Implementation of `ToStr` for `Document`

### Method `to_str`

~~~ {.rust}
fn to_str(&self) -> ~str
~~~

