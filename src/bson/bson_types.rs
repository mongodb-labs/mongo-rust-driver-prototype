//TODO: when linked_hashmap enters libextra, replace this

use std::to_bytes::*;
use std::cast::transmute;
use extra::serialize::*;
use ord_hash::*;

static l_end: bool = true;
///Trait for document notations which can be represented as BSON.
pub trait BsonFormattable {
    fn bson_doc_fmt(&self) -> Document;
}

///Enumeration of individual BSON types.
#[deriving(Eq)]
pub enum Document {
    Double(f64),                    //x01
    UString(~str),                    //x02
    Embedded(~BsonDocument),            //x03
    Array(~BsonDocument),                //x04
    Binary(u8, ~[u8]),                //x05
    //deprecated: x06 undefined
    ObjectId(~[u8]),                //x07
    Bool(bool),                    //x08
    UTCDate(i64),                    //x09
    Null,                        //x0A
    Regex(~str, ~str),                //x0B
    //deprecated: x0C dbpointer
    JScript(~str),                    //x0D
    JScriptWithScope(~str, ~BsonDocument),        //x0F
    //deprecated: x0E symbol
    Int32(i32),                    //x10
    Timestamp(i64),                    //x11
    Int64(i64),                    //x12
    MinKey,                        //xFF
    MaxKey                        //x7F

}

/*The type of a complete BSON document.
*Contains an ordered map of fields and values and the size of the document as i32.
*/
#[deriving(Eq)]
pub struct BsonDocument {
    size: i32,
    fields: ~OrderedHashmap<~str, Document>
}

///serialize::Encoder object for Bson.
pub struct BsonDocEncoder {
    priv buf: ~[u8]
}

macro_rules! cstr(
    ($val:ident) => {
        |e| (
            for $val.iter().advance |c| {
                e.emit_char(c);
            }
        )
    }
)
//TODO: most functions are in standalone impl. Clean this up?
///serialize::Encoder implementation.
impl Encoder for BsonDocEncoder {
    fn emit_nil(&mut self) { }
    fn emit_uint(&mut self, _: uint) { }
    fn emit_u8(&mut self, v: u8) { self.buf.push(v) }
    fn emit_u16(&mut self, _: u16) { }
    fn emit_u32(&mut self, _: u32) { }
    fn emit_u64(&mut self, _: u64) { }
    //TODO target architectures with cfg
    fn emit_int(&mut self, v: int) { self.emit_i32(v as i32); }
    fn emit_i64(&mut self, v: i64) {
        self.buf.push_all(v.to_bytes(l_end))
    }
    fn emit_i32(&mut self, v: i32) {
        self.buf.push_all(v.to_bytes(l_end))
    }
    fn emit_i16(&mut self, v: i16) { self.emit_i32(v as i32); }
    fn emit_i8(&mut self, v: i8) { self.emit_i32(v as i32); }
    fn emit_bool(&mut self, v: bool) {
        self.buf.push_all((if v {~[1]} else {~[0]}))
    }
    fn emit_f64(&mut self, v: f64) {
        let x: [u8,..8] = unsafe { transmute(v) };
        self.buf.push_all(x);
    }
    fn emit_f32(&mut self, v: f32) { self.emit_f64(v as f64); }
    fn emit_float(&mut self, v: float) { self.emit_f64(v as f64); }
    fn emit_str(&mut self, v: &str) {
        self.buf.push_all(((v.to_bytes(l_end) + [0]).len() as i32).to_bytes(l_end)
            + v.to_bytes(l_end) + [0]);
        }

    fn emit_map_elt_key(&mut self, l: uint, f: &fn(&mut BsonDocEncoder)) {
        if l == 0 { return; } //if the key is empty, return
        f(self);
        self.emit_u8(0u8);
    }
    fn emit_map_elt_val(&mut self, _: uint, f: &fn(&mut BsonDocEncoder)) {
        f(self);
    }
    fn emit_char(&mut self, c: char) { self.buf.push(c as u8); }
    
    //unimplemented junk
    fn emit_struct(&mut self, _: &str, _: uint, _: &fn(&mut BsonDocEncoder)) { }
    fn emit_enum(&mut self, _: &str, _: &fn(&mut BsonDocEncoder)) { }
    fn emit_enum_variant(&mut self, _: &str, _: uint, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_enum_variant_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_enum_struct_variant(&mut self, _: &str, _: uint, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_enum_struct_variant_field(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_struct_field(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_tuple(&mut self, _: uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_tuple_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_tuple_struct(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_tuple_struct_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_option(&mut self, _:&fn(&mut BsonDocEncoder)) { }
    fn emit_option_none(&mut self) { }
    fn emit_option_some(&mut self, _: &fn(&mut BsonDocEncoder)) { }
    fn emit_seq(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { }
    fn emit_seq_elt(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { }
    fn emit_map(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { }
    

}

///Encoder for BsonDocuments
impl BsonDocEncoder {
    fn new() -> BsonDocEncoder { BsonDocEncoder { buf: ~[] } }
}

///Light wrapper around a typical Map implementation.
impl<E:Encoder> Encodable<E> for BsonDocument {
    fn encode(&self, encoder: &mut E) {
        encoder.emit_i32(self.size);
        for self.fields.iter().advance |&(@k, @v)| {
            let b = match v {
               Double(_) => 0x01,
               UString(_) => 0x02,
               Embedded(_) => 0x03,
               Array(_) => 0x04,
               Binary(_,_) => 0x05,
               ObjectId(_) => 0x07,
               Bool(_) => 0x08,
               UTCDate(_) => 0x09,
               Null => 0x0A,
               Regex(_,_) => 0x0B,
               JScript(_) => 0x0D,
               JScriptWithScope(_,_) => 0x0F,
               Int32(_) => 0x10,
               Timestamp(_) => 0x11,
               Int64(_) => 0x12,
               MinKey => 0xFF,
               MaxKey => 0x7F
            };

            encoder.emit_u8(b);
            encoder.emit_map_elt_key(k.len(), cstr!(k));
            do encoder.emit_map_elt_val(0) |e| {
                v.encode(e);
            }
        }
        encoder.emit_u8(0u8);
    }
}
///Encodable implementation for Document.
impl<E:Encoder> Encodable<E> for Document {
    ///After encode is run, the field 'buf' in the Encoder object will contain the encoded value.
    ///See bson_types.rs:203
    fn encode(&self, encoder: &mut E) {
        match *self {
            Double(f) => {
                encoder.emit_f64(f as f64);
            }
            UString(ref s) => {
                encoder.emit_str(*s);
            }
            Embedded(ref doc) => {
                doc.encode(encoder);
            }
            Array(ref doc) => {
                doc.encode(encoder);
            }
            ObjectId(ref id) => {
                if !(id.len() == 12) {
                    fail!(fmt!("invalid object id found: %?", id));
                }
                for id.iter().advance |&elt| {
                    encoder.emit_u8(elt);
                }
            }
            Bool(b) => {
                encoder.emit_bool(b);
            }
            UTCDate(i) => {
                encoder.emit_i64(i);
            }
            Null => { }
            Regex(ref s1, ref s2) => {
                encoder.emit_map_elt_val(0, cstr!(s1));
                encoder.emit_u8(0u8);
                encoder.emit_map_elt_val(0, cstr!(s2));
                encoder.emit_u8(0u8);
            }
            JScript(ref s) => {
                encoder.emit_str(*s);
            }
            JScriptWithScope(ref s, ref doc) => {
                encoder.emit_i32(5 + doc.size + (s.to_bytes(l_end).len() as i32));
                encoder.emit_map_elt_val(0, cstr!(s));
                encoder.emit_u8(0u8);
                doc.encode(encoder);
            }
            Int32(i) => {
                encoder.emit_i32(i);
            }
            Timestamp(i) => {
                encoder.emit_i64(i);
            }
            Int64(i) => {
                encoder.emit_i64(i); }
            MinKey => { }
            MaxKey => { }
            _ => fail!("binary pls")
        }
    }
}


impl<'self> BsonDocument {
    pub fn to_bson(&self) -> ~[u8] {
        let mut encoder = BsonDocEncoder::new();
        self.encode(&mut encoder);
        encoder.buf //the encoded value is contained here
    }
    //Exposing underlying OrderedHashmap methods
    pub fn contains_key(&self, key: ~str) -> bool {
        self.fields.contains_key(&key)
    }

    pub fn find<'a>(&'a self, key: ~str) -> Option<&'a Document> {
        self.fields.find(&key)
    }
    ///Adds a key/value pair and updates size appropriately. Returns nothing.
    pub fn put(&mut self, key: ~str, val: Document) {
        self.fields.insert(key, val);
        self.size = map_size(self.fields);
    }

    /**
    * Adds a list of key/value pairs and updates size. Returns nothing.
    */
    pub fn put_all(&mut self, pairs: ~[(~str, Document)]) {
        //TODO: when is iter() going to be available?
        for pairs.iter().advance |&(k,v)| {
            self.fields.insert(k, v);
        }
        self.size = map_size(self.fields);
    }
    /**
    * Adds a key/value pair and updates size appropriately. Returns a mutable self reference with a fixed lifetime, allowing calls to be chained.
    * Ex: let a = BsonDocument::inst().append(~"flag", Bool(true)).append(~"msg", UString(~"hello")).append(...);
    * This may cause borrowing errors if used to make embedded objects.
    */
    pub fn append(&'self mut self, key: ~str, val: Document) -> &'self mut BsonDocument {
        self.fields.insert(key, val);
        self.size = map_size(self.fields);
        self
    }

    ///Returns a new BsonDocument struct.
    pub fn new() -> BsonDocument {
        BsonDocument { size: 5, fields: ~OrderedHashmap::new() }
    }

    /**
    * Returns a managed pointer to a new BsonDocument. Use this if you plan on chaining calls to append() directly on your call to inst.
    * Example: let a = BsonDocument::inst().append(...).append(...); //compiles
    * let b = BsonDocument::new().append(...); //error
    * let c = BsonDocument::new();
    * c.append(...).append(...).append(...); //compiles
    */
    pub fn inst() -> @mut BsonDocument {
        @mut BsonDocument::new()
    }

    ///Builds a BSON document from an OrderedHashmap.
    pub fn from_map(m: ~OrderedHashmap<~str, Document>) -> BsonDocument {
        BsonDocument { size: map_size(m), fields: m }
    }

    ///Builds a BSON document from a JSON object. Note that some BSON fields, such as JavaScript, will not be generated.
    pub fn from_formattable<T:BsonFormattable>(json: T) -> BsonDocument {
        let m = json.bson_doc_fmt();
        match m {
            Embedded(ref m) => copy **m,
            _ => fail!("could not correctly format BsonFormattable object")
        }
    }
}

///Allows Documents to report their own size in bytes.
impl Document {
    fn size(&self) -> i32 {
        match *self {
            Double(_) => 8,
            UString(ref s) => 5 + (*s).to_bytes(l_end).len() as i32,
            Embedded(ref doc) => doc.size,
            Array(ref doc) => doc.size,
            Binary(_, ref dat) => 5 + dat.len() as i32,
            ObjectId(_) => 12,
            Bool(_) => 1,
            UTCDate(_) => 8,
            Null => 0,
            Regex(ref s1, ref s2) => 2 + (s1.to_bytes(l_end).len() + s2.to_bytes(l_end).len()) as i32,
            JScript(ref s) => 5 + (*s).to_bytes(l_end).len() as i32,
            JScriptWithScope(ref s, ref doc) => 5 + (*s).to_bytes(l_end).len() as i32 + doc.size,
            Int32(_) => 4,
            Timestamp(_) => 8,
            Int64(_) => 8,
            MinKey => 0,
            MaxKey => 0
        }
    }
}

///Calculate the size of a BSON object based on its fields.
priv fn map_size(m: &OrderedHashmap<~str, Document>)  -> i32{
    let mut sz: i32 = 4; //since this map is going in an object, it has a 4-byte size variable
    for m.iter().advance |&(k, v)| {
        sz += (k.to_bytes(l_end).len() as i32) + v.size() + 2; //1 byte format code, trailing 0 after each key
    }
    sz + 1 //trailing 0 byte
}
