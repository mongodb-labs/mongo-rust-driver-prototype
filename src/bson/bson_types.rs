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
///serialize::Encoder object for Bson.
pub struct BsonDocEncoder {
    priv buf: ~[u8],
    priv curr_key: ~str
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
//TODO: most functions are in standalone impl. Clean this up?
///serialize::Encoder implementation.
impl Encoder for BsonDocEncoder {
    fn emit_nil(&mut self) { let key = self.key(); self.buf.push_all(key); }
    fn emit_uint(&mut self, _: uint) { fail!("uint not implemented") }
    fn emit_u8(&mut self, v: u8) { self.buf.push(v) }
    fn emit_u16(&mut self, _: u16) { fail!("uint not implemented") }
    fn emit_u32(&mut self, _: u32) { fail!("uint not implemented") }
    fn emit_u64(&mut self, _: u64) { fail!("uint not implemented") }
    fn emit_int(&mut self, _: int) { fail!("int not implemented") }
    fn emit_i64(&mut self, v: i64) { let key = self.key(); self.buf.push_all(key + v.to_bytes(l_end)) }
    fn emit_i32(&mut self, v: i32) { let key = self.key(); self.buf.push_all(key + v.to_bytes(l_end)) }
    fn emit_i16(&mut self, _: i16) { fail!("i16 not implemented") }
    fn emit_i8(&mut self, _: i8) { fail!("i8 not implemented") }
    fn emit_bool(&mut self, v: bool) { let key = self.key(); self.buf.push_all(key + (if v {~[1]} else {~[0]})) }
    fn emit_f64(&mut self, v: f64) {
        let x: [u8,..8] = unsafe { transmute(v) };
        let key = self.key();
        self.buf.push_all(key + x);
    } 
    fn emit_f32(&mut self, v: f32) { self.emit_f64(v as f64); }
    fn emit_float(&mut self, v: float) { self.emit_f64(v as f64); }
    fn emit_str(&mut self, v: &str) { let key = self.key(); self.buf.push_all(key + ((v.to_bytes(l_end) + [0]).len() as i32).to_bytes(l_end) + v.to_bytes(l_end) + [0]); }
    
    //embedded
    fn emit_struct(&mut self, _: &str, _: uint, _: &fn(&mut BsonDocEncoder)) {
        fail!("not implemented");
    }
    
    //unimplemented junk
    fn emit_char(&mut self, _: char) { fail!("not implemented") }
    fn emit_enum(&mut self, _: &str, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented") }
    fn emit_enum_variant(&mut self, _: &str, _: uint, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_enum_variant_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_enum_struct_variant(&mut self, _: &str, _: uint, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_enum_struct_variant_field(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_struct_field(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_tuple(&mut self, _: uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_tuple_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_tuple_struct(&mut self, _: &str, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_tuple_struct_arg(&mut self, _:uint, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_option(&mut self, _:&fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_option_none(&mut self) { fail!("not implemented")}
    fn emit_option_some(&mut self, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_seq(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}    
    fn emit_seq_elt(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_map(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_map_elt_key(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}
    fn emit_map_elt_val(&mut self, _: uint, _: &fn(&mut BsonDocEncoder)) { fail!("not implemented")}

}
/**Standalone implementation of BsonDocEncoder.
*Ideally this would largely go away, though with_key and key probably need to remain.
*/
impl BsonDocEncoder {
    fn with_key<'a>(&'a mut self,key: ~str) -> &'a mut BsonDocEncoder{
        self.curr_key = key;
        self
    }

    fn key(&self) -> ~[u8] {
        self.curr_key.to_bytes(l_end) + [0]
    }

    fn emit_size(&mut self, v: i32) {
        self.buf.push_all(v.to_bytes(l_end));
    }

    fn emit_key(&mut self) {
        let key = self.key();
        self.buf.push_all(key);
    }

    fn emit_minkey(&mut self) {
        let key = self.key();
        self.buf.push_all(~[0xFF] + key);
    }

    fn emit_maxkey(&mut self) {
        let key = self.key();
        self.buf.push_all(~[0x7F] + key);
    }

    fn emit_oid(&mut self, v: &[u8]) {
        if !(v.len() == 12) {
            fail!(fmt!("invalid object id found: %?", v))
        }
        let key = self.key();
        self.buf.push_all(key + v)
    }

    fn emit_cstr(&mut self, v: &str) {
        self.buf.push_all(v.to_bytes(l_end) + [0]);
    }

    fn emit_regex(&mut self, v1: &str, v2: &str) {
        let key = self.key();
        self.buf.push_all(key + v1.to_bytes(l_end) + [0] + v2.to_bytes(l_end) + [0]);
    }

    fn new() -> BsonDocEncoder { BsonDocEncoder { buf: ~[], curr_key: ~"" } }
}

///Encodable implementation for BsonDocument.
impl Encodable<BsonDocEncoder> for BsonDocument {
    ///After encode is run, the field 'buf' in the Encoder object will contain the encoded value.
    ///See bson_types.rs:203
    fn encode(&self, encoder: &mut BsonDocEncoder) {
        encoder.emit_size(self.size);
        for self.fields.each |&k,&v| {
            match v {
                Double(f) => { encoder.emit_u8(0x01); encoder.with_key(k).emit_f64(f as f64); }
                UString(ref s) => { encoder.emit_u8(0x02); encoder.with_key(k).emit_str(*s); }
                Embedded(ref doc) => { encoder.emit_u8(0x03); encoder.with_key(k).emit_key(); doc.encode(encoder); }
                Array(ref doc) => { encoder.emit_u8(0x04); encoder.with_key(k).emit_key(); doc.encode(encoder); }
                ObjectId(ref id) => { encoder.emit_u8(0x07); encoder.with_key(k).emit_oid(*id); }
                Bool(b) => { encoder.emit_u8(0x08); encoder.with_key(k).emit_bool(b); }
                UTCDate(i) => { encoder.emit_u8(0x09); encoder.with_key(k).emit_i64(i); }
                Null => { encoder.emit_u8(0x0A); encoder.with_key(k).emit_nil(); }
                Regex(ref s1, ref s2) => { encoder.emit_u8(0x0B); encoder.with_key(k).emit_regex(*s1, *s2); }
                JScript(ref s) => { encoder.emit_u8(0x0D); encoder.with_key(k).emit_str(*s); }
                JScriptWithScope(ref s, ref doc) => { 
                    encoder.emit_u8(0x0F); 
                    encoder.with_key(k).emit_i32(5 + doc.size + (s.to_bytes(l_end).len() as i32));
                    encoder.emit_cstr(*s); 
                    doc.encode(encoder);
                }
                Int32(i) => { encoder.emit_u8(0x10); encoder.with_key(k).emit_i32(i); }
                Timestamp(i) => { encoder.emit_u8(0x11); encoder.with_key(k).emit_i64(i); }
                Int64(i) => { encoder.emit_u8(0x12); encoder.with_key(k).emit_i64(i); }
                _ => fail!("herp")
            }
        }
        encoder.emit_u8(0);
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
        BsonDocument { size: 0, fields: ~OrderedHashmap::new() }
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
    for m.each |&k, &v| {
        sz += (k.to_bytes(l_end).len() as i32) + v.size() + 2; //1 byte format code, trailing 0 after each key
    }
    sz + 1 //trailing 0 byte
}
