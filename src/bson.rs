#[link(name="bson", vers="0.1", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];
extern mod extra;
extern mod stream;
//TODO: when linked_hashmap enters libextra, replace this
extern mod ord_hashmap;

use std::util::id;
use std::to_bytes::*;
use extra::json::*;
use ord_hashmap::*;
use stream::*;

static l_end: bool = true;

#[deriving(Eq)]
pub enum Document {
	Double(f64),					//x01
	UString(~str),					//x02
	Embedded(~BsonDocument),			//x03
	Array(~BsonDocument),				//x04
	Binary(u8, ~[u8]),				//x05
	//deprecated: x06 undefined
	ObjectId(~[u8]),				//x07
	Bool(bool),					//x08
	UTCDate(i64),					//x09
	Null,						//x0A
	Regex(~str, ~str),				//x0B
	//deprecated: x0C dbpointer
	JScript(~str),					//x0D	
	JScriptWithScope(~str, ~BsonDocument),		//x0F
	//deprecated: x0E symbol
	Int32(i32),					//x10
	Timestamp(i64),					//x11
	Int64(i64),					//x12
	MinKey,						//xFF
	MaxKey						//x7F
	
}

//BSON-supported types implement this trait
pub trait ToBson {
	fn to_bson(&self, key: ~str) -> ~[u8];
}

pub trait BsonFormat {
	fn bson_doc_fmt(&self) -> Document;
}

/**
* The type of a complete BSON document. Contains an ordered map of fields and values and the size of the document as i32.
*/
#[deriving(ToStr,Eq)]
pub struct BsonDocument {
	size: i32,
	fields: ~OrderedHashmap<~str, Document>
}

impl<'self> BsonDocument {
	fn contains_key(&self, key: ~str) -> bool {
		self.fields.contains_key(&key)
	}

	fn find<'a>(&'a self, key: ~str) -> Option<&'a Document> {
		self.fields.find(&key)
	} 
	/**
	* Adds a key/value pair and updates size appropriately. Returns nothing.
	*/
	fn put(&mut self, key: ~str, val: Document) {
		self.fields.insert(key, val);
		self.size = map_size(self.fields);
	}
	
	/**
	* Adds a list of key/value pairs and updates size. Returns nothing.
	*/
	fn put_all(&mut self, pairs: ~[(~str, Document)]) {
		//TODO: when is iter() going to be available?
		for pairs.each |&(k,v)| {
			self.fields.insert(k, v);
		}
		self.size = map_size(self.fields);
	}
	/**
	* Adds a key/value pair and updates size appropriately. Returns a mutable self reference with a fixed lifetime, allowing calls to be chained.
	* Ex: let a = BsonDocument::inst().append(~"flag", Bool(true)).append(~"msg", UString(~"hello")).append(...);
	* This may cause borrowing errors if used to make embedded objects.
	*/
	fn append(&'self mut self, key: ~str, val: Document) -> &'self mut BsonDocument {
		self.fields.insert(key, val);
		self.size = map_size(self.fields);
		self
	}

	/**
	* Returns a new BsonDocument struct.
	*/
	fn new() -> BsonDocument {
		BsonDocument { size: 0, fields: ~OrderedHashmap::new() }
	}

	/**
	* Returns a managed pointer to a new BsonDocument. Use this if you plan on chaining calls to append() directly on your call to inst.
	* Example: let a = BsonDocument::inst().append(...).append(...); //compiles
	* let b = BsonDocument::new().append(...); //error
	* let c = BsonDocument::new();
	* c.append(...).append(...).append(...); //compiles
	*/
	fn inst() -> @mut BsonDocument {
		@mut BsonDocument::new()
	}

	/**
	* Builds a BSON document from an OrderedHashmap.
	*/
	fn from_map(m: ~OrderedHashmap<~str, Document>) -> BsonDocument {	
		BsonDocument { size: map_size(m), fields: m }
	}

	/**
	* Builds a BSON document from a JSON object. Note that some BSON fields, such as JavaScript, will not be generated.
	*/
	fn from_json(json: &Json) -> BsonDocument {
		let mut m: ~OrderedHashmap<~str, Document> = ~OrderedHashmap::new();
		match *json {
			Object(ref jm) => { for jm.each_key |&k| {
				m.insert(copy k, jm.find(&k).unwrap().bson_doc_fmt());
				}
			}
			_ => fail!("An invalid JSON object was given!")
		}
		BsonDocument { size: map_size(m) as i32, fields: m }
	}
}

impl BsonFormat for Json {
	fn bson_doc_fmt(&self) -> Document {
		match *self {
			Number(f) => Double(f as f64),
			String(ref s) => UString(copy (*s)),
			Boolean(b) => Bool(b),
			extra::json::Null => Null,
			List(ref l) => { let nl = l.map(|&item| item.bson_doc_fmt()); 
				let mut m: OrderedHashmap<~str, Document> = OrderedHashmap::new();
				let mut i: int = 0;
				for nl.each |&item| {
					m.insert(i.to_str(), item);
					i += 1;
				}
				Array( ~BsonDocument {size: map_size(&m), fields: ~m} )
			}
			Object(ref m) => { let mut nm: OrderedHashmap<~str, Document> = OrderedHashmap::new();
				for m.each_key |&k| {
					nm.insert(copy k, m.get(&k).bson_doc_fmt());
				}
				Embedded( ~BsonDocument {size: map_size(&nm), fields: ~nm} )
			}
		}
	}
}

impl ToBson for Document {
	fn to_bson(&self, key: ~str) -> ~[u8] {
		match *self {
			//TODO float
			//Double(f) => ~[0x01] + key.to_bson(~"") 	
			UString(ref s) => ~[0x02] + key.to_bson(~"") + (s.to_bson(~"").len() as i32).to_bytes(l_end) + s.to_bson(~""), 
			Embedded(ref m) => ~[0x03] + key.to_bson(~"") + encode(*m),
			Array(ref m) => ~[0x04] + key.to_bson(~"") + encode(*m),
			Binary(st, ref bits) => ~[0x05] + key.to_bson(~"") + bits.len().to_bytes(l_end) + [st] + *bits, 	
			ObjectId(ref id) => if id.len() != 12 { fail!("Invalid ObjectId: length must be 12 bytes") } else { ~[0x07] + key.to_bson(~"") + *id },
			Bool(b) => ~[0x08] + key.to_bson(~"") + (if b {~[1]} else {~[0]}),
			UTCDate(i) => ~[0x09] + key.to_bson(~"") + i.to_bytes(l_end),
			Null => ~[0x0A] + key.to_bson(~""),
			Regex(ref s1, ref s2) => ~[0x0B] + key.to_bson(~"") + s1.to_bson(~"") + s2.to_bson(~""), //TODO: scrub regexes
			JScript(ref s) => ~[0x0D] + key.to_bson(~"") + (s.to_bson(~"").len() as i32).to_bytes(l_end) + s.to_bson(~""),
			JScriptWithScope(ref s, ref doc) => ~[0x0F] + key.to_bson(~"") + (4 + doc.size  + s.to_bson(~"").len() as i32).to_bytes(l_end) + s.to_bson(~"") + encode(*doc),	
			Int32(i) => ~[0x10] + key.to_bson(~"") + i.to_bytes(l_end),
			Timestamp(i) => ~[0x11] + key.to_bson(~"") + i.to_bytes(l_end),
			Int64(i) => ~[0x12] + key.to_bson(~"") + i.to_bytes(l_end),
			MinKey => ~[0xFF] + key.to_bson(~""),
			MaxKey => ~[0x7F] + key.to_bson(~""),
			_ => fail!() 
		}
	}
}

impl ToBson for ~str {
	fn to_bson(&self, _: ~str) -> ~[u8] {
		self.to_bytes(l_end) + [0]
	}
}

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

/* Public encode and decode functions */

//convert any object that can be validly represented in BSON into a BsonDocument
pub fn decode(b: ~[u8]) -> BsonDocument {
	let mut bson = b;
	let sizebits = bson.process(4, |x| *x);
	let mut doc = BsonDocument::new();
	doc.size = byte_sum(sizebits) as i32;
	doc
}

priv fn map_size(m: &OrderedHashmap<~str, Document>)  -> i32{
	let mut sz: i32 = 4; //since this map is going in an object, it has a 4-byte size variable
	for m.each |&k, &v| {
		sz += (k.to_bytes(l_end).len() as i32) + v.size() + 2; //1 byte format code, trailing 0 after each key
	}
	sz + 1 //trailing 0 byte
}

priv fn byte_sum(bytes: ~[u8]) -> u64 {
	let mut i = 0;
	let mut ret: u64 = 0;
	for bytes.each |&byte| {
		ret += (byte as u64) >> (8 * i);
		i += 1;
	}
	ret
}
//convert a Rust object representing a BSON document into a bytestring
pub fn encode(obj: &BsonDocument) -> ~[u8] {
	let mut bson = obj.size.to_bytes(l_end);
	let dict = &obj.fields;
	for obj.fields.each_key |&k| {
		bson += dict.find(&k).unwrap().to_bson(k);
	}
	return bson + [0];
}

#[cfg(test)]
mod tests {
	extern mod extra;
	extern mod ord_hashmap;
	use super::*;
	use extra::json::*;
	use ord_hashmap::*;

	static l: bool = true;
	
	
	//testing size computation
	#[test]
	fn test_obj_size() {
		let mut m: OrderedHashmap<~str, Document> = OrderedHashmap::new();
		m.insert(~"0", UString(~"hello"));
		m.insert(~"1", Bool(false));
		
		let doc = BsonDocument::from_map(~m);

		assert_eq!(doc.size, 22);

		let mut n = OrderedHashmap::new();
		n.insert(~"foo", UString(~"bar"));
		n.insert(~"baz", UString(~"qux"));
		n.insert(~"doc", Embedded(~doc));

		let doc2 = BsonDocument::from_map(~n);
		assert_eq!(doc2.size, 58);
	}

	//testing BsonFormat implementation
	#[test]
	fn test_string_bson_doc_fmt() {
		assert_eq!(String(~"hello").bson_doc_fmt(), UString(~"hello"));
	}

	#[test]
	fn test_list_bson_doc_fmt() {
		let lst = List(~[Boolean(true), String(~"hello")]);
		match lst.bson_doc_fmt() {
			super::Array(~l1) => { assert_eq!(l1.find(~"0").unwrap(), &Bool(true));
					       assert_eq!(l1.find(~"1").unwrap(), &UString(~"hello")); }
			_ => fail!()
		}
	}

	#[test]
	fn test_object_bson_doc_fmt() {
		//TODO	
	}

	#[test]
	fn test_decode_size() {
		let doc = decode(~[4,0,0,0,5,6,7,8]);
		assert_eq!(doc.size, 4);
	}

	//testing encode
	#[test]
	fn test_string_encode() {
		let doc = BsonDocument::inst().append(~"foo", UString(~"bar"));
		assert_eq!(encode(doc), ~[18,0,0,0,2,102,111,111,0,4,0,0,0,98,97,114,0,0]);
	}

	#[test]
	fn test_bool_encode() {
		let doc = BsonDocument::inst().append(~"foo", Bool(true));
		assert_eq!(encode(doc), ~[11,0,0,0,8,102,111,111,0,1,0] );
	}

	#[test]
	fn test_32bit_encode() {
		let doc = BsonDocument::inst().append(~"foo", Int32(56 as i32));
		assert_eq!(encode(doc), ~[14,0,0,0,16,102,111,111,0,56,0,0,0,0]);
	}

	#[test]
	fn test_embed_encode() {
		//lists
		let mut inside = BsonDocument::new();
		inside.put_all(~[(~"0", UString(~"hello")), (~"1", Bool(false))]);
		let mut doc2 = BsonDocument::new();
		doc2.put_all(~[(~"foo", Array(~ copy inside)), (~"baz", UString(~"qux"))]);

		assert_eq!(encode(&doc2), ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
		
		//embedded objects
		let mut doc3 = BsonDocument::new();
		doc3.put_all(~[(~"foo", Embedded(~ copy inside)), (~"baz", UString(~"qux"))]);	
		
		assert_eq!(encode(&doc3), ~[45,0,0,0,3,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	
		let mut doc4 = BsonDocument::new();
		doc4.put_all(~[(~"foo", JScriptWithScope(~"wat", ~ copy inside)), (~"baz", UString(~"qux"))]);	
		assert_eq!(encode(&doc4), ~[53,0,0,0,15,102,111,111,0,30,0,0,0,119,97,116,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	

	}

	#[test]
	fn test_64bit_encode() {
		let doc1 = BsonDocument::inst().append(~"foo", UTCDate(4040404 as i64));
		assert_eq!(encode(doc1), ~[18,0,0,0,9,102,111,111,0,212,166,61,0,0,0,0,0,0] );	

		let doc2 = BsonDocument::inst().append(~"foo", Int64(4040404 as i64));
		assert_eq!(encode(doc2), ~[18,0,0,0,18,102,111,111,0,212,166,61,0,0,0,0,0,0] );	

		let doc3 = BsonDocument::inst().append(~"foo", Timestamp(4040404 as i64));
		assert_eq!(encode(doc3), ~[18,0,0,0,17,102,111,111,0,212,166,61,0,0,0,0,0,0] );	
	}

	#[test]
	fn test_null_encode() {
		let doc = BsonDocument::inst().append(~"foo", super::Null);

		assert_eq!(encode(doc), ~[10,0,0,0,10,102,111,111,0,0]);
	}

	#[test]
	fn test_regex_encode() {
		let doc = BsonDocument::inst().append(~"foo", Regex(~"bar", ~"baz"));

		assert_eq!(encode(doc), ~[18,0,0,0,11,102,111,111,0,98,97,114,0,98,97,122,0,0]);	
	}

	#[test]
	fn test_jscript_encode() {
		let doc = BsonDocument::inst().append(~"foo", JScript(~"return 1;"));
		assert_eq!(encode(doc), ~[24,0,0,0,13,102,111,111,0,10,0,0,0,114,101,116,117,114,110,32,49,59,0,0]);
	}
	#[test]
	fn test_valid_objid_encode() {
		let doc = BsonDocument::inst().append(~"foo", ObjectId(~[0,1,2,3,4,5,6,7,8,9,10,11]));
	
		assert_eq!(encode(doc), ~[22,0,0,0,7,102,111,111,0,0,1,2,3,4,5,6,7,8,9,10,11,0]);
	}

	#[test]
	#[should_fail]
	fn test_invalid_objid_encode() {
		let doc = BsonDocument::inst().append(~"foo", ObjectId(~[1,2,3]));
		encode(doc);
	}

	#[test]
	fn test_multi_encode() {

		let doc = BsonDocument::inst().append(~"foo", Bool(true)).append(~"bar", UString(~"baz")).append(~"qux", Int32(404));

		let enc = encode(doc);
	
		assert_eq!(enc, ~[33,0,0,0,8,102,111,111,0,1,2,98,97,114,0,4,0,0,0,98,97,122,0,16,113,117,120,0,148,1,0,0,0]);
	}
}
