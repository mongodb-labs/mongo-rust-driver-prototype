#[link(name="bson_types", vers="0.1", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];
//TODO: when linked_hashmap enters libextra, replace this
extern mod ord_hashmap;

use std::to_bytes::*;
use ord_hashmap::*;

static l_end: bool = true;

pub trait BsonFormattable {
	fn bson_doc_fmt(&self) -> Document;
}

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

#[deriving(Eq)]
pub enum PureJson {
	PureJsonString(~str),
	PureJsonNumber(float),
	PureJsonBoolean(bool),
	PureJsonList(~[PureJson]),
	PureJsonObject(OrderedHashmap<~str, PureJson>),
	PureJsonNull,
	PureJsonObjID(~[u8])
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
	pub fn contains_key(&self, key: ~str) -> bool {
		self.fields.contains_key(&key)
	}

	pub fn find<'a>(&'a self, key: ~str) -> Option<&'a Document> {
		self.fields.find(&key)
	} 
	/**
	* Adds a key/value pair and updates size appropriately. Returns nothing.
	*/
	pub fn put(&mut self, key: ~str, val: Document) {
		self.fields.insert(key, val);
		self.size = map_size(self.fields);
	}
	
	/**
	* Adds a list of key/value pairs and updates size. Returns nothing.
	*/
	pub fn put_all(&mut self, pairs: ~[(~str, Document)]) {
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
	pub fn append(&'self mut self, key: ~str, val: Document) -> &'self mut BsonDocument {
		self.fields.insert(key, val);
		self.size = map_size(self.fields);
		self
	}

	/**
	* Returns a new BsonDocument struct.
	*/
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

	/**
	* Builds a BSON document from an OrderedHashmap.
	*/
	pub fn from_map(m: ~OrderedHashmap<~str, Document>) -> BsonDocument {	
		BsonDocument { size: map_size(m), fields: m }
	}

	/**
	* Builds a BSON document from a JSON object. Note that some BSON fields, such as JavaScript, will not be generated.
	*/
	pub fn from_formattable<T:BsonFormattable>(json: T) -> BsonDocument {
		let m = json.bson_doc_fmt();
		match m {
			Embedded(ref m) => copy **m,
			_ => fail!("could not correctly format BsonFormattable object")
		}
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
/*
impl ToBson for Document {
	fn to_bson(&self, key: ~str) -> ~[u8] {
		match *self {
			Double(f) => {
				let x: [u8,..8] = unsafe { std::cast::transmute(f as f64) };
				~[0x01] + key.to_bson(~"") + x
			}
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
			MaxKey => ~[0x7F] + key.to_bson(~"")
		}
	}
}
*/

impl BsonFormattable for PureJson {
	fn bson_doc_fmt(&self) -> Document{
		match *self {
			PureJsonNumber(f) => Double(f as f64),
			PureJsonString(ref s) => UString(copy *s),
			PureJsonBoolean(b) => Bool(b),
			PureJsonNull => Null,
			//PureJsonObjID(l) => ObjectId(l),
			PureJsonList(ref l) => {
				let nl = l.map(|&item| item.bson_doc_fmt()); 
				let mut doc = BsonDocument::new();
				let mut i: int = 0;
				for nl.each |&item| {
					doc.put(i.to_str(), item);
					i += 1;
				}
				Array(~doc)
			}
			PureJsonObject(ref m) => {
				let mut doc = BsonDocument::new();
				for m.each_key |&k| {
					doc.put(copy k, m.find(&k).unwrap().bson_doc_fmt());
				}
				Embedded(~doc)
			}
			_ => fail!("objid not implemented")
		}
	}
}

priv fn map_size(m: &OrderedHashmap<~str, Document>)  -> i32{
	let mut sz: i32 = 4; //since this map is going in an object, it has a 4-byte size variable
	for m.each |&k, &v| {
		sz += (k.to_bytes(l_end).len() as i32) + v.size() + 2; //1 byte format code, trailing 0 after each key
	}
	sz + 1 //trailing 0 byte
}
