#[link(name="bson", vers="0.1", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];
extern mod extra;
extern mod stream;
//TODO: when linked_hashmap enters libextra, replace this
extern mod ord_hashmap;
extern mod json_parse;
extern mod bson_types;

use std::util::id;
use std::to_bytes::*;
use std::str::from_bytes;
use bson_types::*;
use ord_hashmap::*;
use stream::*;

static l_end: bool = true;

static DOUBLE: u8 = 0x01;
static STRING: u8 = 0x02;
static EMBED: u8 = 0x03;
static ARRAY: u8 = 0x04;
static BINARY: u8 = 0x05;
static OBJID: u8 = 0x07;
static BOOL: u8 = 0x08;
static UTCDATE: u8 = 0x09;
static NULL: u8 = 0x0A;
static REGEX: u8 = 0x0B;
static JSCRIPT: u8 = 0x0D;
static JSCOPE: u8 = 0x0F;
static INT32: u8 = 0x10;
static TSTAMP: u8 = 0x11;
static INT64: u8 = 0x12;
static MINKEY: u8 = 0xFF;
static MAXKEY: u8 = 0x7F;

pub trait ToBson {
	fn to_bson(&self, key: ~str) -> ~[u8];
}
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

impl ToBson for ~str {
	fn to_bson(&self, _: ~str) -> ~[u8] {
		self.to_bytes(l_end) + [0]
	}
}

/* Public encode and decode functions */

//convert any object that can be validly represented in BSON into a BsonDocument
pub fn decode(mut b: ~[u8]) -> BsonDocument {
	document(&mut b)
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

/* Utility functions, mostly for decode */

priv fn bytesum(bytes: ~[u8]) -> u64 {
	let mut i = 0;
	let mut ret: u64 = 0;
	for bytes.each |&byte| {
		ret += (byte as u64) >> (8 * i);
		i += 1;
	}
	ret
}

priv fn map_size(m: &OrderedHashmap<~str, Document>)  -> i32{
	let mut sz: i32 = 4; //since this map is going in an object, it has a 4-byte size variable
	for m.each |&k, &v| {
		sz += (k.to_bytes(l_end).len() as i32) + v.size() + 2; //1 byte format code, trailing 0 after each key
	}
	sz + 1 //trailing 0 byte
}

pub fn document<T:Stream<u8>>(stream: &mut T) -> BsonDocument {
	let size = bytesum(stream.aggregate(4)) as i32;
	let mut elemcode = stream.expect(&~[DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,BOOL,UTCDATE,NULL,REGEX,JSCRIPT,JSCOPE,INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
	stream.pass(1);
	let mut ret = BsonDocument::new();
	while elemcode != None {
		let key = cstring(stream);
		let val: Document = match elemcode {
			Some(DOUBLE) => _double(stream),
			Some(STRING) => _string(stream),
			Some(EMBED) => _embed(stream),
			Some(ARRAY) => _array(stream),
			Some(BINARY) => _binary(stream),
			Some(OBJID) => ObjectId(stream.aggregate(12)), 
			Some(BOOL) => _bool(stream),
			Some(UTCDATE) => UTCDate(bytesum(stream.aggregate(8)) as i64),
			Some(NULL) => Null,
			Some(REGEX) => _regex(stream),
			Some(JSCRIPT) => _jscript(stream),
			Some(JSCOPE) => _jscope(stream),
			Some(INT32) => Int32(bytesum(stream.aggregate(4)) as i32),
			Some(TSTAMP) => Timestamp(bytesum(stream.aggregate(8)) as i64),
			Some(INT64) => Int64(bytesum(stream.aggregate(8)) as i64),
			Some(MINKEY) => MinKey,
			Some(MAXKEY) => MaxKey,
			_ => fail!("an invalid element code was found!")
		};
		ret.put(key, val);
		elemcode = stream.expect(&~[DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,BOOL,UTCDATE,NULL,REGEX,JSCRIPT,JSCOPE,INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
		if stream.has_next() { stream.pass(1); }
	}
	ret.size = size;
	ret
}

pub fn cstring<T:Stream<u8>>(stream: &mut T) -> ~str {
	let is_0: &fn(&u8) -> bool = |&x| x == 0x00;
	let s = from_bytes(stream.until(is_0));
	stream.pass(1);
	s
}

pub fn _double<T:Stream<u8>>(stream: &mut T) -> Document {
	//TODO: this doesn't work at all
	let b: ~[u8] = stream.aggregate(8);
	let bytes: [u8,..8] = unsafe { std::cast::transmute(b) };
	let v: f64 = unsafe { std::cast::transmute(bytes) };
	//let s = bytesum(b) as u64;
	//Double(s as f64);
	Double(v)
}

pub fn _string<T:Stream<u8>>(stream: &mut T) -> Document {
	stream.pass(4);
	UString(cstring(stream))
}

pub fn _embed<T:Stream<u8>>(stream: &mut T) -> Document {
	Embedded(~document(stream))	
}

pub fn _array<T:Stream<u8>>(stream: &mut T) -> Document {
	Array(~document(stream))
}

pub fn _binary<T:Stream<u8>>(mut stream: &mut T) -> Document {
	let bytes = stream.aggregate(4);
	let count = bytesum(bytes);
	let subtype = (&mut stream).first();
	stream.pass(1);
	let data = stream.aggregate(count as int);
	Binary(*subtype, data)
}

pub fn _bool<T:Stream<u8>>(stream: &mut T) -> Document {
	let ret = (*stream.first()) as bool;
	stream.pass(1);
	Bool(ret)
}

pub fn _regex<T:Stream<u8>>(stream: &mut T) -> Document {
	let s1 = cstring(stream);
	let s2 = cstring(stream);
	Regex(s1, s2)
}

pub fn _jscript<T:Stream<u8>>(stream: &mut T) -> Document {
	let s = _string(stream);
	//using this to avoid irrefutable pattern error
	match s {
		UString(s) => JScript(s),
		_ => fail!("invalid string in javascript")
	}
}

fn _jscope<T:Stream<u8>>(stream: &mut T) -> Document {
	stream.pass(4);
	let s = cstring(stream);
	let doc = document(stream);
	JScriptWithScope(s, ~doc)
}

#[cfg(test)]
mod tests {
	extern mod bson_types;
	extern mod json_parse;
	extern mod ord_hashmap;

	use super::*;
	use bson_types::*;
	use json_parse::*;
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

	//testing conversions for PureBson implementation
	//MINOR: move these to different testing module
	#[test]
	fn test_string_bson_doc_fmt() {
		assert_eq!(PureJsonString(~"hello").bson_doc_fmt(), UString(~"hello"));
	}

	#[test]
	fn test_list_bson_doc_fmt() {
		let lst = PureJsonList(~[PureJsonBoolean(true), PureJsonString(~"hello")]);
		match lst.bson_doc_fmt() {
			Array(~l1) => { assert_eq!(l1.find(~"0").unwrap(), &Bool(true));
					       assert_eq!(l1.find(~"1").unwrap(), &UString(~"hello")); }
			_ => fail!()
		}
	}

	#[test]
	fn test_object_bson_doc_fmt() {
		let jstring = "{\"foo\": true}";
		let mut doc = BsonDocument::new();
		doc.put(~"foo", Bool(true));
		assert_eq!(ObjParser::from_string::<~[char],PureJson,PureJsonParser<~[char]>>(jstring).bson_doc_fmt(), Embedded(~doc))	
	}

	//testing encode

	#[test]
	fn test_double_encode() {
		let doc = BsonDocument::inst().append(~"foo", Double(3.14159f64));
		assert_eq!(encode(doc), ~[18,0,0,0,1,102,111,111,0,110,134,27,240,249,33,9,64,0]);
	}
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
		let doc = BsonDocument::inst().append(~"foo", Null);

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

	//test decode

	#[test]
	fn test_decode_size() {
		let doc = decode(~[10,0,0,0,10,100,100,100,0]);
		assert_eq!(doc.size, 10);
	}


	#[test]
	fn test_cstring_decode() {
		//the stream needs an extra 0 for this test since in practice, an object _cannot_ end with a cstring; this allows cstring to pass an extra time
		let mut stream: ~[u8] = ~[104,101,108,108,111,0];
		assert_eq!(cstring(&mut stream), ~"hello");
	}
	
	//#[test]
	fn test_double_decode() {
		let mut stream: ~[u8] = ~[110,134,27,240,249,33,9,64];
		let d = _double(&mut stream);
		match d {
			Double(d2) => {
				println(fmt!(":::::::::d2 is %?", d2));
				assert!(d2.approx_eq(&3.14159f64));
			}
			_ => fail!("failed in a test case; how did I get here?")
		}
	}
	#[test]
	fn test_document_decode() {
		let mut stream1: ~[u8] = ~[11,0,0,0,8,102,111,111,0,1,0];
		let mut doc1 = BsonDocument::new();
		doc1.put(~"foo", Bool(true));
		assert_eq!(document(&mut stream1), doc1); 

		let stream2: ~[u8] = ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0];
		let mut inside = BsonDocument::new();
		inside.put_all(~[(~"0", UString(~"hello")), (~"1", Bool(false))]);
		let mut doc2 = BsonDocument::new();
		doc2.put_all(~[(~"foo", Array(~ copy inside)), (~"baz", UString(~"qux"))]);
		assert_eq!(decode(stream2), doc2);
	}

	#[test]
	fn test_binary_decode() {
		let mut stream: ~[u8] = ~[6,0,0,0,0,1,2,3,4,5,6];
		assert_eq!(_binary(&mut stream), Binary(0, ~[1,2,3,4,5,6]));
	}

	//full encode path testing
	#[test]
	fn test_string_whole_encode() {
		let jstring = "{\"foo\": \"bar\"}";
		let doc = BsonDocument::from_formattable(ObjParser::from_string::<~[char], PureJson, PureJsonParser<~[char]>>(jstring));
		assert_eq!(encode(&doc), ~[18,0,0,0,2,102,111,111,0,4,0,0,0,98,97,114,0,0]);
	}

	//#[test]
	fn test_embed_whole_encode() {
		let jstring = "{\"foo\": [\"hello\", false], \"baz\": \"qux\"}";
		let doc = BsonDocument::from_formattable(ObjParser::from_string::<~[char], PureJson, PureJsonParser<~[char]>>(jstring));
		
		assert_eq!(encode(&doc), ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	}
}

