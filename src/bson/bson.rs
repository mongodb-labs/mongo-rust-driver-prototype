#[link(name="bson", vers="0.2", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];
extern mod extra;
extern mod stream;
extern mod ord_hashmap;
extern mod json_parse;
extern mod bson_types;

use std::str::from_bytes;
use bson_types::*;
use stream::*;

//TODO: find a way to remove this, see bson.rs:128
#[link_args = "-ltypecast"]
extern {
	fn bytes_to_double(buf: *u8) -> f64;
}

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

///parser object for BSON. T is constrained to Stream<u8>.
pub struct BsonParser<T> {
	stream: T
}

///Collects up to 8 bytes in order as a u64.
priv fn bytesum(bytes: ~[u8]) -> u64 {
	let mut i = 0;
	let mut ret: u64 = 0;
	for bytes.each |&byte| {
		ret += (byte as u64) >> (8 * i);
		i += 1;
	}
	ret
}

impl<T:Stream<u8>> BsonParser<T> {
	///Parse a document. Returns an error string on parse failure. 
	pub fn document(&mut self) -> Result<BsonDocument,~str> {
		let size = bytesum(self.stream.aggregate(4)) as i32;
		let mut elemcode = self.stream.expect(&~[DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,BOOL,UTCDATE,NULL,REGEX,JSCRIPT,JSCOPE,INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
		self.stream.pass(1);
		let mut ret = BsonDocument::new();
		while elemcode != None {
			let key = self.cstring();
			let val: Document = match elemcode {
				Some(DOUBLE) => self._double(),
				Some(STRING) => self._string(),
				Some(EMBED) => {
					let doc = self._embed();
					match doc {
						Ok(d) => d,
						Err(e) => return Err(e)
					}
				}
				Some(ARRAY) => {
					let doc = self._array();
					match doc {
						Ok(d) => d,
						Err(e) => return Err(e)
					}
				}
				Some(BINARY) => self._binary(),
				Some(OBJID) => ObjectId(self.stream.aggregate(12)), 
				Some(BOOL) => self._bool(),
				Some(UTCDATE) => UTCDate(bytesum(self.stream.aggregate(8)) as i64),
				Some(NULL) => Null,
				Some(REGEX) => self._regex(),
				Some(JSCRIPT) => {
					let doc = self._jscript();
					match doc {
						Ok(d) => d,
						Err(e) => return Err(e)
					}
				}
				Some(JSCOPE) => {
					let doc = self._jscope();
					match doc {
						Ok(d) => d,
						Err(e) => return Err(e)
					}
				}
				Some(INT32) => Int32(bytesum(self.stream.aggregate(4)) as i32),
				Some(TSTAMP) => Timestamp(bytesum(self.stream.aggregate(8)) as i64),
				Some(INT64) => Int64(bytesum(self.stream.aggregate(8)) as i64),
				Some(MINKEY) => MinKey,
				Some(MAXKEY) => MaxKey,
				_ => return Err(~"an invalid element code was found")
			};
			ret.put(key, val);
			elemcode = self.stream.expect(&~[DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,BOOL,UTCDATE,NULL,REGEX,JSCRIPT,JSCOPE,INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
			if self.stream.has_next() { self.stream.pass(1); }
		}
		ret.size = size;
		Ok(ret)
	}
	///Parse a string without denoting its length. Mainly for keys.
	pub fn cstring(&mut self) -> ~str {
		let is_0: &fn(&u8) -> bool = |&x| x == 0x00;
		let s = from_bytes(self.stream.until(is_0));
		self.stream.pass(1);
		s
	}
	///Parse a double.
	pub fn _double(&mut self) -> Document {
		let b = self.stream.aggregate(8);
		//TODO this is bad
		let v: f64 = unsafe { 
			bytes_to_double(std::vec::raw::to_ptr(b))
		};
		Double(v)
	}
	///Parse a string with length.
	pub fn _string(&mut self) -> Document {
		self.stream.pass(4); //skip length
		let v = self.cstring();
		UString(v)
	}
	///Parse an embedded object. May fail.
	pub fn _embed(&mut self) -> Result<Document,~str> {
		return self.document().chain(|s| Ok(Embedded(~s)));
	}
	///Parse an embedded array. May fail.
	pub fn _array(&mut self) -> Result<Document,~str> {
		return self.document().chain(|s| Ok(Array(~s)));
	}
	///Parse generic binary data.
	pub fn _binary(&mut self) -> Document {
		let count = bytesum(self.stream.aggregate(4));
		let subtype = *(self.stream.first());
		self.stream.pass(1);
		let data = self.stream.aggregate(count as int);
		Binary(subtype, data)
	}
	///Parse a boolean.
	pub fn _bool(&mut self) -> Document {
		let ret = (*self.stream.first()) as bool;
		self.stream.pass(1);
		Bool(ret)
	}
	///Parse a regex.
	pub fn _regex(&mut self) -> Document {
		let s1 = self.cstring();
		let s2 = self.cstring();
		Regex(s1, s2)
	}
	///Parse a javascript object.
	pub fn _jscript(&mut self) -> Result<Document, ~str> {
		let s = self._string();
		//using this to avoid irrefutable pattern error
		match s {
			UString(s) => Ok(JScript(s)),
			_ => Err(~"invalid string found in javascript")
		}
	}
	///Parse a scoped javascript object.
	pub fn _jscope(&mut self) -> Result<Document,~str> {
		self.stream.pass(4);
		let s = self.cstring();
		let doc = self.document();
		return doc.chain(|d| Ok(JScriptWithScope(copy s,~d)));
	}
	///Create a new parser with a given stream.
	pub fn new(stream: T) -> BsonParser<T> { BsonParser { stream: stream } }
}

///Standalone encode binding.
pub fn encode(doc: &BsonDocument) -> ~[u8] {
	doc.to_bson()
}
///Standalone decode binding.
pub fn decode(b: ~[u8]) -> Result<BsonDocument,~str> {
	let mut parser = BsonParser::new(b);
	parser.document()
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
		assert_eq!((@mut PureJsonParser::new(~[' '])).from_string(jstring).unwrap().bson_doc_fmt(), Embedded(~doc))	
	}

	//testing encode

	#[test]
	fn test_double_encode() {
		let doc = BsonDocument::inst().append(~"foo", Double(3.14159f64));
		assert_eq!(doc.to_bson(), ~[18,0,0,0,1,102,111,111,0,110,134,27,240,249,33,9,64,0]);
	}
	#[test]
	fn test_string_encode() {
		let doc = BsonDocument::inst().append(~"foo", UString(~"bar"));
		assert_eq!(doc.to_bson(), ~[18,0,0,0,2,102,111,111,0,4,0,0,0,98,97,114,0,0]);
	}

	#[test]
	fn test_bool_encode() {
		let doc = BsonDocument::inst().append(~"foo", Bool(true));
		assert_eq!(doc.to_bson(), ~[11,0,0,0,8,102,111,111,0,1,0] );
	}

	#[test]
	fn test_32bit_encode() {
		let doc = BsonDocument::inst().append(~"foo", Int32(56 as i32));
		assert_eq!(doc.to_bson(), ~[14,0,0,0,16,102,111,111,0,56,0,0,0,0]);
	}

	#[test]
	fn test_embed_encode() {
		//lists
		let mut inside = BsonDocument::new();
		inside.put_all(~[(~"0", UString(~"hello")), (~"1", Bool(false))]);
		let mut doc2 = BsonDocument::new();
		doc2.put_all(~[(~"foo", Array(~ copy inside)), (~"baz", UString(~"qux"))]);

		assert_eq!(doc2.to_bson(), ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
		
		//embedded objects
		let mut doc3 = BsonDocument::new();
		doc3.put_all(~[(~"foo", Embedded(~ copy inside)), (~"baz", UString(~"qux"))]);	
		
		assert_eq!(doc3.to_bson(), ~[45,0,0,0,3,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	
		let mut doc4 = BsonDocument::new();
		doc4.put_all(~[(~"foo", JScriptWithScope(~"wat", ~ copy inside)), (~"baz", UString(~"qux"))]);	
		assert_eq!(doc4.to_bson(), ~[53,0,0,0,15,102,111,111,0,30,0,0,0,119,97,116,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	

	}

	#[test]
	fn test_64bit_encode() {
		let doc1 = BsonDocument::inst().append(~"foo", UTCDate(4040404 as i64));
		assert_eq!(doc1.to_bson(), ~[18,0,0,0,9,102,111,111,0,212,166,61,0,0,0,0,0,0] );	

		let doc2 = BsonDocument::inst().append(~"foo", Int64(4040404 as i64));
		assert_eq!(doc2.to_bson(), ~[18,0,0,0,18,102,111,111,0,212,166,61,0,0,0,0,0,0] );	

		let doc3 = BsonDocument::inst().append(~"foo", Timestamp(4040404 as i64));
		assert_eq!(doc3.to_bson(), ~[18,0,0,0,17,102,111,111,0,212,166,61,0,0,0,0,0,0] );	
	}

	#[test]
	fn test_null_encode() {
		let doc = BsonDocument::inst().append(~"foo", Null);

		assert_eq!(doc.to_bson(), ~[10,0,0,0,10,102,111,111,0,0]);
	}

	#[test]
	fn test_regex_encode() {
		let doc = BsonDocument::inst().append(~"foo", Regex(~"bar", ~"baz"));

		assert_eq!(doc.to_bson(), ~[18,0,0,0,11,102,111,111,0,98,97,114,0,98,97,122,0,0]);	
	}

	#[test]
	fn test_jscript_encode() {
		let doc = BsonDocument::inst().append(~"foo", JScript(~"return 1;"));
		assert_eq!(doc.to_bson(), ~[24,0,0,0,13,102,111,111,0,10,0,0,0,114,101,116,117,114,110,32,49,59,0,0]);
	}
	#[test]
	fn test_valid_objid_encode() {
		let doc = BsonDocument::inst().append(~"foo", ObjectId(~[0,1,2,3,4,5,6,7,8,9,10,11]));
	
		assert_eq!(doc.to_bson(), ~[22,0,0,0,7,102,111,111,0,0,1,2,3,4,5,6,7,8,9,10,11,0]);
	}

	#[test]
	#[should_fail]
	fn test_invalid_objid_encode() {
		let doc = BsonDocument::inst().append(~"foo", ObjectId(~[1,2,3]));
		doc.to_bson();
	}

	#[test]
	fn test_multi_encode() {

		let doc = BsonDocument::inst().append(~"foo", Bool(true)).append(~"bar", UString(~"baz")).append(~"qux", Int32(404));

		let enc = doc.to_bson();
	
		assert_eq!(enc, ~[33,0,0,0,8,102,111,111,0,1,2,98,97,114,0,4,0,0,0,98,97,122,0,16,113,117,120,0,148,1,0,0,0]);
	}

	//test decode

	#[test]
	fn test_decode_size() {
		let doc = decode(~[10,0,0,0,10,100,100,100,0]);
		assert_eq!(doc.unwrap().size, 10);
	}


	#[test]
	fn test_cstring_decode() {
		//the stream needs an extra 0 for this test since in practice, an object _cannot_ end with a cstring; this allows cstring to pass an extra time
		let stream: ~[u8] = ~[104,101,108,108,111,0];
		let mut parser = BsonParser::new(stream);
		assert_eq!(parser.cstring(), ~"hello");
	}
	
	#[test]
	fn test_double_decode() {
		let stream: ~[u8] = ~[110,134,27,240,249,33,9,64];
		let mut parser = BsonParser::new(stream);
		let d = parser._double();
		match d {
			Double(d2) => {
				assert!(d2.approx_eq(&3.14159f64));
			}
			_ => fail!("failed in a test case; how did I get here?")
		}
	}
	#[test]
	fn test_document_decode() {
		let stream1: ~[u8] = ~[11,0,0,0,8,102,111,111,0,1,0];
		let mut parser1 = BsonParser::new(stream1);
		let mut doc1 = BsonDocument::new();
		doc1.put(~"foo", Bool(true));
		assert_eq!(parser1.document().unwrap(), doc1); 

		let stream2: ~[u8] = ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0];
		let mut inside = BsonDocument::new();
		inside.put_all(~[(~"0", UString(~"hello")), (~"1", Bool(false))]);
		let mut doc2 = BsonDocument::new();
		doc2.put_all(~[(~"foo", Array(~ copy inside)), (~"baz", UString(~"qux"))]);
		assert_eq!(decode(stream2).unwrap(), doc2);
	}

	#[test]
	fn test_binary_decode() {
		let stream: ~[u8] = ~[6,0,0,0,0,1,2,3,4,5,6];
		let mut parser = BsonParser::new(stream);
		assert_eq!(parser._binary(), Binary(0, ~[1,2,3,4,5,6]));
	}

	//full encode path testing
	#[test]
	fn test_string_whole_encode() {
		let jstring = "{\"foo\": \"bar\"}";
		let doc = BsonDocument::from_formattable((@mut PureJsonParser::new(~[' '])).from_string(jstring).unwrap());
		assert_eq!(encode(&doc), ~[18,0,0,0,2,102,111,111,0,4,0,0,0,98,97,114,0,0]);
	}

	//#[test]
	fn test_embed_whole_encode() {
		let jstring = "{\"foo\": [\"hello\", false], \"baz\": \"qux\"}";
		let doc = BsonDocument::from_formattable((@mut PureJsonParser::new(~[' '])).from_string(jstring).unwrap());
		
		assert_eq!(encode(&doc), ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,113,117,120,0,0]);
	}
}

