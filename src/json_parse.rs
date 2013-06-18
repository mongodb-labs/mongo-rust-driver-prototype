#[link(name="json_parse", vers="0.1", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];

extern mod ord_hashmap;
extern mod stream;
extern mod extra;
extern mod bson_types;

use std::char::is_digit;
use std::str::from_chars;
use std::float::from_str;
use stream::*;
use ord_hashmap::*;
use bson_types::*;

//This trait is for parsing non-BSON object notations such as JSON, XML, etc.
pub trait ObjParser<T:Stream<char>, V> {
	pub fn from_string(&self, s: &str) -> V;
}

pub struct PureJsonParser<T> {
	stream: T
}

impl<T:Stream<char>> ObjParser<T, PureJson> for PureJsonParser<T> {
	pub fn from_string(&self, s: &str) -> PureJson {
		let mut stream = s.iter().collect::<~[char]>();
		stream.pass_while(&~[' ', '\n', '\r', '\t']);
		if !(stream.first() == &'{') {
			fail!("invalid object given!"); 
		}
		let mut parser = PureJsonParser::new(stream);
		parser.object()
	}
}

impl<T:Stream<char>> PureJsonParser<T> {
	pub fn object(&mut self) -> PureJson {
		self.stream.pass(1); //pass over brace
		let mut ret: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		while !(self.stream.first() == &'}') {
			if self.stream.expect(&~['\"']).is_none() { fail!("keys must begin with quote marks") }
			let key = match self._string() {
				PureJsonString(s) => s,
				_ => fail!("invalid key found")
			};
			self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
			if self.stream.expect(&~[':']).is_none() { fail!("keys and values should be separated by :") }
			self.stream.pass(1); //pass over :
			self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
			let c = self.stream.expect(&~['\"', 't', 'f', '[', '{']);
			match c {
				Some('\"') => { ret.insert(key, self._string()); }
				Some('t') => { ret.insert(key, self._bool()); }
				Some('v') => { ret.insert(key, self._bool()); }
				Some('[') => { ret.insert(key, self._list()); }
				Some('{') => {
					let obj = self.object();
					let id = PureJsonParser::_objid::<T>(&obj);
					if id.is_none() { ret.insert(key, id.unwrap()); }
					else { ret.insert(key, obj); }
				}
				_ => if is_digit(*self.stream.first()) { ret.insert(key, self._number()); } else { fail!(fmt!("invalid value found: %?", self.stream.first())) }
			}
			self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
			let comma = self.stream.expect(&~[',', '}']);
			if comma.is_none() { fail!("expected ',' after object element") } else {self.stream.pass(1); self.stream.pass_while(&~[' ', '\n', '\r', '\t']); }
			if !self.stream.has_next() { break; }
		}
		self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
		PureJsonObject(ret)
	}
	pub fn _string(&mut self) -> PureJson {
		self.stream.pass(1); //pass over begin quote
		let ret: ~[char] = self.stream.until(|c| *c == '\"'); 
		self.stream.pass(1); //pass over end quote
		self.stream.pass_while(&~[' ', '\n', '\r', '\t']); //pass over trailing whitespace
		PureJsonString(from_chars(ret))
	}

	pub fn _number(&mut self) -> PureJson {
		let ret = self.stream.until(|c| (*c == ',') || std::vec::contains([' ', '\n', '\r', '\t'], c));
		PureJsonNumber(from_str(from_chars(ret)).unwrap())
	}

	pub fn _bool(&mut self) -> PureJson {
		let c1 = self.stream.expect(&~['t', 'f']);
		match c1 {
			Some('t') => { self.stream.pass(1); 	
					let next = ~['r', 'u', 'e'];
					let mut i = 0;
					while i < 3 {
						let c = self.stream.expect(&~[next[i]]);
						if c.is_none() { fail!("invalid boolean value while expecting true!"); }
						i += 1;
						self.stream.pass(1);
					}
					self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
					PureJsonBoolean(true)
				     }
			Some('f') => { self.stream.pass(1);
					let next = ~['a', 'l', 's', 'e'];
					let mut i = 0;
					while i < 4 {
						let c = self.stream.expect(&~[next[i]]);
						if c.is_none() { fail!("invalid boolean value while expecting false!"); }
						i += 1;
						self.stream.pass(1);
					}
					self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
					PureJsonBoolean(false)
				     }
			_ => fail!("invalid boolean value!")
		}	
	}

	pub fn _list(&mut self) -> PureJson {
		self.stream.pass(1); //pass over [
		let mut ret: ~[PureJson] = ~[];
		while !(self.stream.first() == &']') {
			let c = self.stream.expect(&~['\"', 't', 'f']);
			match c {
				Some('\"') => ret.push(self._string()),
				Some('t') => ret.push(self._bool()),
				Some('v') => ret.push(self._bool()),
				_ => if is_digit(*self.stream.first()) { ret.push(self._number()) } else { fail!(fmt!("invalid value found: %?", self.stream.first())) }
			}
			self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
			let comma = self.stream.expect(&~[',', ']']);
			match comma {
				Some(',') => { self.stream.pass(1); self.stream.pass_while(&~[' ', '\n', '\r', '\t']); }
				Some(']') => { self.stream.pass(1); self.stream.pass_while(&~[' ', '\n', '\r', '\t']); return PureJsonList(ret); }
				_ => fail!(fmt!("invalid value found: %?", self.stream.first()))
			}
			if !self.stream.has_next() { break; } //this should only happen during tests
		}
		self.stream.pass_while(&~[' ', '\n', '\r', '\t']);
		PureJsonList(ret)
	}

	pub fn _objid(json: &PureJson) -> Option<PureJson> {
		match *json {
			PureJsonObject(ref m) => {
				if m.len() == 1 && m.contains_key(&~"$oid") {
					match *(m.find(&~"$oid").unwrap()) {
						PureJsonString(ref st) => return Some(PureJsonObjID(st.bytes_iter().collect::<~[u8]>())),
						_ => fail!("invalid objid found!")
					}
				}
			}
			_ => fail!("invalid json string being objid checked")
		}
		None
	}

	pub fn new(stream: T) -> PureJsonParser<T> { PureJsonParser {stream: stream} }
}

#[cfg(test)]
mod tests {
	extern mod ord_hashmap;
	extern mod bson_types;
	
	use super::*;
	use bson_types::*;
	use ord_hashmap::*;

	#[test]
	fn test_string_fmt() {
		let stream = "\"hello\"".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let val = parser._string();
		assert_eq!(PureJsonString(~"hello"), val);
	}

	#[test]
	fn test_number_fmt() {
		let stream = "2".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let val = parser._number();
		assert_eq!(PureJsonNumber(2f), val);
	}

	#[test]
	fn test_bool_fmt() {
		let stream_true = "true".iter().collect::<~[char]>();
		let stream_false = "false".iter().collect::<~[char]>();
		let mut parse_true = PureJsonParser::new(stream_true);
		let mut parse_false = PureJsonParser::new(stream_false);	
		let val_t = parse_true._bool();
		let val_f = parse_false._bool();
	
		assert_eq!(PureJsonBoolean(true), val_t);
		assert_eq!(PureJsonBoolean(false), val_f);
	}

	#[test]
	#[should_fail]
	fn test_invalid_true_fmt() {
		let stream = "tasdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		parser._bool();
	}

	#[test]
	#[should_fail]
	fn test_invalid_false_fmt() {
		let stream = "fasdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		parser._bool();
	}

	#[test]
	#[should_fail]
	fn test_invalid_bool_fmt() {
		let stream = "asdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		parser._bool();
	}
	#[test]
	fn test_list_fmt() {
		let stream = "[5.01, true, \"hello\"]".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let val = parser._list();
	
		assert_eq!(PureJsonList(~[PureJsonNumber(5.01), PureJsonBoolean(true), PureJsonString(~"hello")]), val);
	}

	#[test]
	fn test_object_fmt() {
		let stream = "{\"foo\": true, \"bar\": 2, \"baz\": [\"qux\"]}".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let mut m: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		m.insert(~"foo", PureJsonBoolean(true));
		m.insert(~"bar", PureJsonNumber(2f));
		m.insert(~"baz", PureJsonList(~[PureJsonString(~"qux")]));
		
		assert_eq!(PureJsonObject(m), parser.object());
	}

	#[test]
	fn test_objid_fmt() {
		let stream = "{\"$oid\": \"abcdefg\"}".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let v = parser.object();
		let val = PureJsonParser::_objid::<~[char]>(&v).unwrap();
		
		assert_eq!(PureJsonObjID("abcdefg".bytes_iter().collect::<~[u8]>()), val);
	}
}
