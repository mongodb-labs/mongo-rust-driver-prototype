use std::char::is_digit;
use std::str::from_chars;
use std::float::from_str;
use stream::*;
use ord_hash::*;
use bson_types::*;
use std::vec::contains;

///This trait is for parsing non-BSON object notations such as JSON, XML, etc.
pub trait ObjParser<T:Stream<char>, V> {
	pub fn from_string(&mut self, s: &str) -> Result<V,~str>;
}
///JSON parsing struct. T is a Stream<char>.
pub struct PureJsonParser<T> {
	stream: T
}
///Publicly exposes from_string.
impl ObjParser<~[char], PureJson> for PureJsonParser<~[char]> {
	pub fn from_string(&mut self, s: &str) -> Result<PureJson,~str> {
		let mut stream = s.iter().collect::<~[char]>();
		stream.pass_while(&[' ', '\n', '\r', '\t']);
		if !(stream.first() == &'{') {
			return Err(~"invalid json string found!");
		}
		self.stream = stream;
		self.object()
	}
}

///Main parser implementation for JSON
impl<T:Stream<char>> PureJsonParser<T> {
	///Parse an object. Returns an error string on parse failure
	pub fn object(&mut self) -> Result<PureJson,~str> {
		self.stream.pass(1); //pass over brace
		let mut ret: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		while !(self.stream.first() == &'}') {
			self.stream.pass_while(&[' ', '\n', '\r', '\t']);
			if self.stream.expect(&['\"']).is_none() { println(fmt!("stream head is: %?\n", self.stream.first())); return Err(~"keys must begin with quote marks"); }
			let key = match self._string() {
				PureJsonString(s) => s,
				_ => fail!("invalid key found")//TODO
			};
			self.stream.pass_while(&[' ', '\n', '\r', '\t']);
			if self.stream.expect(&[':']).is_none() { return Err(~"keys and values should be separated by :"); }
			self.stream.pass(1); //pass over :
			self.stream.pass_while(&[' ', '\n', '\r', '\t']);
			let c = self.stream.expect(&['\"', 't', 'f', '[', '{']);
			match c {
				Some('\"') => { ret.insert(key, self._string()); }
				Some('t') => { 
					let b = self._bool();
					match b {
						Ok(bl) => { ret.insert(key, bl); }
						Err(e) => return Err(e)	
					}
				}
				Some('f') => { 
					let b = self._bool();
					match b {
						Ok(bl) => { ret.insert(key, bl); }
						Err(e) => return Err(e)
					}
				}
				Some('[') => {
					let l = self._list();
					match l {
						Ok(ls) => { ret.insert(key, ls); }
						Err(e) => return Err(e)
					} 
				}
				Some('{') => {
					let o = self.object();
					if o.is_err() { return o; }
					let obj = o.unwrap();
					let id = PureJsonParser::_objid::<T>(&obj);
					if !id.is_none() { ret.insert(key, id.unwrap()); }
					else { ret.insert(key, obj); }
				}
				_ => if is_digit(*self.stream.first()) { ret.insert(key, self._number()); } else { return Err(fmt!("invalid value found: %?", self.stream.first())); }
			}
			self.stream.pass_while(&[' ', '\n', '\r', '\t']);
			let comma = self.stream.expect(&[',', '}']);
			match comma { 
				Some(',') => { self.stream.pass(1); self.stream.pass_while(&[' ', '\n', '\r', '\t']) }
				Some('}') => { self.stream.pass(1); self.stream.pass_while(&[' ', '\n', '\r', '\t']); return Ok(PureJsonObject(ret)); }
				_ => return Err(~"invalid end to object: expecting , or }")
			}
			if !self.stream.has_next() { break; }
		}
		self.stream.pass_while(&[' ', '\n', '\r', '\t']);
		Ok(PureJsonObject(ret))
	}
	///Parse a string.
	pub fn _string(&mut self) -> PureJson {
		self.stream.pass(1); //pass over begin quote
		let ret: ~[char] = self.stream.until(|c| *c == '\"'); 
		self.stream.pass(1); //pass over end quote
		self.stream.pass_while(&[' ', '\n', '\r', '\t']); //pass over trailing whitespace
		PureJsonString(from_chars(ret))
	}
	///Parse a number; converts it to float.
	pub fn _number(&mut self) -> PureJson {
		let ret = self.stream.until(|c| (*c == ',') || contains([' ', '\n', '\r', '\t', ']', '}'], c));
		PureJsonNumber(from_str(from_chars(ret)).unwrap())
	}
	///Parse a boolean. Errors for values other than 'true' or 'false'.
	pub fn _bool(&mut self) -> Result<PureJson,~str> {
		let c1 = self.stream.expect(&['t', 'f']);
		match c1 {
			Some('t') => { self.stream.pass(1); 	
					let next = ~['r', 'u', 'e'];
					let mut i = 0;
					while i < 3 {
						let c = self.stream.expect(&[next[i]]);
						if c.is_none() { return Err(~"invalid boolean value while expecting true!"); }
						i += 1;
						self.stream.pass(1);
					}
					self.stream.pass_while(&[' ', '\n', '\r', '\t']);
					Ok(PureJsonBoolean(true))
				     }
			Some('f') => { self.stream.pass(1);
					let next = ~['a', 'l', 's', 'e'];
					let mut i = 0;
					while i < 4 {
						let c = self.stream.expect(&[next[i]]);
						if c.is_none() { return Err(~"invalid boolean value while expecting false!"); }
						i += 1;
						self.stream.pass(1);
					}
					self.stream.pass_while(&[' ', '\n', '\r', '\t']);
					Ok(PureJsonBoolean(false))
				     }
			_ => return Err(~"invalid boolean value!")
		}	
	}
	///Parse null. Errors for values other than 'null'.
	pub fn _null(&mut self) -> Result<PureJson,~str> {
		let c1 = self.stream.expect(&['n']);
		match c1 {
			Some('n') => { self.stream.pass(1);
				let next = ~['u', 'l', 'l'];
				let mut i = 0;
				while i < 3 {
					let c = self.stream.expect(&[next[i]]);
					if c.is_none() { return Err(~"invalid null value!"); }
					i += 1;
					self.stream.pass(1);
				}
				self.stream.pass_while(&[' ', '\n', '\r', '\t']);
				Ok(PureJsonNull)
			}
			_ => return Err(~"invalid null value!")
		}
	}
	///Parse a list.
	pub fn _list(&mut self) -> Result<PureJson,~str> {
		self.stream.pass(1); //pass over [
		let mut ret: ~[PureJson] = ~[];
		while !(self.stream.first() == &']') {
			let c = self.stream.expect(&['\"', 't', 'f', '[', '{']);
			match c {
				Some('\"') => ret.push(self._string()),
				Some('t') => {
					let b = self._bool();
					match b {
						Ok(bl) => ret.push(bl),
						Err(e) => return Err(e)
					}
				}
				Some('f') => {
					let b = self._bool();
					match b {
						Ok(bl) => ret.push(bl),
						Err(e) => return Err(e)
					}
				}
				Some('[') => {
					let l = self._list();
					match l {
						Ok(ls) => ret.push(ls),
						Err(e) => return Err(e)
					}
				}
				Some('{') => {
					let o = self.object();
					if o.is_err() { return o; }
					let obj = o.unwrap();
					let id = PureJsonParser::_objid::<T>(&obj);
					if !id.is_none() { ret.push(id.unwrap()); }
					else { ret.push(obj); }
				}
				_ => if is_digit(*self.stream.first()) { ret.push(self._number()) } else { return Err(fmt!("invalid value found: %?", self.stream.first())); }
			}
			self.stream.pass_while(&[' ', '\n', '\r', '\t']);
			let comma = self.stream.expect(&[',', ']']);
			match comma {
				Some(',') => { self.stream.pass(1); self.stream.pass_while(&[' ', '\n', '\r', '\t']); }
				Some(']') => { self.stream.pass(1); self.stream.pass_while(&[' ', '\n', '\r', '\t']); return Ok(PureJsonList(ret)); }
				_ => return Err(fmt!("invalid value found: %?", self.stream.first()))
			}
			if !self.stream.has_next() { break; } //this should only happen during tests
		}
		self.stream.pass_while(&[' ', '\n', '\r', '\t']);
		Ok(PureJsonList(ret))
	}
	///If this object was an $oid, return an ObjID.
	pub fn _objid(json: &PureJson) -> Option<PureJson> {
		match *json {
			PureJsonObject(ref m) => {
				if m.len() == 1 && m.contains_key(&~"$oid") {
					match (m.find(&~"$oid")) {
						Some(&PureJsonString(ref st)) => return Some(PureJsonObjID(st.bytes_iter().collect::<~[u8]>())),
						_ => return None //fail more silently here
					}
				}
			}
			_ => return None
		}
		None
	}

	///Return a new JSON parser with a given stream.
	pub fn new(stream: T) -> PureJsonParser<T> { PureJsonParser {stream: stream} }
}

#[cfg(test)]
mod tests {

	use super::*;
	use bson_types::*;
	use ord_hash::*;

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
		let val_t = parse_true._bool().unwrap();
		let val_f = parse_false._bool().unwrap();
	
		assert_eq!(PureJsonBoolean(true), val_t);
		assert_eq!(PureJsonBoolean(false), val_f);
	}

	#[test]
	#[should_fail]
	fn test_invalid_true_fmt() {
		let stream = "tasdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		if parser._bool().is_err() { fail!("invalid_true_fmt") }
	}

	#[test]
	#[should_fail]
	fn test_invalid_false_fmt() {
		let stream = "fasdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		if parser._bool().is_err() { fail!("invalid_false_fmt") }
	}

	#[test]
	#[should_fail]
	fn test_invalid_bool_fmt() {
		let stream = "asdf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		if parser._bool().is_err() { fail!("invalid_bool_fmt") }
	}
	#[test]
	fn test_list_fmt() {
		let stream = "[5.01, true, \"hello\"]".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let val = parser._list().unwrap();
	
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
		
		assert_eq!(PureJsonObject(m), parser.object().unwrap());
	}

	#[test]
	fn test_objid_fmt() {
		let stream = "{\"$oid\": \"abcdefg\"}".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let v = parser.object();
		let val = PureJsonParser::_objid::<~[char]>(&(v.unwrap())).unwrap();
		
		assert_eq!(PureJsonObjID("abcdefg".bytes_iter().collect::<~[u8]>()), val);
	}

	#[test]
	fn test_nested_obj_fmt() {
		let stream = "{\"qux\": {\"foo\": 5.0, \"bar\": \"baz\"}, \"fizzbuzz\": false}".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		let mut m1: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		m1.insert(~"foo", PureJsonNumber(5.0));
		m1.insert(~"bar", PureJsonString(~"baz"));
		
		let mut m2: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		m2.insert(~"qux", PureJsonObject(m1));
		m2.insert(~"fizzbuzz", PureJsonBoolean(false));
	
		let v = parser.object();
		match v {
			Ok(_) => { }
			Err(e) => { fail!(fmt!("Object with internal object failed: %s", e)) }
		}

		assert_eq!(PureJsonObject(m2), v.unwrap());
	}

	#[test]
	fn test_nested_list_fmt() {
		//list with object inside
		let stream1 = "[5.0, {\"foo\": true}, \"bar\"]".iter().collect::<~[char]>();
		let mut parser1 = PureJsonParser::new(stream1);
		let mut m = OrderedHashmap::new();
		m.insert(~"foo", PureJsonBoolean(true));
		let v1 = parser1._list();
		match v1 {
			Ok(_) => { }
			Err(e) => { fail!(fmt!("List with internal object failed: %s", e)) }
		}
		
		assert_eq!(PureJsonList(~[PureJsonNumber(5.0), PureJsonObject(m), PureJsonString(~"bar")]), v1.unwrap());
	
		//list with list inside
		let stream2 = "[5.0, [true, false], \"foo\"]".iter().collect::<~[char]>();
		let mut parser2 = PureJsonParser::new(stream2);	
		let v2 = parser2._list();
		match v2 {
			Ok(_) => { }
			Err(e) => { fail!(fmt!("List with internal list failed: %s", e)) }
		}
		
		assert_eq!(PureJsonList(~[PureJsonNumber(5.0), PureJsonList(~[PureJsonBoolean(true), PureJsonBoolean(false)]), PureJsonString(~"foo")]), v2.unwrap());
	}

	#[test]
	fn test_null_fmt() {
		let stream = "null".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		assert_eq!(PureJsonNull, parser._null().unwrap());
		
	}

	#[test]
	#[should_fail]
	fn test_invalid_null_fmt() {
		let stream = "nulf".iter().collect::<~[char]>();
		let mut parser = PureJsonParser::new(stream);
		if parser._null().is_err() { fail!("invalid null value") }
	}
}
