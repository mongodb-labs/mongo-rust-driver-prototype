#[link(name="json_parse", vers="0.1", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];

extern mod ord_hashmap;
extern mod stream;
extern mod extra;
extern mod bson_types;

use std::char::is_digit;
use std::str::{to_chars, from_chars};
use std::float::from_str;
use stream::*;
use ord_hashmap::*;
use bson_types::*;

pub fn from_string(s: &str) -> PureJson {
	let mut stream = to_chars(s);
	stream.pass_while(&~[' ', '\n', '\r', '\t']);
	if !(stream.first() == &'{') {
		fail!("invalid object given!"); 
	}
	_object(&mut stream)
}

pub fn _string<T:Stream<char>>(stream: &mut T) -> PureJson {
	stream.pass(1); //pass over begin quote
	let ret: ~[char] = stream.until(|c| *c == '\"'); 
	stream.pass(1); //pass over end quote
	stream.pass_while(&~[' ', '\n', '\r', '\t']); //pass over trailing whitespace
	PureJsonString(from_chars(ret))
}

pub fn _number<T:Stream<char>>(stream: &mut T) -> PureJson {
	let ret = stream.until(|c| (*c == ',') || std::vec::contains([' ', '\n', '\r', '\t'], c));
	PureJsonNumber(from_str(from_chars(ret)).unwrap())
}

pub fn _bool<T:Stream<char>>(stream: &mut T) -> PureJson {
	let c1 = stream.expect(&~['t', 'f']);
	match c1 {
		Some('t') => { stream.pass(1); 	
				let next = ~['r', 'u', 'e'];
				let mut i = 0;
				while i < 3 {
					let c = stream.expect(&~[next[i]]);
					if c.is_none() { fail!("invalid boolean value while expecting true!"); }
					i += 1;
					stream.pass(1);
				}
				stream.pass_while(&~[' ', '\n', '\r', '\t']);
				PureJsonBoolean(true)
			     }
		Some('f') => { stream.pass(1);
				let next = ~['a', 'l', 's', 'e'];
				let mut i = 0;
				while i < 4 {
					let c = stream.expect(&~[next[i]]);
					if c.is_none() { fail!("invalid boolean value while expecting false!"); }
					i += 1;
					stream.pass(1);
				}
				stream.pass_while(&~[' ', '\n', '\r', '\t']);
				PureJsonBoolean(false)
			     }
		_ => fail!("invalid boolean value!")
	}	
}

pub fn _list<T:Stream<char>>(stream: &mut T) -> PureJson {
	stream.pass(1); //pass over [
	let mut ret: ~[PureJson] = ~[];
	while !(stream.first() == &']') {
		let c = stream.expect(&~['\"', 't', 'f']);
		match c {
			Some('\"') => ret += [_string(stream)],
			Some('t') => ret += [_bool(stream)],
			Some('v') => ret += [_bool(stream)],
			_ => if is_digit(*stream.first()) { ret += [_number(stream)] } else { fail!(fmt!("invalid value found: %?", stream.first())) }
		}
		stream.pass_while(&~[' ', '\n', '\r', '\t']);
		let comma = stream.expect(&~[',', ']']);
		match comma {
			Some(',') => { stream.pass(1); stream.pass_while(&~[' ', '\n', '\r', '\t']); }
			Some(']') => { stream.pass(1); stream.pass_while(&~[' ', '\n', '\r', '\t']); return PureJsonList(ret); }
			_ => fail!(fmt!("invalid value found: %?", stream.first()))
		}
		if !stream.has_next() { break; } //this should only happen during tests
	}
	stream.pass_while(&~[' ', '\n', '\r', '\t']);
	PureJsonList(ret)
}

pub fn _object<T:Stream<char>>(stream: &mut T) -> PureJson {
	stream.pass(1); //pass over brace
	let mut ret: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
	while !(stream.first() == &'}') {
		if stream.expect(&~['\"']).is_none() { fail!("keys must begin with quote marks") }
		let key = match _string(stream) {
			PureJsonString(s) => s,
			_ => fail!("invalid key found")
		};
		stream.pass_while(&~[' ', '\n', '\r', '\t']);
		if stream.expect(&~[':']).is_none() { fail!("keys and values should be separated by :") }
		stream.pass(1); //pass over :
		stream.pass_while(&~[' ', '\n', '\r', '\t']);
		let c = stream.expect(&~['\"', 't', 'f', '[']);
		match c {
			Some('\"') => { ret.insert(key, _string(stream)); }
			Some('t') => { ret.insert(key, _bool(stream)); }
			Some('v') => { ret.insert(key, _bool(stream)); }
			Some('[') => { ret.insert(key, _list(stream)); }
			_ => if is_digit(*stream.first()) { ret.insert(key, _number(stream)); } else { fail!(fmt!("invalid value found: %?", stream.first())) }
		}
		stream.pass_while(&~[' ', '\n', '\r', '\t']);
		let comma = stream.expect(&~[',', '}']);
		if comma.is_none() { fail!("expected ',' after object element") } else {stream.pass(1); stream.pass_while(&~[' ', '\n', '\r', '\t']); }
		if !stream.has_next() { break; }
	}
	stream.pass_while(&~[' ', '\n', '\r', '\t']);
	PureJsonObject(ret)
}

#[cfg(test)]
mod tests {
	extern mod ord_hashmap;

	use super::*;
	use bson_types::*;
	use std::str::to_chars;
	use ord_hashmap::*;

	#[test]
	fn test_string_fmt() {
		let mut stream = to_chars("\"hello\"");
		let val = _string(&mut stream);
		assert_eq!(PureJsonString(~"hello"), val);
	}

	#[test]
	fn test_number_fmt() {
		let mut stream = to_chars("2");
		let val = _number(&mut stream);
		assert_eq!(PureJsonNumber(2f), val);
	}

	#[test]
	fn test_bool_fmt() {
		let mut stream_true = to_chars("true");
		let mut stream_false = to_chars("false");
		
		let val_t = _bool(&mut stream_true);
		let val_f = _bool(&mut stream_false);
	
		assert_eq!(PureJsonBoolean(true), val_t);
		assert_eq!(PureJsonBoolean(false), val_f);
	}

	#[test]
	#[should_fail]
	fn test_invalid_true_fmt() {
		let mut stream = to_chars("tasdf");
		_bool(&mut stream);
	}

	#[test]
	#[should_fail]
	fn test_invalid_false_fmt() {
		let mut stream = to_chars("fasdf");
		_bool(&mut stream);
	}

	#[test]
	#[should_fail]
	fn test_invalid_bool_fmt() {
		let mut stream = to_chars("asdf");
		 _bool(&mut stream);
	}
	#[test]
	fn test_list_fmt() {
		let mut stream = to_chars("[5.01, true, \"hello\"]");
		let val = _list(&mut stream);
	
		assert_eq!(PureJsonList(~[PureJsonNumber(5.01), PureJsonBoolean(true), PureJsonString(~"hello")]), val);
	}

	#[test]
	fn test_object_fmt() {
		let mut stream = to_chars("{\"foo\": true, \"bar\": 2, \"baz\": [\"qux\"]}");
		let mut m: OrderedHashmap<~str, PureJson> = OrderedHashmap::new();
		m.insert(~"foo", PureJsonBoolean(true));
		m.insert(~"bar", PureJsonNumber(2f));
		m.insert(~"baz", PureJsonList(~[PureJsonString(~"qux")]));
		
		assert_eq!(PureJsonObject(m), _object(&mut stream));
	}
}
