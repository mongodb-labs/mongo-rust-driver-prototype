#[link(name="cursor", vers="0.2", author="jaoke.chinlee@10gen.com, austin.estep@10gen.com")];
#[crate_type="lib"]

extern mod extra;
extern mod bson;
extern mod bson_types;
extern mod json_parse;

use extra::deque::Deque; 
use bson::*;
use bson_types::*;
use json_parse::*;

//TODO temporary
pub struct MongoCollection;

///Structure representing a cursor
pub struct Cursor {
	id : i64,
	collection : @MongoCollection,
	flags : i32, // tailable, slave_ok, oplog_replay, no_timeout, await_data, exhaust, partial, can set during find() too
	skip : i32,
	limit : i32,
	open : bool,
	query_spec : BsonDocument,
	data : Deque<BsonDocument>
}

/**
* Enumeration allowing "overloading" of various property addition methods.
* Functions like hint() and sort() take a QuerySpec, meaning users can pass
* either a pre-constructed BsonDocument or a string in any BsonFormattable notation.
*/
pub enum QuerySpec {
	SpecObj(BsonDocument),
	SpecNotation(~str)
}

///Iterator implementation, opens access to powerful functions like collect, advance, map, etc.
impl Iterator<BsonDocument> for Cursor {
	pub fn next(&mut self) -> Option<BsonDocument> {
		if !self.has_next() || !self.open {
			return None;
		}
		Some(self.data.pop_front())
	}
}
///Cursor API
impl Cursor {
	pub fn new(collection: @MongoCollection, flags: i32) -> Cursor {
		Cursor {
			id: 0, //TODO
			collection: collection,
			flags: flags,
			skip: 0,
			limit: 0,
			open: true,
			query_spec: BsonDocument::new(),
			data: Deque::new::<BsonDocument>() 
		}
	}
	fn explain(&mut self, explain: bool) {
		let mut doc = BsonDocument::new();
		doc.put(~"explain", Bool(explain));
		self.add_query_spec(&doc);
	}
	pub fn hint(&mut self, index: QuerySpec) -> Result<~str,~str> {
		match index {
			SpecObj(doc) => {
				let mut hint = BsonDocument::new();
				hint.put(~"$hint", Embedded(~doc));
				self.add_query_spec(&hint);
				Ok(~"added hint to query spec")
			}
			SpecNotation(ref s) => {
				let mut parser = PureJsonParser::new(~[]);
				let obj = parser.from_string(copy *s);
				match obj {
					Ok(o) => return self.hint(SpecObj(BsonDocument::from_formattable(o))),
					Err(e) => return Err(e)
				}
			}
		}
	} 
	pub fn limit<'a>(&'a mut self, n : int) -> &'a mut Cursor { 
		self.limit = n as i32; self
	}
	pub fn skip<'a>(&'a mut self, n : int) -> &'a mut Cursor { 
		self.skip = n as i32; self
	}
	pub fn has_next(&self) -> bool {
		!self.data.is_empty()
	}
	pub fn close(&mut self) {
		//self.collection.db.connection.close_cursor(self.id);
		self.open = false
	}
	///Add a flag with a bitmask
	pub fn add_flag(&mut self, mask: i32) {
		self.flags |= mask;
	}
	///Remove a flag with a bitmask
	pub fn remove_flag(&mut self, mask: i32) {
		self.flags &= !mask;
	}
	fn add_query_spec(&mut self, doc: &BsonDocument) {
		for doc.fields.each |&k, &v| {
			self.query_spec.put(k,v);
		}
	}
	/*fn send_request(&mut self, Message) -> Result<~str, ~str>{
		if self.open {
			if self.has_next() {
				getmore = self.collection.get_more(self.limit);
				dbresult = self.collection.db.send_msg(getmore); //ideally the db class would parse the OP_REPLY and give back a result
				match dbresult {
					Ok(docs) => {
						for docs.iter().advance |&doc| {
							self.data.add_back(doc);
						}
					}
					Err(e) => return Err(e)
				}
				Ok(~"success")
			}
			
		}
		else {
			Err(~"cannot send a request through a closed cursor")
		}
	}*/
}

#[cfg(test)]
mod tests {
	extern mod bson;
	extern mod bson_types;
	extern mod extra;

	use super::*; 
	use bson::*;
	use bson_types::*;

	#[test]
	fn test_add_index_obj() {
		let mut doc = BsonDocument::new();
		doc.put(~"foo", Double(1f64));
		let mut cursor = Cursor::new(@MongoCollection, 10i32);
		cursor.hint(SpecObj(doc));
	
		let mut spec = BsonDocument::new();	
		let mut speci = BsonDocument::new();
		speci.put(~"foo", Double(1f64));
		spec.put(~"$hint", Embedded(~speci));

		assert_eq!(cursor.query_spec, spec);
	}
	#[test]
	fn test_add_index_str() {
		let hint = ~"{\"foo\": 1}";
		let mut cursor = Cursor::new(@MongoCollection, 10i32);
		cursor.hint(SpecNotation(hint));

		let mut spec = BsonDocument::new();
		let mut speci = BsonDocument::new();
		speci.put(~"foo", Double(1f64));
		spec.put(~"$hint", Embedded(~speci));
		
		assert_eq!(cursor.query_spec, spec);
	}	
}
