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

pub enum QueryIndex {
	IndexObj(BsonDocument),
	IndexSpecifier(~str)
}

impl Iterator<BsonDocument> for Cursor {
	pub fn next(&mut self) -> Option<BsonDocument> {
		if !self.has_next() || !self.open {
			return None;
		}
		Some(self.data.pop_front())
	}
}

impl Cursor {
    //fn explain(&self)/* -> Json */ { }

    //fn sort(&self/*, order : Json*/) -> MongoCursor { cursor_tmp() }
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
	pub fn hint(&mut self, index: QueryIndex) -> Result<~str,~str> {
		match index {
			IndexObj(doc) => {
				for doc.fields.each |&k, &v| {
					self.query_spec.put(k,v);
				}
				Ok(~"added hint to query spec")
			}
			IndexSpecifier(ref s) => {
				let mut parser = PureJsonParser::new(~[]);
				let obj = parser.from_string(copy *s);
				match obj {
					Ok(o) => return self.hint(IndexObj(BsonDocument::from_formattable(o))),
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
		self.open = false
	}
	pub fn add_flag(&mut self, mask: i32) {
		self.flags |= mask;
	}
	pub fn remove_flag(&mut self, mask: i32) {
		self.flags &= !mask;
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
	fn test_add_index() {
		let mut doc = BsonDocument::new();
		doc.put(~"foo", Double(1f64));
		let hint = ~"{\"bar\": 1}"; 
		let mut cursor = Cursor::new(@MongoCollection, 10 as i32);
		cursor.hint(IndexObj(doc));
		cursor.hint(IndexSpecifier(hint));
		
		let mut spec = BsonDocument::new();
		spec.put(~"foo", Double(1f64));
		spec.put(~"bar", Double(1f64));

		assert_eq!(cursor.query_spec, spec);
	}	
}
