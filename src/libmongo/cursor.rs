use extra::deque::Deque; 
use bson::bson_types::*;
use bson::json_parse::*;
//use std::cmp::min;
use util::*;
//use coll::Collection;

//TODO temporary
pub struct Collection;

///Structure representing a cursor
pub struct Cursor {
	id : i64,
	//collection : @Collection,
	collection : Option<@Collection>,   // TODO temporory so tests pass
	flags : i32, // tailable, slave_ok, oplog_replay, no_timeout, await_data, exhaust, partial, can set during find() too
	skip : i32,
	limit : i32,
	open : bool,
	retrieved: i32,
	batch_size: i32,
	query_spec : BsonDocument,
	data : Deque<BsonDocument>
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
	pub fn new(collection : Option<@Collection>, id : i64, n : i32, flags : i32, vec : ~[BsonDocument]) -> Cursor {
        let mut docs = Deque::new::<BsonDocument>();
        for vec.iter().advance |&doc| {
            docs.add_back(doc);
        }

		Cursor {
			id: id,
			collection: collection,
			flags: flags,
			skip: 0,
			limit: 0,
			open: true,
			retrieved: n,
			batch_size: 0,
			query_spec: BsonDocument::new(),
			data: docs,
		}
	}
	pub fn explain(&mut self, explain: bool) {
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
				let obj = ObjParser::from_string::<PureJson, PureJsonParser<~[char]>>(copy *s);
				match obj {
					Ok(o) => return self.hint(SpecObj(BsonDocument::from_formattable(o))),
					Err(e) => return Err(e)
				}
			}
		}
	}
	pub fn sort(&mut self, orderby: QuerySpec) -> Result<~str,~str> {
		match orderby {
			SpecObj(doc) => {
				let mut ord = BsonDocument::new();
				ord.put(~"$orderby", Embedded(~doc));
				self.add_query_spec(&ord);
				Ok(~"added hint to query spec")
			}
			SpecNotation(ref s) => {
				let obj = ObjParser::from_string::<PureJson, PureJsonParser<~[char]>>(copy *s);
				match obj {
					Ok(o) => return self.sort(SpecObj(BsonDocument::from_formattable(o))),
					Err(e) => return Err(e)
				}
			}
		}
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
				let batch_amt = if self.batch_size != 0 { min(self.limit-self.retrieved, self.batch_size) } else { self.limit-selfretrieved }
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
	extern mod extra;

	use super::*; 
	use bson::bson_types::*;
	use util::*;
    //use coll::*;

	#[test]
	fn test_add_index_obj() {
		let mut doc = BsonDocument::new();
		doc.put(~"foo", Double(1f64));
		//let mut cursor = Cursor::new(@Collection, 10i32);
		let mut cursor = Cursor::new(None, 0i64, 0i32, 10i32, ~[]);
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
		//let mut cursor = Cursor::new(@Collection, 10i32);
		let mut cursor = Cursor::new(None, 0i64, 0i32, 10i32, ~[]);
		cursor.hint(SpecNotation(hint));

		let mut spec = BsonDocument::new();
		let mut speci = BsonDocument::new();
		speci.put(~"foo", Double(1f64));
		spec.put(~"$hint", Embedded(~speci));
		
		assert_eq!(cursor.query_spec, spec);
	}	
}
