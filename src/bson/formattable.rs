use encode::*;
use json_parse::*;
use extra::json;
use std::hashmap::HashMap;
use std::hash::Hash;

///Trait for document notations which can be represented as BSON.
pub trait BsonFormattable {
    fn to_bson_t(&self) -> Document;
    fn from_bson_t(doc: Document) -> Result<Self,~str>;
}

impl BsonFormattable for ~str {
    fn to_bson_t(&self) -> Document {
        match ObjParser::from_string::<Document, ExtendedJsonParser<~[char]>>(*self) {
            Ok(doc) => doc,
            Err(e) => fail!("invalid string for parsing: %s", e),
        }
    }

    fn from_bson_t(doc: Document) -> Result<~str,~str> {
        match doc {
            UString(s) => Ok(copy s),
            _ => Err(fmt!("could nto convert %? to string", doc))
        }
    }
}

impl BsonFormattable for json::Json {
    fn to_bson_t(&self) -> Document {
        match *self {
            json::Null => Null,
            json::Number(f) => Double(f as f64),
            json::String(ref s) => UString(copy *s),
            json::Boolean(b) => Bool(b),
            json::List(ref l) => l.to_bson_t(),
            json::Object(ref l) => l.to_bson_t(),
        }
    }

    fn from_bson_t(doc: Document) -> Result<json::Json, ~str> {
        match doc {
            Double(f) => Ok(json::Number(f as float)),
            UString(s) => Ok(json::String(copy s)),
            Embedded(_) => fail!("TODO"),
            Array(_) => fail!("TODO"),
            Binary(_,_) => Err(~"bindata cannot be translated to Json"),
            ObjectId(_) => Err(~"objid cannot be translated to Json"),
            Bool(b) => Ok(json::Boolean(b)),
            UTCDate(i) => Ok(json::Number(i as float)),
            Null => Ok(json::Null),
            Regex(_,_) => Err(~"regex cannot be translated to Json"),
            JScript(s) => Ok(json::String(copy s)),
            JScriptWithScope(_,_) => Err(~"jscope cannot be translated to Json"),
            Int32(i) => Ok(json::Number(i as float)),
            Timestamp(i) => Ok(json::Number(i as float)),
            Int64(i) => Ok(json::Number(i as float)),
            MinKey => Err(~"minkey cannot be translated to Json"),
            MaxKey => Err(~"maxkey cannot be translated to Json")
        }
    }
}

macro_rules! list_fmt {
    (impl $t:ty : $empty:expr) => {
        impl<T:BsonFormattable + Copy> BsonFormattable for $t {
            fn to_bson_t(&self) -> Document {
                let mut doc = BsonDocument::new();
                let s = self.map(|elt| elt.to_bson_t());
                for s.iter().enumerate().advance |(i, &elt)| {
                    doc.put(i.to_str(), elt);
                }
                return Array(~doc);
            }

            fn from_bson_t(doc: Document) -> Result<$t, ~str> {
                match doc {
                    Array(d) => {
                        let mut ret: $t = $empty;
                        for d.fields.iter().advance |&(_,@v)| {
                             match BsonFormattable::from_bson_t::<$t>(v) {
                                Ok(elt) => ret += elt,
                                Err(e) => return Err(e)
                             }
                        }
                        return Ok(ret);
                    }
                    _ => Err(~"only Arrays can be converted to lists")
                }
            }
        }
    }
}

list_fmt!{impl ~[T]: ~[]}
list_fmt!{impl @[T]: @[]}

//TODO macro lifetimes to implement &[T] 
impl<K:ToStr + Eq + Hash, V:BsonFormattable> BsonFormattable for HashMap<K,V> {
    fn to_bson_t(&self) -> Document {
            let mut doc = BsonDocument::new();
            for self.iter().advance |(&k,&v)| {
                doc.put(k.to_str(),v.to_bson_t());
            }
        return Embedded(~doc);
    }

    fn from_bson_t(doc: Document) -> Result<HashMap<K,V>, ~str> {
        fail!("TODO")
    }
}
