use encode::*;
use json_parse::*;
use extra::json;
use std::hashmap::HashMap;

///Trait for document notations which can be represented as BSON.
///This trait allows any type to be easily serialized and deserialized as BSON.
pub trait BsonFormattable {
    fn to_bson_t(&self) -> Document;
    fn from_bson_t(doc: Document) -> Result<Self,~str>;
}

macro_rules! float_fmt {
    (impl $t:ty) => {
        impl BsonFormattable for $t {
            fn to_bson_t(&self) -> Document {
                (*self as f64).to_bson_t()
            }

            fn from_bson_t(doc: Document) -> Result<$t, ~str> {
                match BsonFormattable::from_bson_t::<f64>(doc) {
                    Ok(i) => Ok(i as $t),
                    Err(e) => Err(e)
                }
            }
        }
    }
}

macro_rules! i32_fmt {
    (impl $t:ty) => {
            impl BsonFormattable for $t {
            fn to_bson_t(&self) -> Document {
                (*self as i32).to_bson_t()
            }
            
            fn from_bson_t(doc: Document) -> Result<$t, ~str> {
                match BsonFormattable::from_bson_t::<i32>(doc) {
                    Ok(i) => Ok(i as $t),
                    Err(e) => Err(e)
                }
            }
        }
    }
}

macro_rules! list_fmt {
    (impl $list:ty ($inner:ty): $empty:expr) => {
        impl<'self, T:BsonFormattable + Copy> BsonFormattable for $list {
            fn to_bson_t(&self) -> Document {
                let mut doc = BsonDocument::new();
                let s = self.map(|elt| elt.to_bson_t());
                for s.iter().enumerate().advance |(i, &elt)| {
                    doc.put(i.to_str(), elt);
                }
                return Array(~doc);
            }

            fn from_bson_t(doc: Document) -> Result<$list, ~str> {
                match doc {
                    Array(d) => {
                        let mut ret = $empty;
                        for d.fields.iter().advance |&(_,@v)| {
                             match BsonFormattable::from_bson_t::<$inner>(v) {
                                Ok(elt) => ret += [elt],
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

float_fmt!{impl f32}
float_fmt!{impl float}
i32_fmt!{impl i8}
i32_fmt!{impl i16}
i32_fmt!{impl int}
i32_fmt!{impl u8}
i32_fmt!{impl u16}
i32_fmt!{impl u32}
i32_fmt!{impl uint}
i32_fmt!{impl char}
list_fmt!{impl ~[T] (T): ~[]}
list_fmt!{impl @[T] (T): @[]}

impl BsonFormattable for f64 {
    fn to_bson_t(&self) -> Document { Double(*self) }

    fn from_bson_t(doc: Document) -> Result<f64,~str> {
        match doc {
            Double(f) => Ok(f),
            _ => Err(~"can only cast Double to f64")
        }   
    }
}

impl BsonFormattable for i32 {
    fn to_bson_t(&self) -> Document { Int32(*self) }

    fn from_bson_t(doc: Document) -> Result<i32,~str> {
        match doc {
            Int32(i) => Ok(i),
            _ => Err(~"can only cast Int32 to i32")
        }
    }
}

impl BsonFormattable for i64 {
    fn to_bson_t(&self) -> Document { Int64(*self) }

    fn from_bson_t(doc: Document) -> Result<i64,~str> {
        match doc {
            Int64(i) => Ok(i),
            UTCDate(i) => Ok(i),
            Timestamp(i) => Ok(i),
            _ => Err(~"can only cast Int64, Date, and Timestamp to i64")
        }
    }
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

impl<V:BsonFormattable> BsonFormattable for HashMap<~str,V> {
    fn to_bson_t(&self) -> Document {
            let mut doc = BsonDocument::new();
            for self.iter().advance |(&k,&v)| {
                doc.put(k.to_str(),v.to_bson_t());
            }
        return Embedded(~doc);
    }

    fn from_bson_t(doc: Document) -> Result<HashMap<~str,V>, ~str> {
        match doc {
            Embedded(d) => {
                let mut m = HashMap::new();
                for d.fields.iter().advance |&(@k, @v)| {
                    match BsonFormattable::from_bson_t::<V>(v) {
                        Ok(elt) => m.insert(k, elt),
                        Err(e) => return Err(e)
                    };
                }
                return Ok(m);
            }
            Array(d) => {
                let mut m = HashMap::new();
                for d.fields.iter().advance |&(@k, @v)| {
                    match BsonFormattable::from_bson_t::<V>(v) {
                        Ok(elt) => m.insert(k, elt),
                        Err(e) => return Err(e)
                    };
                }
                return Ok(m);
            }
            _ => return Err(~"can only convert Embedded or Array to hashmap")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use encode::*;
    use extra::json;

    #[test]
    fn test_json_to_bson() {
        let json = json::List(~[json::Null, json::Number(5f), json::String(~"foo"), json::Boolean(false)]);
        let mut doc = BsonDocument::new();
        doc.put(~"0", Null);
        doc.put(~"1", Double(5f64));
        doc.put(~"2", UString(~"foo"));
        doc.put(~"3", Bool(false));
        assert_eq!(Array(~doc), json.to_bson_t());
    }

    #[test]
    fn test_bson_to_json() {
        assert!(BsonFormattable::from_bson_t::<json::Json>(Double(5.01)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(UString(~"foo")).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Binary(0u8, ~[0u8])).is_err());
        assert!(BsonFormattable::from_bson_t::<json::Json>(ObjectId(~[0u8])).is_err());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Bool(true)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(UTCDate(150)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Null).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Regex(~"A", ~"B")).is_err());
        assert!(BsonFormattable::from_bson_t::<json::Json>(JScript(~"foo")).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Int32(1i32)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Timestamp(1i64)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(Int64(1i64)).is_ok());
        assert!(BsonFormattable::from_bson_t::<json::Json>(MinKey).is_err());
        assert!(BsonFormattable::from_bson_t::<json::Json>(MaxKey).is_err());
    }

    #[test]
    fn test_list_to_bson() {
       let l = ~[1,2,3]; 
       let mut doc = BsonDocument::new();
       doc.put(~"0", Int32(1i32));
       doc.put(~"1", Int32(2i32));
       doc.put(~"2", Int32(3i32));
       assert_eq!(l.to_bson_t(), Array(~doc));
    }
    
    #[test]
    fn test_bson_to_list() {
       let l = ~[1i32,2,3]; 
       let mut doc = BsonDocument::new();
       doc.put(~"0", Int32(1i32));
       doc.put(~"1", Int32(2i32));
       doc.put(~"2", Int32(3i32));
       assert_eq!(Ok(l), BsonFormattable::from_bson_t::<~[i32]>(Array(~doc)));
    }
}
