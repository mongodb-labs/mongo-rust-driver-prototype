use bson::Bson;
use mongodb::client::coll::options::{AggregateOptions, CountOptions, FindOptions};
use rustc_serialize::json::{Object, Json};

pub trait FromJson {
    fn from_json(object: &Object) -> Self;
}

impl FromJson for AggregateOptions {
    fn from_json(object: &Object) -> AggregateOptions {
        let mut options = AggregateOptions::new();

        options.batch_size = match object.get("batchSize") {
            Some(&Json::I64(n)) => n as i32,
            Some(&Json::U64(n)) => n as i32,
            Some(&Json::F64(n)) => n as i32,
            _ => options.batch_size
        };

        options
    }
}

impl FromJson for CountOptions {
    fn from_json(object: &Object) -> CountOptions {
        let mut options = CountOptions::new();

        options.skip = match object.get("skip") {
            Some(&Json::I64(n)) => n as u64,
            Some(&Json::U64(n)) => n as u64,
            Some(&Json::F64(n)) => n as u64,
            _ => options.skip
        };

        options.limit = match object.get("limit") {
            Some(&Json::I64(n)) => n as i64,
            Some(&Json::U64(n)) => n as i64,
            Some(&Json::F64(n)) => n as i64,
            _ => options.limit
        };

        options
    }
}

impl FromJson for FindOptions {
    fn from_json(object: &Object) -> FindOptions {
        let mut options = FindOptions::new();

        let f = |x| Some(Bson::from_json(x));
        options.sort = match object.get("sort").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None
        };

        options.skip = match object.get("skip") {
            Some(&Json::I64(n)) => n as u32,
            Some(&Json::U64(n)) => n as u32,
            Some(&Json::F64(n)) => n as u32,
            _ => options.skip
        };

        options.limit = match object.get("limit") {
            Some(&Json::I64(n)) => n as i32,
            Some(&Json::U64(n)) => n as i32,
            Some(&Json::F64(n)) => n as i32,
            _ => options.limit
        };

        options.batch_size = match object.get("batchSize") {
            Some(&Json::I64(n)) => n as i32,
            Some(&Json::U64(n)) => n as i32,
            Some(&Json::F64(n)) => n as i32,
            _ => options.batch_size
        };

        options
    }
}
