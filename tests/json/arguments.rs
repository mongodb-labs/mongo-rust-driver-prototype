use bson::{Bson, Document};
use json::options::FromJson;
use mongodb::client::coll::options::FindOptions;
use rustc_serialize::json::Object;

pub enum Arguments {
    Find {
        filter: Option<Document>,
        options: FindOptions
    },
}

impl Arguments {
    pub fn new_find_from_json(object: &Object) -> Arguments {
        let options = FindOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = match object.get("filter").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None
        };

        Arguments::Find{ filter: filter, options: options }
    }
}
