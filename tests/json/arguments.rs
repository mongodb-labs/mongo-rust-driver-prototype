use bson::{Bson, Document};
use json::options::FromJson;
use mongodb::client::coll::options::FindOptions;
use rustc_serialize::json::Object;

pub enum Arguments {
    Find {
        filter: Option<Document>,
        options: FindOptions
    },
    InsertOne {
        document: Document,
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

    pub fn new_insert_one_from_json(object: &Object) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let document = val_or_err!(object.get("document").and_then(f),
                                   Some(Bson::Document(doc)) => doc,
                                   "`insert_one` requires document");

        Ok(Arguments::InsertOne { document: document })
    }
}
