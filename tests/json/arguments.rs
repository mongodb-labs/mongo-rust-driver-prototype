use bson::{Bson, Document};
use json::options::FromJson;
use mongodb::client::coll::options::FindOptions;
use rustc_serialize::json::Object;

pub enum Arguments {
    Delete {
        filter: Document,
        many: bool,
    },
    Find {
        filter: Option<Document>,
        options: FindOptions,
    },
    InsertOne {
        document: Document,
    },
    InsertMany {
        documents: Vec<Document>,
    },
}

impl Arguments {
    pub fn delete_from_json(object: &Object,
                            many: bool) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let document = val_or_err!(object.get("filter").and_then(f),
                                   Some(Bson::Document(doc)) => doc,
                                   "`delete` requires document");

        Ok(Arguments::Delete { filter: document, many: many })
    }

    pub fn find_from_json(object: &Object) -> Arguments {
        let options = FindOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = match object.get("filter").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None
        };

        Arguments::Find{ filter: filter, options: options }
    }

    pub fn insert_many_from_json(object: &Object) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));

        let bsons = val_or_err!(object.get("documents").and_then(f),
                                Some(Bson::Array(arr)) => arr,
                                "`insert_many` requires documents");

        let mut docs = vec![];

        for bson in bsons.into_iter() {
            match bson {
                Bson::Document(doc) => docs.push(doc),
                _ => return Err("`insert_many` can only insert documents".to_owned())
            };
        }

        Ok(Arguments::InsertMany { documents: docs })
    }

    pub fn insert_one_from_json(object: &Object) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let document = val_or_err!(object.get("document").and_then(f),
                                   Some(Bson::Document(doc)) => doc,
                                   "`insert_one` requires document");

        Ok(Arguments::InsertOne { document: document })
    }
}
