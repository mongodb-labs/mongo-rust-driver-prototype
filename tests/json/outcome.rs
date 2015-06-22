use bson::{Bson, Document};
use rustc_serialize::json::{Json, Object};

pub struct Collection {
    pub name: Option<String>,
    pub data: Vec<Document>,
}

pub struct Outcome {
    pub result: Bson,
    pub collection: Option<Collection>,
}

impl Outcome {
    pub fn from_json(object: &Object) -> Result<Outcome, String> {
        let result = match object.get("result") {
            Some(ref json) => Bson::from_json(&json),
            None => Bson::Null
        };

        let coll_obj = match object.get("collection") {
            Some(&Json::Object(ref obj)) => obj.clone(),
            _ => return Ok(Outcome { result: result, collection: None })
        };

        let name = match coll_obj.get("name") {
            Some(&Json::String(ref s)) => Some(s.clone()),
            _ => None
        };

        let array = val_or_err!(coll_obj.get("data"),
                               Some(&Json::Array(ref arr)) => arr,
                              "`result` must be an array");

        let mut data = vec![];

        for json in array {
            match Bson::from_json(&json) {
                Bson::Document(doc) => data.push(doc),
                _ => return Err("`data` array must contain only objects".to_owned())
            }
        }

        let collection = Collection { name: name, data: data };

        Ok(Outcome { result: result, collection: Some(collection) })
    }
}
