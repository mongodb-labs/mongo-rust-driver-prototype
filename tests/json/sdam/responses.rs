use bson::{Bson, Document};
use mongodb::connstring::{self, Host};
use rustc_serialize::json::Json;

pub struct Responses {
    pub data: Vec<(Host, Document)>,
}

impl Responses {
    pub fn from_json(array: &Vec<Json>) -> Result<Responses, String> {
        let mut data = Vec::new();

        for json in array {
            let inner_array = val_or_err!(json,
                                          &Json::Array(ref arr) => arr,
                                          "`responses` must be an array of arrays.");

            if inner_array.len() != 2 {
                return Err("Response item must contain the host string and ismaster object.".to_owned());
            }

            let host = val_or_err!(inner_array[0],
                                   Json::String(ref s) => s.to_owned(),
                                   "Response item must contain the host string as the first argument.");

            let ismaster = val_or_err!(inner_array[1],
                                       Json::Object(ref obj) => Bson::from_json(&Json::Object(obj.clone())),
                                       "Response item must contain the ismaster object as \
                                        the second argument.");

            match ismaster {
                Bson::Document(doc) => { data.push((connstring::parse_host(&host).unwrap(), doc)); },
                _ => return Err("`ismaster` parse must return a Bson Document".to_owned()),
            }
        }

        Ok(Responses { data: data })
    }
}
