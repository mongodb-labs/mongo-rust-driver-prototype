use mongodb::connstring::{self, Host};
use mongodb::topology::server::ServerType;

use rustc_serialize::json::{Json, Object};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(PartialEq, Eq)]
pub struct Server {
    pub host: Host,
    pub rtt: i64,
    pub tags: BTreeMap<String, String>,
    pub stype: ServerType,
}

impl Server {
    pub fn from_json(object: &Object) -> Result<Server, String> {
        let address = val_or_err!(object.get("address"),
                                  Some(&Json::String(ref s)) => s.to_owned(),
                                  "server must have an address.");

        let rtt = val_or_err!(object.get("avg_rtt_ms"),
                              Some(&Json::U64(v)) => v as i64,
                              "server must have an average rtt.");

        let mut tags = BTreeMap::new();        
        let json_doc = val_or_err!(object.get("tags"),
                                   Some(&Json::Object(ref obj)) => obj.clone(),
                                   "server must have tags.");

        for (key, json) in json_doc.into_iter() {
            match json {
                Json::String(val) => { tags.insert(key, val); },
                _ => return Err("server must have tags that are string => string maps.".to_owned()),
            }
        }

        let stype = val_or_err!(object.get("type"),
                                Some(&Json::String(ref s)) => ServerType::from_str(s)
                                .ok().expect("Failed to parse server type"),
                                "server must have a type.");

        Ok(Server {
            host: connstring::parse_host(&address).ok().expect("Failed to parse host."),
            rtt: rtt,
            tags: tags,
            stype: stype,
        })
    }
}
