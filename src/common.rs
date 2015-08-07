use Error::{self, ArgumentError};
use Result;

use bson::{self, Bson};
use std::ascii::AsciiExt;
use std::collections::BTreeMap;
use std::str::FromStr;

/// Indicates how a server should be selected during read operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
    Nearest,
}

impl FromStr for ReadMode {   
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Primary" => ReadMode::Primary,
            "PrimaryPreferred" => ReadMode::PrimaryPreferred,
            "Secondary" => ReadMode::Secondary,
            "SecondaryPreferred" => ReadMode::SecondaryPreferred,
            "Nearest" => ReadMode::Nearest,
            _ => return Err(ArgumentError(format!("Could not convert '{}' to ReadMode.", s))),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadPreference {
    /// Indicates how a server should be selected during read operations.
    pub mode: ReadMode,
    /// Filters servers based on the first tag set that matches at least one server.
    pub tag_sets: Vec<BTreeMap<String, String>>,
}

impl ReadPreference {
    pub fn new(mode: ReadMode, tag_sets: Option<Vec<BTreeMap<String, String>>>) -> ReadPreference {
        ReadPreference {
            mode: mode,
            tag_sets: tag_sets.unwrap_or(Vec::new()),
        }
    }

    pub fn to_document(&self) -> bson::Document {
        let mut doc = doc! { "mode" => (stringify!(self.mode).to_ascii_lowercase()) };
        let bson_tag_sets: Vec<_> = self.tag_sets.iter().map(|map| {
            let mut bson_map = bson::Document::new();
            for (key, val) in map.iter() {
                bson_map.insert(key.to_owned(), Bson::String(val.to_owned()));
            }
            Bson::Document(bson_map)
        }).collect();

        doc.insert("tag_sets".to_owned(), Bson::Array(bson_tag_sets));
        doc
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConcern {
    pub w: i32,          // Write replication
    pub w_timeout: i32,  // Used in conjunction with 'w'. Propagation timeout in ms.
    pub j: bool,         // If true, will block until write operations have been committed to journal.
    pub fsync: bool,     // If true and server is not journaling, blocks until server has synced all data files to disk.
}

impl WriteConcern {
    pub fn new() -> WriteConcern {
        WriteConcern {
            w: 1,
            w_timeout: 0,
            j: false,
            fsync: false,
        }
    }

    pub fn to_bson(&self) -> bson::Document {
        let mut bson = bson::Document::new();
        bson.insert("w".to_owned(), Bson::I32(self.w));
        bson.insert("wtimeout".to_owned(), Bson::I32(self.w_timeout));
        bson.insert("j".to_owned(), Bson::Boolean(self.j));
        bson
    }
}
