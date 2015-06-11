use bson;
use bson::Bson;

#[derive(Clone, PartialEq, Eq)]
pub enum ReadPreference {
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
    Nearest,
}

#[derive(Clone)]
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
