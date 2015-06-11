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
    pub w: i8,           // Write replication
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
}
