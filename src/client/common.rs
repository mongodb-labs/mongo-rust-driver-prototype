#[derive(Clone, PartialEq)]
pub enum ReadPreference {
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
    Nearest,
}

#[derive(Clone)]
pub struct WriteConcern {
    w: i8,           // Write replication
    w_timeout: i32,  // Used in conjunction with 'w'. Propagation timeout in ms.
    j: bool,         // If true, will block until write operations have been committed to journal.
    fsync: bool,     // If true and server is not journaling, blocks until server has synced all data files to disk.
}

impl WriteConcern {
    pub fn new(w: i8, w_timeout: i32, j: bool, fsync: bool) -> WriteConcern {
        WriteConcern {
            w: w,
            w_timeout: w_timeout,
            j: j,
            fsync: fsync,
        }
    }
}
