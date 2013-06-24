use std::to_bytes::*;

pub struct MongoErr {
    //err_code : int,
    err_type : ~str,
    err_name : ~str,
    err_msg : ~str,
}

/**
 * MongoErr to propagate errors; would be called Err except that's
 * taken by rust...
 */
impl MongoErr {
    /**
     * Create a new MongoErr of given type (e.g. "connection", "query"),
     * name (more specific error), and msg (description of error).
     */
    pub fn new(typ : ~str, name : ~str, msg : ~str) -> MongoErr {
        MongoErr { err_type : typ, err_name : name, err_msg : msg }
    }

    /**
     * Print a MongoErr to string in a standard format.
     */
    pub fn to_str(e : MongoErr) -> ~str {
        fmt!("ERR | %s | %s => %s", e.err_type, e.err_name, e.err_msg)
    }
}

/**
 * CRUD option flags.
 */
pub enum UPDATE_FLAG {
    UPSERT = 1 << 0,
    MULTI = 1 << 1,
}
pub enum UPDATE_OPTION {
    // nothing yet
    // update as update operation takes more options;
    //      intended for non-mask-type options
}

pub enum INSERT_FLAG {
    CONT_ON_ERR = 1 << 0,
}
pub enum INSERT_OPTION {
    // nothing yet
    // update as insert operation takes more options;
    //      intended for non-mask-type options
}

pub enum QUERY_FLAG {
    // bit 0 reserved
    CUR_TAILABLE = 1 << 1,
    SLAVE_OK = 1 << 2,
    OPLOG_REPLAY = 1 << 3,          // driver should not set
    NO_CUR_TIMEOUT = 1 << 4,
    AWAIT_DATA = 1 << 5,
    EXHAUST = 1 << 6,
    PARTIAL = 1 << 7,
}
pub enum QUERY_OPTION {
    // update as query operation takes more options;
    //      intended for non-mask-type options
    SKIP(int),
    RET(int),
}

pub enum DELETE_FLAG {
    SINGLE_REMOVE = 1 << 0,
}
pub enum DELETE_OPTION {
    // nothing yet
    // update as delete operation takes more options;
    //      intended for non-mask-type options
}

/**
 * Reply flags, but user shouldn't deal with them directly.
 */
pub enum REPLY_FLAG {
    CUR_NOT_FOUND = 1,
    QUERY_FAIL = 1 << 1,
    SHARD_CONFIG_STALE = 1 << 2,    // driver should ignore
    AWAIT_CAPABLE = 1 << 3,
}

/**
 * Misc
 */
pub static LITTLE_ENDIAN_TRUE : bool = true;

// TODO write concern
// TODO read preference

// XXX Bson, Json placeholders
pub struct Bson {
    data : ~[u8],
    size : uint,
}
impl Bson {
    pub fn tmp() -> Bson {
        let j = Json::tmp();
        let dat = j.to_bytes(true);
        Bson { data : copy dat, size : dat.len() as uint }
    }
    pub fn tmp2(x : int, y : int) -> Bson {
        let j = Json::tmp2(x, y);
        let dat = j.to_bytes(true);
        Bson { data : copy dat, size : dat.len() as uint }
    }
    pub fn to_json(&self) -> Json {
        // hrm... wrong?
        Json { key : 0, val : 0 }
    }
    pub fn to_bytes(&self, lsb0 : bool) -> ~[u8] {
        let mut bytes = copy self.data;
        bytes += self.size.to_bytes(true);
        bytes
    }
}

pub struct Json {
    key : int,
    val : int,
}
impl Json {
    pub fn tmp() -> Json { Json { key : 0, val : 0 } }
    pub fn tmp2(x : int, y : int) -> Json { Json { key : x, val : y } }
    pub fn to_bson(&self) -> Bson { Bson { data : ~[], size : 0 } }
    pub fn to_bytes(&self, lsb0 : bool) -> ~[u8] {
        let mut bytes : ~[u8] = ~[];
        bytes += self.key.to_bytes(lsb0);
        bytes += self.val.to_bytes(lsb0);
        bytes
    }
}
