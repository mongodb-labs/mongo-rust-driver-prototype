use bson::bson_types::*;

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
 * If options ever change, modify:
 *      util.rs: appropriate enums (_FLAGs or _OPTIONs)
 *      coll.rs: appropriate flag and option helper parser functions
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
    NSKIP(int),
    NRET(int),
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

// TODO write concern
pub enum WRITE_CONCERN {
    JOURNAL(bool),      // wait for next journal commit?
    W_N(int),           // replicate to how many? (number)
    W_STR(~str),        // replicate to how many? (string, e.g. "majority")
    //W_TAGSET(~str),     // replicate to what tagset? (string to parse)
    WTIMEOUT(int),      // timeout after how many ms?
    FSYNC(bool),        // wait for write to disk?
}

pub enum QuerySpec {
	SpecObj(BsonDocument),
	SpecNotation(~str)
}
// TODO read preference

/**
 * Collections for admin purposes.
 */
/*
pub static SYSTEM_NAMESPACE : &'static str = &'static "system.namespaces";
pub static SYSTEM_INDEX : &'static str = &'static "system.indexes";
pub static SYSTEM_PROFILE : &'static str = &'static "system.profile";
pub static SYSTEM_USER : &'static str = &'static "system.users";
pub static SYSTEM_COMMAND : &'static str = &'static "$cmd";
pub static SYSTEM_JS : &'static str = &'static "system.js";
*/

/**
 * Misc
 */
pub static LITTLE_ENDIAN_TRUE : bool = true;

