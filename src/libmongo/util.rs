/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bson::encode::*;

/**
 * Utility module for use internal and external to crate.
 * Users must access functionality for proper use of options, etc.
 */

pub struct MongoErr {
    //err_code : int,
    err_type : ~str,
    err_name : ~str,
    err_msg : ~str,
}

/**
 * MongoErr to propagate errors; would be called Err except that's
 * taken by Rust...
 */
impl MongoErr {
    /**
     * Create a new MongoErr of given type (e.g. "connection", "query"),
     * name (more specific error), and msg (description of error).
     */
    pub fn new(typ : ~str, name : ~str, msg : ~str) -> MongoErr {
        MongoErr { err_type : typ, err_name : name, err_msg : msg }
    }
}

impl ToStr for MongoErr {
    /**
     * Print a MongoErr to string in a standard format.
     */
    pub fn to_str(&self) -> ~str {
        fmt!("ERR | %s | %s => %s", self.err_type, self.err_name, self.err_msg)
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
    CUR_NOT_FOUND = 1 << 0,
    QUERY_FAIL = 1 << 1,
    SHARD_CONFIG_STALE = 1 << 2,    // driver should ignore
    AWAIT_CAPABLE = 1 << 3,
}

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
 * Indexing.
 */
pub enum INDEX_ORDER {
    ASC = 1,
    DESC = -1,
}

pub enum INDEX_FLAG {
    BACKGROUND = 1 << 0,
    UNIQUE = 1 << 1,
    DROP_DUPS = 1 << 2,
    SPARSE = 1 << 3,
}

pub enum INDEX_OPTION {
    INDEX_NAME(~str),
    EXPIRE_AFTER_SEC(int),
    VERS(int),
}

pub enum INDEX_GEOTYPE {
    SPHERICAL,                          // "2dsphere"
    FLAT,                               // "2d"
}

pub enum INDEX_FIELD {
    NORMAL(~[(~str, INDEX_ORDER)]),
    HASHED(~str),
    GEOSPATIAL(~str, INDEX_GEOTYPE),
    GEOHAYSTACK(~str, ~str, uint),
}

/**
 * Collections.
 */
pub enum COLLECTION_FLAG {
    AUTOINDEX_ID = 1 << 0,      // enable automatic index on _id?
}

pub enum COLLECTION_OPTION {
    CAPPED(uint),   // max size of capped collection
    SIZE(uint),     // preallocated size of uncapped collection
    MAX_DOCS(uint), // max cap in number of documents
}

/**
 * Misc
 */
pub static LITTLE_ENDIAN_TRUE : bool = true;
pub static MONGO_DEFAULT_PORT : uint = 27017;

/// INTERNAL UTILITIES
/**
 * Special collections for database operations, but users should not
 * access directly.
 */
pub static SYSTEM_NAMESPACE : &'static str = &'static "system.namespaces";
pub static SYSTEM_INDEX : &'static str = &'static "system.indexes";
pub static SYSTEM_PROFILE : &'static str = &'static "system.profile";
pub static SYSTEM_USER : &'static str = &'static "system.users";
pub static SYSTEM_COMMAND : &'static str = &'static "$cmd";
pub static SYSTEM_JS : &'static str = &'static "system.js";

// macro for compressing options array into single i32 flag
macro_rules! process_flags(
    ($options:ident) => (
        match $options {
            None => 0i32,
            Some(opt_array) => {
                let mut tmp = 0i32;
                for opt_array.iter().advance |&f| { tmp |= f as i32; }
                tmp
            }
        }
    );
)
