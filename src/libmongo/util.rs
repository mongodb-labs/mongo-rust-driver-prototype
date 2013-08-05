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

use std::int::*;
use extra::treemap::*;

use bson::encode::*;
use bson::formattable::*;

/**
 * Utility module for use internal and external to crate.
 * Users must access functionality for proper use of options, etc.
 */

#[deriving(Clone,Eq)]
pub struct MongoErr {
    //err_code : int,
    err_type : ~str,
    err_name : ~str,
    err_msg : ~str,
    // TODO: error codes for finer granularity of error provenance (than
    //      just a bunch of strings, e.g. connection error, run_command
    //      error, BSON parsing error, etc.)
}

/**
 * MongoErr to propagate errors.
 */
impl MongoErr {
    /**
     * Creates a new MongoErr of given type (e.g. "connection", "query"),
     * name (more specific error), and msg (description of error).
     */
    pub fn new(typ : ~str, name : ~str, msg : ~str) -> MongoErr {
        MongoErr { err_type : typ, err_name : name, err_msg : msg }
    }

    /**
     * Like to_str, but omits staring "ERR | ".
     */
    pub fn tail(&self) -> ~str {
        fmt!("%s | %s => %s", self.err_type, self.err_name, self.err_msg)
    }
}

impl ToStr for MongoErr {
    /**
     * Prints a MongoErr to string in a standard format.
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

pub enum QuerySpec {
    SpecObj(BsonDocument),
    SpecNotation(~str)
}
impl ToStr for QuerySpec {
    pub fn to_str(&self) -> ~str {
        match self {
            &SpecObj(ref bson) => bson.fields.to_str(),
            &SpecNotation(ref s) => s.clone(),
        }
    }
}

#[deriving(Eq)]
pub struct TagSet {
    tags : TreeMap<~str, ~str>,
}
impl Clone for TagSet {
    pub fn clone(&self) -> TagSet {
        let mut tags = TreeMap::new();
        for self.tags.iter().advance |(&k,&v)| {
            tags.insert(k, v);
        }
        TagSet { tags : tags }
    }
}
impl BsonFormattable for TagSet {
    pub fn to_bson_t(&self) -> Document {
        let mut ts_doc = BsonDocument::new();
        for self.tags.iter().advance |(&k,&v)| {
            ts_doc.put(k, UString(v));
        }
        Embedded(~ts_doc)
    }
    pub fn from_bson_t(doc : &Document) -> Result<TagSet, ~str> {
        let mut ts = TagSet::new(~[]);
        match doc {
            &Embedded(ref bson_doc) => {
                for bson_doc.fields.iter().advance |&(@k,@v)| {
                    match v {
                        UString(s) => ts.set(k,s),
                        _ => return Err(~"not TagSet struct (val not UString)"),
                    }
                }
            }
            _ => return Err(~"not TagSet struct (not Embedded BsonDocument)"),
        }
        Ok(ts)
    }
}
impl TagSet {
    pub fn new(tag_list : &[(&str, &str)]) -> TagSet {
        let mut tags = TreeMap::new();
        for tag_list.iter().advance |&(field, val)| {
            tags.insert(field.to_owned(), val.to_owned());
        }
        TagSet { tags : tags }
    }

    pub fn get_ref<'a>(&'a self, field : ~str) -> Option<&'a ~str> {
        self.tags.find(&field)
    }

    pub fn get_mut_ref<'a>(&'a mut self, field : ~str) -> Option<&'a mut ~str> {
        self.tags.find_mut(&field)
    }

    /**
     * Sets tag in TagSet, whether or not it existed previously.
     */
    pub fn set(&mut self, field : ~str, val : ~str) {
        self.tags.remove(&field);
        if val.len() != 0 {
            self.tags.insert(field, val);
        }
    }

    /**
     * Returns if self matches the other TagSet,
     * i.e. if all of the other TagSet's tags are
     * in self's TagSet.
     *
     * Usage: member.matches(tagset)
     */
    pub fn matches(&self, other : &TagSet) -> bool {
        for other.tags.iter().advance |(f0, &v0)| {
            match self.tags.find(f0) {
                None => return false,
                Some(v1) => {
                    if v0 != *v1 { return false; }
                }
            }
        }

        true
    }
}

pub enum WRITE_CONCERN {
    JOURNAL(bool),      // wait for next journal commit?
    W_N(int),           // replicate to how many? (number)
    W_STR(~str),        // replicate to how many? (string, e.g. "majority")
    W_TAGSET(TagSet),   // replicate to what tagset?
    WTIMEOUT(int),      // timeout after how many ms?
    FSYNC(bool),        // wait for write to disk?
}

#[deriving(Clone, Eq)]
pub enum READ_PREFERENCE {
    PRIMARY_ONLY,
    PRIMARY_PREF(Option<~[TagSet]>),
    SECONDARY_ONLY(Option<~[TagSet]>),
    SECONDARY_PREF(Option<~[TagSet]>),
    NEAREST(Option<~[TagSet]>),
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
pub static MONGO_RECONN_MSECS : u64 = (1000*3);
pub static MONGO_TIMEOUT_SECS : u64 = 30; // XXX units...
pub static LOCALHOST : &'static str = &'static "127.0.0.1"; // XXX tmp

/// INTERNAL UTILITIES
/**
 * Special collections for database operations, but generally, users should not
 * access directly.
 */
pub static SYSTEM_NAMESPACE : &'static str = &'static "system.namespaces";
pub static SYSTEM_INDEX : &'static str = &'static "system.indexes";
pub static SYSTEM_PROFILE : &'static str = &'static "system.profile";
pub static SYSTEM_USERS : &'static str = &'static "system.users";
pub static SYSTEM_COMMAND : &'static str = &'static "$cmd";
pub static SYSTEM_JS : &'static str = &'static "system.js";
pub static SYSTEM_REPLSET : &'static str = &'static "system.replset";

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

pub fn parse_host(host_str : &~str) -> Result<(~str, uint), MongoErr> {
    let mut port_str = fmt!("%?", MONGO_DEFAULT_PORT);
    let mut ip_str = match host_str.find_str(":") {
        None => host_str.to_owned(),
        Some(i) => {
            port_str = host_str.slice_from(i+1).to_owned();
            host_str.slice_to(i).to_owned()
        }
    };

    if ip_str == ~"localhost" { ip_str = LOCALHOST.to_owned(); }    // XXX must exist better soln

    match from_str(port_str) {
        None => Err(MongoErr::new(
                        ~"conn_replica::parse_host",
                        ~"unexpected host string format",
                        fmt!("host string should be \"[IP ~str]:[uint]\",
                                    found %s:%s", ip_str, port_str))),
        Some(k) => Ok((ip_str, k as uint)),
    }
}
