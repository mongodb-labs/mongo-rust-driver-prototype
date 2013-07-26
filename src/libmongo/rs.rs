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

use std::cell::*;

use bson::decode::*;
use bson::encode::*;
use bson::formattable::*;

use util::*;
use client::Client;
use coll::Collection;

pub struct RS {
    //seed : ~[(~str, uint)], // uri?
    priv client : @Client,
}

#[deriving(Clone,Eq)]
pub struct RSMember {
    priv _id : Cell<uint>,
    host : ~str,
    opts : Option<~[RS_MEMBER_OPTION]>,
}
impl BsonFormattable for RSMember {
    // NB don't use this in normal usage, since intended for use
    // as part of *array* of RSMembers (to have correct _id)
    pub fn to_bson_t(&self) -> Document {
        let mut member_doc = BsonDocument::new();

        if !self._id.is_empty() {
            let id = self._id.take();
            member_doc.put(~"_id", Int32(id as i32));
            self._id.put_back(id);
        }
        member_doc.put(~"host", UString(self.host.clone()));

        match self.opts.clone() {
            None => (),
            Some(a) => {
                for a.iter().advance |&opt| {
                    member_doc.union(opt.to_bson_t());
                }
            },
        }

        Embedded(~member_doc)
    }
    pub fn from_bson_t(doc : Document) -> Result<RSMember, ~str> {
        let bson_doc = match doc {
            Embedded(bson) => *bson,
            _ => return Err(~"not RSMember struct (not Embedded BsonDocument)"),
        };

        let mut member = RSMember::new(~"", None);
        let mut opts = ~[];

        for bson_doc.fields.iter().advance |&(@k,@v)| {
            match k {
                ~"_id" => if !member._id.is_empty() {
                    return Err(~"duplicate _id for RSMember");
                } else {
                    match v {
                        Int32(i) => member._id.put_back(i as uint),
                        _ => return Err(~"not RSMember struct (_id field not Int32)"),
                    }
                },
                ~"host" => match v {
                    UString(s) => member.host = s,
                    _ => return Err(~"not RSMember struct (host field not UString)"),
                },
                _ => {
                    let mut tmp = BsonDocument::new();
                    tmp.put(k,v);
                    match BsonFormattable::from_bson_t::<RS_MEMBER_OPTION>(Embedded(~tmp)) {
                        Ok(opt) => opts.push(opt),
                        Err(e) => return Err(fmt!("not RSMember struct (err parsing options: %s)", e)),
                    }
                }
            }
        }

        if member._id.is_empty() {
            return Err(~"not RSMember struct (_id field not Int32)");
        }
        if opts.len() > 0 { member.opts = Some(opts); }
        Ok(member)
    }
}
impl RSMember {
    pub fn new( host : ~str,
                opts : Option<~[RS_MEMBER_OPTION]>) -> RSMember {
        RSMember {
            _id : Cell::new_empty(),
            host : host,
            opts : opts,
        }
    }
}

#[deriving(Clone)]
pub struct RSConfig {
    _id : Option<~str>,
    version : Option<i32>,
    members : ~[RSMember],
    settings : Option<~[RS_OPTION]>,
}
impl BsonFormattable for RSConfig {
    pub fn to_bson_t(&self) -> Document {
        let mut conf_doc = BsonDocument::new();

        if self._id.is_some() {
            let s = self._id.clone().unwrap();
            conf_doc.put(~"_id", UString(s));
        }

        if self.version.is_some() {
            let v = self.version.clone().unwrap();
            conf_doc.put(~"version", Int32(v));
        }

        let mut i = 0;
        let mut tmp_doc = BsonDocument::new();
        for self.members.iter().advance |&member| {
            if !member._id.is_empty() { member._id.take(); }
            member._id.put_back(i);
            tmp_doc.put(i.to_str(), member.to_bson_t());
            i += 1;
        }
        conf_doc.put(~"members", Array(~tmp_doc));

        tmp_doc = BsonDocument::new();
        match self.settings.clone() {
            None => (),
            Some(a) => {
                for a.iter().advance |&opt| {
                    tmp_doc.union(opt.to_bson_t());
                }
                conf_doc.put(~"settings", Embedded(~tmp_doc));
            }
        }

        Embedded(~conf_doc)
    }

    pub fn from_bson_t(doc : Document) -> Result<RSConfig, ~str> {
        let bson_doc = match doc {
            Embedded(bson) => *bson,
            _ => return Err(~"not RSConfig struct (not Embedded BsonDocument)"),
        };

        let _id = match bson_doc.find(~"_id") {
            None => None,
            Some(doc) => match copy *doc {
                UString(s) => Some(s),
                _ => return Err(~"not RSConfig struct (_id field not UString)"),
            },
        };
        let version = match bson_doc.find(~"version") {
            None => None,
            Some(doc) => match copy *doc {
                Int32(v) => Some(v),
                _ => return Err(~"not RSConfig struct (version field not Int32)"),
            },
        };
        let members = match bson_doc.find(~"members") {
            None => return Err(~"not RSConfig struct (no members field)"),
            Some(doc) => match copy *doc {
                Array(a) => match BsonFormattable::from_bson_t::<~[RSMember]>(Array(a)) {
                    Ok(arr) => arr,
                    Err(e) => return Err(fmt!("not RSConfig struct (members field: %s)", e)),
                },
                _ => return Err(~"not RSConfig struct (members field not Array)"),
            },
        };
        let mut s_arr = ~[];
        match bson_doc.find(~"settings") {
            None => (),
            Some(doc) => match copy *doc {
                Embedded(sub) => {
                    for sub.fields.iter().advance |&(@k,@v)| {
                        let mut tmp = BsonDocument::new();
                        tmp.put(k,v);
                        match BsonFormattable::from_bson_t::<RS_OPTION>(Embedded(~tmp)) {
                            Ok(s) => s_arr.push(s),
                            Err(e) => return Err(fmt!("not RSConfig struct (error formatting settings: %s)", e)),
                        }
                    }
                }
                _ => return Err(~"not RSConfig struct (settings field not Embedded BsonDocument)"),
            }
        }

        let settings = if s_arr.len() > 0 { Some(s_arr) } else { None };
        Ok(RSConfig::new(_id, version, members, settings))
    }
}
impl RSConfig {
    pub fn new( _id : Option<~str>,
                version : Option<i32>,
                members : ~[RSMember],
                settings : Option<~[RS_OPTION]>) -> RSConfig {
        RSConfig {
            _id : _id,
            version : version,
            members : members,
            settings : settings,
        }
    }
}

/**
 * Replica set options.
 */
#[deriving(Clone,Eq)]
pub enum RS_OPTION {
    CHAINING_ALLOWED(bool),
}
impl BsonFormattable for RS_OPTION {
    pub fn to_bson_t(&self) -> Document {
        let mut opt_doc = BsonDocument::new();
        let (k, v) = match self {
            &CHAINING_ALLOWED(v) => (~"chainingAllowed", Bool(v)),
        };
        opt_doc.put(k, v);
        Embedded(~opt_doc)
    }
    pub fn from_bson_t(doc : Document) -> Result<RS_OPTION, ~str> {
        let bson_doc = match doc {
            Embedded(bson) => *bson,
            _ => return Err(~"not RS_OPTION (not Embedded BsonDocument)"),
        };

        match bson_doc.find(~"chainingAllowed") {
            None => (),
            Some(s) => match copy *s {
                Bool(v) => return Ok(CHAINING_ALLOWED(v)),
                _ => return Err(~"not RS_OPTION (chainingAllowed field not Bool)"),
            },
        }

        Err(~"not RS_OPTION (could not find any member in enum)")
    }
}
#[deriving(Clone,Eq)]
pub enum RS_MEMBER_OPTION {
    ARB_ONLY(bool),
    BUILD_INDS(bool),
    HIDDEN(bool),
    PRIORITY(f64),
    TAGS(TagSet),
    SLAVE_DELAY(i32),
    VOTES(i32),
}
impl BsonFormattable for RS_MEMBER_OPTION {
    pub fn to_bson_t(&self) -> Document {
        let mut opt_doc = BsonDocument::new();
        let (k, v) = match (*self).clone() {
            ARB_ONLY(v) => (~"arbiterOnly", Bool(v)),
            BUILD_INDS(v) => (~"buildIndexes", Bool(v)),
            HIDDEN(v) => (~"hidden", Bool(v)),
            PRIORITY(p) => (~"priority", Double(p)),
            TAGS(ts) => (~"tags", ts.clone().to_bson_t()),
            SLAVE_DELAY(d) => (~"slaveDelay", Int32(d)),
            VOTES(n) => (~"votes", Int32(n)),
        };
        opt_doc.put(k, v);
        Embedded(~opt_doc)
    }
    // NB don't use this in normal usage, since intended for use
    // with *single* RS_MEMBER_OPTION, and doc might contain more
    pub fn from_bson_t(doc : Document) -> Result<RS_MEMBER_OPTION, ~str> {
        let bson_doc = match doc {
            Embedded(bson) => *bson,
            _ => return Err(~"not RS_OPTION (not Embedded BsonDocument)"),
        };

        match bson_doc.find(~"arbiterOnly") {
            None => (),
            Some(s) => match copy *s {
                Bool(v) => return Ok(ARB_ONLY(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (arbiterOnly field not Bool)"),
            },
        }
        match bson_doc.find(~"buildIndexes") {
            None => (),
            Some(s) => match copy *s {
                Bool(v) => return Ok(BUILD_INDS(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (buildIndexes field not Bool)"),
            },
        }
        match bson_doc.find(~"hidden") {
            None => (),
            Some(s) => match copy *s {
                Bool(v) => return Ok(HIDDEN(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (hidden field not Bool)"),
            },
        }
        match bson_doc.find(~"priority") {
            None => (),
            Some(s) => match copy *s {
                Double(v) => return Ok(PRIORITY(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (priority field not Double)"),
            },
        }
        match bson_doc.find(~"tags") {
            None => (),
            Some(s) => match BsonFormattable::from_bson_t::<TagSet>(copy *s) {
                Ok(ts) => return Ok(TAGS(ts)),
                Err(e) => return Err(e),
            },
        }
        match bson_doc.find(~"slaveDelay") {
            None => (),
            Some(s) => match copy *s {
                Int32(v) => return Ok(SLAVE_DELAY(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (slaveDelay field not Int32)"),
            },
        }
        match bson_doc.find(~"votes") {
            None => (),
            Some(s) => match copy *s {
                Int32(v) => return Ok(VOTES(v)),
                _ => return Err(~"not RS_MEMBER_OPTION (votes field not Int32)"),
            },
        }

        Err(~"not RS_MEMBER_OPTION (could not find any member in enum)")
    }
}

impl RS {
    //pub fn new(seed : ~[(~str, uint)], client : @Client) -> RS {
    pub fn new(client : @Client) -> RS {
        RS {
            //seed : seed,
            client : client,
        }
    }

    pub fn get_config(&self) -> Result<RSConfig, MongoErr> {
        let coll = Collection::new(~"local", SYSTEM_REPLSET.to_owned(), self.client);
        let doc = match coll.find_one(None, None, None) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        match BsonFormattable::from_bson_t::<RSConfig>(Embedded(doc)) {
            Ok(conf) => Ok(conf),
            Err(e) => Err(MongoErr::new(
                            ~"rs::get_config",
                            ~"error formatting document into RSConfig",
                            e)),
        }
    }

    pub fn add(&self, host : RSMember) -> Result<(), MongoErr> {
        let mut conf = match self.get_config() {
            Ok(c) => c,
            Err(e) => return Err(e),
        };
        conf.members.push(host);
        let conf_doc = conf.to_bson_t();
        let db = self.client.get_admin();
        let mut cmd_doc = BsonDocument::new();
        cmd_doc.put(~"replSetReconfig", conf_doc);
        match db.run_command(SpecObj(cmd_doc)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn add_arbiter(&self, host_str : ~str) -> Result<(), MongoErr> {
        let mut host = RSMember::new(host_str, Some(~[ARB_ONLY(true)]));
        self.add(host)
    }

    pub fn get_status(&self) -> Result<~BsonDocument, MongoErr> {
        let db = self.client.get_admin();
        db.run_command(SpecNotation(~"{ 'replSetGetStatus':1 }"))
    }

    pub fn initiate(&self, conf : RSConfig) -> Result<(), MongoErr> {
        let conf_doc = conf.to_bson_t();
        let db = self.client.get_admin();
        let mut cmd_doc = BsonDocument::new();
        cmd_doc.put(~"replSetInitiate", conf_doc);
        match db.run_command(SpecObj(cmd_doc)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn reconfig(&self, conf : RSConfig, force : bool)
                -> Result<(), MongoErr> {
        let conf_doc = conf.to_bson_t();
        let db = self.client.get_admin();
        let mut cmd_doc = BsonDocument::new();
        cmd_doc.put(~"replSetReconfig", conf_doc);
        cmd_doc.put(~"force", Bool(force));
        match db.run_command(SpecObj(cmd_doc)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
