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

use bson::encode::*;
use bson::formattable::*;

use util::*;
use client::Client;
use coll::Collection;

pub struct RS {
    priv client : @Client,
}

#[deriving(Clone,Eq)]
pub struct RSMember {
    priv _id : Cell<uint>,
    host : ~str,
    opts : ~[RS_MEMBER_OPTION],
}
impl BsonFormattable for RSMember {
    // NB: not intended for normal usage, since intended for use
    // as part of *array* of RSMembers (to have correct _id)
    pub fn to_bson_t(&self) -> Document {
        let mut member_doc = BsonDocument::new();

        if !self._id.is_empty() {
            let id = self._id.take();
            member_doc.put(~"_id", Int32(id as i32));
            self._id.put_back(id);
        }
        member_doc.put(~"host", UString(self.host.clone()));

        for self.opts.iter().advance |&opt| {
            member_doc.union(opt.to_bson_t());
        }

        Embedded(~member_doc)
    }
    pub fn from_bson_t(doc : &Document) -> Result<RSMember, ~str> {
        let bson_doc = match doc {
            &Embedded(ref bson) => bson,
            _ => return Err(~"not RSMember struct (not Embedded BsonDocument)"),
        };

        let mut member = RSMember::new(~"", ~[]);
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
                    match BsonFormattable::from_bson_t::<RS_MEMBER_OPTION>(&Embedded(~tmp)) {
                        Ok(opt) => opts.push(opt),
                        Err(e) => return Err(fmt!("not RSMember struct (err parsing options: %s)", e)),
                    }
                }
            }
        }

        if member._id.is_empty() {
            return Err(~"not RSMember struct (_id field not Int32)");
        }
        member.opts = opts;
        Ok(member)
    }
}
macro_rules! mk_get (
    ($prop_find:ident) => ({
        for self.opts.iter().advance |opt| {
            match opt {
                &$prop_find(ref x) => return Some(x),
                _ => (),
            }
        }
        None
    });
)
macro_rules! mk_get_mut (
    ($get:expr, $prop_find:ident, $default:expr) => ({
        let mut ptr = None;
        {
            if $get.is_none() {
                self.opts.push($default);
            }
        }
        {
            for self.opts.mut_iter().advance |opt| {
                match opt {
                    &$prop_find(ref mut x) => {
                        ptr = Some(x);
                        break;
                    }
                    _ => (),
                }
            }
        }
        ptr.unwrap()
    });
)
impl RSMember {
    pub fn new( host : ~str,
                opts : ~[RS_MEMBER_OPTION]) -> RSMember {
        RSMember {
            _id : Cell::new_empty(),
            host : host,
            opts : opts,
        }
    }

    /**
     * Gets read-only reference to tags.
     *
     * # Returns
     * None if there are no tags set, Some(ptr) to the tags if there are
     */
    // XXX inefficient
    pub fn get_tags<'a>(&'a self) -> Option<&'a TagSet> {
        mk_get!(TAGS)
    }
    /**
     * Gets writeable reference to tags, initializing with default
     * (empty) if there were previously none set. Intended for user
     * manipulation.
     *
     * # Returns
     * reference to tags, possibly initializing them within the `RSMember`
     */
    // XXX inefficient
    pub fn get_mut_tags<'a>(&'a mut self) -> &'a mut TagSet {
        mk_get_mut!(self.get_tags(), TAGS, TAGS(TagSet::new(~[])))
    }
    /**
     * Gets read-only reference to priority.
     *
     * # Returns
     * None if there is no priority set, Some(ptr) to the priority if there is
     */
    // XXX inefficient
    pub fn get_priority<'a>(&'a self) -> Option<&'a float> {
        mk_get!(PRIORITY)
    }
    /**
     * Gets writeable reference to priority, initializing with default
     * (1) if there was previously none set. Intended for user
     * manipulation.
     *
     * # Returns
     * reference to priority, possibly initializing them within the `RSMember`
     */
    // XXX inefficient
    pub fn get_mut_priority<'a>(&'a mut self) -> &'a mut float {
        mk_get_mut!(self.get_priority(), PRIORITY, PRIORITY(1f))
    }
}

#[deriving(Clone)]
pub struct RSConfig {
    _id : Option<~str>,
    priv version : Cell<i32>,
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

        if !self.version.is_empty() {
            let v = self.version.take();
            conf_doc.put(~"version", Int32(v));
            self.version.put_back(v);
        }

        let mut tmp_doc = BsonDocument::new();
        for self.members.iter().enumerate().advance |(i,&member)| {
            if member._id.is_empty() { member._id.put_back(i); }
            tmp_doc.put(i.to_str(), member.to_bson_t());
        }
        conf_doc.put(~"members", Array(~tmp_doc));

        tmp_doc = BsonDocument::new();
        match &self.settings {
            &None => (),
            &Some(ref a) => {
                for a.iter().advance |&opt| {
                    tmp_doc.union(opt.to_bson_t());
                }
                conf_doc.put(~"settings", Embedded(~tmp_doc));
            }
        }

        Embedded(~conf_doc)
    }

    pub fn from_bson_t(doc : &Document) -> Result<RSConfig, ~str> {
        let bson_doc = match doc {
            &Embedded(ref bson) => bson,
            _ => return Err(~"not RSConfig struct (not Embedded BsonDocument)"),
        };

        let _id = match bson_doc.find(~"_id") {
            None => None,
            Some(doc) => match doc {
                &UString(ref s) => Some(s.to_owned()),
                _ => return Err(~"not RSConfig struct (_id field not UString)"),
            },
        };
        let version = match bson_doc.find(~"version") {
            None => return Err(~"not RSConfig struct (no version field)"),
            Some(doc) => match doc {
                &Int32(ref v) => *v,
                _ => return Err(~"not RSConfig struct (version field not Int32)"),
            },
        };
        let members = match bson_doc.find(~"members") {
            None => return Err(~"not RSConfig struct (no members field)"),
            Some(doc) => match doc {
                &Array(_) => match BsonFormattable::from_bson_t::<~[RSMember]>(doc) {
                    Ok(arr) => arr,
                    Err(e) => return Err(fmt!("not RSConfig struct (members field: %s)", e)),
                },
                _ => return Err(~"not RSConfig struct (members field not Array)"),
            },
        };
        let mut s_arr = ~[];
        match bson_doc.find(~"settings") {
            None => (),
            Some(doc) => match doc {
                &Embedded(ref sub) => {
                    for sub.fields.iter().advance |&(@k,@v)| {
                        let mut tmp = BsonDocument::new();
                        tmp.put(k,v);
                        match BsonFormattable::from_bson_t::<RS_OPTION>(&Embedded(~tmp)) {
                            Ok(s) => s_arr.push(s),
                            Err(e) => return Err(fmt!("not RSConfig struct (error formatting settings: %s)", e)),
                        }
                    }
                }
                _ => return Err(~"not RSConfig struct (settings field not Embedded BsonDocument)"),
            }
        }

        let settings = if s_arr.len() > 0 { Some(s_arr) } else { None };
        let member = RSConfig::new(_id, members, settings);
        member.version.put_back(version);
        Ok(member)
    }
}
impl RSConfig {
    pub fn new( _id : Option<~str>,
                members : ~[RSMember],
                settings : Option<~[RS_OPTION]>) -> RSConfig {
        RSConfig {
            _id : _id,
            version : Cell::new_empty(),
            members : members,
            settings : settings,
        }
    }

    pub fn get_version(&self) -> Option<i32> {
        match self.version.is_empty() {
            true => None,
            false => Some(self.version.clone().take()),
        }
    }
}

/**
 * Replica set options.
 */
#[deriving(Clone,Eq)]
// alternative would be to split off _FLAGs from _OPTIONs, but
//      not all defaults being "false" makes such a split difficult
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
    pub fn from_bson_t(doc : &Document) -> Result<RS_OPTION, ~str> {
        let bson_doc = match doc {
            &Embedded(ref bson) => bson,
            _ => return Err(~"not RS_OPTION (not Embedded BsonDocument)"),
        };

        match bson_doc.find(~"chainingAllowed") {
            None => (),
            Some(s) => match s {
                &Bool(ref v) => return Ok(CHAINING_ALLOWED(*v)),
                _ => return Err(~"not RS_OPTION (chainingAllowed field not Bool)"),
            },
        }

        Err(~"not RS_OPTION (could not find any member in enum)")
    }
}
#[deriving(Clone,Eq)]
// alternative would be to split off _FLAGs from _OPTIONs, but
//      not all defaults being "false" makes such a split difficult
pub enum RS_MEMBER_OPTION {
    ARB_ONLY(bool),
    BUILD_INDS(bool),
    HIDDEN(bool),
    PRIORITY(float),
    TAGS(TagSet),
    SLAVE_DELAY(int),
    VOTES(int),
}
impl BsonFormattable for RS_MEMBER_OPTION {
    pub fn to_bson_t(&self) -> Document {
        let mut opt_doc = BsonDocument::new();
        let (k, v) = match self {
            &ARB_ONLY(v) => (~"arbiterOnly", Bool(v)),
            &BUILD_INDS(v) => (~"buildIndexes", Bool(v)),
            &HIDDEN(v) => (~"hidden", Bool(v)),
            &PRIORITY(p) => (~"priority", Double(p as f64)),
            &TAGS(ref ts) => (~"tags", ts.to_bson_t()),
            &SLAVE_DELAY(d) => (~"slaveDelay", Int32(d as i32)),
            &VOTES(n) => (~"votes", Int32(n as i32)),
        };
        opt_doc.put(k, v);
        Embedded(~opt_doc)
    }
    // not intended for normal usage, since intended for use
    // with *single* RS_MEMBER_OPTION, and doc might contain more
    pub fn from_bson_t(doc : &Document) -> Result<RS_MEMBER_OPTION, ~str> {
        let bson_doc = match doc {
            &Embedded(ref bson) => bson,
            _ => return Err(~"not RS_OPTION (not Embedded BsonDocument)"),
        };

        match bson_doc.find(~"arbiterOnly") {
            None => (),
            Some(s) => match s {
                &Bool(ref v) => return Ok(ARB_ONLY(*v)),
                _ => return Err(~"not RS_MEMBER_OPTION (arbiterOnly field not Bool)"),
            },
        }
        match bson_doc.find(~"buildIndexes") {
            None => (),
            Some(s) => match s {
                &Bool(ref v) => return Ok(BUILD_INDS(*v)),
                _ => return Err(~"not RS_MEMBER_OPTION (buildIndexes field not Bool)"),
            },
        }
        match bson_doc.find(~"hidden") {
            None => (),
            Some(s) => match s {
                &Bool(ref v) => return Ok(HIDDEN(*v)),
                _ => return Err(~"not RS_MEMBER_OPTION (hidden field not Bool)"),
            },
        }
        match bson_doc.find(~"priority") {
            None => (),
            Some(s) => match s {
                &Double(ref v) => return Ok(PRIORITY(*v as float)),
                _ => return Err(~"not RS_MEMBER_OPTION (priority field not Double)"),
            },
        }
        match bson_doc.find(~"tags") {
            None => (),
            Some(s) => match BsonFormattable::from_bson_t::<TagSet>(s) {
                Ok(ts) => return Ok(TAGS(ts)),
                Err(e) => return Err(e),
            },
        }
        match bson_doc.find(~"slaveDelay") {
            None => (),
            Some(s) => match s {
                &Int32(ref v) => return Ok(SLAVE_DELAY(*v as int)),
                _ => return Err(~"not RS_MEMBER_OPTION (slaveDelay field not Int32)"),
            },
        }
        match bson_doc.find(~"votes") {
            None => (),
            Some(s) => match s {
                &Int32(ref v) => return Ok(VOTES(*v as int)),
                _ => return Err(~"not RS_MEMBER_OPTION (votes field not Int32)"),
            },
        }

        Err(~"not RS_MEMBER_OPTION (could not find any member in enum)")
    }
}

/**
 * Handle to replica set itself for functionality pertaining to
 * replica set-related characteristics, e.g. configuration.
 *
 * For functionality handling how the replica set is to be interacted
 * with, e.g. setting read preference, etc. go through the client.
 */
impl RS {
    pub fn new(client : @Client) -> RS {
        RS {
            client : client,
        }
    }

    /**
     * Gets configuration of replica set referred to by this handle.
     *
     * # Returns
     * RSConfig struct on success, MongoErr on failure
     */
    pub fn get_config(&self) -> Result<RSConfig, MongoErr> {
        let coll = Collection::new(~"local", SYSTEM_REPLSET.to_owned(), self.client);
        let doc = match coll.find_one(None, None, None) {
            Ok(d) => d,
            Err(e) => return Err(e),
        };
        match BsonFormattable::from_bson_t::<RSConfig>(&Embedded(doc)) {
            Ok(conf) => Ok(conf),
            Err(e) => Err(MongoErr::new(
                            ~"rs::get_config",
                            ~"error formatting document into RSConfig",
                            e)),
        }
    }

    /**
     * Adds specified host to replica set; specify options directly
     * within host struct.
     *
     * # Arguments
     * * `host` - host, with options, to add to replica set
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn add(&self, host : RSMember) -> Result<(), MongoErr> {
        let mut conf = match self.get_config() {
            Ok(c) => c,
            Err(e) => return Err(e),
        };
        conf.members.push(host);
        self.reconfig(conf, false)
    }

    /**
     * Removes specified host from replica set.
     *
     * # Arguments
     * * `host` - host (as string) to remove
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn remove(&self, host : ~str) -> Result<(), MongoErr> {
        let op = match self.client.set_read_pref(PRIMARY_ONLY) {
            Ok(pref) => pref,
            Err(e) => return Err(MongoErr::new(
                                    ~"rs::step_down",
                                    ~"could not reset preference to primary",
                                    e.to_str())),
        };

        // get present configuration
        let mut conf = match self.get_config() {
            Ok(c) => c,
            Err(e) => return Err(e),
        };

        // figure out which node to remove
        let mut ind = None;
        for conf.members.iter().enumerate().advance |(i,&m)| {
            if m.host == host {
                ind = Some(i);
            }
        }

        let reset = self.client.set_read_pref(op.clone());
        let result = match ind {
            None => Err(MongoErr::new(
                            ~"rs::remove",
                            fmt!("could not remove nonexistent host %s", host),
                            ~"")),
            Some(i) => {
                conf.members.remove(i);
                self.reconfig(conf, false)
            }
        };
        match (result.is_ok(), reset.is_ok()) {
            (true, true) => Ok(()),
            (true, false) => Err(MongoErr::new(
                                    ~"rs::remove",
                                    fmt!("could not reset preference to %?", op),
                                    reset.unwrap_err().to_str())),
            (false, true) => Err(MongoErr::new(
                                    ~"rs::remove",
                                    fmt!("error removing host %s", host),
                                    result.unwrap_err().to_str())),
            (false, false) => Err(MongoErr::new(
                                    ~"rs::remove",
                                    fmt!("error removing host %s AND could not reset preference to %?", host, op),
                                    fmt!("%s; %s", result.unwrap_err().to_str(), reset.unwrap_err().to_str()))),
        }
    }

    /**
     * Gets status of replica set.
     *
     * # Returns
     * ~BsonDocument containing status information, MongoErr on failure
     */
    pub fn get_status(&self) -> Result<~BsonDocument, MongoErr> {
        let op = match self.client.set_read_pref(PRIMARY_PREF(None)) {
            Ok(pref) => pref,
            Err(e) => return Err(MongoErr::new(
                                    ~"rs::get_status",
                                    ~"could not reset preference to primary preferred",
                                    e.to_str())),
        };
        let db = self.client.get_admin();
        let result = db.run_command(SpecNotation(~"{ 'replSetGetStatus':1 }"));
        self.client.set_read_pref(op);
        result
    }

    /**
     * Reconfigure replica set to have given configuration.
     *
     * # Arguments
     * * `conf` - new configuration for replica set
     * * `force` - whether or not to force the reconfiguration
     *              WARNING: use with caution; may lead to rollback and
     *              other difficult-to-recover-from situations
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    pub fn reconfig(&self, conf : RSConfig, force : bool)
                -> Result<(), MongoErr> {
        // be sure to increment version number
        let tmp_conf = match self.get_config() {
            Ok(c) => c,
            Err(e) => return Err(MongoErr::new(
                                    ~"rs::reconfig",
                                    ~"failure getting latest config version no",
                                    e.to_str())),
        };
        if !conf.version.is_empty() { conf.version.take(); }
        conf.version.put_back(tmp_conf.version.take()+1);

        let old_pref = self.client.set_read_pref(PRIMARY_ONLY);
        let conf_doc = conf.to_bson_t();
        let db = self.client.get_admin();
        let mut cmd_doc = BsonDocument::new();
        cmd_doc.put(~"replSetReconfig", conf_doc);
        cmd_doc.put(~"force", Bool(force));
        let result = match db.run_command(SpecObj(cmd_doc)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };
        // force reconnect to give conn chance to update
        self.client.reconnect();
        match old_pref {
            Err(e) => { return Err(e); }    // should never happen
            Ok(p) => { self.client.set_read_pref(p); }
        }
        result
    }

    /**
     * Prevent specified node from seeking election for
     * specified number of seconds.
     */
    // XXX require &RSMember to be passed? check that node actually in RS?
    pub fn node_freeze(&self, host : ~str, sec : u64)
                -> Result<(), MongoErr> {
        let client = @Client::new();
        let (ip, port) = match parse_host(host.as_slice()) {
            Ok(p) => p,
            Err(e) => return Err(e),
        };
        match client.connect(ip, port) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let admin = client.get_admin();
        let result = match admin.run_command(SpecNotation(fmt!("{ 'replSetFreeze':%? }", sec))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };
        client.disconnect();

        result
    }

    // convenience function
    pub fn node_unfreeze(&self, host : ~str) -> Result<(), MongoErr> {
        self.node_freeze(host, 0)
    }

    /**
     * Forces current primary to step down for specified number of seconds.
     *
     * # Arguments
     * * `sec` - number of seconds for current primary to step down
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    // XXX better way to do this while maintaining
    //      RS/ReplicaSetConnection barrier?
    pub fn step_down(&self, sec : u64) -> Result<(), MongoErr> {
        let op = match self.client.set_read_pref(PRIMARY_ONLY) {
            Ok(pref) => pref,
            Err(e) => return Err(MongoErr::new(
                                    ~"rs::step_down",
                                    ~"could not reset preference to primary",
                                    e.to_str())),
        };

        let admin = self.client.get_admin();
        let result = match admin.run_command(SpecNotation(fmt!(" { 'replSetStepDown':%? } ", sec))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };

        self.client.reconnect();

        let reset = self.client.set_read_pref(op.clone());
        match (result.is_ok(), reset.is_ok()) {
            (true, true) => Ok(()),
            (true, false) => Err(MongoErr::new(
                                    ~"rs::step_down",
                                    fmt!("could not reset preference to %?", op),
                                    reset.unwrap_err().to_str())),
            (false, true) => Err(MongoErr::new(
                                    ~"rs::step_down",
                                    ~"error stepping down",
                                    result.unwrap_err().to_str())),
            (false, false) => Err(MongoErr::new(
                                    ~"rs::step_down",
                                    fmt!("error stepping down AND could not reset preference to %?", op),
                                    fmt!("%s; %s", result.unwrap_err().to_str(), reset.unwrap_err().to_str()))),
        }
    }

    /**
     * Sync given node from another node.
     *
     * # Arguments
     * `node` - node to sync
     * `from` - node from which to sync
     *
     * # Return
     * () on success, MongoErr on failure
     */
    // TODO: input args (format, check, etc.)
    pub fn node_sync_from(&self, node : ~str, from : ~str)
                -> Result<(), MongoErr> {
        let client = @Client::new();
        let (ip, port) = match parse_host(node.as_slice()) {
            Ok(p) => p,
            Err(e) => return Err(e),
        };
        match client.connect(ip, port) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        let admin = client.get_admin();
        let result = match admin.run_command(SpecNotation(fmt!("{ 'replSetSyncFrom':'%s' }", from ))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        };

        client.disconnect();
        result
    }
}
