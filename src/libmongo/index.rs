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
use bson::formattable::*;

#[deriving(Clone,Eq)]
pub enum MongoIndexSpec {
    MongoIndexName(~str),
    MongoIndexFields(~[INDEX_TYPE]),
    MongoIndex(MongoIndex),
}

#[deriving(Clone,Eq)]
pub struct MongoIndex {
    version : int,
    keys : ~[INDEX_TYPE],
    ns : ~str,
    name : ~str,
    flags : Option<~[INDEX_FLAG]>,
    options : Option<~[INDEX_OPTION]>,
}

/**
 * Indexing.
 */
#[deriving(Clone,Eq)]
pub enum INDEX_ORDER {
    ASC = 1,
    DESC = -1,
}
#[deriving(Clone,Eq)]
pub enum INDEX_FLAG {
    BACKGROUND = 1 << 0,
    UNIQUE = 1 << 1,
    DROP_DUPS = 1 << 2,
    SPARSE = 1 << 3,
}
#[deriving(Clone,Eq)]
pub enum INDEX_OPTION {
    INDEX_NAME(~str),
    EXPIRE_AFTER_SEC(int),
    VERS(int),
    //WEIGHTS(~[(~str, int)]),
    DEFAULT_LANG(~str),
    LANG_OVERRIDE(~str),
}
#[deriving(Clone,Eq)]
pub enum INDEX_GEOTYPE {
    SPHERICAL,                          // "2dsphere"
    FLAT,                               // "2d"
}
#[deriving(Clone,Eq)]
pub enum INDEX_TYPE {
    NORMAL(~[(~str, INDEX_ORDER)]),
    HASHED(~str),
    GEOSPATIAL(~str, INDEX_GEOTYPE),
    GEOHAYSTACK(~str, ~str, uint),
}
impl BsonFormattable for INDEX_TYPE {
    pub fn to_bson_t(&self) -> Document {
        let mut bson_doc = BsonDocument::new();
        match self {
            &NORMAL(ref arr) => {
                for arr.iter().advance |&(key, order)| {
                    bson_doc.put(key, Int32(order as i32));
                }
            }
            &HASHED(ref key) => bson_doc.put(key.to_owned(), UString(~"hashed")),
            &GEOSPATIAL(ref key, geotype) => {
                let typ = match geotype {
                    SPHERICAL => ~"2dsphere",
                    FLAT => ~"2d",
                };
                bson_doc.put(key.to_owned(), UString(typ));
            }
            &GEOHAYSTACK(ref loc, ref snd, sz) => {
                bson_doc.put(loc.to_owned(), UString(~"geoHaystack"));
                bson_doc.put(snd.to_owned(), Int32(1));
                bson_doc.put(~"bucketSize", Int32(sz as i32));
            }
        }
        Embedded(~bson_doc)
    }
    pub fn from_bson_t(_ : &Document) -> Result<INDEX_TYPE, ~str> {
        Err(~"do not call from_bson_t to INDEX_TYPE")
    }
}

impl BsonFormattable for MongoIndex {
    pub fn to_bson_t(&self) -> Document {
        let mut bson_doc = BsonDocument::new();
        for self.keys.iter().advance |&f| {
            bson_doc.union(f.to_bson_t());
        }
        Embedded(~bson_doc)
    }
    pub fn from_bson_t(doc : &Document) -> Result<MongoIndex, ~str> {
        let bson_doc = match doc {
            &Embedded(ref b) => b,
            _ => return Err(~"not MongoIndex struct (not Embedded BsonDocument)"),
        };

        // index fields
        let mut version = None;
        let mut ns = None;
        let mut name = None;
        let mut arr = ~[];
        let mut flags = ~[];
        let mut opts = ~[];

        // index key parts
        let mut normal = ~[];
        let mut hay = (None, None);
        for bson_doc.fields.iter().advance |&(@k,@v)| {
            match k {
                // basic fields parsing
                ~"v" => match v {
                    Int32(vers) => version = Some(vers as int),
                    Double(vers) => version = Some(vers as int),
                    _ => return Err(~"not MongoIndex struct (version field not Int32)"),
                },
                ~"ns" => match v {
                    UString(s) => ns = Some(s),
                    _ => return Err(~"not MongoIndex struct (ns field not UString)"),
                },
                ~"name" => match v {
                    UString(s) => name = Some(s),
                    _ => return Err(~"not MongoIndex struct (name field not UString)"),
                },
                // key parsing
                ~"key" => match v {
                    Embedded(f) => {
                        for f.fields.iter().advance |&(@k,@v)| {
                            match v {
                                Int32(ord) => match ord {
                                    1 => normal.push((k, ASC)),
                                    -1 => normal.push((k, DESC)),
                                    _ => return Err(
        fmt!("not MongoIndex struct (index order expected /pm 1 (ASC/DESC), found %?", v)),
                                },
                                Double(ord) => match ord {
                                    1f64 => normal.push((k, ASC)),
                                    -1f64 => normal.push((k, DESC)),
                                    _ => return Err(
        fmt!("not MongoIndex struct (index order expected /pm 1 (ASC/DESC), found %?", v)),
                                },
                                UString(s) => match s {
                                    ~"hashed" => arr.push(HASHED(k)),
                                    ~"2dsphere" => {
                                        if normal.len() > 0 {
                                            // compound ind is prefix
                                            arr.push(NORMAL(normal.clone()));
                                            normal = ~[];
                                        }
                                        arr.push(GEOSPATIAL(k, SPHERICAL));
                                    }
                                    ~"2d" => {
                                        arr.push(GEOSPATIAL(k, FLAT));
                                    }
                                    ~"geoHaystack" => {
                                        let (_,y) = hay.clone();
                                        hay = (Some(k), y);
                                    }
                                    _ => return Err(
        fmt!("not MongoIndex struct (unknown value %?)", s)),
                                },
                                _ => return Err(
        fmt!("not MongoIndex struct (unexpected value %?)", v)),
                            }
                        }
                    }
                    _ => return Err(
        ~"not MongoIndex struct (keys field not Embedded BsonDocument)"),
                },
                // flag parsing --- default is false, doesn't appear
                ~"background" => match v {
                    Int32(flag) => match flag {
                        1 => flags.push(BACKGROUND),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected background flag %?)", flag)),
                    },
                    Double(flag) => match flag {
                        1f64 => flags.push(BACKGROUND),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected background flag %?)", flag)),
                    },
                    _ => return Err(fmt!("not MongoIndex struct (unexpected background flag value %?)", v)),
                },
                ~"unique" => match v {
                    Int32(flag) => match flag {
                        1 => flags.push(UNIQUE),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected unique flag %?)", flag)),
                    },
                    Double(flag) => match flag {
                        1f64 => flags.push(UNIQUE),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected unique flag %?)", flag)),
                    },
                    _ => return Err(fmt!("not MongoIndex struct (unexpected unique flag value %?)", v)),
                },
                ~"dropDups" => match v {
                    Int32(flag) => match flag {
                        1 => flags.push(DROP_DUPS),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected dropDups flag %?)", flag)),
                    },
                    Double(flag) => match flag {
                        1f64 => flags.push(DROP_DUPS),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected dropDups flag %?)", flag)),
                    },
                    _ => return Err(fmt!("not MongoIndex struct (unexpected dropDups flag value %?)", v)),
                },
                ~"sparse" => match v {
                    Int32(flag) => match flag {
                        1 => flags.push(SPARSE),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected sparse flag %?)", flag)),
                    },
                    Double(flag) => match flag {
                        1f64 => flags.push(SPARSE),
                        _ => return Err(fmt!("not MongoIndex struct (unexpected sparse flag %?)", flag)),
                    },
                    _ => return Err(fmt!("not MongoIndex struct (unexpected sparse flag value %?)", v)),
                },
                // option parsing
                ~"bucketSize" => match v {
                    Int32(bucket) => {
                        let (x,_) = hay.clone();
                        hay = (x, Some(bucket as uint));
                    }
                    Double(bucket) => {
                        let (x,_) = hay.clone();
                        hay = (x, Some(bucket as uint));
                    }
                    _ => return Err(~"not MongoIndex struct (bucketSize field not Int32"),
                },
                ~"expireAfterSeconds" => match v {
                    Int32(sec) => opts.push(EXPIRE_AFTER_SEC(sec as int)),
                    Double(sec) => opts.push(EXPIRE_AFTER_SEC(sec as int)),
                    _ => return Err(~"not MongoIndex struct (expireAfterSeconds field not Int32"),
                },
                ~"default_language" => match v {
                    UString(s) => opts.push(DEFAULT_LANG(s)),
                    _ => return Err(~"not MongoIndex struct (default_language field not UString"),
                },
                ~"language_override" => match v {
                    UString(s) => opts.push(LANG_OVERRIDE(s)),
                    _ => return Err(~"not MongoIndex struct (language_override field not UString"),
                },
                _ => return Err(fmt!("not MongoIndex struct (unknown option %?)", k)),
            }
        }

        match hay {
            (Some(key), Some(bucket)) => {
                let snd = match normal.len() {
                    0 => ~"",
                    1 => {
                        let (field, _) = normal[0].clone();
                        field
                    }
                    _ => return Err(fmt!("geohaystack index references too many fields: %?", normal)),
                };
                arr.push(GEOHAYSTACK(key, snd, bucket));
            }
            _ => (),
        }

        if normal.len() > 0 { arr.push(NORMAL(normal)); }

        match (&version, &ns, &name) {
            (&Some(vers), &Some(ref namespace), &Some(ref s)) =>
                Ok(MongoIndex {
                    version : vers,
                    keys : arr,
                    ns : namespace.to_owned(),
                    name : s.to_owned(),
                    flags : if flags.len() > 0 { Some(flags) } else { None },
                    options : if opts.len() > 0 { Some(opts) } else { None },
                }),
            (_, _, _) => Err(fmt!("index missing fields, found: [v]%?; [ns]%?, [name]%?", version, ns, name)),
        }
    }
}

impl MongoIndexSpec {
    pub fn process_index_opts(flags : i32, options : Option<~[INDEX_OPTION]>) -> (Option<~str>, ~[~str]) {
        let mut opts_str: ~[~str] = ~[];

        // flags
        if (flags & BACKGROUND as i32) != 0i32 { opts_str.push(~"\"background\":true"); }
        if (flags & UNIQUE as i32) != 0i32 { opts_str.push(~"\"unique\":true"); }
        if (flags & DROP_DUPS as i32) != 0i32 { opts_str.push(~"\"dropDups\":true"); }
        if (flags & SPARSE as i32) != 0i32 { opts_str.push(~"\"spare\":true"); }

        // options
        let mut name = None;
        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    opts_str.push(match opt {
                        INDEX_NAME(n) => {
                            name = Some(n.clone());
                            fmt!("\"name\":\"%s\"", n)
                        }
                        EXPIRE_AFTER_SEC(exp) => fmt!("\"expireAfterSeconds\":%d", exp),
                        VERS(v) => fmt!("\"v\":%d", v),
                        //WEIGHTS(weights),
                        DEFAULT_LANG(lang) => fmt!("\"default_language\":\"%s\"", lang),
                        LANG_OVERRIDE(lang) => fmt!("\"language_override\":\"%s\"", lang),
                    });
                }
            }
        };

        (name, opts_str)
    }
    pub fn process_index_fields(    index_arr : &[INDEX_TYPE],
                                    index_opts : &mut ~[~str],
                                    get_name : bool)
            -> (~str, ~[~str]) {
        let mut name = ~[];
        let mut index_str = ~[];
        for index_arr.iter().advance |&field| {
            match field {
                NORMAL(arr) => {
                    for arr.iter().advance |&(key, order)| {
                        index_str.push(fmt!("\"%s\":%d", key, order as int));
                        if get_name { name.push(fmt!("%s_%d", key, order as int)); }
                    }
                }
                HASHED(key) => {
                    index_str.push(fmt!("\"%s\":\"hashed\"", key));
                    if get_name { name.push(fmt!("%s_hashed", key)); }
                }
                GEOSPATIAL(key, geotype) => {
                    let typ = match geotype {
                        SPHERICAL => ~"2dsphere",
                        FLAT => ~"2d",
                    };
                    index_str.push(fmt!("\"%s\":\"%s\"", key, typ));
                    if get_name { name.push(fmt!("%s_%s", key, typ)); }
                }
                GEOHAYSTACK(loc, snd, sz) => {
                    index_str.push(fmt!("\"%s\":\"geoHaystack\", \"%s\":1", loc, snd));
                    if get_name { name.push(fmt!("%s_geoHaystack_%s_1", loc, snd)); }
                    (*index_opts).push(fmt!("\"bucketSize\":%?", sz));
                }
            }
        }

        (name.connect("_"), index_str)
    }

    /**
     * From either `~str` or full specification of index, gets name.
     *
     * # Returns
     * name of index (string passed in if `MongoIndexName` passed,
     * default index name if `MongoIndexFields` passed, string as returned
     * from database if `MongoIndex` passed)
     */
    pub fn get_name(&self) -> ~str {
        match self {
            &MongoIndexName(ref s) => s.clone(),
            &MongoIndexFields(ref arr) => {
                let mut tmp = ~[];
                let (name, _) = MongoIndexSpec::process_index_fields(*arr, &mut tmp, true);
                name
            }
            &MongoIndex(ref ind) => ind.name.clone(),
        }
    }
}
