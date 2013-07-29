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

pub enum MongoIndex {
    MongoIndexName(~str),
    MongoIndexFields(~[INDEX_FIELD]),
    //MongoIndex(MongoIndex),
}

/*pub struct MongoIndex {
    version : int,
    keys : ~[INDEX_FIELD],
    ns : ~str,
    name : ~str,
}*/

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

impl MongoIndex {
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
                            name = Some(copy n);
                            fmt!("\"name\":\"%s\"", n)
                        }
                        EXPIRE_AFTER_SEC(exp) => fmt!("\"expireAfterSeconds\":%d", exp).to_owned(),
                        VERS(v) => fmt!("\"v\":%d", v),
                        //WEIGHTS(BsonDocument),
                        //DEFAULT_LANG(~str),
                        //OVERRIDE_LANG(~str),
                    });
                }
            }
        };

        (name, opts_str)
    }
    pub fn process_index_fields(    index_arr : ~[INDEX_FIELD],
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
     * name of index (string passed in if `MongoIndexName` passed),
     * default index name if `MongoIndexFields` passed)
     */
    pub fn get_name(&self) -> ~str {
        match (copy *self) {
            MongoIndexName(s) => s,
            MongoIndexFields(arr) => {
                let mut tmp = ~[];
                let (name, _) = MongoIndex::process_index_fields(arr, &mut tmp, true);
                name
            }
        }
    }
}
