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

use sys = std::sys;
use std::to_bytes::*;
use std::vec::raw::*;

use bson::encode::*;
use bson::decode::*;

use util::*;

// XXX ideally, this could be used in all the len computations
//static header_sz : uint = 4*sys::size_of::<i32>();

enum OpCode {
    OP_REPLY = 1i32 as int,
    OP_MSG = 1000i32 as int,        // deprecated; no need to implement
    OP_UPDATE = 2001i32 as int,
    OP_INSERT = 2002i32 as int,
    RESERVED = 2003i32 as int,
    OP_QUERY = 2004i32 as int,
    OP_GET_MORE = 2005i32 as int,
    OP_DELETE = 2006i32 as int,
    OP_KILL_CURSORS = 2007i32 as int,
}

struct MsgHeader {
    len : i32,      // total message size in bytes
    id : i32,       // client- or db-generated identifier for message
    resp_to : i32,  // id from OP_QUERY or OP_GET_MORE messages from client
    opcode : i32,
}

pub enum ClientMsg {
    // Client request messages
    OpUpdate {                              // gets no response
        header : MsgHeader,
        RESERVED_BITS : i32,
        full_collection_name : ~str,
        flags : i32,
        selector : BsonDocument,
        update_ops : BsonDocument,
    },
    OpInsert {                              // gets no response
        header : MsgHeader,
        flags : i32,
        full_collection_name : ~str,
        docs : ~[BsonDocument]
    },
    OpQuery {                               // response of OpReply
        header : MsgHeader,
        flags : i32,
        full_collection_name : ~str,
        nskip : i32,
        nret : i32,
        query : BsonDocument,
        ret_field_selector : Option<BsonDocument>,
    },
    OpGetMore {                             // response of OpReply
        header : MsgHeader,
        RESERVED_BITS : i32,
        full_collection_name : ~str,
        nret : i32,
        cursor_id : i64                     // from OpReply
    },
    OpDelete {                              // gets no response
        header : MsgHeader,
        RESERVED_BITS : i32,
        full_collection_name : ~str,
        flags : i32,
        selector : BsonDocument,
    },
    OpKillCursors {
        header : MsgHeader,
        RESERVED_BITS : i32,
        ncursor_ids : i32,
        cursor_ids : ~[i64]
    },
}
pub enum ServerMsg {
    // DB response messages
    OpReply {
        header : MsgHeader,
        flags : i32,
        cursor_id : i64,
        start : i32,
        nret : i32,
        docs : ~[~BsonDocument],
    },
}

/**
 * Converts a message to bytes.
 */
fn _header_to_bytes(header : MsgHeader) -> ~[u8] {
    header.len.to_bytes(LITTLE_ENDIAN_TRUE)
        + header.id.to_bytes(LITTLE_ENDIAN_TRUE)
        + header.resp_to.to_bytes(LITTLE_ENDIAN_TRUE)
        + header.opcode.to_bytes(LITTLE_ENDIAN_TRUE)
}
pub fn msg_to_bytes(msg : ClientMsg) -> ~[u8] {
    let mut bytes : ~[u8] = ~[];
    match msg {
        OpUpdate { header:h, RESERVED_BITS:r, full_collection_name:n, flags:f, selector:s, update_ops:u } => {
            bytes = bytes + _header_to_bytes(h)
                        + r.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE)
                        + ~[0u8]    // null-terminate name
                        + f.to_bytes(LITTLE_ENDIAN_TRUE)
                        + s.to_bson()
                        + u.to_bson();
        }
        OpInsert { header:h, flags:f, full_collection_name:n, docs:d } => {
            bytes = bytes + _header_to_bytes(h)
                        + f.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE)
                        + ~[0u8];   // null-terminate name
            for d.iter().advance |doc| { bytes = bytes + doc.to_bson(); }
        }
        OpQuery { header:h, flags:f, full_collection_name:n, nskip:ns, nret:nr, query:q, ret_field_selector:fi } => {
            bytes = bytes + _header_to_bytes(h)
                        + f.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE)
                        + ~[0u8]    // null-terminate name
                        + ns.to_bytes(LITTLE_ENDIAN_TRUE)
                        + nr.to_bytes(LITTLE_ENDIAN_TRUE)
                        + q.to_bson()
                        + match fi { None => ~[], Some(f) => f.to_bson() };
        }
        OpGetMore { header:h, RESERVED_BITS:r, full_collection_name:n, nret:nr, cursor_id:id } => {
            bytes = bytes + _header_to_bytes(h)
                        + r.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE)
                        + ~[0u8]    // null-terminate name
                        + nr.to_bytes(LITTLE_ENDIAN_TRUE)
                        + id.to_bytes(LITTLE_ENDIAN_TRUE);
        }
        OpDelete { header:h, RESERVED_BITS:r, full_collection_name:n, flags:f, selector:s } => {
            bytes = bytes + _header_to_bytes(h)
                        + r.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE)
                        + ~[0u8]    // null-terminate name
                        + f.to_bytes(LITTLE_ENDIAN_TRUE)
                        + s.to_bson();
        }
        OpKillCursors { header:h, RESERVED_BITS:r, ncursor_ids:n, cursor_ids:ids } => {
            bytes = bytes + _header_to_bytes(h)
                        + r.to_bytes(LITTLE_ENDIAN_TRUE)
                        + n.to_bytes(LITTLE_ENDIAN_TRUE);
            for ids.iter().advance |&cur| { bytes = bytes + cur.to_bytes(LITTLE_ENDIAN_TRUE); }
        }
    }
    bytes
}

/**
 * Boilerplate for update op.
 */
pub fn mk_update(id : i32, db : &~str, name : &~str, flags : i32, selector : BsonDocument, update_ops : BsonDocument) -> ClientMsg {
    let full = fmt!("%s.%s", *db, *name);
    let len = (   4*sys::size_of::<i32>()
                + 2*sys::size_of::<i32>()
                + full.len() + 1
                + selector.size as uint
                + update_ops.size as uint) as i32;

    OpUpdate {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_UPDATE as i32 },
        RESERVED_BITS : 0i32,
        full_collection_name : full,
        flags : flags,
        selector : selector,
        update_ops : update_ops,
    }
}

/**
 * Boilerplate for insert op.
 */
pub fn mk_insert(id : i32, db : &~str, name : &~str, flags : i32, docs : ~[BsonDocument]) -> ClientMsg {
    let full = fmt!("%s.%s", *db, *name);
    let mut len = (   4*sys::size_of::<i32>()
                + sys::size_of::<i32>()
                + full.len() + 1) as i32;
    for docs.iter().advance |&d| { len = len + d.size as i32; }

    OpInsert {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_INSERT as i32 },
        flags : flags,
        full_collection_name : full,
        docs : docs,
    }
}

/**
 * Boilerplate for query op.
 */
pub fn mk_query(id : i32, db : &~str, name : &~str, flags : i32, nskip : i32, nret : i32, query : BsonDocument, ret_field_selector : Option<BsonDocument>) -> ClientMsg {
    let full = fmt!("%s.%s", *db, *name);
    let mut len = (   4*sys::size_of::<i32>()
                + 3*sys::size_of::<i32>()
                + full.len() + 1
                + query.size as uint) as i32;
    len = len + match ret_field_selector {
        None => 0,
        Some(ref bson) => bson.size as i32,
    };

    OpQuery {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_QUERY as i32 },
        flags : flags,
        full_collection_name : full,
        nskip : nskip,
        nret : nret,
        query : query,
        ret_field_selector : ret_field_selector,
    }
}

/**
 * Boilerplate for get_more op.
 */
pub fn mk_get_more(id : i32, db : &~str, name : &~str, nret : i32, cursor_id : i64) -> ClientMsg {
    let full = fmt!("%s.%s", *db, *name);
    let len = (   4*sys::size_of::<i32>()
                + 2*sys::size_of::<i32>()
                + 1*sys::size_of::<i64>()
                + full.len() + 1) as i32;

    OpGetMore {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_GET_MORE as i32 },
        RESERVED_BITS : 0i32,
        full_collection_name : full,
        nret : nret,
        cursor_id : cursor_id,
    }
}

/**
 * Boilerplate for delete op.
 */
pub fn mk_delete(id : i32, db : &~str, name : &~str, flags : i32, selector : BsonDocument) -> ClientMsg {
    let full = fmt!("%s.%s", *db, *name);
    let len = (   4*sys::size_of::<i32>()
                + 2*sys::size_of::<i32>()
                + full.len() + 1
                + selector.size as uint) as i32;

    OpDelete {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_DELETE as i32 },
        RESERVED_BITS : 0i32,
        full_collection_name : full,
        flags : flags,
        selector : selector
    }
}

/**
 * Boilerplate for cursor kill op.
 */
pub fn mk_kill_cursor(id : i32, ncursor_ids : i32, cursor_ids : ~[i64]) -> ClientMsg {
    let len = (   4*sys::size_of::<i32>()
                + 2*sys::size_of::<i32>()
                + cursor_ids.len()*sys::size_of::<i64>()) as i32;

    OpKillCursors {
        header : MsgHeader { len : len, id : id, resp_to : 0i32, opcode : OP_KILL_CURSORS as i32 },
        RESERVED_BITS : 0i32,
        ncursor_ids : ncursor_ids,
        cursor_ids : cursor_ids
    }
}

/**
 * Parses bytes into msg, for reply op.
 * Assumes machine little-endian; messages are.
 */
pub fn parse_reply(bytes : ~[u8]) -> Result<ServerMsg, MongoErr> {
    // prepare to pull out non-document fields with pointer arithmetic
    let len_addr = to_ptr::<u8>(bytes) as uint;
    let id_addr = len_addr + sys::size_of::<i32>();
    let resp_to_addr = id_addr + sys::size_of::<i32>();
    let opcode_addr = resp_to_addr + sys::size_of::<i32>();
    let flags_addr = opcode_addr + sys::size_of::<i32>();
    let cursor_id_addr = flags_addr + sys::size_of::<i32>();
    let start_addr = cursor_id_addr + sys::size_of::<i64>();
    let nret_addr = start_addr + sys::size_of::<i32>();

    unsafe {
        // pull out documents one-by-one
        let mut docs : ~[~BsonDocument] = ~[];
        let mut head = nret_addr + sys::size_of::<i32>() - len_addr;

        for (*(nret_addr as *i32) as uint).times {
            let size = *((head+len_addr) as *i32);

            let doc_bytes = bytes.slice(head, head+(size as uint)).to_owned();
            let tmp = match decode(doc_bytes) {
                Ok(d) => d,
                Err(e) => return Err(MongoErr::new(
                                        ~"msg::parse_reply",
                                        ~"error unpacking documents",
                                        fmt!("-->\n%s", e))),
            };
            docs = docs + ~[~tmp];
            head = head + size as uint;
        }

        // construct reply
        Ok(OpReply {
            header : MsgHeader {    len : *(len_addr as *i32),
                                    id : *(id_addr as *i32),
                                    resp_to : *(resp_to_addr as *i32),
                                    opcode : *(opcode_addr as *i32) },
            flags : *(flags_addr as *i32),
            cursor_id : *(cursor_id_addr as *i64),
            start : *(start_addr as *i32),
            nret : *(nret_addr as *i32),
            docs : docs,
        })
    }
}
