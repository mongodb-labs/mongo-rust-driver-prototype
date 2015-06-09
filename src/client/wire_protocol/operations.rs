use bson::Document as BsonDocument;
use bson;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use client::wire_protocol::flags::{Flags, OpQueryFlags, OpInsertFlags};
use client::wire_protocol::header::{Header, OpCode};
use std::io::{Read, Write};
use std::mem;
use std::result::Result::{Ok, Err};

trait ByteLength {
    /// Calculates the number of bytes in the serialized version of the struct.
    fn byte_length(&self) -> Result<i32, String>;
}

impl ByteLength for BsonDocument {
    /// Gets the length of a BSON document.
    ///
    /// # Return value
    ///
    /// Returns the number of bytes in the serialized BSON document.
    fn byte_length(&self) -> Result<i32, String> {
        let mut temp_buffer = vec![];

        match bson::encode_document(&mut temp_buffer, self) {
            Ok(_) => Ok(temp_buffer.len() as i32),
            Err(_) => Err("unable to serialize BSON document".to_owned())
        }
    }
}

impl OpQueryFlags {
    pub fn new(tc: bool, so: bool, or: bool, nct: bool, ad: bool, e: bool,
           p: bool) -> OpQueryFlags {
        OpQueryFlags{
            tailable_cursor: tc,
            slave_ok: so,
            oplog_relay: or,
            no_cursor_timeout: nct,
            await_data: ad,
            exhaust: e,
            partial: p,
        }
    }
}

/// Represents a message in the MongoDB Wire Protocol.
pub enum Message {
    OpReply {
        header: Header,
        flags: i32,
        cursor_id: i64,
        starting_from: i32,
        number_returned: i32,
        documents: Vec<BsonDocument>,
    },
    OpInsert {
        header: Header,
        flags: OpInsertFlags,
        full_collection_name: String,
        documents: Vec<BsonDocument>,
    },
    OpQuery {
        header: Header,
        flags: OpQueryFlags,
        full_collection_name: String,
        number_to_skip: i32,
        number_to_return: i32,
        query: BsonDocument,
        return_field_selector: Option<BsonDocument>,
    },
}

impl Message {
    pub fn with_insert(header_request_id: i32, flags: OpInsertFlags,
                       full_collection_name: String,
                       documents: Vec<BsonDocument>) -> Result<Message, String> {
        let header_length = mem::size_of::<Header>() as i32;
        let flags_length = mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = full_collection_name.len() as i32 + 1;

        let mut total_length = header_length + flags_length + string_length;

        for bson in documents.iter() {
            total_length += match bson.byte_length() {
                Ok(i) => i,
                Err(_) => return Err("Unable to serialize documents".to_owned())
            }
        }

        let header = Header::with_insert(total_length, header_request_id);

        Ok(Message::OpInsert { header: header, flags: flags,
                               full_collection_name: full_collection_name,
                               documents: documents })
    }

    /// Constructs a new message request for a query.
    ///
    /// # Arguments
    ///
    /// `header_request_id` - The request ID to be placed in the message header.
    /// `flags` - Bit vector of query options.
    /// `full_collection_name` - The full qualified name of the collection,
    ///                          beginning with the database name and a dot.
    /// `number_to_skip` - The number of initial documents to skip over in the query
    ///                    results.
    /// `number_to_return - The total number of documents that should be returned by
    ///                     the query.
    /// `return_field_selector - An optional projection of which fields should be
    ///                          present in the documents to be returned by the
    ///                          query.
    ///
    /// # Return value
    ///
    /// Returns the newly-created Message.
    pub fn with_query(header_request_id: i32, flags: OpQueryFlags,
                     full_collection_name: String, number_to_skip: i32,
                     number_to_return: i32, query: BsonDocument,
                     return_field_selector: Option<BsonDocument>) -> Result<Message, String> {
        let header_length = mem::size_of::<Header>() as i32;

        // There are three i32 fields in the an OpQuery (since OpQueryFlags is
        // represented as an 32-bit vector in the wire protocol).
        let i32_length = 3 * mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = full_collection_name.len() as i32 + 1;

        let bson_length = match query.byte_length() {
            Ok(i) => i,
            Err(_) => return Err("Unable to serialize query".to_owned())
        };

        // Add the length of the optional BSON document only if it exists.
        let option_length = match return_field_selector {
            Some(ref bson) => match bson.byte_length() {
                Ok(i) => i,
                Err(_) => return Err("Unable to serialize return_field_selector".to_owned())
            },
            None => 0
        };

        let total_length = header_length + i32_length + string_length +
                           bson_length + option_length;

        let header = Header::with_query(total_length, header_request_id);

        Ok(Message::OpQuery { header: header, flags: flags,
                              full_collection_name: full_collection_name,
                              number_to_skip: number_to_skip,
                              number_to_return: number_to_return, query: query,
                              return_field_selector: return_field_selector })
    }

    /// Writes a serialized BSON document to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `bson` - The document to serialize and write.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    fn write_bson_document(buffer: &mut Write, bson: &BsonDocument) -> Result<(), String>{
        let mut temp_buffer = vec![];

        match bson::encode_document(&mut temp_buffer, bson) {
            Ok(_) => match buffer.write(&temp_buffer) {
                Ok(_) => Ok(()),
                Err(_) => Err("unable to write BSON".to_owned())
            },
            Err(_) => Err("unable to encode BSON".to_owned())
        }
    }

    /// Writes a serialized query message to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `header` - The header for the given message.
    /// `flags` - Bit vector of query option.
    /// `full_collection_name` - The full qualified name of the collection,
    ///                          beginning with the database name and a dot.
    /// `number_to_skip` - The number of initial documents to skip over in the
    ///                    query results.
    /// `number_to_return - The total number of documents that should be
    ///                     returned by the query.
    /// `return_field_selector - An optional projection of which fields should
    ///                          be present in the documents to be returned by
    ///                          the query.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    fn write_query(buffer: &mut Write, header: &Header,
                   flags: &OpQueryFlags, full_collection_name: &str,
                   number_to_skip: i32, number_to_return: i32, query: &BsonDocument,
                   return_field_selector: &Option<BsonDocument>) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        match buffer.write_i32::<LittleEndian>(flags.to_i32()) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        for byte in full_collection_name.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write full_collection_name".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write full_collection_name".to_owned())
        };

        match buffer.write_i32::<LittleEndian>(number_to_skip) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write number_to_skip".to_owned())
        };

        match buffer.write_i32::<LittleEndian>(number_to_return) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write number_to_return".to_owned())
        };

        match Message::write_bson_document(buffer, query) {
            Ok(_) => (),
            Err(s) => return Err(format!("Unable to write query: {}", s))
        };

        match return_field_selector {
            &Some(ref bson) => match Message::write_bson_document(buffer, bson) {
                Ok(_) => (),
                Err(s) => {
                    let str = format!("Unable to write return_field_selector: {}", s);

                    return Err(str)
                }
            },
            &None => ()
        };

        let _ = buffer.flush();

        Ok(())
    }

    fn write_insert(buffer: &mut Write, header: &Header, flags: &OpInsertFlags,
                    full_collection_name: &str,
                    documents: &[BsonDocument]) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        match buffer.write_i32::<LittleEndian>(flags.to_i32()) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        for byte in full_collection_name.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write full_collection_name".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write full_collection_name".to_owned())
        };


        for bson in documents {
            match Message::write_bson_document(buffer, bson) {
                Ok(_) => (),
                Err(s) => return Err(format!("Unable to insert document: {}", s))
            };

        }

        let _ = buffer.flush();

        Ok(())
    }


    /// Attemps to write a serialized message to a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    pub fn write(&self, buffer: &mut Write) -> Result<(), String> {
        match self {
            /// Only the server should sent replies
            &Message::OpReply {..} => Err("OP_REPLY should not be sent by the client".to_owned()),
            &Message::OpInsert {
                header: ref h,
                flags: ref f,
                full_collection_name: ref fcn,
                documents: ref d,
            } => Message::write_insert(buffer, &h, &f, &fcn, &d),
            &Message::OpQuery {
                header: ref h,
                flags: ref f,
                full_collection_name: ref fcn,
                number_to_skip: ns,
                number_to_return: nr,
                query: ref q,
                return_field_selector: ref rfs
            } => Message::write_query(buffer, &h, &f, &fcn, ns, nr, &q, &rfs)
        }
    }

    /// Reads a serialized reply message from a buffer
    ///
    /// Right now, this returns only the first BSON document from the
    /// response; if there are more, it ignores the rest, and if there are none,
    /// it fails.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns a single BSON document on success, or an error string on
    /// failure.
    fn read_reply(buffer: &mut Read) -> Result<BsonDocument, String> {
        let _flags = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read flags".to_owned())
        };

        let _cid = match buffer.read_i64::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read cursor_id".to_owned())
        };

        let _sf = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read starting_from".to_owned())
        };

        let _nr = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read number_returned".to_owned())
        };

        match bson::decode_document(buffer) {
            Ok(bson) => Ok(bson),
            Err(_) => Err("Unable to read BSON".to_owned())
        }
    }

    /// Attempts to read a serialized reply Message from a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns a single BSON document on success, or an error string on
    /// failure.
    pub fn read(buffer: &mut Read) -> Result<BsonDocument, String> {
        let header = match Header::read(buffer) {
            Ok(h) => h,
            Err(s) => {
                let str = format!("Unable to read reply header: {}", s);

                return Err(str)
            }
        };

        match header.op_code {
            OpCode::Reply => Message::read_reply(buffer),
            opcode => {
                let s = format!("Expected to read response but instead found: {}", opcode.to_string());
                Err(s)
            }
        }
    }
}
