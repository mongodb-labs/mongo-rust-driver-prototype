use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

use client::Result;
use client::Error::ResponseError;

/// Represents an opcode in the MongoDB Wire Protocol.
#[derive(Clone)]
pub enum OpCode {
    Reply = 1,
    Update = 2001,
    Insert = 2002,
    Query = 2004,
    GetMore = 2005,
}

impl OpCode {
    /// Maps integer values to OpCodes
    ///
    /// # Arguments
    ///
    /// `i` - The integer to map.
    ///
    /// # Return value
    ///
    /// Returns the matching opcode, or `None` if the integer isn't a valid
    /// opcode.
    pub fn from_i32(i: i32) -> Option<OpCode> {
        match i {
            1 => Some(OpCode::Reply),
            2001 => Some(OpCode::Update),
            2002 => Some(OpCode::Insert),
            2004 => Some(OpCode::Query),
            2005 => Some(OpCode::GetMore),
            _ => None
        }
    }
}

impl ToString for OpCode {
    /// Gets the string representation of an opcode.
    ///
    /// # Return value
    ///
    /// Returns the string represetnation of the opcode.
    fn to_string(&self) -> String {
        match self {
            &OpCode::Reply => "OP_REPLY".to_owned(),
            &OpCode::Update => "OP_UPDATE".to_owned(),
            &OpCode::Insert => "OP_INSERT".to_owned(),
            &OpCode::Query => "OP_QUERY".to_owned(),
            &OpCode::GetMore => "OP_GET_MORE".to_owned(),
        }
    }
}

/// Represents a header in the MongoDB Wire Protocol.
///
/// # Fields
///
/// `message_length` - The length of the entire message in bytes.
/// `request_id` - Identifies the request being sent. This should be `0` in a
///                response from the server.
/// `response_to` - Identifies which response the message is a response to. This
///                 should be `0` in a request from the client.
/// `op_code`     - Identifies which type of message is being sent.
// #[derive(Clone)]
pub struct Header {
    pub message_length: i32,
    pub request_id: i32,
    response_to: i32,
    pub op_code: OpCode,
}

impl Header {
    /// Constructs a new Header.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// `response_to` - Identifies which request the message is in response to,
    ///                or `0` if the the message is a request.
    /// `op_code` - Identifies which type of message is being sent.
    ///
    /// # Return value
    ///
    /// Returns the newly-created Header.
    pub fn new(message_length: i32, request_id: i32, response_to: i32,
           op_code: OpCode) -> Header {
        Header { message_length: message_length, request_id: request_id,
                 response_to: response_to, op_code: op_code }
    }

    /// Construcs a new Header for a request.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// `op_code` - Identifies which type of message is being sent.
    ///
    /// # Return value
    ///
    /// Returns a new Header with `response_to` set to 0.
    fn new_request(message_length: i32, request_id: i32,
                   op_code: OpCode) -> Header {
        Header::new(message_length, request_id, 0, op_code)
    }

    /// Construcs a new Header for a reply.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// `op_code` - Identifies which type of message is being sent.
    ///
    /// # Return value
    ///
    /// Returns a new Header with `request_id` set to 0.
    fn new_reply(message_length: i32, response_to: i32,
                 op_code: OpCode) -> Header {
        Header::new(message_length, 0, response_to, op_code)
    }

    /// Constructs a new Header for an OP_UPDATE.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// # Return value
    ///
    /// Returns a new Header with `response_to` set to 0 and `op_code`
    /// set to `Update`.
    pub fn with_update(message_length: i32, request_id: i32) -> Header {
        Header::new_request(message_length, request_id, OpCode::Update)
    }

    /// Constructs a new Header for an OP_INSERT.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// # Return value
    ///
    /// Returns a new Header with `response_to` set to 0 and `op_code`
    /// set to `Insert`.
    pub fn with_insert(message_length: i32, request_id: i32) -> Header {
        Header::new_request(message_length, request_id, OpCode::Insert)
    }

    /// Constructs a new Header for an OP_QUERY.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// # Return value
    ///
    /// Returns a new Header with `response_to` set to 0 and `op_code`
    /// set to `Query`.
    pub fn with_query(message_length: i32, request_id: i32) -> Header {
        Header::new_request(message_length, request_id, OpCode::Query)
    }

    /// Constructs a new Header for an OP_GET_MORE.
    ///
    /// # Arguments
    ///
    /// `message_length` - The length of the message in bytes.
    /// `request_id` - Identifier for the request, or `0` if the the message
    ///                is a response.
    /// # Return value
    ///
    /// Returns a new Header with `response_to` set to 0 and `op_code`
    /// set to `GetMore`.
    pub fn with_get_more(message_length: i32, request_id: i32) -> Header {
        Header::new_request(message_length, request_id, OpCode::GetMore)
    }

    /// Writes the serialized Header to a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    pub fn write(&self, buffer: &mut Write) -> Result<()> {
        try!(buffer.write_i32::<LittleEndian>(self.message_length));
        try!(buffer.write_i32::<LittleEndian>(self.request_id));
        try!(buffer.write_i32::<LittleEndian>(self.response_to));
        try!(buffer.write_i32::<LittleEndian>(self.op_code.clone() as i32));
        let _ = buffer.flush();

        Ok(())
    }

    /// Reads a serialized Header from a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns the parsed Header on success, or an error string on failure.
    pub fn read(buffer: &mut Read) -> Result<Header> {
        let message_length = try!(buffer.read_i32::<LittleEndian>());
        let request_id = try!(buffer.read_i32::<LittleEndian>());
        let response_to = try!(buffer.read_i32::<LittleEndian>());

        let op_code_i32 = try!(buffer.read_i32::<LittleEndian>());
        let op_code = match OpCode::from_i32(op_code_i32) {
            Some(code) => code,
            _ => return Err(ResponseError(format!("Invalid header opcode from server: {}", op_code_i32))),
        };

        Ok(Header::new(message_length, request_id, response_to, op_code))
    }
}
