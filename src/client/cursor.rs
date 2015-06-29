use bson;
use bson::Bson;

use client::MongoClient;
use client::{Error, Result};

use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;

use std::collections::vec_deque::VecDeque;
use std::io::{Read, Write};

use std::ops::DerefMut;

pub const DEFAULT_BATCH_SIZE: i32 = 20;

/// Maintains a connection to the server and lazily returns documents from a
/// query.
///
/// # Fields
///
/// `client` - The client to read from.
/// `namespace` - The namespace to read and write from.
/// `batch_size` - How many documents to fetch at a given time from the server.
/// `cursor_id` - Uniquely identifies the cursor being returned by the reply.
pub struct Cursor<'a> {
    client: &'a MongoClient,
    namespace: String,
    batch_size: i32,
    cursor_id: i64,
    limit: i32,
    count: i32,
    buffer: VecDeque<bson::Document>,
}

impl <'a> Cursor<'a> {

    pub fn command_cursor(client: &'a MongoClient, db: &str, doc: bson::Document) -> Result<Cursor<'a>> {
        Cursor::query_with_batch_size(client, format!("{}.$cmd", db),
                                      1, OpQueryFlags::no_flags(), 0, 0,
                                      doc, None, true)
    }

    /// Gets the cursor id and BSON documents from a reply Message.
    ///
    /// # Arguments
    ///
    /// `message` - The reply message to get the documents from.
    ///
    /// # Return value.
    ///
    ///
    fn get_bson_and_cid_from_message(message: Message) -> Result<(VecDeque<bson::Document>, i64)> {
        match message {
            Message::OpReply { header: _, flags: _, cursor_id: cid,
                               starting_from: _, number_returned: _,
                               documents: docs } => {
                let mut v = VecDeque::new();

                for doc in docs {
                    v.push_back(doc.clone());
                }

                Ok((v, cid))
            },
            _ => Err(Error::CursorNotFoundError)
        }
    }

    fn get_bson_and_cursor_info_from_command_message(message: Message) -> Result<(VecDeque<bson::Document>, i64, String)> {
        let (v, _) = try!(Cursor::get_bson_and_cid_from_message(message));
        if v.len() != 1 {
            return Err(Error::CursorNotFoundError);
        }

        let ref doc = v[0];

        // Extract cursor information
        if let Some(&Bson::Document(ref cursor)) = doc.get("cursor") {
            if let Some(&Bson::I64(ref id)) = cursor.get("id") {
                if let Some(&Bson::String(ref ns)) = cursor.get("ns") {
                    if let Some(&Bson::Array(ref batch)) = cursor.get("firstBatch") {

                        // Extract first batch documents
                        let map = batch.iter().filter_map(|bdoc| {
                            if let &Bson::Document(ref doc) = bdoc {
                                Some(doc.clone())
                            } else {
                                None
                            }
                        }).collect();

                        return Ok((map, *id, ns.to_owned()))
                    }
                }
            }
        }

        Err(Error::CursorNotFoundError)
    }

    /// Executes a query where the batch size of the returned cursor is
    /// specified.
    ///
    /// # Arguments
    ///
    /// `client` - The client to read from.
    /// `namespace` - The namespace to read and write from.
    /// `batch_size` - How many documents the cursor should return at a time.
    /// `flags` - Bit vector of query options.
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
    /// Returns the cursor for the query results on success, or an error string
    /// on failure.
    pub fn query_with_batch_size<'b>(client: &'a MongoClient,
                                     namespace: String,
                                     batch_size: i32,
                                     flags: OpQueryFlags,
                                     number_to_skip: i32, number_to_return: i32,
                                     query: bson::Document,
                                     return_field_selector: Option<bson::Document>,
                                     is_cmd_cursor: bool) -> Result<Cursor<'a>> {

        let result = Message::with_query(client.get_req_id(), flags,
                                         namespace.to_owned(),
                                         number_to_skip, batch_size,
                                         query.clone(), return_field_selector);

        let stream = try!(client.acquire_stream());
        let mut locked = try!(stream.socket.lock());
        let mut socket = locked.deref_mut();

        let message = try!(result);
        try!(message.write(&mut socket));
        let reply = try!(Message::read(&mut socket));

        let (buf, cursor_id, namespace) = if is_cmd_cursor {
            try!(Cursor::get_bson_and_cursor_info_from_command_message(reply))
        } else {
            let (buf, id) = try!(Cursor::get_bson_and_cid_from_message(reply));
            (buf, id, namespace)
        };

        Ok(Cursor {
            client: client,
            namespace: namespace,
            batch_size: batch_size,
            cursor_id: cursor_id,
            limit: number_to_return,
            count: 0,
            buffer: buf,
        })
    }

    /// Executes a query with the default batch size.
    ///
    /// # Arguments
    ///
    /// `client` - The client to read from.
    /// `namespace` - The namespace to read and write from.
    /// `flags` - Bit vector of query options.
    /// `number_to_skip` - The number of initial documents to skip over in the
    ///                    query results.
    /// `number_to_return - The total number of documents that should be
    ///                     returned by the query.
    /// `query` - Specifies which documents to return.
    /// `return_field_selector - An optional projection of which fields should
    ///                          be present in the documents to be returned by
    ///                          the query.
    ///
    /// # Return value
    ///
    /// Returns the cursor for the query results on success, or an error string
    /// on failure.
    pub fn query(client: &'a MongoClient, namespace: String,
                 flags: OpQueryFlags, number_to_skip: i32, number_to_return: i32,
                 query: bson::Document, return_field_selector: Option<bson::Document>,
                 is_cmd_cursor: bool) -> Result<Cursor<'a>> {

        Cursor::query_with_batch_size(client, namespace, DEFAULT_BATCH_SIZE, flags,
                                      number_to_skip,
                                      number_to_return, query,
                                      return_field_selector, is_cmd_cursor)
    }

    /// Helper method to create a "get more" request.
    ///
    /// # Return value
    ///
    /// Returns the newly-created method.
    fn new_get_more_request(&mut self) -> Message {
        Message::with_get_more(self.client.get_req_id(),
                               self.namespace.to_owned(),
                               self.batch_size, self.cursor_id)
    }

    /// Attempts to read another batch of BSON documents from the stream.
    fn get_from_stream(&mut self) -> Result<()> {
        let stream = try!(self.client.acquire_stream());
        let mut locked = try!(stream.socket.lock());
        let mut socket = locked.deref_mut();

        let get_more = self.new_get_more_request();
        try!(get_more.write(&mut socket));
        let reply = try!(Message::read(&mut socket));

        let (v, _) = try!(Cursor::get_bson_and_cid_from_message(reply));
        self.buffer.extend(v);
        Ok(())
    }

    /// Attempts to read another batch of BSON documents from the stream.
    ///
    /// # Return value
    ///
    /// Returns the first BSON document returned from the stream, or `None` if
    /// there are no more documents to read.
    fn next_from_stream(&mut self) -> Result<Option<bson::Document>> {
        try!(self.get_from_stream());
        Ok(self.buffer.pop_front())
    }

    /// Attempts to read a specified number of BSON documents from the cursor.
    ///
    /// # Arguments
    ///
    /// `n` - The number of documents to read.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn next_n(&mut self, n: i32) -> Result<Vec<bson::Document>> {
        let mut vec = vec![];

        for _ in 0..n {
            let bson_option = self.next();

            match bson_option {
                Some(Ok(bson)) => vec.push(bson),
                Some(Err(err)) => return Err(err),
                None => break,
            };
        }

        Ok(vec)
    }

    /// Attempts to read a batch of BSON documents from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn next_batch(&mut self) -> Result<Vec<bson::Document>> {
        let n = self.batch_size;
        self.next_n(n)
    }

    pub fn has_next(&mut self) -> Result<bool> {
        if self.limit > 0 && self.count >= self.limit {
            Ok(false)
        } else {
            if self.buffer.is_empty() && self.limit != 1 && self.cursor_id != 0 {
                try!(self.get_from_stream());
            }
            Ok(!self.buffer.is_empty())
        }
    }
}

impl <'a> Iterator for Cursor<'a> {
    type Item = Result<bson::Document>;

    /// Attempts to read a BSON document from the cursor.
    ///
    /// # Return value
    ///
    /// Returns the document that was read, or `None` if there are no more
    /// documents to read.
    fn next(&mut self) -> Option<Result<bson::Document>> {
        match self.has_next() {
            Ok(true) => {
                self.count += 1;
                match self.buffer.pop_front() {
                    Some(bson) => Some(Ok(bson)),
                    None => None,
                }
            },
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
