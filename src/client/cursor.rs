use bson;

use client::coll::Collection;

use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;

use std::collections::vec_deque::VecDeque;
use std::io::{Read, Write};

pub const DEFAULT_BATCH_SIZE: i32 = 20;

/// Maintains a connection to the server and lazily returns documents from a
/// query.
///
/// # Fields
///
/// `collection` - The collection to read from.
/// `batch_size` - How many documents to fetch at a given time from the server.
/// `cursor_id` - Uniquely identifies the cursor being returned by the reply.
pub struct Cursor<'a> {
    collection: &'a Collection<'a>,
    batch_size: i32,
    cursor_id: i64,
    limit: i32,
    count: i32,
    buffer: VecDeque<bson::Document>,
}

impl <'a> Cursor<'a> {
    /// Gets the cursor id and BSON documents from a reply Message.
    ///
    /// # Arguments
    ///
    /// `message` - The reply message to get the documents from.
    ///
    /// # Return value.
    ///
    ///
    fn get_bson_and_cid_from_message(message: Message) -> Option<(VecDeque<bson::Document>, i64)> {
        match message {
            Message::OpReply { header: _, flags: _, cursor_id: cid,
                               starting_from: _, number_returned: _,
                               documents: docs } => {
                let mut v = VecDeque::new();

                for doc in docs {
                    v.push_back(doc.clone());
                }

                Some((v, cid))
            },
            _ => None
        }
    }

    /// Executes a query where the batch size of the returned cursor is
    /// specified.
    ///
    /// # Arguments
    ///
    /// `collection` - The collection to read from.
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
    pub fn query_with_batch_size(collection: &'a Collection<'a>,
                                 batch_size: i32,
                                 flags: OpQueryFlags,
                                 number_to_skip: i32, number_to_return: i32,
                                 query: bson::Document,
                                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a>, String> {
        let result = Message::with_query(collection.get_req_id(), flags,
                                         collection.namespace.to_owned(),
                                         number_to_skip, number_to_return,
                                         query, return_field_selector);

        let socket = match collection.db.client.socket.lock() {
            Ok(val) => val,
            Err(_) => return Err("Socket lock is poisoned.".to_owned()),
        };

        let message = try!(result);
        try!(message.write(&mut *socket.borrow_mut()));
        let reply = try!(Message::read(&mut *socket.borrow_mut()));

        match Cursor::get_bson_and_cid_from_message(reply) {
            Some((buf, cursor_id)) => Ok(Cursor {
                collection: collection,
                batch_size: batch_size,
                cursor_id: cursor_id,
                limit: number_to_return,
                count: 0,
                buffer: buf,
            }),
            None => Err("Invalid response received".to_owned()),
        }
    }

    /// Executes a query with the default batch size.
    ///
    /// # Arguments
    ///
    /// `collection` - The collection to read from.
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
    pub fn query(collection: &'a Collection<'a>, flags: OpQueryFlags,
                 number_to_skip: i32, number_to_return: i32, query: bson::Document,
                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a>, String> {

        Cursor::query_with_batch_size(collection, DEFAULT_BATCH_SIZE, flags,
                                      number_to_skip,
                                      number_to_return, query,
                                      return_field_selector)
    }

    /// Helper method to create a "get more" request.
    ///
    /// # Return value
    ///
    /// Returns the newly-created method.
    fn new_get_more_request(&mut self) -> Message {
        Message::with_get_more(self.collection.get_req_id(),
                               self.collection.namespace.to_owned(),
                               self.batch_size, self.cursor_id)
    }

    /// Attempts to read another batch of BSON documents from the stream.
    fn get_from_stream(&mut self) -> Result<(), String> {
        let socket = match self.collection.db.client.socket.lock() {
            Ok(val) => val,
            Err(_) => return Err("Socket lock is poisoned.".to_owned()),
        };

        let get_more = self.new_get_more_request();
        try!(get_more.write(&mut *socket.borrow_mut()));
        let reply = try!(Message::read(&mut *socket.borrow_mut()));

        match Cursor::get_bson_and_cid_from_message(reply) {
            Some((v, _)) => {
                self.buffer.extend(v);
                Ok(())
            },
            None => Err("No bson found from server reply.".to_owned()),
        }
    }

    /// Attempts to read another batch of BSON documents from the stream.
    ///
    /// # Return value
    ///
    /// Returns the first BSON document returned from the stream, or `None` if
    /// there are no more documents to read.
    fn next_from_stream(&mut self) -> Option<bson::Document> {
        self.get_from_stream();
        self.buffer.pop_front()
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
    pub fn next_n(&mut self, n: i32) -> Vec<bson::Document> {
        let mut vec = vec![];

        for _ in 0..n {
            let bson_option = self.next();

            match bson_option {
                Some(bson) => vec.push(bson),
                None => break
            };
        }

        vec
    }

    /// Attempts to read a batch of BSON documents from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn next_batch(&mut self) -> Vec<bson::Document> {
        let n = self.batch_size;

        self.next_n(n)
    }

    pub fn has_next(&mut self) -> bool {
        if self.limit > 0 && self.count >= self.limit {
            false
        } else {
            if self.buffer.is_empty() {
                self.get_from_stream();
            }
            !self.buffer.is_empty()
        }
    }
}

impl <'a> Iterator for Cursor<'a> {
    type Item = bson::Document;

    /// Attempts to read a BSON document from the cursor.
    ///
    /// # Return value
    ///
    /// Returns the document that was read, or `None` if there are no more
    /// documents to read.
    fn next(&mut self) -> Option<bson::Document> {
        if self.limit != 0 && self.count >= self.limit {
            return None;
        }

        self.count += 1;

        match self.buffer.pop_front() {
            Some(bson) => Some(bson),
            None => {
                if self.limit != 1 {
                    self.next_from_stream()
                } else {
                    None
                }
            }
        }
    }
}
