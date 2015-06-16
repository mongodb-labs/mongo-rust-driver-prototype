use bson;
use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;
use std::collections::vec_deque::VecDeque;
use std::io::{Read, Write};

/// Maintains a connection to the server and lazily returns documents from a
/// query.
///
/// # Fields
///
/// `request_id` - Uniquely identifies the request being sent.
/// `namespace` - The full qualified name of the collection,
///                          beginning with the database name and a period.
/// `batch_size` - How many documents to fetch at a given time from the server.
/// `cursor_id` - Uniquely identifies the cursor being returned by the reply.
pub struct Cursor<'a, T> where T: Read + Write + 'a {
    request_id: i32,
    namespace: String,
    batch_size: i32,
    cursor_id: i64,
    limit: i32,
    count: i32,
    buffer: VecDeque<bson::Document>,
    stream: &'a mut T,
}

impl <'a, T> Cursor<'a, T> where T: Read + Write + 'a {
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
                    v.push_back(doc);
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
    /// `stream` - The stream to read the input from and write the output to.
    /// `batch_size` - How many documents the cursor should return at a time.
    /// `request_id` - The request ID to be placed in the message header.
    /// `flags` - Bit vector of query options.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
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
    pub fn query_with_batch_size(stream: &'a mut T, batch_size: i32,
                                 request_id: i32, flags: OpQueryFlags,
                                 namespace: &str, number_to_skip: i32,
                                 number_to_return: i32, query: bson::Document,
                                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a, T>, String> {
        let result = Message::with_query(request_id, flags,
                                         namespace.to_owned(),
                                         number_to_skip, batch_size,
                                         query, return_field_selector);

        let message = match result {
            Ok(m) => m,
            Err(s) => return Err(s)
        };

        match message.write(stream) {
            Ok(_) => (),
            Err(s) => return Err(s)
        };

        let reply  = match Message::read(stream) {
            Ok(m) => m,
            Err(s) => return Err(s)
        };

        match Cursor::<T>::get_bson_and_cid_from_message(reply) {
            Some((buf, cursor_id)) => Ok(Cursor {
                request_id: request_id,
                namespace: namespace.to_owned(),
                batch_size: batch_size,
                cursor_id: cursor_id,
                limit: number_to_return,
                count: 0,
                buffer: buf,
                stream: stream }),
            None => Err("Invalid response received".to_owned()),
        }
    }

    /// Executes a query with the default batch size.
    ///
    /// # Arguments
    ///
    /// `stream` - The stream to read the input from and write the output to.
    /// `request_id` - The request ID to be placed in the message header.
    /// `flags` - Bit vector of query options.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
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
    pub fn query(stream: &'a mut T, request_id: i32, flags: OpQueryFlags,
                 namespace: &str, number_to_skip: i32,
                 number_to_return: i32, query: bson::Document,
                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a, T>, String> {

        Cursor::query_with_batch_size(stream, 20, request_id, flags,
                                      namespace, number_to_skip,
                                      number_to_return, query,
                                      return_field_selector)
    }

    /// Helper method to create a "get more" request.
    ///
    /// # Return value
    ///
    /// Returns the newly-created method.
    fn new_get_more_request(&mut self) -> Message {
        Message::with_get_more(self.request_id,
                               self.namespace.to_owned(),
                               self.batch_size, self.cursor_id)
    }

    /// Attempts to read another batch of BSON documents from the stream.
    ///
    /// # Return value
    ///
    /// Returns the first BSON document returned from the stream, or `None` if
    /// there are no more documents to read.
    fn next_from_stream(&mut self) -> Option<bson::Document> {
        let get_more = self.new_get_more_request();

        match get_more.write(&mut self.stream) {
            Ok(_) => (),
            Err(_) => return None
        };

        let reply = match Message::read(&mut self.stream) {
            Ok(m) => m,
            Err(_) => return None
        };

        match Cursor::<T>::get_bson_and_cid_from_message(reply) {
            Some((v, _)) => {
                self.buffer.extend(v);
                self.buffer.pop_front()
            },
            None => None
        }
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
}

impl <'a, T> Iterator for Cursor<'a, T> where T: Read + Write + 'a {
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
            None => self.next_from_stream()
        }
    }
}
