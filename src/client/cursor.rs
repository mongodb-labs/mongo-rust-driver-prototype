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
/// `full_collection_name` - The full qualified name of the collection,
///                          beginning with the database name and a period.
/// `batch_size` - How many documents to fetch at a given time from the server.
/// `cursor_id` - Uniquely identifies the cursor being returned by the reply.
pub struct Cursor<'a, T> where T: Read + Write + 'a {
    request_id: i32,
    full_collection_name: String,
    batch_size: i32,
    cursor_id: i64,
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

                for bson in docs {
                    v.push_back(bson);
                }

                Some((v, cid))
            },
            _ => None
        }
    }

    pub fn query_with_batch_size(stream: &'a mut T, batch_size: i32,
                                 request_id: i32, flags: OpQueryFlags,
                                 full_collection_name: &str,
                                 number_to_skip: i32, number_to_return: i32,
                                 query: bson::Document,
                                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a, T>, String> {
        let result = Message::with_query(request_id, flags,
                                         full_collection_name.to_owned(),
                                         number_to_skip, number_to_return,
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
                full_collection_name: full_collection_name.to_owned(),
                batch_size: batch_size,
                cursor_id: cursor_id,
                buffer: buf, stream: stream }),
            None => Err("Invalid resonse received".to_owned()),
        }
    }

    pub fn query(stream: &'a mut T, request_id: i32, flags: OpQueryFlags,
                 full_collection_name: &str, number_to_skip: i32,
                 number_to_return: i32, query: bson::Document,
                 return_field_selector: Option<bson::Document>) -> Result<Cursor<'a, T>, String> {

        Cursor::query_with_batch_size(stream, 20, request_id, flags,
                                      full_collection_name, number_to_skip,
                                      number_to_return, query,
                                      return_field_selector)
    }

    fn new_get_more_request(&mut self) -> Message {
        Message::with_get_more(self.request_id,
                               self.full_collection_name.to_owned(),
                               self.batch_size, self.cursor_id)
    }

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

    pub fn next_batch(&mut self) -> Vec<bson::Document> {
        let n = self.batch_size;

        self.next_n(n)
    }
}

impl <'a, T> Iterator for Cursor<'a, T> where T: Read + Write + 'a {
    type Item = bson::Document;

    fn next(&mut self) -> Option<bson::Document> {
        match self.buffer.pop_front() {
            Some(bson) => Some(bson),
            None => self.next_from_stream()
        }
    }
}
