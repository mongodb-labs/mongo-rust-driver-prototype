use apm::{CommandStarted, CommandResult, EventRunner};
use bson::{self, Bson};
use time;
use command_type::CommandType;
use {Client, Error, ErrorCode, Result, ThreadedClient};
use wire_protocol::flags::OpQueryFlags;
use wire_protocol::operations::Message;
use std::collections::vec_deque::VecDeque;
use std::io::{Read, Write};

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
/// `limit` - An upper bound on the total number of documents this cursor
///           should return.
/// `count` - How many documents have been returned so far.
/// `buffer` - A cache for documents received from the query that have not
///            yet been returned.
pub struct Cursor {
    client: Client,
    namespace: String,
    batch_size: i32,
    cursor_id: i64,
    limit: i32,
    count: i32,
    buffer: VecDeque<bson::Document>,
}

macro_rules! try_or_emit {
    ($cmd_name:expr, $req_id:expr, $connstring:expr, $result:expr, $client:expr) => {
        match $result {
            Ok(val) => val,
            Err(e) => {
                let hook_result = $client.run_completion_hooks(&CommandResult::Failure {
                    duration: 0,
                    command_name: $cmd_name.to_owned(),
                    failure: &e,
                    request_id: $req_id as i64,
                    connection_string: $connstring,
                });

                return match hook_result {
                    Ok(_) => Err(e),
                    Err(_) => Err(Error::EventListenerError(Some(Box::new(e))))
                }
            }
        }
    };
}

impl Cursor {
    /// Construcs a new Cursor for a database command.
    ///
    /// # Arguments
    ///
    /// `client` - Client making the request.
    /// `db` - Which database the command is being sent to.
    /// `doc` - Specifies the command that is being run.
    ///
    /// # Return value
    ///
    /// Returns the newly created Cursor on success, or an Error on failure.
    pub fn command_cursor(client: Client, db: &str,
                          doc: bson::Document, cmd_type: CommandType) -> Result<Cursor> {
        Cursor::query(client.clone(), format!("{}.$cmd", db), 1, OpQueryFlags::no_flags(), 0, 0,
                      doc, None, cmd_type, true)
    }

    fn get_bson_and_cid_from_message(message: Message) -> Result<(VecDeque<bson::Document>, i64, i32)> {
        match message {
            Message::OpReply { header: _, flags: _, cursor_id: cid,
                               starting_from: _, number_returned: n,
                               documents: docs } => {
                let mut v = VecDeque::new();

                if !docs.is_empty() {
                    if let Some(&Bson::I32(ref code)) = docs[0].get("code") {
                        // If command doesn't exist or namespace not found, return
                        // an empty array instead of throwing an error.
                        if *code == ErrorCode::CommandNotFound as i32 ||
                           *code == ErrorCode::NamespaceNotFound as i32 {
                            return Ok((v, cid, n));
                        } else if let Some(&Bson::String(ref msg)) = docs[0].get("errmsg") {
                            return Err(Error::OperationError(msg.to_owned()));
                        }
                    }
                }

                for doc in docs {
                    v.push_back(doc.clone());
                }

                Ok((v, cid, n))
            },
            _ => Err(Error::CursorNotFoundError)
        }
    }

    fn get_bson_and_cursor_info_from_command_message(message: Message) ->
           Result<(VecDeque<bson::Document>, i64, String, i32)> {
        let (v, _, n) = try!(Cursor::get_bson_and_cid_from_message(message));
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

                        return Ok((map, *id, ns.to_owned(), n))
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
    /// `is_cmd_cursor` - Whether or not the Cursor is for a database command.
    ///
    /// # Return value
    ///
    /// Returns the cursor for the query results on success, or an Error on
    /// failure.
    pub fn query(client: Client, namespace: String,
                                 batch_size: i32, flags: OpQueryFlags,
                                 number_to_skip: i32, number_to_return: i32,
                                 query: bson::Document,
                                 return_field_selector: Option<bson::Document>,
                                 cmd_type: CommandType, is_cmd_cursor: bool) -> Result<Cursor> {

        let req_id = client.get_req_id();

        let stream = try!(client.acquire_stream());
        let mut socket = stream.get_socket();


        let index = namespace.rfind(".").unwrap_or(namespace.len());
        let db_name = namespace[..index].to_owned();
        let cmd_name = cmd_type.to_str();

        let connstring = format!("{}", try!(socket.peer_addr()));

        let init_time = time::precise_time_ns();

        let result = Message::new_query(req_id, flags,
                                        namespace.to_owned(),
                                        number_to_skip, batch_size,
                                        query.clone(), return_field_selector);

        let message = try!(result);

        let hook_result = client.run_start_hooks(&CommandStarted {
            command: query.clone(),
            database_name: db_name,
            command_name: cmd_name.to_owned(),
            request_id: req_id as i64,
            connection_string: connstring.clone(),
        });

        match hook_result {
            Ok(_) => (),
            Err(_) => return Err(Error::EventListenerError(None))
        };

        try_or_emit!(cmd_name, req_id, connstring, message.write(&mut socket), client);
        let reply = try_or_emit!(cmd_name, req_id, connstring, Message::read(&mut socket),
                                 client);

        let fin_time = time::precise_time_ns();

        let (buf, cursor_id, namespace, n) = if is_cmd_cursor {
            try_or_emit!(cmd_name, req_id, connstring,
                         Cursor::get_bson_and_cursor_info_from_command_message(reply), client)
        } else {
            let (buf, id, n) = try_or_emit!(cmd_name, req_id, connstring,
                                            Cursor::get_bson_and_cid_from_message(reply),
                                            client);
            (buf, id, namespace, n)
        };


        let _hook_result = client.run_completion_hooks(&CommandResult::Success {
            duration: fin_time - init_time,
            reply: doc! { },
            command_name: cmd_name.to_owned(),
            request_id: req_id as i64,
            connection_string: connstring,
        });

        Ok(Cursor { client: client, namespace: namespace,
                    batch_size: batch_size, cursor_id: cursor_id,
                    limit: number_to_return, count: 0, buffer: buf, })
    }

    /// Helper method to create a "get more" request.
    ///
    /// # Return value
    ///
    /// Returns the newly-created method.
    fn new_get_more_request(&mut self) -> Message {
        Message::new_get_more(self.client.get_req_id(),
                              self.namespace.to_owned(),
                              self.batch_size, self.cursor_id)
    }

    fn get_from_stream(&mut self) -> Result<()> {
        let stream = try!(self.client.acquire_stream());
        let mut socket = stream.get_socket();

        let req_id = self.client.get_req_id();


        let get_more = Message::new_get_more(req_id, self.namespace.to_owned(), self.batch_size,
                              self.cursor_id);


        let index = self.namespace.rfind(".").unwrap_or(self.namespace.len());
        let db_name = self.namespace[..index].to_owned();
        let cmd_name = "get_more".to_owned();
        let connstring = format!("{}", try!(socket.peer_addr()));


        let hook_result = self.client.run_start_hooks(&CommandStarted {
            command: doc! { "cursor_id" => (self.cursor_id) },
            database_name: db_name,
            command_name: cmd_name.clone(),
            request_id: req_id as i64,
            connection_string: connstring.clone(),
        });

        match hook_result {
            Ok(_) => (),
            Err(_) => return Err(Error::EventListenerError(None))
        };

        try_or_emit!(cmd_name, req_id, connstring, get_more.write(&mut socket), self.client);
        let reply = try!(Message::read(&mut socket));

        let (v, _, _) = try!(Cursor::get_bson_and_cid_from_message(reply));
        self.buffer.extend(v);
        Ok(())
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

    /// Checks whether there are any more documents for the cursor to return.
    ///
    /// # Return value
    ///
    /// Returns `true` if the cursor is not yet exhausted, or `false` if it is.
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

impl Iterator for Cursor {
    type Item = Result<bson::Document>;

    /// Attempts to read a BSON document from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a BSON document if there is another one to return; `None` if
    /// there are no more documents to return; or an Error if the request for
    /// another document fails.
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
