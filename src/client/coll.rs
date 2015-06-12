use bson;
use client::db::Database;
use client::common::{ReadPreference, WriteConcern};
use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;

/// Interfaces with a MongoDB collection.
pub struct Collection<'a> {
    db: &'a Database<'a>,
    pub namespace: String,
    read_preference: ReadPreference,
    write_concern: WriteConcern,
}

#[derive(Clone, PartialEq, Eq)]
pub enum CursorType {
    NonTailable,
    Tailable,
    TailableAwait,
}

#[derive(Clone)]
pub struct AggregateOptions {
    pub allow_disk_use: bool,
    pub use_cursor: bool,
    pub batch_size: Option<i32>,
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

#[derive(Clone)]
pub struct CountOptions {
    pub hint: Option<bson::Document>,
    pub limit: Option<i64>,
    pub max_time_ms: Option<i64>,
    pub skip: Option<u64>,
    pub read_preference: Option<ReadPreference>,
}

#[derive(Clone)]
pub struct DistinctOptions {
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

#[derive(Clone)]
pub struct FindOptions {
    pub allow_partial_results: bool,
    pub no_cursor_timeout: bool,
    pub op_log_replay: bool,
    pub skip: u32,
    pub limit: i32,
    pub cursor_type: CursorType,
    pub batch_size: Option<i32>,
    pub comment: Option<String>,
    pub max_time_ms: Option<i64>,
    pub modifiers: Option<bson::Document>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub read_preference: Option<ReadPreference>,
}

impl FindOptions {
    /// Creates a new FindOptions struct with default parameters.
    pub fn new() -> FindOptions {
        FindOptions {
            allow_partial_results: false,
            no_cursor_timeout: false,
            op_log_replay: false,
            skip: 0,
            limit: 0,
            cursor_type: CursorType::NonTailable,
            batch_size: None,
            comment: None,
            max_time_ms: None,
            modifiers: None,
            projection: None,
            sort: None,
            read_preference: None,
        }
    }

    pub fn with_limit(&self, limit: i32) -> FindOptions {
        let mut new_opts = self.clone();
        new_opts.limit = limit;
        new_opts
    }
}

impl<'a> Collection<'a> {
    /// Creates a collection representation with optional read and write controls.
    ///
    /// If `create` is specified, the collection will be explicitly created in the database.
    pub fn new(db: &'a Database<'a>, name: &str, create: bool,
               read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Collection<'a> {

        let rp = match read_preference {
            Some(rp) => rp,
            None => db.read_preference.to_owned(),
        };

        let wc = match write_concern {
            Some(wc) => wc,
            None => db.write_concern.to_owned(),
        };

        Collection {
            db: db,
            namespace: format!("{}.{}", db.name, name),
            read_preference: rp,
            write_concern: wc,
        }
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.db.client.get_req_id()
    }

    /// Extracts the collection name from the namespace.
    pub fn name(&self) -> String {
        match self.namespace.find(".") {
            Some(idx) => self.namespace[idx+1..].to_owned(),
            None => {
                // '.' is inserted in Collection::new, so this should only panic due to user error.
                let msg = format!("Invalid namespace specified: '{}'.", self.namespace);
                panic!(msg);
            }
        }
    }

    // Read Spec
    pub fn aggregate(pipeline: &[bson::Document], options: AggregateOptions) -> Result<Vec<bson::Document>, String> {
        unimplemented!()
    }

    pub fn count(filter: bson::Document, options: CountOptions) -> Result<i64, String> {
        unimplemented!()
    }

    pub fn distinct(field_name: &str, filter: bson::Document, options: DistinctOptions) -> Result<Vec<String>, String> {
        unimplemented!()
    }

    /// Returns a list of documents within the collection that match the filter.
    pub fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> Result<Vec<bson::Document>, String> {

        let doc = match filter {
            Some(bson) => bson,
            None => bson::Document::new(),
        };

        let options = match options {
            Some(opts) => opts,
            None => FindOptions::new(),
        };

        let flags = OpQueryFlags {
            tailable_cursor: options.cursor_type != CursorType::NonTailable,
            slave_ok: false,
            oplog_relay: options.op_log_replay,
            no_cursor_timeout: options.no_cursor_timeout,
            await_data: options.cursor_type == CursorType::TailableAwait,
            exhaust: false,
            partial: options.allow_partial_results,
        };

        let req = try!(Message::with_query(self.get_req_id(), flags, self.namespace.to_owned(),
                                           options.skip as i32, options.limit, doc, options.projection));

        let socket = match self.db.client.socket.lock() {
            Ok(val) => val,
            _ => return Err("Client socket lock poisoned.".to_owned()),
        };

        try!(req.write(&mut *socket.borrow_mut()));
        let message = try!(Message::read(&mut *socket.borrow_mut()));

	match message {
	  Message::OpReply { header: _, flags: _, cursor_id: _,
	                     starting_from: _, number_returned: _,
			     documents: documents } => Ok(documents),
	  _ => Err("Invalid response received from server".to_owned())
	}
    }

    /// Returns the first document within the collection that matches the filter, or None.
    pub fn find_one(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                    -> Result<Option<bson::Document>, String> {

        let options = match options {
            Some(opts) => opts,
            None => FindOptions::new(),
        };

        let res = try!(self.find(filter, Some(options.with_limit(1))));
        match res.len() {
            0 => Ok(None),
            _ => Ok(Some(res[0].to_owned())),
        }
    }

    // Write Spec
    fn insert(&self, docs: &[bson::Document], write_concern: WriteConcern) -> Result<bson::Document, String> {
        unimplemented!()
    }

    pub fn insert_one(&self, doc: bson::Document, write_concern: Option<WriteConcern>) -> Result<bson::Document, String> {
        unimplemented!()
    }

    pub fn insert_many(&self, docs: &[bson::Document], ordered: bool, write_concern: Option<WriteConcern>) -> Result<bson::Document, String> {
        unimplemented!()
    }
}
