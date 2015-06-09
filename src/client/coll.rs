use bson;
use client::db::Database;
use client::common::{ReadPreference, WriteConcern};
use client::wire_protocol::operations::{OpQueryFlags, Message};

/// Interfaces with a MongoDB collection.
pub struct Collection<'a> {
    db: &'a Database<'a>,
    pub name: String,
    pub full_name: String,
    read_preference: Option<ReadPreference>,
    write_concern: Option<WriteConcern>,
}

#[derive(Clone, PartialEq)]
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
}

#[derive(Clone)]
pub struct CountOptions {
    pub hint: Option<bson::Document>,
    pub limit: Option<i64>,
    pub max_time_ms: Option<i64>,
    pub skip: Option<i64>,
}

#[derive(Clone)]
pub struct DistinctOptions {
    pub max_time_ms: Option<i64>,
}

#[derive(Clone)]
pub struct FindOptions {
    pub allow_partial_results: bool,
    pub no_cursor_timeout: bool,
    pub op_log_replay: bool,
    pub skip: i32,
    pub limit: i32,
    pub cursor_type: CursorType,
    pub batch_size: Option<i32>,
    pub comment: Option<String>,
    pub max_time_ms: Option<i64>,
    pub modifiers: Option<bson::Document>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
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
        }
    }

    pub fn with_skip(&self, skip: i32) -> FindOptions {
        let mut new_opts = self.clone();
        new_opts.skip = skip;
        new_opts
    }

    pub fn with_limit(&self, limit: i32) -> FindOptions {
        let mut new_opts = self.clone();
        new_opts.limit = limit;
        new_opts
    }

    pub fn with_projection(&self, proj: Option<bson::Document>) -> FindOptions {
        let mut new_opts = self.clone();
        new_opts.projection = proj;
        new_opts
    }
}

impl<'a> Collection<'a> {
    /// Creates a collection representation with optional read and write controls.
    ///
    /// If `create` is specified, the collection will be explicitly created in the database.
    pub fn new(db: &'a Database<'a>, name: &str, create: bool,
               read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Collection<'a> {

        let coll = Collection {
            full_name: format!("{}.{}", db.name, name),
            db: db,
            name: name.to_owned(),
            read_preference: read_preference,
            write_concern: write_concern,
        };

        /*
        // Since standard collections are implicitly created on insert,
        // this should only be used to create capped collections.
        if create {
            coll.create();
        };
         */

        coll
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.db.client.get_req_id()
    }

    // Read Spec
    pub fn aggregate(pipeline: &[bson::Document], options: AggregateOptions) -> Result<Vec<bson::Document>, String> {
        Err("IMPL".to_owned())
    }

    pub fn count(filter: bson::Document, options: CountOptions) -> Result<i64, String> {
        Err("IMPL".to_owned())
    }

    pub fn distinct(field_name: &str, filter: bson::Document, options: DistinctOptions) -> Result<Vec<String>, String> {
        Err("IMPL".to_owned())
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

        let req = try!(Message::with_query(self.get_req_id(), flags, self.full_name.to_owned(),
                                           options.skip, options.limit, doc, options.projection));

        try!(req.write(&mut *self.db.client.socket.borrow_mut()));
        let bson = try!(Message::read(&mut *self.db.client.socket.borrow_mut()));
        Ok(vec!(bson))
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
    fn insert(&self, docs: &[bson::Document]) -> Result<bson::Document, String> {
        Err("IMPL".to_owned())
    }

    pub fn insert_one(&self, doc: bson::Document) -> Result<bson::Document, String> {
        Err("IMPL".to_owned())
    }

    pub fn insert_many(&self, docs: &[bson::Document], ordered: bool) -> Result<bson::Document, String> {
        Err("IMPL".to_owned())
    }
}
