use bson;
use client::cursor;
use client::common::{ReadPreference, WriteConcern};

/// Describes the type of cursor to return on collection queries.
#[derive(Clone, PartialEq, Eq)]
pub enum CursorType {
    NonTailable,
    Tailable,
    TailableAwait,
}

/// Describes the type of document to return on write operations.
#[derive(Clone, PartialEq, Eq)]
pub enum ReturnDocument {
    Before,
    After,
}

/// Marker interface for writes that can be batched together.
#[derive(Clone)]
pub enum WriteModel {
    InsertOneModel {
        document: bson::Document,
    },
    DeleteOneModel {
        filter: bson::Document,
    },
    DeleteManyModel {
        filter: bson::Document,
    },
    ReplaceOneModel {
        filter: bson::Document,
        replacement: bson::Document,
        upsert: bool,
    },
    UpdateOneModel {
        filter: bson::Document,
        update: bson::Document,
        upsert: bool,
    },
    UpdateManyModel {
        filter: bson::Document,
        update: bson::Document,
        upsert: bool,
    }
}

/// Options for aggregation queries.
#[derive(Clone)]
pub struct AggregateOptions {
    pub allow_disk_use: bool,
    pub use_cursor: bool,
    pub batch_size: i32,
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

/// Options for count queries.
#[derive(Clone)]
pub struct CountOptions {
    pub skip: u64,
    pub limit: i64,
    pub hint: Option<String>,
    pub hint_doc: Option<bson::Document>,
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

/// Options for distinct queries.
#[derive(Clone)]
pub struct DistinctOptions {
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

/// Options for collection queries.
#[derive(Clone)]
pub struct FindOptions {
    pub allow_partial_results: bool,
    pub no_cursor_timeout: bool,
    pub op_log_replay: bool,
    pub skip: u32,
    pub limit: i32,
    pub cursor_type: CursorType,
    pub batch_size: i32,
    pub comment: Option<String>,
    pub max_time_ms: Option<i64>,
    pub modifiers: Option<bson::Document>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub read_preference: Option<ReadPreference>,
}

/// Options for findOneAndDelete operations.
#[derive(Clone)]
pub struct FindOneAndDeleteOptions {
    pub max_time_ms: Option<i64>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub write_concern: Option<WriteConcern>,
}

/// Options for findOneAndReplace operations.
#[derive(Clone)]
pub struct FindOneAndReplaceOptions {
    pub return_document: ReturnDocument,
    pub max_time_ms: Option<i64>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub upsert: bool,
    pub write_concern: Option<WriteConcern>,
}

/// Options for findOneAndUpdate operations.
#[derive(Clone)]
pub struct FindOneAndUpdateOptions {
    pub return_document: ReturnDocument,
    pub max_time_ms: Option<i64>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub upsert: bool,
    pub write_concern: Option<WriteConcern>,
}

impl AggregateOptions {
    pub fn new() -> AggregateOptions {
        AggregateOptions {
            allow_disk_use: false,
            use_cursor: true,
            batch_size: cursor::DEFAULT_BATCH_SIZE,
            max_time_ms: None,
            read_preference: None,
        }
    }
}

impl CountOptions {
    pub fn new() -> CountOptions {
        CountOptions {
            skip: 0,
            limit: 0,
            hint: None,
            hint_doc: None,
            max_time_ms: None,
            read_preference: None,
        }
    }
}

impl DistinctOptions {
    pub fn new() -> DistinctOptions {
        DistinctOptions {
            max_time_ms: None,
            read_preference: None,
        }
    }
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
            batch_size: cursor::DEFAULT_BATCH_SIZE,
            comment: None,
            max_time_ms: None,
            modifiers: None,
            projection: None,
            sort: None,
            read_preference: None,
        }
    }

    /// Clone the current options struct with a new limit.
    pub fn with_limit(&self, limit: i32) -> FindOptions {
        let mut new_opts = self.clone();
        new_opts.limit = limit;
        new_opts
    }
}

impl FindOneAndDeleteOptions {
    pub fn new() -> FindOneAndDeleteOptions {
        FindOneAndDeleteOptions {
            max_time_ms: None,
            projection: None,
            sort: None,
            write_concern: None,
        }
    }
}


impl FindOneAndReplaceOptions {
    pub fn new() -> FindOneAndReplaceOptions {
        FindOneAndReplaceOptions {
            return_document: ReturnDocument::Before,
            max_time_ms: None,
            projection: None,
            sort: None,
            upsert: false,
            write_concern: None,
        }
    }
}

impl FindOneAndUpdateOptions {
    pub fn new() -> FindOneAndUpdateOptions {
        FindOneAndUpdateOptions {
            return_document: ReturnDocument::Before,
            max_time_ms: None,
            projection: None,
            sort: None,
            upsert: false,
            write_concern: None,
        }
    }
}

impl ReturnDocument {
    pub fn to_bool(&self) -> bool {
        match self {
            &ReturnDocument::Before => false,
            &ReturnDocument::After => true,
        }
    }
}
