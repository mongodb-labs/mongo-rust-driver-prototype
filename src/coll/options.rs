use bson::{self, Bson};
use cursor;
use common::{ReadPreference, WriteConcern};
use Error::ArgumentError;
use Result;

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
#[derive(Debug, Clone)]
pub enum WriteModel {
    InsertOne {
        document: bson::Document,
    },
    DeleteOne {
        filter: bson::Document,
    },
    DeleteMany {
        filter: bson::Document,
    },
    ReplaceOne {
        filter: bson::Document,
        replacement: bson::Document,
        upsert: bool,
    },
    UpdateOne {
        filter: bson::Document,
        update: bson::Document,
        upsert: bool,
    },
    UpdateMany {
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

/// Options for index operations.
pub struct IndexOptions {
    pub background: Option<bool>,
    pub expire_after_seconds: Option<i32>,
    pub name: Option<String>,
    pub sparse: Option<bool>,
    pub storage_engine: Option<String>,
    pub unique: Option<bool>,
    pub version: Option<i32>,
    // Options for text indexes
    pub default_language: Option<String>,
    pub language_override: Option<String>,
    pub text_version: Option<i32>,
    pub weights: Option<bson::Document>,
    // Options for 2dsphere indexes
    pub sphere_version: Option<i32>,
    // Options for 2d indexes
    pub bits: Option<i32>,
    pub max: Option<f64>,
    pub min: Option<f64>,
    // Options for geoHaystack indexes
    pub bucket_size: Option<i32>,
}

/// A single index model.
pub struct IndexModel {
    pub keys: bson::Document,
    pub options: IndexOptions,
}

#[derive(Clone)]
pub struct InsertManyOptions {
    pub ordered: bool,
    pub write_concern: Option<WriteConcern>,
}

#[derive(Clone)]
pub struct UpdateOptions {
    pub upsert: bool,
    pub write_concern: Option<WriteConcern>,
}

pub type ReplaceOptions = UpdateOptions;

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

impl IndexOptions {
    pub fn new() -> IndexOptions {
        IndexOptions {
            background: None,
            expire_after_seconds: None,
            name: None,
            sparse: None,
            storage_engine: None,
            unique: None,
            version: None,
            default_language: None,
            language_override: None,
            text_version: None,
            weights: None,
            sphere_version: None,
            bits: None,
            max: None,
            min: None,
            bucket_size: None,
        }
    }
}

impl IndexModel {
    pub fn new(keys: bson::Document, options: Option<IndexOptions>) -> IndexModel {
        IndexModel {
            keys: keys,
            options: options.unwrap_or(IndexOptions::new()),
        }
    }

    /// Returns the name of the index as specified by the options, or
    /// as automatically generated using the keys.
    pub fn name(&self) -> Result<String> {
        Ok(match self.options.name {
            Some(ref name) => name.to_owned(),
            None => try!(self.generate_index_name()),
        })
    }

    /// Generates the index name from keys.
    /// Auto-generated names have the form "key1_val1_key2_val2..."
    pub fn generate_index_name(&self) -> Result<String> {
        let mut name = String::new();
        for (key, bson) in self.keys.iter() {
            if !name.is_empty() {
                name.push_str("_");
            }

            name.push_str(&key);
            name.push_str("_");
            match bson {
                &Bson::I32(ref i) => name.push_str(&format!("{}", i)),
                _ => return Err(ArgumentError("Index model keys must map to i32.".to_owned())),
            }
        }
        Ok(name)
    }

    /// Converts the model to its BSON document representation.
    pub fn to_bson(&self) -> Result<bson::Document> {
        let mut doc = bson::Document::new();
        doc.insert("key".to_owned(), Bson::Document(self.keys.clone()));

        if let Some(ref val) = self.options.background {
            doc.insert("background".to_owned(), Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.expire_after_seconds {
            doc.insert("expireAfterSeconds".to_owned(), Bson::I32(*val));
        }
        if let Some(ref val) = self.options.name {
            doc.insert("name".to_owned(), Bson::String(val.to_owned()));
        } else {
            doc.insert("name".to_owned(), Bson::String(try!(self.generate_index_name())));
        }
        if let Some(ref val) = self.options.sparse {
            doc.insert("sparse".to_owned(), Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.storage_engine {
            doc.insert("storageEngine".to_owned(), Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.unique {
            doc.insert("unique".to_owned(), Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.version {
            doc.insert("v".to_owned(), Bson::I32(*val));
        }
        if let Some(ref val) = self.options.default_language {
            doc.insert("default_language".to_owned(), Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.language_override {
            doc.insert("language_override".to_owned(), Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.text_version {
            doc.insert("textIndexVersion".to_owned(), Bson::I32(*val));
        }
        if let Some(ref val) = self.options.weights {
            doc.insert("weights".to_owned(), Bson::Document(val.clone()));
        }
        if let Some(ref val) = self.options.sphere_version {
            doc.insert("2dsphereIndexVersion".to_owned(), Bson::I32(*val));
        }
        if let Some(ref val) = self.options.bits {
            doc.insert("bits".to_owned(), Bson::I32(*val));
        }
        if let Some(ref val) = self.options.max {
            doc.insert("max".to_owned(), Bson::FloatingPoint(*val));
        }
        if let Some(ref val) = self.options.min {
            doc.insert("min".to_owned(), Bson::FloatingPoint(*val));
        }
        if let Some(ref val) = self.options.bucket_size {
            doc.insert("bucketSize".to_owned(), Bson::I32(*val));
        }

        Ok(doc)
    }
}

impl InsertManyOptions {
    pub fn new(ordered: bool, write_concern: Option<WriteConcern>) -> InsertManyOptions {
        InsertManyOptions { ordered: ordered, write_concern: write_concern }
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

impl UpdateOptions {
    pub fn new(upsert: bool, write_concern: Option<WriteConcern>) -> UpdateOptions {
        UpdateOptions { upsert: upsert, write_concern: write_concern }
    }
}
