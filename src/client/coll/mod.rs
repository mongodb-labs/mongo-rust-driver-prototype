pub mod options;
pub mod results;

use bson;
use bson::Bson;

use client::db::Database;
use client::common::{ReadPreference, WriteConcern};
use client::coll::options::*;
use client::coll::results::*;

use client::cursor::Cursor;
use client::MongoResult;
use client::Error::{DefaultError, ReadError};

use client::wire_protocol::flags::OpQueryFlags;

use std::collections::BTreeMap;

/// Interfaces with a MongoDB collection.
pub struct Collection<'a> {
    pub db: &'a Database<'a>,
    pub namespace: String,
    read_preference: ReadPreference,
    write_concern: WriteConcern,
}

impl<'a> Collection<'a> {
    /// Creates a collection representation with optional read and write controls.
    ///
    /// If `create` is specified, the collection will be explicitly created in the database.
    pub fn new(db: &'a Database<'a>, name: &str, create: bool,
               read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Collection<'a> {

        let rp = read_preference.unwrap_or(db.read_preference.to_owned());
        let wc = write_concern.unwrap_or(db.write_concern.to_owned());

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
            Some(idx) => self.namespace[self.namespace.char_indices()
                                        .nth(idx+1).unwrap().0..].to_owned(),
            None => {
                // '.' is inserted in Collection::new, so this should only panic due to user error.
                let msg = format!("Invalid namespace specified: '{}'.", self.namespace);
                panic!(msg);
            }
        }
    }

    /// Permanently deletes the collection from the database.
    pub fn drop(&'a self) -> MongoResult<()> {
        self.db.drop_collection(&self.name()[..])
    }

    /// Runs an aggregation framework pipeline.
    pub fn aggregate(&'a self, pipeline: Vec<bson::Document>, options: Option<AggregateOptions>) -> MongoResult<Cursor<'a>> {
        let opts = options.unwrap_or(AggregateOptions::new());

        let pipeline_map = pipeline.iter().map(|bdoc| {
            Bson::Document(bdoc.to_owned())
        }).collect();

        let mut spec = bson::Document::new();
        let mut cursor = bson::Document::new();
        cursor.insert("batchSize".to_owned(), Bson::I32(opts.batch_size));
        spec.insert("aggregate".to_owned(), Bson::String(self.name()));
        spec.insert("pipeline".to_owned(), Bson::Array(pipeline_map));
        spec.insert("cursor".to_owned(), Bson::Document(cursor));
        if opts.allow_disk_use {
            spec.insert("allowDiskUse".to_owned(), Bson::Boolean(opts.allow_disk_use));
        }

        self.db.command_cursor(spec)
    }

    /// Gets the number of documents matching the filter.
    pub fn count(&self, filter: Option<bson::Document>, options: Option<CountOptions>) -> MongoResult<i64> {
        let opts = options.unwrap_or(CountOptions::new());

        let mut spec = bson::Document::new();
        spec.insert("count".to_owned(), Bson::String(self.name()));
        spec.insert("skip".to_owned(), Bson::I64(opts.skip as i64));
        spec.insert("limit".to_owned(), Bson::I64(opts.limit));
        if filter.is_some() {
            spec.insert("query".to_owned(), Bson::Document(filter.unwrap()));
        }

        // Favor specified hint document over string
        if opts.hint_doc.is_some() {
            spec.insert("hint".to_owned(), Bson::Document(opts.hint_doc.unwrap()));
        } else if opts.hint.is_some() {
            spec.insert("hint".to_owned(), Bson::String(opts.hint.unwrap()));
        }

        let result = try!(self.db.command(spec));
        match result.get("n") {
            Some(&Bson::I32(ref n)) => Ok(*n as i64),
            Some(&Bson::I64(ref n)) => Ok(*n),
            _ => Err(ReadError),
        }
    }

    /// Finds the distinct values for a specified field across a single collection.
    pub fn distinct(&self, field_name: &str, filter: Option<bson::Document>, options: Option<DistinctOptions>) -> MongoResult<Vec<Bson>> {

        let opts = options.unwrap_or(DistinctOptions::new());

        let mut spec = bson::Document::new();
        spec.insert("distinct".to_owned(), Bson::String(self.name()));
        spec.insert("key".to_owned(), Bson::String(field_name.to_owned()));
        if filter.is_some() {
            spec.insert("query".to_owned(), Bson::Document(filter.unwrap()));
        }

        let result = try!(self.db.command(spec));
        if let Some(&Bson::Array(ref vals)) = result.get("values") {
            return Ok(vals.to_owned());
        }

        Err(ReadError)
    }

    /// Returns a list of documents within the collection that match the filter.
    pub fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> MongoResult<Cursor<'a>> {

        let doc = filter.unwrap_or(bson::Document::new());
        let options = options.unwrap_or(FindOptions::new());
        let flags = OpQueryFlags::with_find_options(&options);

        Cursor::query_with_batch_size(&self.db.client, self.namespace.to_owned(),
                                      options.batch_size, flags, options.skip as i32,
                                      options.limit, doc, options.projection.clone(),
                                      false)
    }

    /// Returns the first document within the collection that matches the filter, or None.
    pub fn find_one(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                    -> MongoResult<Option<bson::Document>> {
        let options = options.unwrap_or(FindOptions::new());
        let mut cursor = try!(self.find(filter, Some(options.with_limit(1))));
        Ok(cursor.next())
    }

    // Helper method for all findAndModify commands.
    fn find_and_modify(&self, cmd: &mut bson::Document,
                           filter: bson::Document, max_time_ms: Option<i64>,
                           projection: Option<bson::Document>, sort: Option<bson::Document>,
                           write_concern: Option<WriteConcern>)
                           -> MongoResult<Option<bson::Document>> {

        let wc = write_concern.unwrap_or(self.write_concern.clone());

        let mut new_cmd = bson::Document::new();
        new_cmd.insert("findAndModify".to_owned(), Bson::String(self.name()));
        new_cmd.insert("query".to_owned(), Bson::Document(filter));
        new_cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));
        if sort.is_some() {
            new_cmd.insert("sort".to_owned(), Bson::Document(sort.unwrap()));
        }
        if projection.is_some() {
            new_cmd.insert("fields".to_owned(), Bson::Document(projection.unwrap()));
        }

        for (key, val) in cmd.iter() {
            new_cmd.insert(key.to_owned(), val.to_owned());
        }

        let res = try!(self.db.command(new_cmd));
        match res.get("value") {
            Some(&Bson::Document(ref nested_doc)) => Ok(Some(nested_doc.to_owned())),
            _ => Ok(None),
        }
    }

    // Helper method for validated replace and update commands.
    fn find_one_and_replace_or_update(&self, filter: bson::Document, update: bson::Document,
                                      after: bool, max_time_ms: Option<i64>,
                                      projection: Option<bson::Document>, sort: Option<bson::Document>,
                                      upsert: bool, write_concern: Option<WriteConcern>) -> MongoResult<Option<bson::Document>> {

        let mut cmd = bson::Document::new();
        cmd.insert("update".to_owned(), Bson::Document(update));
        if after {
            cmd.insert("new".to_owned(), Bson::Boolean(true));
        }
        if upsert {
            cmd.insert("upsert".to_owned(), Bson::Boolean(true));
        }

        self.find_and_modify(&mut cmd, filter, max_time_ms, projection, sort, write_concern)
    }

    /// Finds a single document and deletes it, returning the original.
    pub fn find_one_and_delete(&self, filter: bson::Document,
                               options: Option<FindOneAndDeleteOptions>)  -> MongoResult<Option<bson::Document>> {

        let opts = options.unwrap_or(FindOneAndDeleteOptions::new());
        let mut cmd = bson::Document::new();
        cmd.insert("remove".to_owned(), Bson::Boolean(true));
        self.find_and_modify(&mut cmd, filter, opts.max_time_ms,
                             opts.projection, opts.sort, opts.write_concern)
    }

    /// Finds a single document and replaces it, returning either the original
    /// or replaced document.
    pub fn find_one_and_replace(&self, filter: bson::Document, replacement: bson::Document,
                                options: Option<FindOneAndReplaceOptions>)  -> MongoResult<Option<bson::Document>> {
        let opts = options.unwrap_or(FindOneAndReplaceOptions::new());
        try!(Collection::validate_replace(&replacement));
        self.find_one_and_replace_or_update(filter, replacement, opts.return_document.to_bool(),
                                            opts.max_time_ms, opts.projection, opts.sort,
                                            opts.upsert, opts.write_concern)
    }

    /// Finds a single document and updates it, returning either the original
    /// or updated document.
    pub fn find_one_and_update(&self, filter: bson::Document, update: bson::Document,
                               options: Option<FindOneAndUpdateOptions>)  -> MongoResult<Option<bson::Document>> {
        let opts = options.unwrap_or(FindOneAndUpdateOptions::new());
        try!(Collection::validate_update(&update));
        self.find_one_and_replace_or_update(filter, update, opts.return_document.to_bool(),
                                            opts.max_time_ms, opts.projection, opts.sort,
                                            opts.upsert, opts.write_concern)
    }

    /// Sends a batch of writes to the server at the same time.
    pub fn bulk_write(requests: &[WriteModel], ordered: bool) -> BulkWriteResult {
        unimplemented!()
    }

    // Internal insertion helper function.
    fn insert(&self, docs: Vec<bson::Document>, ordered: bool,
              write_concern: Option<WriteConcern>) -> MongoResult<BTreeMap<i64, Bson>> {

        let wc =  write_concern.unwrap_or(WriteConcern::new());
        let mut map = BTreeMap::new();

        let ids = for i in 0..docs.len() {
            match docs[i].get("_id") {
                Some(bson) => {
                    let _ = map.insert(i as i64, bson.clone());
                    ()
                },
                None => ()
            };
        };

        let converted_docs = docs.iter().map(|doc| Bson::Document(doc.to_owned())).collect();

        let mut cmd = bson::Document::new();
        cmd.insert("insert".to_owned(), Bson::String(self.name()));
        cmd.insert("documents".to_owned(), Bson::Array(converted_docs));
        cmd.insert("ordered".to_owned(), Bson::Boolean(ordered));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let _ = try!(self.db.command(cmd));
        Ok(map)
    }

    /// Inserts the provided document. If the document is missing an identifier,
    /// the driver should generate one.
    pub fn insert_one(&self, doc: bson::Document, write_concern: Option<WriteConcern>) -> MongoResult<InsertOneResult> {
        let res = try!(self.insert(vec!(doc), true, write_concern));
        let id = match res.keys().next() {
            Some(ref key) => res.get(key),
            None => None
        };

        match id {
            Some(id) => Ok(InsertOneResult::new(Some(id.clone()))),
            None => Ok(InsertOneResult::new(None))
        }
    }

    /// Inserts the provided documents. If any documents are missing an identifier,
    /// the driver should generate them.
    pub fn insert_many(&self, docs: Vec<bson::Document>, ordered: bool,
                       write_concern: Option<WriteConcern>) -> MongoResult<InsertManyResult> {
        let res = try!(self.insert(docs, ordered, write_concern));
        Ok(InsertManyResult::new(Some(res)))
    }

    // Internal deletion helper function.
    fn delete(&self, filter: bson::Document, limit: i64, write_concern: Option<WriteConcern>) -> MongoResult<DeleteResult> {
        let wc = write_concern.unwrap_or(WriteConcern::new());

        let mut deletes = bson::Document::new();
        deletes.insert("q".to_owned(), Bson::Document(filter));
        deletes.insert("limit".to_owned(), Bson::I64(limit));

        let mut cmd = bson::Document::new();
        cmd.insert("delete".to_owned(), Bson::String(self.name()));
        cmd.insert("deletes".to_owned(), Bson::Array(vec!(Bson::Document(deletes))));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));
        Ok(DeleteResult::new(result))
    }

    /// Deletes a single document.
    pub fn delete_one(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> MongoResult<DeleteResult> {
        self.delete(filter, 1, write_concern)
    }

    /// Deletes multiple documents.
    pub fn delete_many(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> MongoResult<DeleteResult> {
        self.delete(filter, 0, write_concern)
    }

    // Internal update helper function.
    fn update(&self, filter: bson::Document, update: bson::Document, upsert: bool, multi: bool,
              write_concern: Option<WriteConcern>) -> MongoResult<UpdateResult> {

        let wc = write_concern.unwrap_or(WriteConcern::new());

        let mut updates = bson::Document::new();
        updates.insert("q".to_owned(), Bson::Document(filter));
        updates.insert("u".to_owned(), Bson::Document(update));
        updates.insert("upsert".to_owned(), Bson::Boolean(upsert));
        if multi {
            updates.insert("multi".to_owned(), Bson::Boolean(multi));
        }

        let mut cmd = bson::Document::new();
        cmd.insert("update".to_owned(), Bson::String(self.name()));
        cmd.insert("updates".to_owned(), Bson::Array(vec!(Bson::Document(updates))));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));
        Ok(UpdateResult::new(result))
    }

    /// Replaces a single document.
    pub fn replace_one(&self, filter: bson::Document, replacement: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> MongoResult<UpdateResult> {

        let _ = try!(Collection::validate_replace(&replacement));
        self.update(filter, replacement, upsert, false, write_concern)
    }

    /// Updates a single document.
    pub fn update_one(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                      write_concern: Option<WriteConcern>) -> MongoResult<UpdateResult> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, false, write_concern)
    }

    /// Updates multiple documents.
    pub fn update_many(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> MongoResult<UpdateResult> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, true, write_concern)
    }

    fn validate_replace(replacement: &bson::Document) -> MongoResult<()> {
        for key in replacement.keys() {
            if key.starts_with("$") {
                return Err(DefaultError("Replacement cannot include $ operators.".to_owned()));
            }
        }
        Ok(())
    }

    fn validate_update(update: &bson::Document) -> MongoResult<()> {
        for key in update.keys() {
            if !key.starts_with("$") {
                return Err(DefaultError("Update only works with $ operators.".to_owned()));
            }
        }
        Ok(())
    }
}
