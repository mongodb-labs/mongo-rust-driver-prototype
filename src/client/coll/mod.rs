pub mod options;
pub mod results;

use bson;
use bson::Bson;

use client::db::Database;
use client::common::{ReadPreference, WriteConcern};
use client::coll::options::*;
use client::coll::results::*;

use client::cursor::Cursor;

use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;

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
    pub fn drop(&'a self) -> Result<(), String> {
        self.db.drop_collection(&self.name()[..])
    }

    /// Runs an aggregation framework pipeline.
    pub fn aggregate(pipeline: &[bson::Document], options: AggregateOptions) -> Result<Cursor<'a>, String> {
        unimplemented!()
    }

    /// Gets the number of documents matching the filter.
    pub fn count(filter: bson::Document, options: CountOptions) -> Result<i64, String> {
        unimplemented!()
    }

    /// Finds the distinct values for a specified field across a single collection.
    pub fn distinct(field_name: &str, filter: bson::Document, options: DistinctOptions) -> Result<Vec<Bson>, String> {
        unimplemented!()
    }

    /// Returns a list of documents within the collection that match the filter.
    pub fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> Result<Cursor<'a>, String> {

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
                    -> Result<Option<bson::Document>, String> {

        let options = options.unwrap_or(FindOptions::new());
        let mut cursor = try!(self.find(filter, Some(options.with_limit(1))));
        Ok(cursor.next())
    }

    fn find_one_and_modify(&self, cmd: &mut bson::Document,
                           filter: bson::Document, max_time_ms: Option<i64>,
                           projection: Option<bson::Document>, sort: Option<bson::Document>)
                           -> Result<Option<bson::Document>, String> {

        cmd.insert("findAndModify".to_owned(), Bson::String(self.name()));
        cmd.insert("query".to_owned(), Bson::Document(filter));
        if sort.is_some() {
            cmd.insert("sort".to_owned(), Bson::Document(sort.unwrap()));
        }
        if projection.is_some() {
            cmd.insert("fields".to_owned(), Bson::Document(projection.unwrap()));
        }

        let res = try!(self.db.command(cmd.to_owned()));
        match res {
            Some(doc) => match doc.get("value") {
                Some(&Bson::Document(ref nested_doc)) => Ok(Some(nested_doc.to_owned())),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn find_and_update(&self, filter: bson::Document, update: bson::Document,
                       after: bool, max_time_ms: Option<i64>, projection: Option<bson::Document>,
                       sort: Option<bson::Document>, upsert: bool) -> Result<Option<bson::Document>, String> {


        let mut cmd = bson::Document::new();
        cmd.insert("update".to_owned(), Bson::Document(update));
        if after {
            cmd.insert("new".to_owned(), Bson::Boolean(true));
        }
        if upsert {
            cmd.insert("upsert".to_owned(), Bson::Boolean(true));
        }

        self.find_one_and_modify(&mut cmd, filter, max_time_ms, projection, sort)
    }

    /// Finds a single document and deletes it, returning the original.
    pub fn find_one_and_delete(&self, filter: bson::Document,
                               options: Option<FindOneAndDeleteOptions>)  -> Result<Option<bson::Document>, String> {

        let opts = options.unwrap_or(FindOneAndDeleteOptions::new());
        let mut cmd = bson::Document::new();
        cmd.insert("remove".to_owned(), Bson::Boolean(true));
        self.find_one_and_modify(&mut cmd, filter, opts.max_time_ms, opts.projection, opts.sort)
    }

    /// Finds a single document and replaces it, returning either the original
    /// or replaced document.
    pub fn find_one_and_replace(&self, filter: bson::Document, replacement: bson::Document,
                                options: Option<FindOneAndReplaceOptions>)  -> Result<Option<bson::Document>, String> {
        let opts = options.unwrap_or(FindOneAndReplaceOptions::new());
        try!(Collection::validate_replace(&replacement));
        self.find_and_update(filter, replacement, opts.return_document.to_bool(),
                             opts.max_time_ms, opts.projection, opts.sort, opts.upsert)
    }

    /// Finds a single document and updates it, returning either the original
    /// or updated document.
    pub fn find_one_and_update(&self, filter: bson::Document, update: bson::Document,
                               options: Option<FindOneAndUpdateOptions>)  -> Result<Option<bson::Document>, String> {
        let opts = options.unwrap_or(FindOneAndUpdateOptions::new());
        try!(Collection::validate_update(&update));
        self.find_and_update(filter, update, opts.return_document.to_bool(),
                             opts.max_time_ms, opts.projection, opts.sort, opts.upsert)

    }

    /// Sends a batch of writes to the server at the same time.
    pub fn bulk_write(requests: &[WriteModel], ordered: bool) -> BulkWriteResult {
        unimplemented!()
    }

    // Internal insertion helper function.
    fn insert(&self, docs: Vec<bson::Document>, ordered: bool,
              write_concern: Option<WriteConcern>) -> Result<bson::Document, String> {

        let wc =  write_concern.unwrap_or(WriteConcern::new());
        let converted_docs = docs.iter().map(|doc| Bson::Document(doc.to_owned())).collect();

        let mut cmd = bson::Document::new();
        cmd.insert("insert".to_owned(), Bson::String(self.name()));
        cmd.insert("documents".to_owned(), Bson::Array(converted_docs));
        cmd.insert("ordered".to_owned(), Bson::Boolean(ordered));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));
        match result {
            Some(doc) => Ok(doc),
            None => Err("Insertion reply not received from server.".to_owned()),
        }
    }

    /// Inserts the provided document. If the document is missing an identifier,
    /// the driver should generate one.
    pub fn insert_one(&self, doc: bson::Document, write_concern: Option<WriteConcern>) -> Result<InsertOneResult, String> {
        let res = try!(self.insert(vec!(doc), true, write_concern));
        Ok(InsertOneResult::new(res))
    }

    /// Inserts the provided documents. If any documents are missing an identifier,
    /// the driver should generate them.
    pub fn insert_many(&self, docs: Vec<bson::Document>, ordered: bool,
                       write_concern: Option<WriteConcern>) -> Result<InsertManyResult, String> {
        let res = try!(self.insert(docs, ordered, write_concern));
        Ok(InsertManyResult::new(res))
    }

    // Internal deletion helper function.
    fn delete(&self, filter: bson::Document, limit: i64, write_concern: Option<WriteConcern>) -> Result<DeleteResult, String> {
        let wc = write_concern.unwrap_or(WriteConcern::new());

        let mut deletes = bson::Document::new();
        deletes.insert("q".to_owned(), Bson::Document(filter));
        deletes.insert("limit".to_owned(), Bson::I64(limit));

        let mut cmd = bson::Document::new();
        cmd.insert("delete".to_owned(), Bson::String(self.name()));
        cmd.insert("deletes".to_owned(), Bson::Array(vec!(Bson::Document(deletes))));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));
        match result {
            Some(doc) => Ok(DeleteResult::new(doc)),
            None => Err("Delete reply not received from server.".to_owned()),
        }
    }

    /// Deletes a single document.
    pub fn delete_one(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> Result<DeleteResult, String> {
        self.delete(filter, 1, write_concern)
    }

    /// Deletes multiple documents.
    pub fn delete_many(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> Result<DeleteResult, String> {
        self.delete(filter, 0, write_concern)
    }

    // Internal update helper function.
    fn update(&self, filter: bson::Document, update: bson::Document, upsert: bool, multi: bool,
              write_concern: Option<WriteConcern>) -> Result<UpdateResult, String> {

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
        match result {
            Some(doc) => Ok(UpdateResult::new(doc)),
            None => Err("delete_many reply not received from server.".to_owned()),
        }
    }

    /// Replaces a single document.
    pub fn replace_one(&self, filter: bson::Document, replacement: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> Result<UpdateResult, String> {

        let _ = try!(Collection::validate_replace(&replacement));
        self.update(filter, replacement, upsert, false, write_concern)
    }

    /// Updates a single document.
    pub fn update_one(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                      write_concern: Option<WriteConcern>) -> Result<UpdateResult, String> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, false, write_concern)
    }

    /// Updates multiple documents.
    pub fn update_many(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> Result<UpdateResult, String> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, true, write_concern)
    }

    fn validate_replace(replacement: &bson::Document) -> Result<(), String> {
        for key in replacement.keys() {
            if key.starts_with("$") {
                return Err("Replacement cannot include $ operators.".to_owned());
            }
        }
        Ok(())
    }

    fn validate_update(update: &bson::Document) -> Result<(), String> {
        for key in update.keys() {
            if !key.starts_with("$") {
                return Err("Update only works with $ operators.".to_owned());
            }
        }
        Ok(())
    }
}
