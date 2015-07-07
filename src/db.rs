use bson;
use bson::Bson;
use {Client, ThreadedClient, Result};
use Error::OperationError;
use coll::Collection;
use coll::options::FindOptions;
use common::{ReadPreference, WriteConcern};
use cursor::{Cursor, DEFAULT_BATCH_SIZE};
use std::sync::Arc;

/// Interfaces with a MongoDB database.
pub struct DatabaseInner {
    pub name: String,
    pub client: Client,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
}

pub type Database = Arc<DatabaseInner>;

pub trait ThreadedDatabase {
    /// Creates a database representation with optional read and write controls.
    fn open(client: Client, name: &str, read_preference: Option<ReadPreference>,
           write_concern: Option<WriteConcern>) -> Database;
    fn collection(&self, coll_name: &str) -> Collection;
    fn collection_with_prefs(&self, coll_name: &str, create: bool,
                             read_preference: Option<ReadPreference>,
                             write_concern: Option<WriteConcern>) -> Collection;
    fn get_req_id(&self) -> i32;
    fn command_cursor(&self, spec: bson::Document) -> Result<Cursor>;
    fn command(&self, spec: bson::Document) -> Result<bson::Document>;
    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor>;
    fn list_collections_with_batch_size(&self, filter: Option<bson::Document>,
                                        batch_size: i32) -> Result<Cursor>;
    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>>;
    fn create_collection(&self, name: &str) -> Result<()>;
    fn drop_database(&self) -> Result<()>;
    fn drop_collection(&self, name: &str) -> Result<()>;
}

impl ThreadedDatabase for Database {
    /// Creates a database representation with optional read and write controls.
    fn open(client: Client, name: &str, read_preference: Option<ReadPreference>,
               write_concern: Option<WriteConcern>) -> Database {
        let rp = read_preference.unwrap_or(client.read_preference.to_owned());
        let wc = write_concern.unwrap_or(client.write_concern.to_owned());

        Arc::new(DatabaseInner {
            name: name.to_owned(),
            client: client,
            read_preference: rp,
            write_concern: wc,
        })
    }

    /// Creates a collection representation with inherited read and write controls.
    fn collection(&self, coll_name: &str) -> Collection {
        Collection::new(self.clone(), coll_name, false, Some(self.read_preference.to_owned()), Some(self.write_concern.to_owned()))
    }

    /// Creates a collection representation with custom read and write controls.
    fn collection_with_prefs(&self, coll_name: &str, create: bool,
                                 read_preference: Option<ReadPreference>,
                                 write_concern: Option<WriteConcern>) -> Collection {
        Collection::new(self.clone(), coll_name, create, read_preference, write_concern)
    }

    /// Return a unique operational request id.
    fn get_req_id(&self) -> i32 {
        self.client.get_req_id()
    }

    /// Generates a cursor for a relevant operational command.
    fn command_cursor(&self, spec: bson::Document) -> Result<Cursor> {
        Cursor::command_cursor(self.client.clone(), &self.name[..], spec)
    }

    /// Sends an administrative command over find_one.
    fn command(&self, spec: bson::Document) -> Result<bson::Document> {
        let coll = self.collection("$cmd");
        let mut options = FindOptions::new();
        options.batch_size = 1;
        let res = try!(coll.find_one(Some(spec.clone()), Some(options)));
        res.ok_or(OperationError(format!("Failed to execute command with spec {:?}.", spec)))
    }

    /// Returns a list of collections within the database.
    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor> {
        self.list_collections_with_batch_size(filter, DEFAULT_BATCH_SIZE)
    }

    fn list_collections_with_batch_size(&self, filter: Option<bson::Document>,
                                            batch_size: i32) -> Result<Cursor> {

        let mut spec = bson::Document::new();
        let mut cursor = bson::Document::new();

        cursor.insert("batchSize".to_owned(), Bson::I32(batch_size));
        spec.insert("listCollections".to_owned(), Bson::I32(1));
        spec.insert("cursor".to_owned(), Bson::Document(cursor));
        if filter.is_some() {
            spec.insert("filter".to_owned(), Bson::Document(filter.unwrap()));
        }

        self.command_cursor(spec)
    }


    /// Returns a list of collection names within the database.
    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>> {
        let mut cursor = try!(self.list_collections(filter));
        let mut results = vec![];
        loop {
            match cursor.next() {
                Some(Ok(doc)) => if let Some(&Bson::String(ref name)) = doc.get("name") {
                    results.push(name.to_owned());
                },
                Some(Err(err)) => return Err(err),
                None => return Ok(results),
            }
        }
    }

    /// Creates a new collection.
    ///
    /// Note that due to the implicit creation of collections during insertion, this
    /// method should only be used to instantiate capped collections.
    fn create_collection(&self, name: &str) -> Result<()> {
        unimplemented!()
    }

    /// Permanently deletes the database from the server.
    fn drop_database(&self) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("dropDatabase".to_owned(), Bson::I32(1));
        try!(self.command(spec));
        Ok(())
    }

    /// Permanently deletes the collection from the database.
    fn drop_collection(&self, name: &str) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("drop".to_owned(), Bson::String(name.to_owned()));
        try!(self.command(spec));
        Ok(())
    }
}
