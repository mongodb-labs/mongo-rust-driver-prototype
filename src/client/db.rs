use bson;
use bson::Bson;
use client::MongoClient;
use client::coll::Collection;
use client::common::{ReadPreference, WriteConcern};

/// Interfaces with a MongoDB database.
pub struct Database<'a> {
    pub name: String,
    pub client: &'a MongoClient,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
}

impl<'a> Database<'a> {
    /// Creates a database representation with optional read and write controls.
    pub fn new(client: &'a MongoClient, name: &str,
               read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Database<'a> {

        let rp = match read_preference {
            Some(rp) => rp,
            None => client.read_preference.to_owned(),
        };

        let wc = match write_concern {
            Some(wc) => wc,
            None => client.write_concern.to_owned(),
        };

        Database {
            name: name.to_owned(),
            client: client,
            read_preference: rp,
            write_concern: wc,
        }
    }

    /// Creates a collection representation with inherited read and write controls.
    pub fn collection(&'a self, coll_name: &str) -> Collection<'a> {
        Collection::new(self, coll_name, false, Some(self.read_preference.to_owned()), Some(self.write_concern.to_owned()))
    }

    /// Creates a collection representation with custom read and write controls.
    pub fn collection_with_prefs(&'a self, coll_name: &str, create: bool,
                                 read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Collection<'a> {
        Collection::new(self, coll_name, create, read_preference, write_concern)
    }

    /// Return a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.client.get_req_id()
    }

    // Sends an administrative command over find_one.
    fn command(&'a self, spec: bson::Document) -> Result<Option<bson::Document>, String> {
        let coll = Collection::new(self, "$cmd", false, None, None);
        coll.find_one(Some(spec), None)
    }

    /// Returns a list of collections within the database.
    pub fn list_collections(&'a self) -> Result<Vec<Collection<'a>>, String> {
        let mut spec = bson::Document::new();
        spec.insert("listCollections".to_owned(), Bson::I32(1));
        let res = try!(self.command(spec));

        if res.is_none() {
            return Ok(vec!());
        }

        let bson = res.unwrap();

        // Wire/Client proof of concept; replace this with cursor implementation in the future.
        // Unwrap reply
        if let Some(&Bson::Document(ref cursor)) = bson.get("cursor") {
            // Unwrap batched results
            if let Some(&Bson::Array(ref batch)) = cursor.get("firstBatch") {
                // Iterate over each collection returned
                let mut collections = Vec::new();
                for bdoc in batch {
                    // Unwrap document
                    if let &Bson::Document(ref doc) = bdoc {
                        // Unwrap name
                        if let Some(&Bson::String(ref name)) = doc.get("name") {
                            // Push collection into returned results
                            collections.push(Collection::new(self, &name[..], false, None, None));
                        }
                    }
                };
                return Ok(collections);
            }
        }

        Err("Unable to unwrap collections list.".to_owned())
    }

    /// Returns a list of collection names within the database.
    pub fn collection_names(&'a self) -> Result<Vec<String>, String> {
        let results = try!(self.list_collections());
        Ok(results.iter().map(|coll| coll.name()).collect())
    }

    /// Creates a new collection.
    ///
    /// Note that due to the implicit creation of collections during insertion, this
    /// method should only be used to instantiate capped collections.
    pub fn create_collection(&'a self, name: &str) -> Result<(), String> {
        unimplemented!()
    }

    /// Permanently deletes the database from the server.
    pub fn drop_database(&'a self) -> Result<(), String> {
        let mut spec = bson::Document::new();
        spec.insert("dropDatabase".to_owned(), Bson::I32(1));
        try!(self.command(spec));
        Ok(())
    }
}
