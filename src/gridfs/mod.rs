pub mod file;

use bson::{self, Bson, oid};

use db::{Database, ThreadedDatabase};
use coll::Collection;
use coll::options::FindOptions;
use cursor::Cursor;
use Error::ArgumentError;
use Result;

use self::file::{File, Mode};

use std::{io, fs};
use std::sync::Arc;

pub type Store = Arc<StoreInner>;

pub struct StoreInner {
    files: Collection,
    chunks: Collection,
}

pub trait ThreadedStore {
    fn with_db(db: Database) -> Store;
    fn with_prefix(db: Database, prefix: String) -> Store;
    fn create(&self, name: String) -> Result<File>;
    fn open(&self, name: String) -> Result<File>;
    fn open_id(&self, id: oid::ObjectId) -> Result<File>;
    fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
            -> Result<Cursor>;
    // TODO: Make a GridCursor wrapper for this?
    fn open_next(&self, cursor: &mut Cursor) -> Result<Option<File>>;
    fn remove(&self, name: String) -> Result<()>;
    fn remove_id(&self, id: oid::ObjectId) -> Result<()>;
    fn put(&self, name: String) -> Result<()>;
    fn get(&self, name: String) -> Result<()>;
}

impl ThreadedStore for Store {
    fn with_db(db: Database) -> Store {
        Store::with_prefix(db, "fs".to_owned())
    }

    fn with_prefix(db: Database, prefix: String) -> Store {
        Arc::new(StoreInner {
            files: db.collection(&format!("{}.files", prefix)[..]),
            chunks: db.collection(&format!("{}.chunks", prefix)[..]),
        })
    }

    fn create(&self, name: String) -> Result<File> {
        Ok(File::with_name(self.clone(), name, try!(oid::ObjectId::new()), Mode::Writing))
    }

    fn open(&self, name: String) -> Result<File> {
        let mut options = FindOptions::new();
        options.sort = Some(doc!{ "uploadDate" => 1 });

        match try!(self.files.find_one(Some(doc!{ "filename" => name }), Some(options))) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError("File does not exist.".to_owned())),
        }
    }

    fn open_id(&self, id: oid::ObjectId) -> Result<File> {
        match try!(self.files.find_one(Some(doc!{ "_id" => id }), None)) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError("File does not exist.".to_owned())),
        }
    }

    fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
            -> Result<Cursor> {
        self.files.find(filter, options)
    }

    // TODO: Make a GridCursor wrapper for this?
    fn open_next(&self, cursor: &mut Cursor) -> Result<Option<File>> {
        match cursor.next() {
            Some(Ok(bdoc)) => Ok(Some(File::with_doc(self.clone(), bdoc))),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    fn remove(&self, name: String) -> Result<()> {
        let mut options = FindOptions::new();
        options.projection = Some(doc!{ "_id" => 1 });

        let cursor = try!(self.find(Some(doc!{ "filename" => name }), Some(options)));
        for res in cursor {
            let doc = try!(res);
            if let Some(&Bson::ObjectId(ref id)) = doc.get("_id") {
                try!(self.remove_id(id.clone()));
            }
        }

        Ok(())
    }

    fn remove_id(&self, id: oid::ObjectId) -> Result<()> {
        try!(self.files.delete_many(doc!{ "_id" => (id.clone()) }, None));
        try!(self.chunks.delete_many(doc!{ "files_id" => (id.clone()) }, None));
        Ok(())
    }

    fn put(&self, name: String) -> Result<()> {
        let mut file = try!(self.create(name.to_owned()));
        let mut f = try!(fs::File::open(name.to_owned()));
        try!(io::copy(&mut f, &mut file));
        try!(file.close());
        Ok(())
    }

    fn get(&self, name: String) -> Result<()> {
        let mut f = try!(fs::File::create(name.to_owned()));
        let mut file = try!(self.open(name.to_owned()));
        try!(io::copy(&mut file, &mut f));
        try!(file.close());
        Ok(())
    }
}
