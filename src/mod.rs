pub mod db;
pub mod coll;
pub mod common;
pub mod connstring;
pub mod cursor;
pub mod error;
pub mod oid;
pub mod pool;
pub mod wire_protocol;

pub use client::error::{Error, Result};

use bson;
use bson::Bson;

use common::{ReadPreference, WriteConcern};
use connstring::ConnectionString;
use db::Database;
use error::Error::ResponseError;
use pool::{ConnectionPool, PooledStream};

use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering, ATOMIC_ISIZE_INIT};

/// Interfaces with a MongoDB server or replica set.
#[derive(Clone)]
pub struct Client {
    req_id: Arc<AtomicIsize>,
    pool: ConnectionPool,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
}

unsafe impl Sync for Client {}

impl Client {
    /// Creates a new Client connected to a single MongoDB server.
    pub fn new(host: &str, port: u16) -> Result<Client> {
        Client::with_prefs(host, port, None, None)
    }

    /// `new` with custom read and write controls.
    pub fn with_prefs(host: &str, port: u16, read_pref: Option<ReadPreference>,
                      write_concern: Option<WriteConcern>) -> Result<Client> {
        let config = ConnectionString::new(host, port);
        Client::with_config(config, read_pref, write_concern)
    }

    /// Creates a new Client connected to a server or replica set using
    /// a MongoDB connection string URI as defined by
    /// [the manual](http://docs.mongodb.org/manual/reference/connection-string/).
    pub fn with_uri(uri: &str) -> Result<Client> {
        Client::with_uri_and_prefs(uri, None, None)
    }

    /// `with_uri` with custom read and write controls.
    pub fn with_uri_and_prefs(uri: &str, read_pref: Option<ReadPreference>,
                              write_concern: Option<WriteConcern>) -> Result<Client> {
        let config = try!(connstring::parse(uri));
        Client::with_config(config, read_pref, write_concern)
    }

    fn with_config(config: ConnectionString, read_pref: Option<ReadPreference>,
                   write_concern: Option<WriteConcern>) -> Result<Client> {

        let rp = match read_pref {
            Some(rp) => rp,
            None => ReadPreference::Primary,
        };

        let wc = match write_concern {
            Some(wc) => wc,
            None => WriteConcern::new(),
        };

        Ok(Client {
            req_id: Arc::new(ATOMIC_ISIZE_INIT),
            pool: ConnectionPool::new(config),
            read_preference: rp,
            write_concern: wc,
        })
    }

    /// Creates a database representation with default read and write controls.
    pub fn db<'a>(&'a self, db_name: &str) -> Database<'a> {
        Database::new(self, db_name, None, None)
    }

    /// Creates a database representation with custom read and write controls.
    pub fn db_with_prefs<'a>(&'a self, db_name: &str, read_preference: Option<ReadPreference>,
                             write_concern: Option<WriteConcern>) -> Database<'a> {
        Database::new(self, db_name, read_preference, write_concern)
    }

    /// Acquires a connection stream from the pool.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        Ok(try!(self.pool.acquire_stream()))
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.req_id.fetch_add(1, Ordering::SeqCst) as i32
    }

    /// Returns a list of all database names that exist on the server.
    pub fn database_names(&self) -> Result<Vec<String>> {
        let mut doc = bson::Document::new();
        doc.insert("listDatabases".to_owned(), Bson::I32(1));

        let db = self.db("admin");
        let res = try!(db.command(doc));
        if let Some(&Bson::Array(ref batch)) = res.get("databases") {
            // Extract database names
            let map = batch.iter().filter_map(|bdoc| {
                if let &Bson::Document(ref doc) = bdoc {
                    if let Some(&Bson::String(ref name)) = doc.get("name") {
                        return Some(name.to_owned());
                    }
                }
                None
            }).collect();
            return Ok(map)
        }

        Err(ResponseError("Server reply does not contain 'databases'.".to_owned()))
    }

    /// Drops the database defined by `db_name`.
    pub fn drop_database(&self, db_name: &str) -> Result<()> {
        let db = self.db(db_name);
        try!(db.drop_database());
        Ok(())
    }

    /// Reports whether this instance is a primary, master, mongos, or standalone mongod instance.
    pub fn is_master(&self) -> Result<bool> {
        let mut doc = bson::Document::new();
        doc.insert("isMaster".to_owned(), Bson::I32(1));

        let db = self.db("local");
        let res = try!(db.command(doc));

        match res.get("ismaster") {
            Some(&Bson::Boolean(is_master)) => Ok(is_master),
            _ => Err(ResponseError("Server reply does not contain 'ismaster'.".to_owned())),
        }
    }
}
