#[macro_use(bson, doc)] extern crate bson;
extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate rand;
extern crate rustc_serialize;
#[macro_use] extern crate scan_fmt;
extern crate separator;
extern crate textnonce;
extern crate time;

pub mod db;
pub mod coll;
pub mod common;
pub mod connstring;
pub mod cursor;
pub mod error;
pub mod gridfs;
pub mod pool;
pub mod topology;
pub mod wire_protocol;

mod apm;
mod auth;
mod command_type;

pub use apm::{CommandStarted, CommandResult};
pub use command_type::CommandType;
pub use error::{Error, ErrorCode, Result};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicIsize, Ordering, ATOMIC_ISIZE_INIT};

use apm::Listener;
use bson::Bson;
use common::{ReadPreference, ReadMode, WriteConcern};
use connstring::ConnectionString;
use db::{Database, ThreadedDatabase};
use error::Error::ResponseError;
use pool::PooledStream;
use topology::{Topology, TopologyDescription, TopologyType};
use topology::server::Server;

/// Interfaces with a MongoDB server or replica set.
pub struct ClientInner {
    req_id: Arc<AtomicIsize>,
    topology: Topology,
    listener: Listener,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
    log_file: Option<Mutex<File>>,
}

pub trait ThreadedClient: Sync + Sized {
    fn connect(host: &str, port: u16) -> Result<Self>;
    fn connect_with_log_file(host: &str, port: u16, log_file: &str) -> Result<Client>;
    fn with_prefs(host: &str, port: u16, read_pref: Option<ReadPreference>,
                  write_concern: Option<WriteConcern>,
                  log_file: Option<&str>) -> Result<Self>;
    fn with_uri(uri: &str) -> Result<Self>;
    fn with_uri_and_prefs(uri: &str, read_pref: Option<ReadPreference>,
                          write_concern: Option<WriteConcern>,
                          log_file: Option<&str>) -> Result<Self>;
    fn with_config(config: ConnectionString, read_pref: Option<ReadPreference>, write_concern: Option<WriteConcern>,
                   description: Option<TopologyDescription>, log_file: Option<&str>) -> Result<Self>;
    fn db<'a>(&'a self, db_name: &str) -> Database;
    fn db_with_prefs(&self, db_name: &str, read_preference: Option<ReadPreference>,
                     write_concern: Option<WriteConcern>) -> Database;
    fn acquire_stream(&self, read_pref: ReadPreference) -> Result<(PooledStream, bool, bool)>;
    fn acquire_write_stream(&self) -> Result<PooledStream>;
    fn get_req_id(&self) -> i32;
    fn database_names(&self) -> Result<Vec<String>>;
    fn drop_database(&self, db_name: &str) -> Result<()>;
    fn is_master(&self) -> Result<bool>;
    fn add_start_hook(&mut self, hook: fn(Client, &CommandStarted)) -> Result<()>;
    fn add_completion_hook(&mut self, hook: fn(Client, &CommandResult)) -> Result<()>;
}

pub type Client = Arc<ClientInner>;

impl ThreadedClient for Client {
    /// Creates a new Client connected to a single MongoDB server.
    fn connect(host: &str, port: u16) -> Result<Client> {
        Client::with_prefs(host, port, None, None, None)
    }

    /// Creates a new Client connected to a single MongoDB server that prints logging
    /// information to a specified file.
    fn connect_with_log_file(host: &str, port: u16, log_file: &str) -> Result<Client> {
        Client::with_prefs(host, port, None, None, Some(log_file))
    }

    /// `new` with custom read and write controls.
    fn with_prefs(host: &str, port: u16, read_pref: Option<ReadPreference>,
                  write_concern: Option<WriteConcern>, log_file: Option<&str>) -> Result<Client> {
        let config = ConnectionString::new(host, port);
        let mut description = TopologyDescription::new();
        description.topology_type = TopologyType::Single;

        Client::with_config(config, read_pref, write_concern, Some(description), log_file)
    }

    /// Creates a new Client connected to a server or replica set using
    /// a MongoDB connection string URI as defined by
    /// [the manual](http://docs.mongodb.org/manual/reference/connection-string/).
    fn with_uri(uri: &str) -> Result<Client> {
        Client::with_uri_and_prefs(uri, None, None, None)
    }

    /// `with_uri` with custom read and write controls.
    fn with_uri_and_prefs(uri: &str, read_pref: Option<ReadPreference>,
                          write_concern: Option<WriteConcern>, log_file: Option<&str>) -> Result<Client> {
        let config = try!(connstring::parse(uri));
        Client::with_config(config, read_pref, write_concern, None, log_file)
    }

    fn with_config(config: ConnectionString, read_pref: Option<ReadPreference>, write_concern: Option<WriteConcern>,
                   description: Option<TopologyDescription>, log_file: Option<&str>) -> Result<Client> {

        let rp = match read_pref {
            Some(rp) => rp,
            None => ReadPreference::new(ReadMode::Primary, None),
        };

        let wc = match write_concern {
            Some(wc) => wc,
            None => WriteConcern::new(),
        };

        let listener = Listener::new();
        let file = match log_file {
            Some(string) => {
                let _ = listener.add_start_hook(log_command_started);
                let _ = listener.add_completion_hook(log_command_completed);
                Some(Mutex::new(try!(OpenOptions::new().write(true).append(true).create(true).open(string))))
            },
            None => None,
        };

        let client = Arc::new(ClientInner {
            req_id: Arc::new(ATOMIC_ISIZE_INIT),
            topology: try!(Topology::new(config.clone(), description)),
            listener: listener,
            read_preference: rp,
            write_concern: wc,
            log_file: file,
        });

        // Fill servers array
        {
            let ref top_description = client.topology.description;
            let mut top = try!(top_description.write());
            for host in config.hosts.iter() {
                let server = Server::new(client.clone(), host.clone(), top_description.clone(), true);
                top.servers.insert(host.clone(), server);
            }
        }

        Ok(client)
    }

    /// Creates a database representation with default read and write controls.
    fn db(&self, db_name: &str) -> Database {
        Database::open(self.clone(), db_name, None, None)
    }

    /// Creates a database representation with custom read and write controls.
    fn db_with_prefs(&self, db_name: &str, read_preference: Option<ReadPreference>,
                     write_concern: Option<WriteConcern>) -> Database {
        Database::open(self.clone(), db_name, read_preference, write_concern)
    }

    /// Acquires a connection stream from the pool, along with slave_ok and should_send_read_pref.
    fn acquire_stream(&self, read_preference: ReadPreference) -> Result<(PooledStream, bool, bool)> {
        self.topology.acquire_stream(read_preference)
    }

    /// Acquires a connection stream from the pool for write operations.
    fn acquire_write_stream(&self) -> Result<PooledStream> {
        self.topology.acquire_write_stream()
    }
    
    /// Returns a unique operational request id.
    fn get_req_id(&self) -> i32 {
        self.req_id.fetch_add(1, Ordering::SeqCst) as i32
    }

    /// Returns a list of all database names that exist on the server.
    fn database_names(&self) -> Result<Vec<String>> {
        let mut doc = bson::Document::new();
        doc.insert("listDatabases".to_owned(), Bson::I32(1));

        let db = self.db("admin");
        let res = try!(db.command(doc, CommandType::ListDatabases, None));
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
    fn drop_database(&self, db_name: &str) -> Result<()> {
        let db = self.db(db_name);
        try!(db.drop_database());
        Ok(())
    }

    /// Reports whether this instance is a primary, master, mongos, or standalone mongod instance.
    fn is_master(&self) -> Result<bool> {
        let mut doc = bson::Document::new();
        doc.insert("isMaster".to_owned(), Bson::I32(1));

        let db = self.db("local");
        let res = try!(db.command(doc, CommandType::IsMaster, None));

        match res.get("ismaster") {
            Some(&Bson::Boolean(is_master)) => Ok(is_master),
            _ => Err(ResponseError("Server reply does not contain 'ismaster'.".to_owned())),
        }
    }

    /// Sets a function to be run every time a command starts.
    fn add_start_hook(&mut self, hook: fn(Client, &CommandStarted)) -> Result<()> {
        self.listener.add_start_hook(hook)
    }

    /// Sets a function to be run every time a command completes.
    fn add_completion_hook(&mut self, hook: fn(Client, &CommandResult)) -> Result<()> {
        self.listener.add_completion_hook(hook)
    }
}

fn log_command_started(client: Client, command_started: &CommandStarted) {
    let mutex = match client.log_file {
        Some(ref mutex) => mutex,
        None => return
    };

    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(_) => return
    };

    let _ = writeln!(guard.deref_mut(), "{}", command_started);
}

fn log_command_completed(client: Client, command_result: &CommandResult) {
    let mutex = match client.log_file {
        Some(ref mutex) => mutex,
        None => return
    };

    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(_) => return
    };

    let _ = writeln!(guard.deref_mut(), "{}", command_result);
}
