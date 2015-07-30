use Error::{self, ArgumentError, OperationError};
use Result;

use bson::{self, Bson, oid};
use chrono::{DateTime, UTC};

use coll::options::FindOptions;
use connstring::{self, Host};
use cursor::Cursor;
use pool::ConnectionPool;
use wire_protocol::flags::OpQueryFlags;

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::thread;

use super::server::{ServerDescription, ServerType};
use super::{DEFAULT_HEARTBEAT_FREQUENCY_MS, TopologyDescription};

const DEFAULT_MAX_BSON_OBJECT_SIZE: i64 = 16 * 1024 * 1024;
const DEFAULT_MAX_MESSAGE_SIZE_BYTES: i64 = 48000000;

/// The result of an isMaster operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IsMasterResult {
    pub is_master: bool,
    pub max_bson_object_size: i64,
    pub max_message_size_bytes: i64,
    pub local_time: Option<DateTime<UTC>>,
    pub min_wire_version: i64,
    pub max_wire_version: i64,

    // Shards
    pub msg: String,

    // Replica Sets
    pub is_replica_set: bool,
    pub is_secondary: bool,
    pub me: Option<Host>,
    pub hosts: Vec<Host>,
    pub passives: Vec<Host>,
    pub arbiters: Vec<Host>,
    pub arbiter_only: bool,
    pub tags: BTreeMap<String, String>,
    pub set_name: String,
    pub election_id: Option<oid::ObjectId>,
    pub primary: Option<Host>,
    pub hidden: bool,
}

/// Monitors and updates server and topology information.
pub struct Monitor {
    host: Host,
    pool: Arc<ConnectionPool>,
    top_description: Arc<RwLock<TopologyDescription>>,
    server_description: Arc<RwLock<ServerDescription>>,
    socket: TcpStream,
    req_id: Arc<AtomicIsize>,
    pub running: Arc<AtomicBool>,
}

impl IsMasterResult {
    /// Parses an isMaster response document from the server.
    pub fn new(doc: bson::Document) -> Result<IsMasterResult> {
        let is_master = match doc.get("ismaster") {
            Some(&Bson::Boolean(ref b)) => *b,
            _ => return Err(ArgumentError("result does not contain 'ismaster'.".to_owned())),
        };

        let mut result = IsMasterResult {
            is_master: is_master,
            max_bson_object_size: DEFAULT_MAX_BSON_OBJECT_SIZE,
            max_message_size_bytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
            local_time: None,
            min_wire_version: -1,
            max_wire_version: -1,
            msg: String::new(),
            is_secondary: false,
            is_replica_set: false,
            me: None,
            hosts: Vec::new(),
            passives: Vec::new(),
            arbiters: Vec::new(),
            arbiter_only: false,
            tags: BTreeMap::new(),
            set_name: String::new(),
            election_id: None,
            primary: None,
            hidden: false,
        };


        if let Some(&Bson::UtcDatetime(ref datetime)) = doc.get("localTime") {
            result.local_time = Some(datetime.clone());
        }


        if let Some(&Bson::I64(v)) = doc.get("minWireVersion") {
            result.min_wire_version = v;
        }

        if let Some(&Bson::I64(v)) = doc.get("maxWireVersion") {
            result.max_wire_version = v;
        }

        if let Some(&Bson::String(ref s)) = doc.get("msg") {
            result.msg = s.to_owned();
        }

        if let Some(&Bson::Boolean(ref b)) = doc.get("secondary") {
            result.is_secondary = *b;
        }

        if let Some(&Bson::Boolean(ref b)) = doc.get("isreplicaset") {
            result.is_replica_set = *b;
        }

        if let Some(&Bson::String(ref s)) = doc.get("setName") {
            result.set_name = s.to_owned();
        }

        if let Some(&Bson::String(ref s)) = doc.get("me") {
            result.me = Some(try!(connstring::parse_host(s)));
        }

        if let Some(&Bson::Array(ref arr)) = doc.get("hosts") {
            result.hosts = arr.iter().filter_map(|bson| match bson {
                &Bson::String(ref s) => connstring::parse_host(s).ok(),
                _ => None,
            }).collect();
        }

        if let Some(&Bson::Array(ref arr)) = doc.get("passives") {
            result.passives = arr.iter().filter_map(|bson| match bson {
                &Bson::String(ref s) => connstring::parse_host(s).ok(),
                _ => None,
            }).collect();
        }

        if let Some(&Bson::Array(ref arr)) = doc.get("arbiters") {
            result.passives = arr.iter().filter_map(|bson| match bson {
                &Bson::String(ref s) => connstring::parse_host(s).ok(),
                _ => None,
            }).collect();
        }

        if let Some(&Bson::String(ref s)) = doc.get("primary") {
            result.primary = Some(try!(connstring::parse_host(s)));
        }

        if let Some(&Bson::Boolean(ref b)) = doc.get("arbiterOnly") {
            result.arbiter_only = *b;
        }

        if let Some(&Bson::Boolean(ref h)) = doc.get("hidden") {
            result.hidden = *h;
        }

        if let Some(&Bson::Document(ref doc)) = doc.get("tags") {
            for (k, v) in doc.into_iter() {
                if let &Bson::String(ref tag) = v {
                    result.tags.insert(k.to_owned(), tag.to_owned());
                }
            }
        }

        if let Some(&Bson::ObjectId(ref id)) = doc.get("electionId") {
            result.election_id = Some(id.clone());
        }

        Ok(result)
    }
}

impl Monitor {
    /// Returns a new monitor connected to the server.
    pub fn new(host: Host, pool: Arc<ConnectionPool>,
               top_description: Arc<RwLock<TopologyDescription>>,
               server_description: Arc<RwLock<ServerDescription>>,
               req_id: Arc<AtomicIsize>) -> Result<Monitor> {

        Ok(Monitor {
            socket: try!(TcpStream::connect((&host.host_name[..], host.port))),
            req_id: req_id,
            host: host,
            pool: pool,
            top_description: top_description,
            server_description: server_description,
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Reconnects the monitor to the server.
    pub fn reconnect(&mut self) -> Result<()> {
        let ref host_name = self.host.host_name;
        let port = self.host.port;
        self.socket = try!(TcpStream::connect((&host_name[..], port)));
        Ok(())
    }

    // Set server description error field.
    fn set_err(&self, err: Error) {
        let mut server_description = self.server_description.write().unwrap();
        server_description.set_err(err);
    }

    /// Returns an isMaster server response using an owned monitor socket.
    pub fn is_master(&mut self) -> Result<Cursor> {
        let options = FindOptions::new().with_limit(1);
        let flags = OpQueryFlags::with_find_options(&options);
        let mut filter = bson::Document::new();
        filter.insert("isMaster".to_owned(), Bson::I32(1));

        Cursor::query_with_socket(
            &mut self.socket, None, self.req_id.fetch_add(1, Ordering::SeqCst) as i32,
            "local.$cmd".to_owned(), options.batch_size, flags, options.skip as i32,
            options.limit, filter.clone(), options.projection.clone(), false)
    }

    // Updates the server description associated with this monitor using an isMaster server response.
    fn update_server_description(&self, doc: bson::Document) -> Result<ServerDescription> {
        let ismaster_result = IsMasterResult::new(doc);
        let mut server_description = self.server_description.write().unwrap();
        match ismaster_result {
            Ok(ismaster) => server_description.update(ismaster),
            Err(err) => {
                server_description.set_err(err);
                return Err(OperationError("Failed to parse ismaster result.".to_owned()))
            },
        }

        Ok(server_description.clone())
    }

    // Updates the topology description associated with this monitor using a new server description.
    fn update_top_description(&self, description: ServerDescription) {
        let mut top_description = self.top_description.write().unwrap();
        top_description.update(self.host.clone(), description,
                               self.req_id.clone(), self.top_description.clone());
    }

    // Updates server and topology descriptions using a successful isMaster cursor result.
    fn update_with_is_master_cursor(&self, cursor: &mut Cursor) {
        match cursor.next() {
            Some(Ok(doc)) => {
                if let Ok(description) = self.update_server_description(doc) {
                    self.update_top_description(description);
                }
            },
            Some(Err(err)) => {
                let mut server_description = self.server_description.write().unwrap();
                server_description.set_err(err);
            },
            None => {
                let mut server_description = self.server_description.write().unwrap();
                server_description.set_err(OperationError("ismaster returned no response.".to_owned()));
            }
        }
    }

    /// Starts server monitoring.
    pub fn run(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            match self.is_master() {
                Ok(mut cursor) => self.update_with_is_master_cursor(&mut cursor),
                Err(err) => {
                    // Refresh all connections
                    self.pool.clear();
                    if let Err(err) = self.reconnect() {
                        self.set_err(err);
                        break;
                    }

                    let stype = self.server_description.read().unwrap().stype;

                    if stype == ServerType::Unknown {
                        self.set_err(err);
                    } else {
                        // Retry once
                        match self.is_master() {
                            Ok(mut cursor) => self.update_with_is_master_cursor(&mut cursor),
                            Err(err) => self.set_err(err),
                        }
                    }
                }
            }

            if let Ok(description) = self.top_description.read() {
                thread::sleep_ms(description.heartbeat_frequency_ms);
            } else {
                thread::sleep_ms(DEFAULT_HEARTBEAT_FREQUENCY_MS);
            }
        }
    }
}
