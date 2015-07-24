use Error::{self, ArgumentError, OperationError};
use Result;

use bson::{self, Bson, oid};
use chrono::{DateTime, UTC};

use coll::options::FindOptions;
use connstring::{self, ConnectionString, Host};
use cursor::Cursor;
use pool::{ConnectionPool, PooledStream};
use wire_protocol::flags::OpQueryFlags;

use std::collections::{BTreeMap, HashMap};
use std::net::TcpStream;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread;

const DEFAULT_HEARTBEAT_FREQUENCY_MS: u32 = 10000;
const DEFAULT_RETRY_FREQUENCY_MS: u32 = 1000;
const DEFAULT_MAX_BSON_OBJECT_SIZE: i64 = 16 * 1024 * 1024;
const DEFAULT_MAX_MESSAGE_SIZE_BYTES: i64 = 48000000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopologyType {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerType {
    Standalone,
    Mongos,
    RSPrimary,
    RSSecondary,
    RSArbiter,
    RSOther,
    RSGhost,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IsMasterResult {
    is_master: bool,
    max_bson_object_size: i64,
    max_message_size_bytes: i64,
    local_time: DateTime<UTC>,
    min_wire_version: i64,
    max_wire_version: i64,

    // Shards
    msg: String,

    // RS
    is_replica_set: bool,
    is_secondary: bool,
    me: Option<Host>,
    hosts: Vec<Host>,
    passives: Vec<Host>,
    arbiters: Vec<Host>,
    arbiter_only: bool,
    tags: BTreeMap<String, String>,
    set_name: String,
    election_id: Option<oid::ObjectId>,
    primary: Option<Host>,
    hidden: bool,
}

#[derive(Clone)]
pub struct Topology {
    config: ConnectionString,
    description: TopologyDescription,
    servers: Arc<HashMap<Host, RwLock<Server>>>,
    compatible: bool,
    compat_error: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyDescription {
    ttype: TopologyType,
    set_name: String,
    heartbeat_frequency_ms: u32,
    max_election_id: Option<oid::ObjectId>,
}

#[derive(Clone)]
pub struct Server {
    pub host: Host,
    pool: ConnectionPool,
    description: Arc<RwLock<ServerDescription>>,
}

#[derive(Clone, Debug)]
pub struct ServerDescription {
    stype: ServerType,
    err: Arc<Option<Error>>,
    round_trip_time: Option<i64>,
    min_wire_version: i64,
    max_wire_version: i64,
    me: Option<Host>,
    hosts: Vec<Host>,
    passives: Vec<Host>,
    arbiters: Vec<Host>,
    tags: BTreeMap<String, String>,
    set_name: String,
    election_id: Option<oid::ObjectId>,
    primary: Option<Host>,
}

struct Monitor {
    host: Host,
    description: Arc<RwLock<ServerDescription>>,
    socket: TcpStream,
}

impl IsMasterResult {
    pub fn new(doc: bson::Document) -> Result<IsMasterResult> {
        let is_master = match doc.get("ismaster") {
            Some(&Bson::Boolean(ref b)) => *b,
            _ => return Err(ArgumentError("result does not contain 'ismaster'.".to_owned())),
        };

        let local_time = match doc.get("localTime") {
            Some(&Bson::UtcDatetime(ref datetime)) => datetime.clone(),
            _ => return Err(ArgumentError("result does not contain 'localTime'.".to_owned())),
        };

        let min_version = match doc.get("minWireVersion") {
            Some(&Bson::I64(ref v)) => *v,
            _ => return Err(ArgumentError("result does not contain 'minWireVersion'.".to_owned())),
        };

        let max_version = match doc.get("maxWireVersion") {
            Some(&Bson::I64(ref v)) => *v,
            _ => return Err(ArgumentError("result does not contain 'maxWireVersion'.".to_owned())),
        };

        let mut result = IsMasterResult {
            is_master: is_master,
            max_bson_object_size: DEFAULT_MAX_BSON_OBJECT_SIZE,
            max_message_size_bytes: DEFAULT_MAX_MESSAGE_SIZE_BYTES,
            local_time: local_time,
            min_wire_version: min_version,
            max_wire_version: max_version,
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

impl TopologyDescription {
    pub fn new() -> TopologyDescription {
        TopologyDescription {
            ttype: TopologyType::Unknown,
            set_name: String::new(),
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            max_election_id: None,
        }
    }
}

impl Topology {
    pub fn new(req_id: Arc<AtomicIsize>, config: ConnectionString,
               description: Option<TopologyDescription>) -> Result<Topology> {

        let options = description.unwrap_or(TopologyDescription::new());

        if config.hosts.len() > 1 && options.ttype == TopologyType::Single {
            return Err(ArgumentError(
                "TopologyType::Single cannot be used with multiple seeds.".to_owned()));
        }

        if !options.set_name.is_empty() && options.ttype != TopologyType::ReplicaSetNoPrimary {
            return Err(ArgumentError(
                "TopologyType must be ReplicaSetNoPrimary if set_name is provided.".to_owned()));
        }

        // TODO: Determine driver's wire compatibility, and check overlap with
        // all servers in topology.

        let mut servers = HashMap::new();
        for host in config.hosts.iter() {
            let server = Server::new(req_id.clone(), host.clone());
            servers.insert(host.clone(), RwLock::new(server));
        }

        Ok(Topology {
            config: config,
            description: options,
            servers: Arc::new(servers),
            compatible: true,
            compat_error: String::new(),
        })
    }

    pub fn acquire_stream(&self) -> Result<PooledStream> {
        for (_, server) in self.servers.iter() {
            let read_server = try!(server.read());
            return read_server.acquire_stream()
        }
        Err(OperationError("No servers found in configuration.".to_owned()))
    }
}

impl ServerDescription {
    pub fn new() -> ServerDescription {
        ServerDescription {
            stype: ServerType::Unknown,
            err: Arc::new(None),
            round_trip_time: None,
            min_wire_version: 0,
            max_wire_version: 0,
            me: None,
            hosts: Vec::new(),
            passives: Vec::new(),
            arbiters: Vec::new(),
            tags: BTreeMap::new(),
            set_name: String::new(),
            election_id: None,
            primary: None,
        }
    }

    pub fn update(&mut self, ismaster: IsMasterResult) {
        self.min_wire_version = ismaster.min_wire_version;
        self.max_wire_version = ismaster.max_wire_version;
        self.me = ismaster.me;
        self.hosts = ismaster.hosts;
        self.passives = ismaster.passives;
        self.arbiters = ismaster.arbiters;
        self.tags = ismaster.tags;
        self.set_name = ismaster.set_name;
        self.election_id = ismaster.election_id;
        self.primary = ismaster.primary;

        let hosts_empty = self.hosts.is_empty();
        let set_name_empty = self.set_name.is_empty();
        let msg_empty = ismaster.msg.is_empty();
        
        self.stype = if msg_empty && set_name_empty && hosts_empty {
            ServerType::Standalone
        } else if !msg_empty {
            ServerType::Mongos
        } else if ismaster.is_master && !set_name_empty {
            ServerType::RSPrimary
        } else if ismaster.is_secondary && !set_name_empty {
            ServerType::RSSecondary
        } else if ismaster.arbiter_only && !set_name_empty {
            ServerType::RSArbiter
        } else if !set_name_empty {
            ServerType::RSOther
        } else if ismaster.is_replica_set {
            ServerType::RSGhost
        } else {
            ServerType::Unknown
        }
    }

    pub fn set_err(&mut self, err: Error) {
        self.err = Arc::new(Some(err));
        self.stype = ServerType::Unknown;
    }
}

impl Monitor {
    pub fn new(host: Host, description: Arc<RwLock<ServerDescription>>) -> Result<Monitor> {
        Ok(Monitor {
            socket: try!(TcpStream::connect((&host.host_name[..], host.port))),
            host: host,
            description: description,
        })
    }

    pub fn reconnect(&mut self) -> Result<()> {
        let ref host_name = self.host.host_name;
        let port = self.host.port;
        self.socket = try!(TcpStream::connect((&host_name[..], port)));
        Ok(())
    }
}

impl Server {
    pub fn new(req_id: Arc<AtomicIsize>, host: Host) -> Server {
        let description = Arc::new(RwLock::new(ServerDescription::new()));

        // Create new monitor thread
        let host_clone = host.clone();
        let desc_clone = description.clone();

        thread::spawn(move|| {
            let mut monitor = Monitor::new(host_clone, desc_clone).ok().expect("Failed to connect monitor to server.");

            // Call ismaster on socket at low level to avoid using client resources
            let options = FindOptions::new().with_limit(1);
            let flags = OpQueryFlags::with_find_options(&options);
            let mut filter = bson::Document::new();
            filter.insert("isMaster".to_owned(), Bson::I32(1));

            loop {
                // break on some condition somehow

                let result = Cursor::query_with_socket(
                    &mut monitor.socket, None, req_id.fetch_add(1, Ordering::SeqCst) as i32,
                    "local.$cmd".to_owned(), options.batch_size, flags, options.skip as i32,
                    options.limit, filter.clone(), options.projection.clone(), false);

                match result {
                    Ok(mut cursor) => {     
                        match cursor.next() {
                            Some(Ok(doc)) => {
                                // Parse ismaster result and update server description.
                                let ismaster_result = IsMasterResult::new(doc);
                                {
                                    let mut server_description = monitor.description.write().unwrap();
                                    match ismaster_result {
                                        Ok(ismaster) => server_description.update(ismaster),
                                        Err(err) => server_description.set_err(err),
                                    }
                                }
                            },
                            Some(Err(err)) => panic!(err),
                            None => panic!("ismaster returned no response."),
                        }
                        thread::sleep_ms(DEFAULT_HEARTBEAT_FREQUENCY_MS);
                    },
                    Err(err) => {
                        {
                            let mut server_description = monitor.description.write().unwrap();
                            server_description.set_err(err);
                        }
                        thread::sleep_ms(DEFAULT_RETRY_FREQUENCY_MS);
                    }
                }
            }
        });

        Server {
            host: host.clone(),
            pool: ConnectionPool::new(host),
            description: description.clone(),
        }
    }

    pub fn acquire_stream(&self) -> Result<PooledStream> {
        self.pool.acquire_stream()
    }
}
