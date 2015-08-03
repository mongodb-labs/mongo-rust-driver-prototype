use Error::{self, OperationError};
use Result;

use bson::oid;
use connstring::Host;
use pool::{ConnectionPool, PooledStream};

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::thread;

use super::monitor::{IsMasterResult, Monitor};
use super::TopologyDescription;

/// Describes the server role within a server set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerType {
    /// Standalone server.
    Standalone,
    /// Shard router.
    Mongos,
    /// Replica set primary.
    RSPrimary,
    /// Replica set secondary.
    RSSecondary,
    /// Replica set arbiter.
    RSArbiter,
    /// Replica set member of some other type.
    RSOther,
    /// Replica set ghost member.
    RSGhost,
    /// Server type is currently unknown.
    Unknown,
}

/// Server information gathered from server monitoring.
#[derive(Clone, Debug)]
pub struct ServerDescription {
    /// The server type.
    pub server_type: ServerType,
    /// Any error encountered while monitoring this server.
    pub err: Arc<Option<Error>>,
    /// The average round-trip time over the last 5 monitoring checks.
    pub round_trip_time: Option<i64>,
    /// The minimum wire version supported by this server.
    pub min_wire_version: i64,
    /// The maximum wire version supported by this server.
    pub max_wire_version: i64,
    /// The server's host information, if it is part of a replica set.
    pub me: Option<Host>,
    /// All hosts in the replica set known by this server.
    pub hosts: Vec<Host>,
    /// All passive members of the replica set known by this server.
    pub passives: Vec<Host>,
    /// All arbiters in the replica set known by this server.
    pub arbiters: Vec<Host>,
    /// Server tags for targeted read operations on specific replica set members.
    pub tags: BTreeMap<String, String>,
    /// The replica set name.
    pub set_name: String,
    /// The server's current election id, if it believes it is a primary.
    pub election_id: Option<oid::ObjectId>,
    /// The server's opinion of who the primary is.
    pub primary: Option<Host>,
}

/// Holds status and connection information about a single server.
#[derive(Clone)]
pub struct Server {
    /// Host connection details.
    pub host: Host,
    /// Monitored server information.
    pub description: Arc<RwLock<ServerDescription>>,
    /// The connection pool for this server.
    pool: Arc<ConnectionPool>,
    /// A reference to the associated monitor's running bool.
    /// When this server is dropped, the monitor will be stopped.
    monitor_running: Arc<AtomicBool>,
}

impl FromStr for ServerType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Standalone" => ServerType::Standalone,
            "Mongos" => ServerType::Mongos,
            "RSPrimary" => ServerType::RSPrimary,
            "RSSecondary" => ServerType::RSSecondary,
            "RSArbiter" => ServerType::RSArbiter,
            "RSOther" => ServerType::RSOther,
            "RSGhost" => ServerType::RSGhost,
            _ => ServerType::Unknown,
        })
    }
}

impl ServerDescription {
    /// Returns a default, unknown server description.
    pub fn new() -> ServerDescription {
        ServerDescription {
            server_type: ServerType::Unknown,
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

    // Updates the server description using an isMaster server response.
    pub fn update(&mut self, ismaster: IsMasterResult) {
        if !ismaster.ok {
            self.set_err(OperationError("ismaster returned a not-ok response.".to_owned()));
            return;
        }

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

        let set_name_empty = self.set_name.is_empty();
        let msg_empty = ismaster.msg.is_empty();

        self.server_type = if msg_empty && set_name_empty && !ismaster.is_replica_set {
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

    // Sets an encountered error and reverts the server type to Unknown.
    pub fn set_err(&mut self, err: Error) {
        self.err = Arc::new(Some(err));
        self.server_type = ServerType::Unknown;
        self.set_name = String::new();
        self.election_id = None;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.monitor_running.store(false, Ordering::SeqCst);
    }
}

impl Server {
    /// Returns a new server with the given host, initializing a new connection pool and monitor.
    pub fn new(req_id: Arc<AtomicIsize>, host: Host,
               top_description: Arc<RwLock<TopologyDescription>>) -> Server {

        let description = Arc::new(RwLock::new(ServerDescription::new()));

        // Create new monitor thread
        let host_clone = host.clone();
        let desc_clone = description.clone();

        let pool = Arc::new(ConnectionPool::new(host.clone()));

        // Fails silently
        let monitor = Monitor::new(host_clone, pool.clone(), top_description, desc_clone, req_id);

        let monitor_running = if monitor.is_ok() {
            monitor.as_ref().unwrap().running.clone()
        } else {
            Arc::new(AtomicBool::new(false))
        };

        if monitor.is_ok() {
            thread::spawn(move || {
                monitor.unwrap().run();
            });
        }

        Server {
            host: host,
            pool: pool,
            description: description.clone(),
            monitor_running: monitor_running,
        }
    }

    /// Returns a server stream from the connection pool.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        self.pool.acquire_stream()
    }
}
