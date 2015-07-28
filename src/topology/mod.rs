pub mod server;
pub mod monitor;

use Error::{ArgumentError, OperationError};
use Result;

use bson::oid;

use connstring::{ConnectionString, Host};
use pool::PooledStream;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicIsize;

use self::server::Server;

const DEFAULT_HEARTBEAT_FREQUENCY_MS: u32 = 10000;

/// Describes the type of topology for a server set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyType {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    Unknown,
}

/// Topology information gathered from server set monitoring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyDescription {
    pub ttype: TopologyType,
    pub set_name: String,
    pub heartbeat_frequency_ms: u32,
    max_election_id: Option<oid::ObjectId>,
    compatible: bool,
    compat_error: String,
}

/// Holds status and connection information about a server set.
#[derive(Clone)]
pub struct Topology {
    config: ConnectionString,
    description: Arc<RwLock<TopologyDescription>>,
    servers: Arc<HashMap<Host, RwLock<Server>>>,
}

impl TopologyDescription {
    /// Returns a default, unknown topology description.
    pub fn new() -> TopologyDescription {
        TopologyDescription {
            ttype: TopologyType::Unknown,
            set_name: String::new(),
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            max_election_id: None,
            compatible: true,
            compat_error: String::new(),
        }
    }
}

impl Topology {
    /// Returns a new topology with the given configuration and description.
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

        let top_description = Arc::new(RwLock::new(options));

        let mut servers = HashMap::new();
        for host in config.hosts.iter() {
            let server = Server::new(req_id.clone(), host.clone(), top_description.clone());
            servers.insert(host.clone(), RwLock::new(server));
        }

        Ok(Topology {
            config: config,
            description: top_description,
            servers: Arc::new(servers),
        })
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        for (_, server) in self.servers.iter() {
            let read_server = try!(server.read());
            return read_server.acquire_stream()
        }
        Err(OperationError("No servers found in configuration.".to_owned()))
    }
}
