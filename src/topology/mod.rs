pub mod server;
pub mod monitor;

use {Client, Result};
use Error::{self, ArgumentError, OperationError};

use bson::oid;

use common::{ReadPreference, ReadMode};
use connstring::{ConnectionString, Host};
use pool::PooledStream;

use rand::{thread_rng, Rng};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread;

use self::server::{Server, ServerDescription, ServerType};

const DEFAULT_HEARTBEAT_FREQUENCY_MS: u32 = 10000;
const MAX_SERVER_RETRY: usize = 3;

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
#[derive(Clone)]
pub struct TopologyDescription {
    pub topology_type: TopologyType,
    /// The set name for a replica set topology. If the topology
    /// is not a replica set, this will be an empty string.
    pub set_name: String,
    /// The server connection health check frequency.
    /// The default is 10 seconds.
    pub heartbeat_frequency_ms: u32,
    /// Known servers within the topology.
    pub servers: HashMap<Host, Server>,
    // The largest election id seen from a server in the topology.
    max_election_id: Option<oid::ObjectId>,
    // If true, all servers in the topology fall within the compatible
    // mongodb version for this driver.
    compatible: bool,
    compat_error: String,
}

/// Holds status and connection information about a server set.
#[derive(Clone)]
pub struct Topology {
    /// The initial connection configuration.
    pub config: ConnectionString,
    /// Monitored topology information.
    pub description: Arc<RwLock<TopologyDescription>>,
}

impl FromStr for TopologyType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Single" => TopologyType::Single,
            "ReplicaSetNoPrimary" => TopologyType::ReplicaSetNoPrimary,
            "ReplicaSetWithPrimary" => TopologyType::ReplicaSetWithPrimary,
            "Sharded" => TopologyType::Sharded,
            _ => TopologyType::Unknown,
        })
    }
}

impl TopologyDescription {
    /// Returns a default, unknown topology description.
    pub fn new() -> TopologyDescription {
        TopologyDescription {
            topology_type: TopologyType::Unknown,
            set_name: String::new(),
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            servers: HashMap::new(),
            max_election_id: None,
            compatible: true,
            compat_error: String::new(),
        }
    }

    fn get_rand_server_stream(&self) -> Result<PooledStream> {
        let len = self.servers.len();
        let key = self.servers.keys().nth(thread_rng().gen_range(0, len)).unwrap();
        self.servers.get(&key).unwrap().acquire_stream()
    }

    fn get_nearest_server_stream(&self) -> Result<PooledStream> {
        self.get_rand_server_stream()
    }

    fn get_rand_from_vec(&self, servers: &mut Vec<Host>) -> Result<PooledStream> {
        while !servers.is_empty() {
            let len = servers.len();
            let index = thread_rng().gen_range(0, len);

            if let Some(server) = self.servers.get(servers.get(index).unwrap()) {
                if let Ok(stream) = server.acquire_stream() {
                    return Ok(stream);
                }
            }
            servers.remove(index);
        }
        Err(OperationError("No servers available for the provided ReadPreference.".to_owned()))
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self, read_preference: ReadPreference) -> Result<PooledStream> {
        if self.servers.is_empty() {
            return Err(OperationError("No servers are available for the given topology.".to_owned()));
        }

        match self.topology_type {
            TopologyType::Unknown => Err(OperationError("Topology is not yet known.".to_owned())),
            TopologyType::Single => self.get_rand_server_stream(),
            TopologyType::Sharded => self.get_nearest_server_stream(),
            _ => {

                // Handle replica set server selection
                let mut primaries = Vec::new();
                let mut secondaries = Vec::new();

                // Collect a list of primaries and secondaries in the set
                for (host, server) in self.servers.iter() {
                    let stype = server.description.read().unwrap().server_type;
                    match stype {
                        ServerType::RSPrimary => primaries.push(host.clone()),
                        ServerType::RSSecondary => secondaries.push(host.clone()),
                        _ => (),
                    }
                }

                // Choose an appropriate server at random based on the read preference.
                match read_preference.mode {
                    ReadMode::Primary => self.get_rand_from_vec(&mut primaries),
                    ReadMode::PrimaryPreferred => {
                        self.get_rand_from_vec(&mut primaries).or(
                            self.get_rand_from_vec(&mut secondaries))
                    },
                    ReadMode::Secondary => self.get_rand_from_vec(&mut secondaries),
                    ReadMode::SecondaryPreferred => {
                        self.get_rand_from_vec(&mut secondaries).or(
                            self.get_rand_from_vec(&mut primaries))
                    },
                    ReadMode::Nearest => self.get_nearest_server_stream(),
                }
            }
        }
    }

    pub fn update_without_monitor(&mut self, host: Host, description: ServerDescription,
                                  client: Client, top_arc: Arc<RwLock<TopologyDescription>>) {
        self.update_private(host, description, client, top_arc, false);
    }

    /// Updates the topology description based on an updated server description.
    pub fn update(&mut self, host: Host, description: ServerDescription,
                  client: Client, top_arc: Arc<RwLock<TopologyDescription>>) {
        self.update_private(host, description, client, top_arc, true);
    }

    fn update_private(&mut self, host: Host, description: ServerDescription,
                      client: Client, top_arc: Arc<RwLock<TopologyDescription>>, run_monitor: bool) {
        let stype = description.server_type;
        match self.topology_type {
            TopologyType::Unknown => {
                match stype {
                    ServerType::Standalone => self.update_unknown_with_standalone(host),
                    ServerType::Mongos => self.topology_type = TopologyType::Sharded,
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, client, top_arc, run_monitor),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_without_primary(host, description, client, top_arc, run_monitor),
                    _ => (),
                }
            },
            TopologyType::ReplicaSetNoPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => {
                        self.servers.remove(&host);
                        self.check_if_has_primary();
                    },
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, client, top_arc, run_monitor),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_without_primary(host, description, client, top_arc, run_monitor),
                    _ => self.check_if_has_primary(),
                }
            },
            TopologyType::ReplicaSetWithPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => {
                        self.servers.remove(&host);
                        self.check_if_has_primary();
                    },
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, client, top_arc, run_monitor),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_with_primary_from_member(host, description),
                    _ => self.check_if_has_primary(),
                }
            },
            TopologyType::Sharded => {
                match stype {
                    ServerType::Unknown | ServerType::Mongos => (),
                    _ => { self.servers.remove(&host); },
                }
            },
            TopologyType::Single => (),
        }
    }

    // Sets the correct replica set topology type.
    fn check_if_has_primary(&mut self) {
        for (_, server) in self.servers.iter() {
            let stype = server.description.read().unwrap().server_type;
            if stype == ServerType::RSPrimary {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                return;
            }
        }
        self.topology_type = TopologyType::ReplicaSetNoPrimary;
    }


    // Updates an unknown topology with a new standalone server description.
    fn update_unknown_with_standalone(&mut self, host: Host) {
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.servers.len() == 1 {
            self.topology_type = TopologyType::Single;
        } else {
            self.servers.remove(&host);
        }
    }

    // Updates a replica set topology with a new primary server description.
    fn update_rs_from_primary(&mut self, host: Host, description: ServerDescription,
                              client: Client, top_arc: Arc<RwLock<TopologyDescription>>, run_monitor: bool) {

        if !self.servers.contains_key(&host) {
            return;
        }

        if self.set_name.is_empty() {
            self.set_name = description.set_name.to_owned();
        } else if self.set_name != description.set_name {
            // Primary found, but it doesn't have the setName
            // provided by the user or previously discovered.
            self.servers.remove(&host);
            self.check_if_has_primary();
            return;
        }

        if description.election_id.is_some() {
            if self.max_election_id.is_some() &&
                self.max_election_id.as_ref().unwrap() > description.election_id.as_ref().unwrap() {
                    // Stale primary
                    if let Some(server) = self.servers.get(&host) {
                        {
                            let mut server_description = server.description.write().unwrap();
                            server_description.server_type = ServerType::Unknown;
                            server_description.set_name = String::new();
                            server_description.election_id = None;
                        }
                    }
                    self.check_if_has_primary();
                    return;
                } else {
                    self.max_election_id = description.election_id.clone();
                }
        }

        // Invalidate any old primaries
        for (top_host, server) in self.servers.iter() {
            if *top_host != host {
                let mut server_description = server.description.write().unwrap();
                if server_description.server_type == ServerType::RSPrimary {
                    server_description.server_type = ServerType::Unknown;
                    server_description.set_name = String::new();
                    server_description.election_id = None;
                }
            }
        }

        self.add_missing_hosts(&description, client, top_arc, run_monitor);

        // Remove hosts that are not reported by the primary.
        let mut hosts_to_remove = Vec::new();
        for (host, _) in self.servers.iter() {
            if !description.hosts.contains(&host) &&
                !description.passives.contains(&host) &&
                !description.arbiters.contains(&host) {
                    hosts_to_remove.push(host.clone());
                }
        }

        for host in hosts_to_remove {
            self.servers.remove(&host);
        }

        self.check_if_has_primary();
    }

    // Updates a replica set topology with a missing primary.
    fn update_rs_without_primary(&mut self, host: Host, description: ServerDescription,
                                 client: Client, top_arc: Arc<RwLock<TopologyDescription>>, run_monitor: bool) {

        self.topology_type = TopologyType::ReplicaSetNoPrimary;
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.set_name.is_empty() {
            self.set_name = description.set_name.to_owned();
        } else if self.set_name != description.set_name {
            self.servers.remove(&host);
            self.check_if_has_primary();
            return;
        }

        self.add_missing_hosts(&description, client, top_arc, run_monitor);

        if let Some(me) = description.me {
            if host != me {
                self.servers.remove(&host);
                self.check_if_has_primary();
            }
        }
    }

    // Updates a replica set topology with an updated member description.
    fn update_rs_with_primary_from_member(&mut self, host: Host, description: ServerDescription) {
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.set_name != description.set_name {
            self.servers.remove(&host);
            return;
        }

        if let Some(me) = description.me {
            if host != me {
                self.servers.remove(&host);
            }
        }

        self.check_if_has_primary();
    }

    // Begins monitoring hosts that are not currently being monitored.
    fn add_missing_hosts(&mut self, description: &ServerDescription, client: Client,
                         top_arc: Arc<RwLock<TopologyDescription>>, run_monitor: bool) {

        for host in description.hosts.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(client.clone(), host.clone(), top_arc.clone(), run_monitor);
                self.servers.insert(host.clone(), server);
            }
        }

        for host in description.passives.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(client.clone(), host.clone(), top_arc.clone(), run_monitor);
                self.servers.insert(host.clone(), server);
            }
        }

        for host in description.arbiters.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(client.clone(), host.clone(), top_arc.clone(), run_monitor);
                self.servers.insert(host.clone(), server);
            }
        }
    }
}

impl Topology {
    /// Returns a new topology with the given configuration and description.
    pub fn new(config: ConnectionString, description: Option<TopologyDescription>) -> Result<Topology> {

        let mut options = description.unwrap_or(TopologyDescription::new());

        if config.hosts.len() > 1 && options.topology_type == TopologyType::Single {
            return Err(ArgumentError(
                "TopologyType::Single cannot be used with multiple seeds.".to_owned()));
        }

        if let Some(ref config_opts) = config.options {
            if let Some(name) = config_opts.options.get("replicaSet") {
                options.set_name = name.to_owned();
                options.topology_type = TopologyType::ReplicaSetNoPrimary;
            }
        }

        if !options.set_name.is_empty() && options.topology_type != TopologyType::ReplicaSetNoPrimary {
            return Err(ArgumentError(
                "TopologyType must be ReplicaSetNoPrimary if set_name is provided.".to_owned()));
        }

        let top_description = Arc::new(RwLock::new(options));

        Ok(Topology {
            config: config,
            description: top_description,
        })
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self, read_preference: ReadPreference) -> Result<PooledStream> {
        let mut retry = 0;
        loop {
            let description = try!(self.description.read());
            let result = description.acquire_stream(read_preference.to_owned());
            match result {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    if retry == MAX_SERVER_RETRY {
                        return Err(err)
                    }
                    thread::sleep_ms(500);
                },
            }
            retry += 1;
        }
    }
}
