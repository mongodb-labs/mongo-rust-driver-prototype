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

use self::server::{Server, ServerDescription, ServerType};

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
#[derive(Clone)]
pub struct TopologyDescription {
    pub ttype: TopologyType,
    pub set_name: String,
    pub heartbeat_frequency_ms: u32,
    servers: HashMap<Host, Server>,
    max_election_id: Option<oid::ObjectId>,
    compatible: bool,
    compat_error: String,
}

/// Holds status and connection information about a server set.
#[derive(Clone)]
pub struct Topology {
    pub config: ConnectionString,
    pub description: Arc<RwLock<TopologyDescription>>,
}

impl TopologyDescription {
    /// Returns a default, unknown topology description.
    pub fn new() -> TopologyDescription {
        TopologyDescription {
            ttype: TopologyType::Unknown,
            set_name: String::new(),
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            servers: HashMap::new(),
            max_election_id: None,
            compatible: true,
            compat_error: String::new(),
        }
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        for (_, server) in self.servers.iter() {
            return server.acquire_stream()
        }
        Err(OperationError("No servers found in configuration.".to_owned()))
    }

    /// Updates the topology description based on an updated server description.
    pub fn update(&mut self, host: Host, description: ServerDescription,
                  req_id: Arc<AtomicIsize>, top_arc: Arc<RwLock<TopologyDescription>>) {

        let stype = description.stype;
        match self.ttype {
            TopologyType::Unknown => {
                match stype {
                    ServerType::Standalone => self.update_unknown_with_standalone(host),
                    ServerType::Mongos => self.ttype = TopologyType::Sharded,
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, req_id, top_arc),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_without_primary(host, description, req_id, top_arc),
                    _ => (),
                }
            },
            TopologyType::ReplicaSetNoPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => {
                        self.servers.remove(&host);
                        self.check_if_has_primary();
                    },
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, req_id, top_arc),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_with_primary_from_member(host, description),
                    _ => self.check_if_has_primary(),
                }
            },
            TopologyType::ReplicaSetWithPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => { self.servers.remove(&host); },
                    ServerType::RSPrimary => self.update_rs_from_primary(host, description, req_id, top_arc),
                    ServerType::RSSecondary |
                    ServerType::RSArbiter |
                    ServerType::RSOther => self.update_rs_without_primary(host, description, req_id, top_arc),
                    _ => (),
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
            let stype = server.description.read().unwrap().stype;
            if stype == ServerType::RSPrimary {
                self.ttype = TopologyType::ReplicaSetWithPrimary;
                return;
            }
        }
        self.ttype = TopologyType::ReplicaSetNoPrimary;
    }

    // Updates an unknown topology with a new standalone server description.
    fn update_unknown_with_standalone(&mut self, host: Host) {
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.servers.len() == 1 {
            self.ttype = TopologyType::Single;
        } else {
            self.servers.remove(&host);
        }
    }

    // Updates a replica set topology with a new primary server description.
    fn update_rs_from_primary(&mut self, host: Host, description: ServerDescription,
                              req_id: Arc<AtomicIsize>, top_arc: Arc<RwLock<TopologyDescription>>) {

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
                            server_description.stype = ServerType::Unknown;
                        }
                    }
                    self.check_if_has_primary();
                    return;
                }

            self.max_election_id = description.election_id.clone();
        }

        // Invalidate any old primaries
        for (top_host, server) in self.servers.iter() {
            if *top_host != host {
                let mut server_description = server.description.write().unwrap();
                if server_description.stype == ServerType::RSPrimary {
                    server_description.stype = ServerType::Unknown;
                }
            }
        }

        self.add_missing_hosts(&description, req_id, top_arc);

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
    }

    // Updates a replica set topology with a missing primary.
    fn update_rs_without_primary(&mut self, host: Host, description: ServerDescription,
                                 req_id: Arc<AtomicIsize>, top_arc: Arc<RwLock<TopologyDescription>>) {

        self.ttype = TopologyType::ReplicaSetNoPrimary;
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.set_name.is_empty() {
            self.set_name = description.set_name.to_owned();
        } else if self.set_name != description.set_name {
            self.servers.remove(&host);
            return;
        }

        self.add_missing_hosts(&description, req_id, top_arc);

        if host != description.me.unwrap() {
            self.servers.remove(&host);
            return;
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

        if host != description.me.unwrap() {
            self.servers.remove(&host);
        }

        self.check_if_has_primary();
    }

    // Begins monitoring hosts that are not currently being monitored.
    fn add_missing_hosts(&mut self, description: &ServerDescription, req_id: Arc<AtomicIsize>,
                         top_arc: Arc<RwLock<TopologyDescription>>) {

        for host in description.hosts.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(req_id.clone(), host.clone(), top_arc.clone());
                self.servers.insert(host.clone(), server);
            }
        }

        for host in description.passives.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(req_id.clone(), host.clone(), top_arc.clone());
                self.servers.insert(host.clone(), server);
            }
        }

        for host in description.arbiters.iter() {
            if !self.servers.contains_key(host) {
                let server = Server::new(req_id.clone(), host.clone(), top_arc.clone());
                self.servers.insert(host.clone(), server);
            }
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

        {
            let mut top = try!(top_description.write());
            for host in config.hosts.iter() {
                let server = Server::new(req_id.clone(), host.clone(), top_description.clone());
                top.servers.insert(host.clone(), server);
            }
        }

        Ok(Topology {
            config: config,
            description: top_description,
        })
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        let description = try!(self.description.read());
        description.acquire_stream()
    }
}
