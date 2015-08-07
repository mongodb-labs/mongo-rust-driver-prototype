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
use std::i64;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread;
use time;

use self::server::{Server, ServerDescription, ServerType};

const DEFAULT_HEARTBEAT_FREQUENCY_MS: u32 = 10000;
const DEFAULT_LOCAL_THRESHOLD_MS: i64 = 15;
const DEFAULT_SERVER_SELECTION_TIMEOUT_MS: i64 = 30000;

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
    /// Known servers within the topology.
    pub servers: HashMap<Host, Server>,
    /// The server connection health check frequency.
    /// The default is 10 seconds.
    pub heartbeat_frequency_ms: u32,
    pub local_threshold_ms: i64,
    pub server_selection_timeout_ms: i64,
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
            server_selection_timeout_ms: DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
            local_threshold_ms: DEFAULT_LOCAL_THRESHOLD_MS,
            servers: HashMap::new(),
            max_election_id: None,
            compatible: true,
            compat_error: String::new(),
        }
    }

    fn get_nearest_from_vec(&self, servers: &mut Vec<Host>) -> Result<(PooledStream, ServerType)> {
        servers.sort_by(|a, b| {
            let mut a_rtt = i64::MAX;
            let mut b_rtt = i64::MAX;
            if let Some(server) = self.servers.get(a) {
                if let Ok(a_description) = server.description.read() {
                    a_rtt = a_description.round_trip_time.unwrap_or(i64::MAX);
                }
            }
            if let Some(server) = self.servers.get(b) {
                if let Ok(b_description) = server.description.read() {
                    b_rtt = b_description.round_trip_time.unwrap_or(i64::MAX);
                }
            }

            a_rtt.cmp(&b_rtt)
        });

        for host in servers.iter() {
            if let Some(server) = self.servers.get(host) {
                if let Ok(description) = server.description.read() {
                    if description.round_trip_time.is_none() {
                        break;
                    } else if let Ok(stream) = server.acquire_stream() {
                        return Ok((stream, description.server_type));
                    }
                }
            }
        }
        Err(OperationError("No servers available for the provided ReadPreference.".to_owned()))
    }

    fn get_rand_from_vec(&self, servers: &mut Vec<Host>) -> Result<(PooledStream, ServerType)> {
        while !servers.is_empty() {
            let len = servers.len();
            let index = thread_rng().gen_range(0, len);

            if let Some(server) = self.servers.get(servers.get(index).unwrap()) {
                if let Ok(stream) = server.acquire_stream() {
                    if let Ok(description) = server.description.read() {
                        return Ok((stream, description.server_type));
                    }
                }
            }
            servers.remove(index);
        }
        Err(OperationError("No servers available for the provided ReadPreference.".to_owned()))
    }

    /// Returns a server stream.
    pub fn acquire_stream(&self, read_preference: &ReadPreference) -> Result<(PooledStream, bool, bool)> {
        let (mut hosts, rand) = self.choose_hosts(&read_preference);

        if self.topology_type != TopologyType::Sharded && self.topology_type != TopologyType::Single {
            self.filter_hosts(&mut hosts, read_preference);
        }

        if hosts.is_empty() && read_preference.mode == ReadMode::SecondaryPreferred {
            let mut read_pref = read_preference.clone();
            read_pref.mode = ReadMode::PrimaryPreferred;
            return self.acquire_stream(&read_pref);
        }

        if hosts.is_empty() {
            for (_, server) in self.servers.iter() {
                server.request_update();
            }
        }

        self.filter_latency_hosts(&mut hosts);

        let (pooled_stream, server_type) = if rand {
            try!(self.get_rand_from_vec(&mut hosts))
        } else {
            try!(self.get_nearest_from_vec(&mut hosts))
        };

        let (slave_ok, send_read_pref) = match self.topology_type {
            TopologyType::Unknown => (false, false),
            TopologyType::Single => match server_type {
                ServerType::Mongos => {
                    match read_preference.mode {
                        ReadMode::Primary => (false, false),
                        ReadMode::Secondary => (true, true),
                        ReadMode::PrimaryPreferred => (true, true),
                        ReadMode::SecondaryPreferred => (true, !read_preference.tag_sets.is_empty()),
                        ReadMode::Nearest => (true, true),
                    }
                },
                _ => (true, false),
            },
            TopologyType::ReplicaSetWithPrimary | TopologyType::ReplicaSetNoPrimary => {
                match read_preference.mode {
                    ReadMode::Primary => (false, false),
                    _ => (true, false),
                }
            },
            TopologyType::Sharded => {
                match read_preference.mode {
                    ReadMode::Primary => (false, false),
                    ReadMode::Secondary => (true, true),
                    ReadMode::PrimaryPreferred => (true, true),
                    ReadMode::SecondaryPreferred => (true, !read_preference.tag_sets.is_empty()),
                    ReadMode::Nearest => (true, true),
                }
            }
        };

        Ok((pooled_stream, slave_ok, send_read_pref))
    }

    pub fn acquire_write_stream(&self) -> Result<PooledStream> {
        let (mut hosts, rand) = self.choose_write_hosts();
        // check no hosts

        if rand {
            Ok(try!(self.get_rand_from_vec(&mut hosts)).0)
        } else {
            Ok(try!(self.get_nearest_from_vec(&mut hosts)).0)
        }
    }

    pub fn filter_hosts(&self, hosts: &mut Vec<Host>, read_preference: &ReadPreference) {
        let mut tag_filter = None;

        if read_preference.tag_sets.is_empty() {
            return;
        }

        for tags in read_preference.tag_sets.iter() {
            for ref host in hosts.iter() {
                if let Some(server) = self.servers.get(host) {
                    let description = server.description.read().unwrap();

                    let mut valid = true;
                    for (key, ref val) in tags.iter() {
                        match description.tags.get(key) {
                            Some(ref v) => if val != v { valid = false; break },
                            None => { valid = false; break },
                        }
                    }

                    if valid {
                        tag_filter = Some(tags);
                        break;
                    }
                }
            }
            if tag_filter.is_some() {
                break;
            }
        }

        match tag_filter {
            None => {
                if self.topology_type == TopologyType::ReplicaSetWithPrimary &&
                    (read_preference.mode == ReadMode::Primary ||
                     read_preference.mode == ReadMode::PrimaryPreferred) {
                        hosts.retain(|host| {
                            if let Some(server) = self.servers.get(host) {
                                let description = server.description.read().unwrap();
                                description.server_type == ServerType::RSPrimary
                            } else {
                                false
                            }
                        });
                    } else {
                        hosts.clear();
                    }
            },
            Some(tag_filter) => {
                hosts.retain(|host| {
                    if let Some(server) = self.servers.get(host) {
                        let description = server.description.read().unwrap();
                        for (key, ref val) in tag_filter.iter() {
                            match description.tags.get(key) {
                                Some(ref v) => if val != v { return false; },
                                None => return false,
                            }
                        }
                        true
                    } else {
                        false
                    }
                });
            }
        }
    }

    pub fn filter_latency_hosts(&self, hosts: &mut Vec<Host>) {
        if hosts.len() <= 1 {
            return;
        }

        let shortest_rtt = hosts.iter().fold({
            let server = self.servers.get(hosts.get(0).unwrap()).unwrap();
            let description = server.description.read().unwrap();
            description.round_trip_time.unwrap_or(i64::MAX)
        }, |acc, host| {
            let server = self.servers.get(&host).unwrap();
            let description = server.description.read().unwrap();
            let item_rtt = description.round_trip_time.unwrap_or(i64::MAX);
            if acc < item_rtt {
                acc
            } else {
                item_rtt
            }
        });

        if shortest_rtt == i64::MAX {
            return;
        }

        let high_rtt = shortest_rtt + self.local_threshold_ms;
        hosts.retain(|host| {
            let server = self.servers.get(&host).unwrap();
            let description = server.description.read().unwrap();
            let rtt = description.round_trip_time.unwrap_or(i64::MAX);
            shortest_rtt <= rtt && rtt <= high_rtt
        });
    }

    pub fn choose_write_hosts(&self) -> (Vec<Host>, bool) {
        if self.servers.is_empty() {
            return (Vec::new(), true);
        }

        match self.topology_type {
            TopologyType::Unknown => (Vec::new(), true),
            TopologyType::Single => (self.servers.keys().map(|host| host.clone()).collect(), true),
            TopologyType::Sharded => (self.servers.keys().map(|host| host.clone()).collect(), false),
            _ => (self.servers.keys().filter_map(|host| {
                if let Some(server) = self.servers.get(host) {
                    if let Ok(description) = server.description.read() {
                        if description.server_type == ServerType::RSPrimary {
                            return Some(host.clone());
                        }
                    }
                }
                None
            }).collect(), true)
        }
    }

    // Returns suitable servers and whether to take a random element.
    pub fn choose_hosts(&self, read_preference: &ReadPreference) -> (Vec<Host>, bool) {
        if self.servers.is_empty() {
            return (Vec::new(), true);
        }

        match self.topology_type {
            TopologyType::Unknown => (Vec::new(), true),
            TopologyType::Single => (self.servers.keys().map(|host| host.clone()).collect(), true),
            TopologyType::Sharded => (self.servers.keys().map(|host| host.clone()).collect(), false),
            _ => {

                // Handle replica set server selection
                // Short circuit if nearest
                if read_preference.mode == ReadMode::Nearest {
                    return (self.servers.keys().map(|host| host.clone()).collect(), false);
                }

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
                    ReadMode::Primary => (primaries, true),
                    ReadMode::PrimaryPreferred => {
                        let servers = if !primaries.is_empty() { primaries } else { secondaries };
                        (servers, true)
                    },
                    ReadMode::Secondary => (secondaries, true),
                    ReadMode::SecondaryPreferred => {
                        let servers = if !secondaries.is_empty() { secondaries } else { primaries };
                        (servers, true)
                    },
                    ReadMode::Nearest => (self.servers.keys().map(|host| host.clone()).collect(), false),
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
    pub fn acquire_stream(&self, read_preference: ReadPreference) -> Result<(PooledStream, bool, bool)> {
        let time = time::get_time();
        let start_ms = time.sec * 1000 + (time.nsec as i64) / 1000000;
        loop {
            let description = try!(self.description.read());
            let result = description.acquire_stream(&read_preference);

            match result {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    let end_time = time::get_time();
                    let end_ms = end_time.sec * 1000 + (end_time.nsec as i64) / 1000000;
                    if end_ms - start_ms >= description.server_selection_timeout_ms {
                        return Err(err)
                    }
                    thread::sleep_ms(500);
                },
            }
        }
    }

    /// Returns a server stream.
    pub fn acquire_write_stream(&self) -> Result<PooledStream> {
        let time = time::get_time();
        let start_ms = time.sec * 1000 + (time.nsec as i64) / 1000000;
        loop {
            let description = try!(self.description.read());
            let result = description.acquire_write_stream();

            match result {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    let end_time = time::get_time();
                    let end_ms = end_time.sec * 1000 + (end_time.nsec as i64) / 1000000;
                    if end_ms - start_ms >= description.server_selection_timeout_ms {
                        return Err(err)
                    }
                    thread::sleep_ms(500);
                },
            }
        }
    }
}
