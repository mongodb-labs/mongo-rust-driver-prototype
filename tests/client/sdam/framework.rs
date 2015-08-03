use mongodb::Error::OperationError;
use mongodb::connstring;
use mongodb::topology::{Topology, TopologyDescription, TopologyType};
use mongodb::topology::monitor::IsMasterResult;

use json::sdam::reader::SuiteContainer;
use rustc_serialize::json::Json;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicIsize;

pub fn run_suite(file: &str, description: Option<TopologyDescription>) {
    let json = Json::from_file(file).unwrap();
    let suite = json.get_suite().unwrap();

    let dummy_req_id = Arc::new(AtomicIsize::new(0));
    let connection_string = connstring::parse(&suite.uri).unwrap();

    // For a standalone topology with multiple startup servers, the user
    // should pass in an unknown topology. For a base standalone topology,
    // the user should note that they expect a standalone by providing TopologyType::Single.
    let should_ignore_description = if let Some(ref inner) = description {
        inner.topology_type == TopologyType::Single && connection_string.hosts.len() != 1
    } else {
        false
    };

    let topology = if should_ignore_description {
        Topology::new(dummy_req_id.clone(), connection_string, None).unwrap()
    } else {
        Topology::new(dummy_req_id.clone(), connection_string, description).unwrap()
    };

    let top_description_arc = topology.description.clone();

    let mut servers = HashMap::new();

    let mut i = 0;
    for phase in suite.phases {
        println!("Running phase {}", i);
        i += 1;

        for (host, response) in phase.operation.data {
            {
                // Save each seen server to replicate monitors for servers
                // that have been removed from the topology.
                let topology_description = topology.description.read().unwrap();
                for (host, server) in topology_description.servers.iter() {
                    servers.insert(host.clone(), server.clone());
                }
            }

            let mut topology_description = topology.description.write().unwrap();

            if response.is_empty() {
                let server = servers.get(&host).expect("Host not found.");
                let mut server_description = server.description.write().unwrap();
                server_description.set_err(OperationError("Simulated network error.".to_owned()));
            } else {
                match IsMasterResult::new(response) {
                    Ok(ismaster) => {
                        let server = servers.get(&host).expect("Host not found.");
                        let mut server_description = server.description.write().unwrap();
                        server_description.update(ismaster)
                    },
                    Err(err) => panic!(err),
                }
            }

            let server_description = {
                let server = servers.get(&host).expect("Host not found.");
                server.description.read().unwrap().clone()
            };

            topology_description.update(host.clone(), server_description.clone(),
                                        dummy_req_id.clone(), top_description_arc.clone());
        }

        // Check server and topology descriptions.
        let topology_description = topology.description.read().unwrap();

        assert_eq!(phase.outcome.servers.len(), topology_description.servers.len());
        for (host, server) in phase.outcome.servers.iter() {
            match topology_description.servers.get(host) {
                Some(top_server) => {
                    let top_server_description = top_server.description.read().unwrap();
                    assert_eq!(server.set_name, top_server_description.set_name);
                    assert_eq!(server.stype, top_server_description.server_type);
                },
                None => panic!("Missing host in outcome."),
            }
        }

        assert_eq!(phase.outcome.set_name, topology_description.set_name);
        assert_eq!(phase.outcome.ttype, topology_description.topology_type);
    }
}
