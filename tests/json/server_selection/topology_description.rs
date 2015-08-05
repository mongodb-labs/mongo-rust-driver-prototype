use mongodb::topology::TopologyType;
use super::server::Server;

pub struct TopologyDescription {
    pub servers: Vec<Server>,
    pub ttype: TopologyType,
}

impl TopologyDescription {
    pub fn new(servers: Vec<Server>, ttype: TopologyType) -> TopologyDescription {
        TopologyDescription {
            servers: servers,
            ttype: ttype,
        }
    }
}
