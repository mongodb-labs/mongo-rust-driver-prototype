use std::string::ToString;

use bson::Bson;

pub enum SingleDatabaseRole {
    Read,
    ReadWrite,
    DbAdmin,
    DbOwner,
    UserAdmin,
    ClusterAdmin,
    ClusterManager,
    ClusterMonitor,
    HostManager,
    Backup,
    Restore,
}

impl ToString for SingleDatabaseRole {
    fn to_string(&self) -> String {
        let string = match self {
            &SingleDatabaseRole::Read => "read",
            &SingleDatabaseRole::ReadWrite => "readWrite",
            &SingleDatabaseRole::DbAdmin => "dbAdmin",
            &SingleDatabaseRole::DbOwner => "dbOwner",
            &SingleDatabaseRole::UserAdmin => "userAdmin",
            &SingleDatabaseRole::ClusterAdmin => "clusterAdmin",
            &SingleDatabaseRole::ClusterManager => "clusterManager",
            &SingleDatabaseRole::ClusterMonitor => "clusterMonitor",
            &SingleDatabaseRole::HostManager => "hostManager",
            &SingleDatabaseRole::Backup => "backup",
            &SingleDatabaseRole::Restore => "restore",
        };

        string.to_owned()
    }
}

pub enum AllDatabaseRole {
    Read,
    ReadWrite,
    UserAdmin,
    DbAdmin,
}

impl ToString for AllDatabaseRole {
    fn to_string(&self) -> String {
        let string = match self {
            &AllDatabaseRole::Read => "read",
            &AllDatabaseRole::ReadWrite => "readWrite",
            &AllDatabaseRole::UserAdmin => "userAdmin",
            &AllDatabaseRole::DbAdmin => "dbAdmin",
        };

        string.to_owned()
    }
}

pub enum Role {
    All(AllDatabaseRole),
    Single {
        role: SingleDatabaseRole,
        db: String,
    },
}

impl Role {
    fn to_bson(&self) -> Bson {
        match self {
            &Role::All(ref role) => Bson::String(role.to_string()),
            &Role::Single { ref role, ref db } => Bson::Document(doc! {
                "role" => (Bson::String(role.to_string())),
                "db" => (Bson::String(db.to_owned()))
            })
        }
    }

    pub fn to_bson_array(vec: Vec<Role>) -> Bson {
        Bson::Array(vec.into_iter().map(|r| r.to_bson()).collect())
    }
}
