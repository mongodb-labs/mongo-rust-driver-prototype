use bson::Document as BsonDocument;
use bson::Bson::I32;
use client::wire_protocol::flags::{Flags, OpQueryFlags};
use client::wire_protocol::operations::Message;
use std::io::{Read, Write};

pub enum Command {
    IsMaster,
}

pub enum AdminCommand {
    ListDatabases,
}

impl ToString for Command {
    fn to_string(&self) -> String {
        match self {
            &Command::IsMaster => "isMaster".to_owned()
        }
    }
}

impl ToString for AdminCommand {
    fn to_string(&self) -> String {
        match self {
            &AdminCommand::ListDatabases => "listDatabases".to_owned()
        }
    }
}

pub enum DatabaseCommand {
    Basic {
        database: String,
        request_id: i32,
        command_type: Command,
    },
    Admin {
        request_id: i32,
        command_type: AdminCommand,
    },
}

impl ToString for DatabaseCommand {
    fn to_string(&self) -> String {
        match self {
            &DatabaseCommand::Basic {
                database: _,
                request_id: _,
                command_type: ref ct
            } => ct.to_string(),
            &DatabaseCommand::Admin {
                request_id: _,
                command_type: ref ct,
            } => ct.to_string(),
        }
    }
}

impl DatabaseCommand {
    pub fn with_basic(database: String, request_id: i32,
                  command: Command) -> DatabaseCommand{
        DatabaseCommand::Basic { database: database, request_id: request_id,
                                 command_type: command }
    }

    pub fn with_admin(request_id: i32, command: AdminCommand) -> DatabaseCommand {
        DatabaseCommand::Admin { request_id: request_id, command_type: command }
    }

    // This function will eventually be merged into Client as methods, at which
    // time it will be rewritten to use the Client methods instead of directly
    // using the write protocol.
    fn run_agnostic<T: Read + Write>(database: &str, request_id: i32, command: &str,
           buffer: &mut T) -> Result<BsonDocument, String> {
        let flags = OpQueryFlags::no_flags();
        let full_collection_name = format!("{}.$cmd", database);
        let mut bson = BsonDocument::new();
        bson.insert(command.to_owned(), I32(1));
        let message_result = Message::with_query(request_id, flags,
                                                 full_collection_name, 0, 1,
                                                 bson, None);
        let message = match message_result {
            Ok(m) => m,
            Err(e) => {
                let s = format!("Unable to run command {}: {}", command, e);
                return Err(s)
            }
        };

        match message.write(buffer) {
            Ok(_) => (),
            Err(e) => {
                let s = format!("Unable to run command {}: {}", command, e);
                return Err(s)
            }
        };

        Message::read(buffer)
    }

    pub fn run<T: Read + Write>(&self, buffer: &mut T) -> Result<BsonDocument, String> {
        let (db, rid, cmd) = match self {
            &DatabaseCommand::Basic {
                database: ref d,
                request_id: rid,
                command_type: ref ct,
            } => (&d[..], rid, ct.to_string()),
            &DatabaseCommand::Admin {
                request_id: rid,
                command_type: ref ct,
            } => ("admin", rid, ct.to_string())
        };

        DatabaseCommand::run_agnostic(db, rid, &cmd, buffer)
    }
}
