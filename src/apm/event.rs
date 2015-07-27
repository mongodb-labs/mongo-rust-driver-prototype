use std::fmt::{Display, Error, Formatter};

use bson::Document;
use error::Error as MongoError;

pub struct CommandStarted {
    pub command: Document,
    pub database_name: String,
    pub command_name: String,
    pub request_id: i64,
    pub connection_string: String,
}

impl Display for CommandStarted {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        fmt.write_fmt(format_args!("COMMAND.{} {} STARTED: {:?}", self.command_name,
                                   self.connection_string, self.command))
    }
}

pub enum CommandResult<'a> {
    Success {
        duration: i64,
        reply: Document,
        command_name: String,
        request_id: i64,
        connection_string: String,
    },
    Failure {
        duration: i64,
        command_name: String,
        failure: &'a MongoError,
        request_id: i64,
        connection_string: String,
    }
}

impl<'a> Display for CommandResult<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match self {
            &CommandResult::Success { duration, ref reply, ref command_name, request_id: _,
                                     ref connection_string } => {
                fmt.write_fmt(format_args!("COMMAND.{} {} COMPLETED: {:?} ({} ns)", command_name,
                                           connection_string, reply, duration))
            },
            &CommandResult::Failure { duration, ref command_name, ref failure, request_id: _,
                                      ref connection_string } => {
                fmt.write_fmt(format_args!("COMMAND.{} {} FAILURE: {} ({} NS)", command_name,
                                           connection_string, failure, duration))
            }
        }
    }
}
