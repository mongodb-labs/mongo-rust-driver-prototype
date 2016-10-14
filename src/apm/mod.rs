//! Command Monitoring
//!
//! The APM module provides an intuitive interface for monitoring and responding to runtime
//! information about commands being executed on the server. All non-suppressed commands trigger
//! start and completion hooks defined on the client. Each non-suppressed command is also logged,
//! if a log file was specified during instantiation of the client.
pub mod client;
mod event;
mod listener;

pub use self::client::EventRunner;
pub use self::event::{CommandStarted, CommandResult};
pub use self::listener::Listener;
