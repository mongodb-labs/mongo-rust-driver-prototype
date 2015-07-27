use std::ops::{Deref, DerefMut};
use std::sync::RwLock;

use apm::event::{CommandStarted, CommandResult};
use error::{Error, Result};

pub struct Listener {
    start_hooks: RwLock<Vec<fn(&CommandStarted)>>,
    completion_hooks: RwLock<Vec<fn(&CommandResult)>>,
}

impl Listener {
    pub fn new() -> Listener {
        Listener { start_hooks: RwLock::new(vec![]), completion_hooks: RwLock::new(vec![]) }
    }

    pub fn add_start_hook(&self, hook: fn(&CommandStarted)) -> Result<()> {
        let mut guard = match self.start_hooks.write() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        Ok(guard.deref_mut().push(hook))
    }

    pub fn add_completion_hook(&self, hook: fn(&CommandResult)) -> Result<()> {
        let mut guard = match self.completion_hooks.write() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        Ok(guard.deref_mut().push(hook))
    }

    pub fn run_start_hooks(&self, started: &CommandStarted) -> Result<()> {
        let guard = match self.start_hooks.read() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        for hook in guard.deref().iter() {
            hook(started);
        }

        Ok(())
    }

    pub fn run_completion_hooks(&self, result: &CommandResult) -> Result<()> {
        let guard = match self.completion_hooks.read() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        for hook in guard.deref().iter() {
            hook(result);
        }

        Ok(())
    }
}
