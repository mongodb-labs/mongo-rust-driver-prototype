use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use apm::event::{CommandStarted, CommandResult};
use Client;
use error::{Error, Result};

pub struct Listener {
    no_start_hooks: AtomicBool,
    no_completion_hooks: AtomicBool,
    start_hooks: RwLock<Vec<fn(Client, &CommandStarted)>>,
    completion_hooks: RwLock<Vec<fn(Client, &CommandResult)>>,
}

impl Listener {
    pub fn new() -> Listener {
        Listener { no_start_hooks: AtomicBool::new(true),
                   no_completion_hooks: AtomicBool::new(true),
                   start_hooks: RwLock::new(vec![]), completion_hooks: RwLock::new(vec![]) }
    }

    pub fn add_start_hook(&self, hook: fn(Client, &CommandStarted)) -> Result<()> {
        let mut guard = match self.start_hooks.write() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        self.no_start_hooks.store(false, Ordering::SeqCst);
        Ok(guard.deref_mut().push(hook))
    }

    pub fn add_completion_hook(&self, hook: fn(Client, &CommandResult)) -> Result<()> {
        let mut guard = match self.completion_hooks.write() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        self.no_completion_hooks.store(false, Ordering::SeqCst);
        Ok(guard.deref_mut().push(hook))
    }

    pub fn run_start_hooks(&self, client: Client, started: &CommandStarted) -> Result<()> {
        if self.no_start_hooks.load(Ordering::SeqCst) {
            return Ok(());
        }

        let guard = match self.start_hooks.read() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        for hook in guard.deref().iter() {
            hook(client.clone(), started);
        }

        Ok(())
    }

    pub fn run_completion_hooks(&self, client: Client, result: &CommandResult) -> Result<()> {
        if self.no_completion_hooks.load(Ordering::SeqCst) {
            return Ok(());
        }

        let guard = match self.completion_hooks.read() {
            Ok(guard) => guard,
            Err(_) => return Err(Error::PoisonLockError)
        };

        for hook in guard.deref().iter() {
            hook(client.clone(), result);
        }

        Ok(())
    }
}
