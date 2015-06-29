use client::Result;
use client::connstring::ConnectionString;
use client::Error::OperationError;

use std::net::TcpStream;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

pub static DEFAULT_POOL_SIZE: usize = 5;

/// Handles threaded connections to a MongoDB server.
#[derive(Clone)]
pub struct ConnectionPool {
    pub config: ConnectionString,
    inner: Arc<Mutex<Pool>>,
    locks: Vec<Arc<Mutex<bool>>>,
    // First unpoisoned socket
    first_socket: Arc<AtomicUsize>,
}

struct Pool {
    /// The maximum number of concurrent connections allowed.
    pub size: usize,
    sockets: Vec<Arc<Mutex<TcpStream>>>,
}

/// Holds an available socket and its associated lock.
pub struct PooledStream<'a> {
    pub socket: Arc<Mutex<TcpStream>>,
    guard: MutexGuard<'a, bool>,
}

impl ConnectionPool {

    /// Returns a connection pool with a default size.
    pub fn new(config: ConnectionString) -> ConnectionPool {
        ConnectionPool::with_size(config, DEFAULT_POOL_SIZE)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size(config: ConnectionString, size: usize) -> ConnectionPool {
        let mut vec = Vec::with_capacity(size);
        for _ in 0..size {
            vec.push(Arc::new(Mutex::new(false)));
        }
        ConnectionPool {
            config: config,
            locks: vec,
            first_socket: Arc::new(ATOMIC_USIZE_INIT),
            inner: Arc::new(Mutex::new(Pool {
                size: size,
                sockets: Vec::with_capacity(size),
            })),
        }
    }

    /// Attempts to acquire a connected socket. If none are available and
    /// the pool has not reached its maximum size, a new socket will connect.
    /// Otherwise, the function will block until a socket is returned to the pool.
    pub fn acquire_stream<'a>(&'a self) -> Result<PooledStream<'a>> {

        {
            // Lock pool to prevent modifications during selection.
            let mut locked = try!(self.inner.lock());
            if locked.size == 0 {
                return Err(OperationError("Connection pool holds no sockets!".to_owned()));
            }

            let len = locked.sockets.len();

            // Acquire available existing socket
            for i in 0..len {
                let lock = self.locks.get(i).unwrap();
                if let Ok(guard) = lock.try_lock() {
                    return Ok(PooledStream {
                        socket: locked.sockets.get(i).unwrap().clone(),
                        guard: guard,
                    });
                }
            }

            // Make a new connection
            if len < locked.size {
                let socket = try!(self.connect());
                locked.sockets.push(Arc::new(Mutex::new((try!(self.connect())))));
                let lock = self.locks.get(len + 1).unwrap();
                let socket_guard = try!(lock.lock());
                return Ok(PooledStream {
                    socket: locked.sockets.get(len).unwrap().clone(),
                    guard: socket_guard,
                });
            }
        }

        // Wait for the first unpoisoned socket, but without holding the connection pool.
        let mut first = self.first_socket.deref().load(Ordering::SeqCst);
        let mut socket_guard;
        loop {
            match self.locks.get(first) {
                Some(lock) => match lock.lock() {
                    Ok(guard) => {
                        socket_guard = guard;
                        break;
                    },
                    Err(_) => {
                        first += 1;
                    }
                },
                None => return Err(OperationError("All pool sockets are poisoned.".to_owned())),
            }
        }

        self.first_socket.store(first, Ordering::SeqCst);
        let mut locked = try!(self.inner.lock());

        Ok(PooledStream {
            socket: locked.sockets.get(0).unwrap().clone(),
            guard: socket_guard,
        })
    }

    // Connects to a MongoDB server as defined by the initial configuration.
    fn connect(&self) -> Result<TcpStream> {
        let host_name = self.config.hosts[0].host_name.to_owned();
        let port = self.config.hosts[0].port;
        let stream = try!(TcpStream::connect((&host_name[..], port)));
        Ok(stream)
    }
}
