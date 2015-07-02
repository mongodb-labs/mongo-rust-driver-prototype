use client::Error::{ArgumentError, OperationError};
use client::Result;
use client::connstring::ConnectionString;

use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

pub static DEFAULT_POOL_SIZE: usize = 5;

/// Handles threaded connections to a MongoDB server.
#[derive(Clone)]
pub struct ConnectionPool {
    /// The connection configuration.
    pub config: ConnectionString,
    // The socket pool.
    inner: Arc<Mutex<Pool>>,
    // A condition variable used for threads waiting for the pool
    // to be repopulated with available connections.
    wait_lock: Arc<Condvar>,
}

struct Pool {
    /// The maximum number of concurrent connections allowed.
    pub size: usize,
    // The current number of open connections.
    pub len: Arc<AtomicUsize>,
    // The idle socket pool.
    sockets: Vec<TcpStream>,
}

/// Holds an available socket, with logic to return the socket
/// to the connection pool when dropped.
pub struct PooledStream {
    // This socket will always be Some until it is
    // returned to the pool using take().
    socket: Option<TcpStream>,
    pool: Arc<Mutex<Pool>>,
    wait_lock: Arc<Condvar>,
}

impl PooledStream {
    /// Returns a reference to the socket.
    pub fn get_socket<'a>(&'a self) -> &'a TcpStream {
        self.socket.as_ref().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        // Attempt to lock and return the socket to the pool,
        // or give up if the pool lock has been poisoned.
        if let Ok(mut locked) = self.pool.lock() {
            let len = locked.len.load(Ordering::SeqCst);
            if len < locked.size {
                locked.sockets.push(self.socket.take().unwrap());
                if len == 0 {
                    // Notify waiting threads that the pool has been repopulated.
                    self.wait_lock.notify_one();
                }
            } else {
                let _ = locked.len.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

impl ConnectionPool {

    /// Returns a connection pool with a default size.
    pub fn new(config: ConnectionString) -> ConnectionPool {
        ConnectionPool::with_size(config, DEFAULT_POOL_SIZE)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size(config: ConnectionString, size: usize) -> ConnectionPool {
        ConnectionPool {
            config: config,
            wait_lock: Arc::new(Condvar::new()),
            inner: Arc::new(Mutex::new(Pool {
                len: Arc::new(ATOMIC_USIZE_INIT),
                size: size,
                sockets: Vec::with_capacity(size),
            })),
        }
    }

    /// Sets the maximum number of open connections.
    pub fn set_size(&self, size: usize) -> Result<()> {
        if size < 1 {
            Err(ArgumentError("The connection pool size must be greater than zero.".to_owned()))
        } else {
            let mut locked = try!(self.inner.lock());
            locked.size = size;
            Ok(())
        }
    }
    
    /// Attempts to acquire a connected socket. If none are available and
    /// the pool has not reached its maximum size, a new socket will connect.
    /// Otherwise, the function will block until a socket is returned to the pool.
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        let mut locked = try!(self.inner.lock());
        if locked.size == 0 {
            return Err(OperationError("The connection pool does not allow connections; \
                                       increase the size of the pool.".to_owned()));
        }

        loop {
            // Acquire available existing socket
            if let Some(stream) = locked.sockets.pop() {
                return Ok(PooledStream {
                    socket: Some(stream),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                });
            }

            // Attempt to make a new connection
            let len = locked.len.load(Ordering::SeqCst);
            if len < locked.size {
                let socket = try!(self.connect());
                let _ = locked.len.fetch_add(1, Ordering::SeqCst);
                return Ok(PooledStream {
                    socket: Some(socket),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                });
            }

            // Release lock and wait for pool to be repopulated
            locked = try!(self.wait_lock.wait(locked));
        }
    }

    // Connects to a MongoDB server as defined by the initial configuration.
    fn connect(&self) -> Result<TcpStream> {
        let host_name = self.config.hosts[0].host_name.to_owned();
        let port = self.config.hosts[0].port;
        let stream = try!(TcpStream::connect((&host_name[..], port)));
        Ok(stream)
    }
}
