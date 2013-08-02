/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use extra::net::tcp::*;
use extra::future::*;

use util::*;

pub type PortResult = Result<~[u8], TcpErrData>;

/**
 * Trait for sockets used by Connection. Used as a traitobject.
 */
pub trait Socket {
    fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData>;
    fn read_stop(&self) -> Result<(), TcpErrData>;
    fn write_future(&self, raw_write_data: ~[u8]) -> Future<Result<(), TcpErrData>>;
}

impl Socket for TcpSocket {
    fn read_start(&self) -> Result<@Port<PortResult>, TcpErrData> {
        self.read_start()
    }
    fn read_stop(&self) -> Result<(), TcpErrData> {
        self.read_stop()
    }
    fn write_future(&self, raw_write_data: ~[u8]) -> Future<Result<(), TcpErrData>> {
        self.write_future(raw_write_data)
    }
}

/**
 * Connection interface all connectors use (ReplicaSetConnection,
 * ShardedClusterConnection, NodeConnection).
 */
pub trait Connection {
    /**
     * Connect from the server.
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    fn connect(&self) -> Result<(), MongoErr>;

    /**
     * Disconnect from the server.
     * Succeeds even if not originally connected.
     *
     * # Returns
     * () on success, MongoErr on failure
     */
    fn disconnect(&self) -> Result<(), MongoErr>;

    /**
     * Convenience function that wraps disconnection and connection.
     *
     * Really should have same implementation across connections.
     *
     * # Returns
     * () on success, MongoErr on failure (of connection)
     */
    fn reconnect(&self) -> Result<(), MongoErr>;

    /**
     * "Fire and forget" asynchronous write to server of given data.
     *
     * # Arguments
     * * `data` - bytes to send
     *
     * # Returns
     * () on success, MongoErr on failure
     *
     * # Failure Types
     * * uninitialized socket
     * * network
     */
    fn send(&self, data : &[u8], read : bool) -> Result<(), MongoErr>;

    /**
     * Pick up a response from the server.
     *
     * # Arguments
     * `buf` - buffer into which to place response
     * `read` - whether the operation was a read [or a write]
     *
     * # Returns
     * number of bytes received on success, MongoErr on failure
     *
     * # Failure Types
     * * uninitialized port
     * * network
     */
    fn recv(&self, buf : &mut ~[u8], read : bool) -> Result<uint, MongoErr>;

    /**
     * Sets timeout in seconds, returning former value.
     *
     * # Arguments
     * `timeout` - amount of time before a send command times out; default
     *              is MONGO_TIMEOUT_SECS
     */
    // XXX units
    fn set_timeout(&self, timeout : u64) -> u64;

    /**
     * Gets timeout in seconds.
     *
     * # Returns
     * timeout of connection send commands, in seconds
     */
    fn get_timeout(&self) -> u64;
}
