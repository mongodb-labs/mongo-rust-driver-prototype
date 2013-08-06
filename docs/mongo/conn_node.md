% Module conn_node

<div class='index'>

* [Struct `NodeConnection`](#struct-nodeconnection) - Connection between Client and Server
* [Implementation ` of Connection for NodeConnection`](#implementation-of-connection-for-nodeconnection)
* [Implementation ` of Eq for NodeConnection`](#implementation-of-eq-for-nodeconnection)
* [Implementation ` for NodeConnection`](#implementation-for-nodeconnection)
* [Implementation ` of Ord for NodeConnection`](#implementation-of-ord-for-nodeconnection) - Comparison of `NodeConnection`s based on their ping times.

</div>

## Struct `NodeConnection`

~~~ {.rust}
pub struct NodeConnection {
    priv server_ip_str: ~str,
    priv server_port: uint,
    priv server_ip: Cell<IpAddr>,
    priv sock: Cell<@Socket>,
    priv port: Cell<@GenericPort<PortResult>>,
    ping: Cell<u64>,
    tags: Cell<TagSet>,
    timeout: Cell<u64>,
}
~~~

Connection between Client and Server. Routes user requests to
server and hands back wrapped result for Client to parse.

## Implementation of `Connection` for `NodeConnection`

### Method `connect`

~~~ {.rust}
fn connect(&self) -> Result<(), MongoErr>
~~~

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

### Method `reconnect`

~~~ {.rust}
fn reconnect(&self) -> Result<(), MongoErr>
~~~

### Method `send`

~~~ {.rust}
fn send(&self, data: &[u8], _: bool) -> Result<(), MongoErr>
~~~

### Method `recv`

~~~ {.rust}
fn recv(&self, buf: &mut ~[u8], _: bool) -> Result<uint, MongoErr>
~~~

### Method `set_timeout`

~~~ {.rust}
fn set_timeout(&self, timeout: u64) -> u64
~~~

### Method `get_timeout`

~~~ {.rust}
fn get_timeout(&self) -> u64
~~~

## Implementation of `Eq` for `NodeConnection`

### Method `eq`

~~~ {.rust}
fn eq(&self, other: &NodeConnection) -> bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, other: &NodeConnection) -> bool
~~~

## Implementation for `NodeConnection`

### Method `new`

~~~ {.rust}
fn new(server_ip_str: &str, server_port: uint) -> NodeConnection
~~~

Create a new NodeConnection with given IP and port.

#### Arguments

`server_ip_str` - string representing IP of server
`server_port` - uint representing port on server

#### Returns

NodeConnection that can be connected to server node
indicated.

### Method `get_ip`

~~~ {.rust}
fn get_ip(&self) -> ~str
~~~

### Method `get_port`

~~~ {.rust}
fn get_port(&self) -> uint
~~~

### Method `is_master`

~~~ {.rust}
fn is_master(&self) -> Result<bool, MongoErr>
~~~

### Method `_check_master_and_do`

~~~ {.rust}
fn _check_master_and_do<T>(&self,
                           cb:
                               &fn(bson: &~BsonDocument)
                                   -> Result<T, MongoErr>) ->
 Result<T, MongoErr>
~~~

Run admin isMaster command and pass document into callback to process.
Helper function.

## Implementation of `Ord` for `NodeConnection`

Comparison of `NodeConnection`s based on their ping times.

### Method `lt`

~~~ {.rust}
fn lt(&self, other: &NodeConnection) -> bool
~~~

### Method `le`

~~~ {.rust}
fn le(&self, other: &NodeConnection) -> bool
~~~

### Method `gt`

~~~ {.rust}
fn gt(&self, other: &NodeConnection) -> bool
~~~

### Method `ge`

~~~ {.rust}
fn ge(&self, other: &NodeConnection) -> bool
~~~

