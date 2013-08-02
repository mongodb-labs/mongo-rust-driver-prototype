% Module conn_node

<div class='index'>

* [Struct `NodeConnection`](#struct-nodeconnection) - Connection between Client and Server
* [Implementation ` of Connection for NodeConnection`](#implementation-of-connection-for-nodeconnection)
* [Implementation ` for NodeConnection`](#implementation-for-nodeconnection)

</div>

## Struct `NodeConnection`

~~~ {.rust}
pub struct NodeConnection {
    priv server_ip_str: ~str,
    priv server_port: uint,
    priv server_ip: Cell<IpAddr>,
    priv sock: Cell<@Socket>,
    priv port: Cell<@GenericPort<PortResult>>,
}
~~~

Connection between Client and Server. Routes user requests to
server and hands back wrapped result for Client to parse.

## Implementation of `Connection` for `NodeConnection`

### Method `connect`

~~~ {.rust}
fn connect(&self) -> Result<(), MongoErr>
~~~

Actually connect to the server with the initialized fields.

#### Returns

() on success, MongoErr on failure

### Method `send`

~~~ {.rust}
fn send(&self, data: ~[u8], _: bool) -> Result<(), MongoErr>
~~~

"Fire and forget" asynchronous write to server of given data.

#### Arguments

* `data` - bytes to send

#### Returns

() on success, MongoErr on failure

#### Failure Types

* uninitialized socket
* network

### Method `recv`

~~~ {.rust}
fn recv(&self, _: bool) -> Result<~[u8], MongoErr>
~~~

Pick up a response from the server.

#### Returns

bytes received on success, MongoErr on failure

#### Failure Types

* uninitialized port
* network

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

Disconnect from the server.
Succeeds even if not originally connected.

#### Returns

() on success, MongoErr on failure

## Implementation for `NodeConnection`

### Method `new`

~~~ {.rust}
fn new(server_ip_str: ~str, server_port: uint) -> NodeConnection
~~~

Create a new NodeConnection with given IP and port.

#### Arguments

`server_ip_str` - string representing IP of server
`server_port` - uint representing port on server

#### Returns

NodeConnection that can be connected to server node
indicated.

