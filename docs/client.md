% Module client

<div class='index'>

* [Struct `Client`](#struct-client) - User interfaces with Client, which processes user requests  and sends them through the connection.
* [Implementation ` for Client`](#implementation-for-client)

</div>

## Struct `Client`

~~~ {.rust}
pub struct Client {
    conn: ~cell::Cell<NodeConnection>,
    db: ~cell::Cell<~str>,
    priv cur_requestId: ~cell::Cell<i32>,
}
~~~

User interfaces with Client, which processes user requests
and sends them through the connection.

All communication to server goes through Client, i.e. database,
collection, etc. all store their associated Client

## Implementation for `Client`

### Method `new`

~~~ {.rust}
fn new() -> Client
~~~

Create a new Mongo Client.

Currently can connect to single unreplicated, unsharded
server via `connect`.

#### Returns

empty Client

### Method `get_admin`

~~~ {.rust}
fn get_admin(@self) -> DB
~~~

### Method `use_db`

~~~ {.rust}
fn use_db(&self, db: ~str)
~~~

### Method `drop_db`

~~~ {.rust}
fn drop_db(@self, db: ~str) -> Result<(), MongoErr>
~~~

Drops the given database.

#### Arguments

* `db` - name of database to drop

#### Returns

() on success, MongoErr on failure

#### Failure Types

* anything propagated from run_command

### Method `connect`

~~~ {.rust}
fn connect(&self, server_ip_str: ~str, server_port: uint) ->
 Result<(), MongoErr>
~~~

Connect to a single server.

#### Arguments

* `server_ip_str` - string containing IP address of server
* `server_port` - port to which to connect

#### Returns

() on success, MongoErr on failure

#### Failure Types

* already connected
* network

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

Disconnect from server.
Simultaneously empties connection cell.

#### Returns

() on success, MongoErr on failure

#### Failure Types

* network

### Method `send`

~~~ {.rust}
fn send(&self, bytes: ~[u8]) -> Result<(), MongoErr>
~~~

Send on connection affiliated with this client.

#### Arguments

* `bytes` - bytes to send

#### Returns

() on success, MongoErr on failure

#### Failure Types

* not connected
* network

### Method `recv`

~~~ {.rust}
fn recv(&self) -> Result<~[u8], MongoErr>
~~~

Receive on connection affiliated with this client.

#### Returns

bytes received over connection on success, MongoErr on failure

#### Failure Types

* not connected
* network

### Method `get_requestId`

~~~ {.rust}
fn get_requestId(&self) -> i32
~~~

Returns first unused requestId.

### Method `inc_requestId`

~~~ {.rust}
fn inc_requestId(&self) -> i32
~~~

Increments first unused requestId and returns former value.

