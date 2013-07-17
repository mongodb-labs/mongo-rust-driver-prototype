% Module client

<div class='index'>

* [Struct `Client`](#struct-client) - User interfaces with `Client`, which processes user requests  and sends them through the connection.
* [Implementation ` for Client`](#implementation-for-client)

</div>

## Struct `Client`

~~~ {.rust}
pub struct Client {
    conn: ~cell::Cell<NodeConnection>,
    priv cur_requestId: ~cell::Cell<i32>,
}
~~~

User interfaces with `Client`, which processes user requests
and sends them through the connection.

All communication to server goes through `Client`, i.e. `DB`,
`Collection`, etc. all store their associated `Client`

## Implementation for `Client`

### Method `new`

~~~ {.rust}
fn new() -> Client
~~~

Creates a new Mongo client.

Currently can connect to single unreplicated, unsharded
server via `connect`.

#### Returns

empty `Client`

### Method `get_admin`

~~~ {.rust}
fn get_admin(@self) -> DB
~~~

### Method `get_dbs`

~~~ {.rust}
fn get_dbs(@self) -> Result<~[~str], MongoErr>
~~~

Returns vector of database names.

#### Returns

vector of database names on success, `MongoErr` on any failure

#### Failure Types

* errors propagated from `run_command`
* response from server not in expected form (must contain
     "databases" field whose value is array of docs containing
     "name" fields of `UString`s)

### Method `get_db`

~~~ {.rust}
fn get_db(@self, db: ~str) -> DB
~~~

Gets the specified `DB`.
Alternative to constructing the `DB` explicitly
(`DB::new(db, client)`).

#### Arguments

* `db` - name of `DB` to get

#### Returns

handle to specified database

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

### Method `get_collection`

~~~ {.rust}
fn get_collection(@self, db: ~str, coll: ~str) -> Collection
~~~

Gets the specified `Collection`.
Alternative to constructing the `Collection` explicitly
(`Collection::new(db, collection, client)`).

#### Arguments

* `db` - database from which to get collection
* `coll` - name of `Collection` to get

#### Returns

handle to specified collection

### Method `connect`

~~~ {.rust}
fn connect(&self, server_ip_str: ~str, server_port: uint) ->
 Result<(), MongoErr>
~~~

Connects to a single server.

#### Arguments

* `server_ip_str` - string containing IP address of server
* `server_port` - port to which to connect

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* already connected
* network

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

Disconnects from server.
Simultaneously empties connection cell.

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* network

### Method `_send_msg`

~~~ {.rust}
fn _send_msg(@self, msg: ~[u8], wc_pair: (&~str, Option<~[WRITE_CONCERN]>),
             auto_get_reply: bool) -> Result<Option<ServerMsg>, MongoErr>
~~~

Sends message on connection; if write, checks write concern,
and if query, picks up OP_REPLY.

#### Arguments

* `msg` - bytes to send
* `wc` - write concern (if applicable)
* `auto_get_reply` - whether `Client` should expect an `OP_REPLY`
                     from the server

#### Returns

if read operation, `OP_REPLY` on success, `MongoErr` on failure;
if write operation, `None` on no last error, `MongoErr` on last error
     or network error

### Method `send`

~~~ {.rust}
fn send(&self, bytes: ~[u8]) -> Result<(), MongoErr>
~~~

Sends on `Connection` affiliated with this `Client`.

#### Arguments

* `bytes` - bytes to send

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* not connected
* network

### Method `recv`

~~~ {.rust}
fn recv(&self) -> Result<~[u8], MongoErr>
~~~

Receives on `Connection` affiliated with this `Client`.

#### Returns

bytes received over connection on success, `MongoErr` on failure

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

