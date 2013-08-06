% Module client

<div class='index'>

* [Struct `Client`](#struct-client) - User interfaces with `Client`, which processes user requests  and sends them through the connection.
* [Implementation ` for Client`](#implementation-for-client)

</div>

## Struct `Client`

~~~ {.rust}
pub struct Client {
    conn: Cell<@Connection>,
    timeout: u64,
    priv rs_conn: Cell<@ReplicaSetConnection>,
    priv cur_requestId: Cell<i32>,
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
server via `connect`, or to a replica set via `connect_to_rs`
(given a seed, if already initiated), or via `initiate_rs`
(given a configuration and single host, if not yet initiated).

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
fn drop_db(@self, db: &str) -> Result<(), MongoErr>
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

### Method `_connect_to_conn`

~~~ {.rust}
fn _connect_to_conn(&self, call: &str, conn: @Connection) ->
 Result<(), MongoErr>
~~~

### Method `connect`

~~~ {.rust}
fn connect(&self, server_ip_str: &str, server_port: uint) ->
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

### Method `connect_to_rs`

~~~ {.rust}
fn connect_to_rs(&self, seed: &[(~str, uint)]) -> Result<(), MongoErr>
~~~

Connect to replica set with specified seed list.

#### Arguments

`seed` - seed list (vector) of ip/port pairs

#### Returns

() on success, MongoErr on failure

### Method `initiate_rs`

~~~ {.rust}
fn initiate_rs(@self, via: (&str, uint), conf: RSConfig) ->
 Result<(), MongoErr>
~~~

Initiates given configuration specified as `RSConfig`, and connects
to the replica set.

#### Arguments

* `via` - host to connect to in order to initiate the set
* `conf` - configuration to initiate

#### Returns

() on success, MongoErr on failure

### Method `set_read_pref`

~~~ {.rust}
fn set_read_pref(&self, np: READ_PREFERENCE) ->
 Result<READ_PREFERENCE, MongoErr>
~~~

Sets read preference as specified, returning former preference.

#### Arguments

* `np` - new read preference

#### Returns

old read preference on success, MongoErr on failure

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

Disconnect from server.
Simultaneously empties connection cell.

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* network

### Method `reconnect`

~~~ {.rust}
fn reconnect(&self) -> Result<(), MongoErr>
~~~

### Method `_send_msg`

~~~ {.rust}
fn _send_msg(@self, msg: ~[u8], wc_pair: (~str, Option<~[WRITE_CONCERN]>),
             read: bool) -> Result<Option<ServerMsg>, MongoErr>
~~~

Sends message on connection; if write, checks write concern,
and if query, picks up OP_REPLY.

#### Arguments

* `msg` - bytes to send
* `wc` - write concern (if applicable)
* `read` - whether read operation; whether `Client` should
                     expect an `OP_REPLY` from the server

#### Returns

if read operation, `OP_REPLY` on success, `MongoErr` on failure;
if write operation, `None` on no last error, `MongoErr` on last error
     or network error

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

### Method `check_version`

~~~ {.rust}
fn check_version(@self, ver: ~str) -> Result<(), MongoErr>
~~~

