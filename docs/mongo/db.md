% Module db

<div class='index'>

* [Struct `DB`](#struct-db)
* [Implementation ` for DB`](#implementation-for-db)

</div>

## Struct `DB`

~~~ {.rust}
pub struct DB {
    name: ~str,
    priv client: @Client,
}
~~~

## Implementation for `DB`

Having created a `Client` and connected as desired
to a server or cluster, users may interact with
databases by creating `DB` handles to those databases.

### Method `new`

~~~ {.rust}
fn new(name: ~str, client: @Client) -> DB
~~~

Creates a new Mongo DB with given name and associated Client.

#### Arguments

* `name` - name of DB
* `client` - Client with which this DB is associated

#### Returns

DB (handle to database)

### Method `get_collection_names`

~~~ {.rust}
fn get_collection_names(&self) -> Result<~[~str], MongoErr>
~~~

Gets names of all collections in this `DB`, returning error
if any fail. Names do not include `DB` name, i.e. are not
full namespaces.

#### Returns

vector of collection names on success, `MongoErr` on failure

#### Failure Types

* error querying `system.indexes` collection
* response from server not in expected form (must contain
     vector of `BsonDocument`s each containing "name" fields of
     `UString`s)

### Method `get_collections`

~~~ {.rust}
fn get_collections(&self) -> Result<~[Collection], MongoErr>
~~~

Gets `Collection`s in this `DB`, returning error if any fail.

#### Returns

vector of `Collection`s on success, `MongoErr` on failure

#### Failure Types

* errors propagated from `get_collection_names`

### Method `create_collection`

~~~ {.rust}
fn create_collection(&self, coll: ~str,
                     flag_array: Option<~[COLLECTION_FLAG]>,
                     option_array: Option<~[COLLECTION_OPTION]>) ->
 Result<Collection, MongoErr>
~~~

Creates collection with given options.

#### Arguments

* `coll` - name of collection to create
* `flag_array` - collection creation flags
* `option_array` - collection creation options

#### Returns

handle to collection on success, `MongoErr` on failure

### Method `get_collection`

~~~ {.rust}
fn get_collection(&self, coll: ~str) -> Collection
~~~

Gets handle to collection with given name, from this `DB`.

#### Arguments

* `coll` - name of `Collection` to get

#### Returns

handle to collection

### Method `drop_collection`

~~~ {.rust}
fn drop_collection(&self, coll: &str) -> Result<(), MongoErr>
~~~

Drops given collection from database associated with this `DB`.

#### Arguments

* `coll` - name of collection to drop

#### Returns

() on success, `MongoErr` on failure

### Method `run_command`

~~~ {.rust}
fn run_command(&self, cmd: QuerySpec) -> Result<~BsonDocument, MongoErr>
~~~

Runs given command (taken as `BsonDocument` or `~str`).

#### Arguments

* `cmd` - command to run, taken as `SpecObj(BsonDocument)` or
             `SpecNotation(~str)`

#### Returns

`~BsonDocument` response from server on success that must be parsed
appropriately by caller, `MongoErr` on failure

### Method `get_last_error`

~~~ {.rust}
fn get_last_error(&self, wc: Option<~[WRITE_CONCERN]>) -> Result<(), MongoErr>
~~~

Parses write concern into bytes and sends to server.

#### Arguments

* `wc` - write concern, i.e. getLastError specifications

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* invalid write concern specification (should never happen)
* network
* getLastError error, e.g. duplicate ```_id```s

### Method `enable_sharding`

~~~ {.rust}
fn enable_sharding(&self) -> Result<(), MongoErr>
~~~

Enable sharding on this database.

### Method `add_user`

~~~ {.rust}
fn add_user(&self, username: ~str, password: ~str, roles: ~[~str]) ->
 Result<(), MongoErr>
~~~

Add a new database user with the given username and password.
If the system.users collection becomes unavailable, this will fail.

### Method `authenticate`

~~~ {.rust}
fn authenticate(&self, username: ~str, password: ~str) -> Result<(), MongoErr>
~~~

Become authenticated as the given username with the given password.

### Method `logout`

~~~ {.rust}
fn logout(&self) -> Result<(), MongoErr>
~~~

Log out of the current user.
Closing a connection will also log out.

### Method `get_profiling_level`

~~~ {.rust}
fn get_profiling_level(&self) -> Result<(int, Option<int>), MongoErr>
~~~

Get the profiling level of the database.

### Method `set_profiling_level`

~~~ {.rust}
fn set_profiling_level(&self, level: int) -> Result<~BsonDocument, MongoErr>
~~~

Set the profiling level of the database.

