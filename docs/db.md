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

### Method `new`

~~~ {.rust}
fn new(name: ~str, client: @Client) -> DB
~~~

Create a new Mongo DB with given name and associated Client.

#### Arguments

* `name` - name of DB
* `client` - Client with which this DB is associated

#### Returns

DB (handle to database)

### Method `get_collection_names`

~~~ {.rust}
fn get_collection_names(&self) -> Result<~[~str], MongoErr>
~~~

Get names of all collections in this db, returning error
if any fail. Names do not include db name.

#### Returns

vector of collection names on success, MongoErr on failure

### Method `get_collection`

~~~ {.rust}
fn get_collection(&self, coll: ~str) -> Collection
~~~

### Method `run_command`

~~~ {.rust}
fn run_command(&self, cmd: QuerySpec) -> Result<(), MongoErr>
~~~

