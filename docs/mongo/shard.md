% Module shard

<div class='index'>

* [Struct `MongosClient`](#struct-mongosclient) - A shard controller
* [Implementation ` for MongosClient`](#implementation-for-mongosclient)

</div>

## Struct `MongosClient`

~~~ {.rust}
pub struct MongosClient {
    mongos: @Client,
}
~~~

A shard controller. An instance of this
wraps a Client connection to a mongos instance.

## Implementation for `MongosClient`

### Method `new`

~~~ {.rust}
fn new(client: @Client) -> MongosClient
~~~

Create a new MongosClient.
Will fail if the given Client is not connected
to a mongos instance.

### Method `enable_sharding`

~~~ {.rust}
fn enable_sharding(&self, db: ~str) -> Result<(), MongoErr>
~~~

Enable sharding on the specified database.
The database must exist or this operation will fail.

### Method `list_shards`

~~~ {.rust}
fn list_shards(&self) -> Result<~BsonDocument, MongoErr>
~~~

Return a list of all shards on the current cluster.

### Method `add_shard`

~~~ {.rust}
fn add_shard(&self, hostname: ~str) -> Result<(), MongoErr>
~~~

Allow this shard controller to manage a new shard.
Hostname can be in a variety of formats:
* <hostname>
* <hostname>:<port>
* <replset>/<hostname>
* <replset>/<hostname>:port

### Method `remove_shard`

~~~ {.rust}
fn remove_shard(&self, shardname: ~str) -> Result<~BsonDocument, MongoErr>
~~~

Begins removing a shard from this cluster.
If called while a shard is being removed, will instead return
a document describing the current removal status.

### Method `shard_collection`

~~~ {.rust}
fn shard_collection(&self, db: ~str, coll: ~str, key: QuerySpec, unique: bool)
 -> Result<(), MongoErr>
~~~

Enable sharding on the specified collection.

### Method `status`

~~~ {.rust}
fn status(&self) -> Result<~str, MongoErr>
~~~

Display the status of the current cluster.
Equivalent to running sh.status() in shell.

### Method `add_shard_tag`

~~~ {.rust}
fn add_shard_tag(&self, shard: ~str, tag: ~str) -> Result<(), MongoErr>
~~~

Add a tag to the given shard.
Requires MongoDB 2.2 or higher.

### Method `remove_shard_tag`

~~~ {.rust}
fn remove_shard_tag(&self, shard: ~str, tag: ~str) -> Result<(), MongoErr>
~~~

Remove a tag from the given shard.
Requires MongoDB 2.2 or higher.

