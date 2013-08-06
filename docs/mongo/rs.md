% Module rs

<div class='index'>

* [Enum `RS_MEMBER_OPTION`](#enum-rs_member_option)
* [Enum `RS_OPTION`](#enum-rs_option) - Replica set options.
* [Struct `RS`](#struct-rs)
* [Struct `RSConfig`](#struct-rsconfig)
* [Struct `RSMember`](#struct-rsmember)
* [Implementation ` of ::std::clone::Clone for RSMember`](#implementation-of-stdcloneclone-for-rsmember) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for RSMember`](#implementation-of-stdcmpeq-for-rsmember) - Automatically derived.
* [Implementation ` of BsonFormattable for RSMember`](#implementation-of-bsonformattable-for-rsmember)
* [Implementation ` for RSMember`](#implementation-for-rsmember)
* [Implementation ` of ::std::clone::Clone for RSConfig`](#implementation-of-stdcloneclone-for-rsconfig) - Automatically derived.
* [Implementation ` of BsonFormattable for RSConfig`](#implementation-of-bsonformattable-for-rsconfig)
* [Implementation ` for RSConfig`](#implementation-for-rsconfig)
* [Implementation ` of ::std::clone::Clone for RS_OPTION`](#implementation-of-stdcloneclone-for-rs_option) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for RS_OPTION`](#implementation-of-stdcmpeq-for-rs_option) - Automatically derived.
* [Implementation ` of BsonFormattable for RS_OPTION`](#implementation-of-bsonformattable-for-rs_option)
* [Implementation ` of ::std::clone::Clone for RS_MEMBER_OPTION`](#implementation-of-stdcloneclone-for-rs_member_option) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for RS_MEMBER_OPTION`](#implementation-of-stdcmpeq-for-rs_member_option) - Automatically derived.
* [Implementation ` of BsonFormattable for RS_MEMBER_OPTION`](#implementation-of-bsonformattable-for-rs_member_option)
* [Implementation ` for RS`](#implementation-for-rs) - Handle to replica set itself for functionality pertaining to  replica set-related characteristics, e

</div>

## Enum `RS_MEMBER_OPTION`

#### Variants


* `ARB_ONLY(bool)`

* `BUILD_INDS(bool)`

* `HIDDEN(bool)`

* `PRIORITY(float)`

* `TAGS(TagSet)`

* `SLAVE_DELAY(int)`

* `VOTES(int)`

## Enum `RS_OPTION`

Replica set options.

#### Variants


* `CHAINING_ALLOWED(bool)`

## Struct `RS`

~~~ {.rust}
pub struct RS {
    priv client: @Client,
}
~~~

## Struct `RSConfig`

~~~ {.rust}
pub struct RSConfig {
    _id: Option<~str>,
    priv version: Cell<i32>,
    members: ~[RSMember],
    settings: Option<~[RS_OPTION]>,
}
~~~

## Struct `RSMember`

~~~ {.rust}
pub struct RSMember {
    priv _id: Cell<uint>,
    host: ~str,
    opts: ~[RS_MEMBER_OPTION],
}
~~~

## Implementation of `::std::clone::Clone` for `RSMember`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> RSMember
~~~

## Implementation of `::std::cmp::Eq` for `RSMember`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &RSMember) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &RSMember) -> ::bool
~~~

## Implementation of `BsonFormattable` for `RSMember`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<RSMember, ~str>
~~~

## Implementation for `RSMember`

### Method `new`

~~~ {.rust}
fn new(host: ~str, opts: ~[RS_MEMBER_OPTION]) -> RSMember
~~~

### Method `get_tags`

~~~ {.rust}
fn get_tags<'a>(&'a self) -> Option<&'a TagSet>
~~~

Gets read-only reference to tags.

#### Returns

None if there are no tags set, Some(ptr) to the tags if there are

### Method `get_mut_tags`

~~~ {.rust}
fn get_mut_tags<'a>(&'a mut self) -> &'a mut TagSet
~~~

Gets writeable reference to tags, initializing with default
(empty) if there were previously none set. Intended for user
manipulation.

#### Returns

reference to tags, possibly initializing them within the `RSMember`

### Method `get_priority`

~~~ {.rust}
fn get_priority<'a>(&'a self) -> Option<&'a float>
~~~

Gets read-only reference to priority.

#### Returns

None if there is no priority set, Some(ptr) to the priority if there is

### Method `get_mut_priority`

~~~ {.rust}
fn get_mut_priority<'a>(&'a mut self) -> &'a mut float
~~~

Gets writeable reference to priority, initializing with default
(1) if there was previously none set. Intended for user
manipulation.

#### Returns

reference to priority, possibly initializing them within the `RSMember`

## Implementation of `::std::clone::Clone` for `RSConfig`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> RSConfig
~~~

## Implementation of `BsonFormattable` for `RSConfig`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<RSConfig, ~str>
~~~

## Implementation for `RSConfig`

### Method `new`

~~~ {.rust}
fn new(_id: Option<~str>, members: ~[RSMember],
       settings: Option<~[RS_OPTION]>) -> RSConfig
~~~

### Method `get_version`

~~~ {.rust}
fn get_version(&self) -> Option<i32>
~~~

## Implementation of `::std::clone::Clone` for `RS_OPTION`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> RS_OPTION
~~~

## Implementation of `::std::cmp::Eq` for `RS_OPTION`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &RS_OPTION) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &RS_OPTION) -> ::bool
~~~

## Implementation of `BsonFormattable` for `RS_OPTION`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<RS_OPTION, ~str>
~~~

## Implementation of `::std::clone::Clone` for `RS_MEMBER_OPTION`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> RS_MEMBER_OPTION
~~~

## Implementation of `::std::cmp::Eq` for `RS_MEMBER_OPTION`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &RS_MEMBER_OPTION) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &RS_MEMBER_OPTION) -> ::bool
~~~

## Implementation of `BsonFormattable` for `RS_MEMBER_OPTION`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<RS_MEMBER_OPTION, ~str>
~~~

## Implementation for `RS`

Handle to replica set itself for functionality pertaining to
replica set-related characteristics, e.g. configuration.

For functionality handling how the replica set is to be interacted
with, e.g. setting read preference, etc. go through the client.

### Method `new`

~~~ {.rust}
fn new(client: @Client) -> RS
~~~

### Method `get_config`

~~~ {.rust}
fn get_config(&self) -> Result<RSConfig, MongoErr>
~~~

Gets configuration of replica set referred to by this handle.

#### Returns

RSConfig struct on success, MongoErr on failure

### Method `add`

~~~ {.rust}
fn add(&self, host: RSMember) -> Result<(), MongoErr>
~~~

Adds specified host to replica set; specify options directly
within host struct.

#### Arguments

* `host` - host, with options, to add to replica set

#### Returns

() on success, MongoErr on failure

### Method `remove`

~~~ {.rust}
fn remove(&self, host: ~str) -> Result<(), MongoErr>
~~~

Removes specified host from replica set.

#### Arguments

* `host` - host (as string) to remove

#### Returns

() on success, MongoErr on failure

### Method `get_status`

~~~ {.rust}
fn get_status(&self) -> Result<~BsonDocument, MongoErr>
~~~

Gets status of replica set.

#### Returns

~BsonDocument containing status information, MongoErr on failure

### Method `reconfig`

~~~ {.rust}
fn reconfig(&self, conf: RSConfig, force: bool) -> Result<(), MongoErr>
~~~

Reconfigure replica set to have given configuration.

#### Arguments

* `conf` - new configuration for replica set
* `force` - whether or not to force the reconfiguration
             WARNING: use with caution; may lead to rollback and
             other difficult-to-recover-from situations

#### Returns

() on success, MongoErr on failure

### Method `node_freeze`

~~~ {.rust}
fn node_freeze(&self, host: ~str, sec: uint) -> Result<(), MongoErr>
~~~

Prevent specified node from seeking election for
specified number of seconds.

### Method `node_unfreeze`

~~~ {.rust}
fn node_unfreeze(&self, host: ~str) -> Result<(), MongoErr>
~~~

### Method `step_down`

~~~ {.rust}
fn step_down(&self, sec: uint) -> Result<(), MongoErr>
~~~

Forces current primary to step down for specified number of seconds.

#### Arguments

* `sec` - number of seconds for current primary to step down

#### Returns

() on success, MongoErr on failure

### Method `node_sync_from`

~~~ {.rust}
fn node_sync_from(&self, node: ~str, from: ~str) -> Result<(), MongoErr>
~~~

Sync given node from another node.

#### Arguments

`node` - node to sync
`from` - node from which to sync

#### Return

() on success, MongoErr on failure

