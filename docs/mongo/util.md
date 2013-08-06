% Module util

<div class='index'>

* [Freeze `LITTLE_ENDIAN_TRUE`](#freeze-little_endian_true) - Misc
* [Freeze `LOCALHOST`](#freeze-localhost)
* [Freeze `MONGO_DEFAULT_PORT`](#freeze-mongo_default_port)
* [Freeze `MONGO_RECONN_MSECS`](#freeze-mongo_reconn_msecs)
* [Freeze `MONGO_TIMEOUT_SECS`](#freeze-mongo_timeout_secs)
* [Freeze `SYSTEM_COMMAND`](#freeze-system_command)
* [Freeze `SYSTEM_INDEX`](#freeze-system_index)
* [Freeze `SYSTEM_JS`](#freeze-system_js)
* [Freeze `SYSTEM_NAMESPACE`](#freeze-system_namespace) - INTERNAL UTILITIES  Special collections for database operations, but generally, users should not  access directly.
* [Freeze `SYSTEM_PROFILE`](#freeze-system_profile)
* [Freeze `SYSTEM_REPLSET`](#freeze-system_replset)
* [Freeze `SYSTEM_USERS`](#freeze-system_users)
* [Enum `COLLECTION_FLAG`](#enum-collection_flag) - Collections.
* [Enum `COLLECTION_OPTION`](#enum-collection_option)
* [Enum `DELETE_FLAG`](#enum-delete_flag)
* [Enum `DELETE_OPTION`](#enum-delete_option)
* [Enum `INSERT_FLAG`](#enum-insert_flag)
* [Enum `INSERT_OPTION`](#enum-insert_option)
* [Enum `QUERY_FLAG`](#enum-query_flag)
* [Enum `QUERY_OPTION`](#enum-query_option)
* [Enum `QuerySpec`](#enum-queryspec)
* [Enum `READ_PREFERENCE`](#enum-read_preference)
* [Enum `REPLY_FLAG`](#enum-reply_flag) - Reply flags, but user shouldn't deal with them directly.
* [Enum `UPDATE_FLAG`](#enum-update_flag) - CRUD option flags
* [Enum `UPDATE_OPTION`](#enum-update_option)
* [Enum `WRITE_CONCERN`](#enum-write_concern)
* [Struct `MongoErr`](#struct-mongoerr) - Utility module for use internal and external to crate
* [Struct `TagSet`](#struct-tagset)
* [Implementation ` of ::std::clone::Clone for MongoErr`](#implementation-of-stdcloneclone-for-mongoerr) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for MongoErr`](#implementation-of-stdcmpeq-for-mongoerr) - Automatically derived.
* [Implementation ` for MongoErr`](#implementation-for-mongoerr) - MongoErr to propagate errors.
* [Implementation ` of ToStr for MongoErr`](#implementation-of-tostr-for-mongoerr)
* [Implementation ` of ToStr for QuerySpec`](#implementation-of-tostr-for-queryspec)
* [Implementation ` of ::std::cmp::Eq for TagSet`](#implementation-of-stdcmpeq-for-tagset) - Automatically derived.
* [Implementation ` of Clone for TagSet`](#implementation-of-clone-for-tagset)
* [Implementation ` of BsonFormattable for TagSet`](#implementation-of-bsonformattable-for-tagset)
* [Implementation ` for TagSet`](#implementation-for-tagset)
* [Implementation ` of ::std::clone::Clone for READ_PREFERENCE`](#implementation-of-stdcloneclone-for-read_preference) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for READ_PREFERENCE`](#implementation-of-stdcmpeq-for-read_preference) - Automatically derived.
* [Function `parse_host`](#function-parse_host)

</div>

## Freeze `LITTLE_ENDIAN_TRUE`

~~~ {.rust}
bool
~~~

Misc

## Freeze `LOCALHOST`

~~~ {.rust}
&'static str
~~~

## Freeze `MONGO_DEFAULT_PORT`

~~~ {.rust}
uint
~~~

## Freeze `MONGO_RECONN_MSECS`

~~~ {.rust}
u64
~~~

## Freeze `MONGO_TIMEOUT_SECS`

~~~ {.rust}
u64
~~~

## Freeze `SYSTEM_COMMAND`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_INDEX`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_JS`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_NAMESPACE`

~~~ {.rust}
&'static str
~~~

INTERNAL UTILITIES
Special collections for database operations, but generally, users should not
access directly.

## Freeze `SYSTEM_PROFILE`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_REPLSET`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_USERS`

~~~ {.rust}
&'static str
~~~

## Enum `COLLECTION_FLAG`

Collections.

#### Variants


* `AUTOINDEX_ID = 1 << 0`

## Enum `COLLECTION_OPTION`

#### Variants


* `CAPPED(uint)`

* `SIZE(uint)`

* `MAX_DOCS(uint)`

## Enum `DELETE_FLAG`

#### Variants


* `SINGLE_REMOVE = 1 << 0`

## Enum `DELETE_OPTION`

## Enum `INSERT_FLAG`

#### Variants


* `CONT_ON_ERR = 1 << 0`

## Enum `INSERT_OPTION`

## Enum `QUERY_FLAG`

#### Variants


* `CUR_TAILABLE = 1 << 1`

* `SLAVE_OK = 1 << 2`

* `OPLOG_REPLAY = 1 << 3`

* `NO_CUR_TIMEOUT = 1 << 4`

* `AWAIT_DATA = 1 << 5`

* `EXHAUST = 1 << 6`

* `PARTIAL = 1 << 7`

## Enum `QUERY_OPTION`

#### Variants


* `NSKIP(int)`

* `NRET(int)`

## Enum `QuerySpec`

#### Variants


* `SpecObj(BsonDocument)`

* `SpecNotation(~str)`

## Enum `READ_PREFERENCE`

#### Variants


* `PRIMARY_ONLY`

* `PRIMARY_PREF(Option<~[TagSet]>)`

* `SECONDARY_ONLY(Option<~[TagSet]>)`

* `SECONDARY_PREF(Option<~[TagSet]>)`

* `NEAREST(Option<~[TagSet]>)`

## Enum `REPLY_FLAG`

Reply flags, but user shouldn't deal with them directly.

#### Variants


* `CUR_NOT_FOUND = 1 << 0`

* `QUERY_FAIL = 1 << 1`

* `SHARD_CONFIG_STALE = 1 << 2`

* `AWAIT_CAPABLE = 1 << 3`

## Enum `UPDATE_FLAG`

CRUD option flags.
If options ever change, modify:
     util.rs: appropriate enums (_FLAGs or _OPTIONs)
     coll.rs: appropriate flag and option helper parser functions

#### Variants


* `UPSERT = 1 << 0`

* `MULTI = 1 << 1`

## Enum `UPDATE_OPTION`

## Enum `WRITE_CONCERN`

#### Variants


* `JOURNAL(bool)`

* `W_N(int)`

* `W_STR(~str)`

* `W_TAGSET(TagSet)`

* `WTIMEOUT(int)`

* `FSYNC(bool)`

## Struct `MongoErr`

~~~ {.rust}
pub struct MongoErr {
    err_type: ~str,
    err_name: ~str,
    err_msg: ~str,
}
~~~

Utility module for use internal and external to crate.
Users must access functionality for proper use of options, etc.

## Struct `TagSet`

~~~ {.rust}
pub struct TagSet {
    tags: TreeMap<~str, ~str>,
}
~~~

## Implementation of `::std::clone::Clone` for `MongoErr`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> MongoErr
~~~

## Implementation of `::std::cmp::Eq` for `MongoErr`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &MongoErr) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &MongoErr) -> ::bool
~~~

## Implementation for `MongoErr`

MongoErr to propagate errors.

### Method `new`

~~~ {.rust}
fn new(typ: ~str, name: ~str, msg: ~str) -> MongoErr
~~~

Creates a new MongoErr of given type (e.g. "connection", "query"),
name (more specific error), and msg (description of error).

### Method `tail`

~~~ {.rust}
fn tail(&self) -> ~str
~~~

Like to_str, but omits staring "ERR | ".

## Implementation of `ToStr` for `MongoErr`

### Method `to_str`

~~~ {.rust}
fn to_str(&self) -> ~str
~~~

Prints a MongoErr to string in a standard format.

## Implementation of `ToStr` for `QuerySpec`

### Method `to_str`

~~~ {.rust}
fn to_str(&self) -> ~str
~~~

## Implementation of `::std::cmp::Eq` for `TagSet`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &TagSet) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &TagSet) -> ::bool
~~~

## Implementation of `Clone` for `TagSet`

### Method `clone`

~~~ {.rust}
fn clone(&self) -> TagSet
~~~

## Implementation of `BsonFormattable` for `TagSet`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<TagSet, ~str>
~~~

## Implementation for `TagSet`

### Method `new`

~~~ {.rust}
fn new(tag_list: &[(&str, &str)]) -> TagSet
~~~

### Method `get_ref`

~~~ {.rust}
fn get_ref<'a>(&'a self, field: ~str) -> Option<&'a ~str>
~~~

### Method `get_mut_ref`

~~~ {.rust}
fn get_mut_ref<'a>(&'a mut self, field: ~str) -> Option<&'a mut ~str>
~~~

### Method `set`

~~~ {.rust}
fn set(&mut self, field: ~str, val: ~str)
~~~

Sets tag in TagSet, whether or not it existed previously.

### Method `matches`

~~~ {.rust}
fn matches(&self, other: &TagSet) -> bool
~~~

Returns if self matches the other TagSet,
i.e. if all of the other TagSet's tags are
in self's TagSet.

Usage: member.matches(tagset)

## Implementation of `::std::clone::Clone` for `READ_PREFERENCE`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> READ_PREFERENCE
~~~

## Implementation of `::std::cmp::Eq` for `READ_PREFERENCE`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &READ_PREFERENCE) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &READ_PREFERENCE) -> ::bool
~~~

## Function `parse_host`

~~~ {.rust}
fn parse_host(host_str: &~str) -> Result<(~str, uint), MongoErr>
~~~

