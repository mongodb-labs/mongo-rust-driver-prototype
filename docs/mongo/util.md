% Module util

<div class='index'>

* [Freeze `LITTLE_ENDIAN_TRUE`](#freeze-little_endian_true) - Misc
* [Freeze `MONGO_DEFAULT_PORT`](#freeze-mongo_default_port)
* [Freeze `SYSTEM_COMMAND`](#freeze-system_command)
* [Freeze `SYSTEM_INDEX`](#freeze-system_index)
* [Freeze `SYSTEM_JS`](#freeze-system_js)
* [Freeze `SYSTEM_NAMESPACE`](#freeze-system_namespace) - INTERNAL UTILITIES  Special collections for database operations, but users should not  access directly.
* [Freeze `SYSTEM_PROFILE`](#freeze-system_profile)
* [Freeze `SYSTEM_USER`](#freeze-system_user)
* [Enum `COLLECTION_FLAG`](#enum-collection_flag) - Collections.
* [Enum `COLLECTION_OPTION`](#enum-collection_option)
* [Enum `DELETE_FLAG`](#enum-delete_flag)
* [Enum `DELETE_OPTION`](#enum-delete_option)
* [Enum `INDEX_FIELD`](#enum-index_field)
* [Enum `INDEX_FLAG`](#enum-index_flag)
* [Enum `INDEX_GEOTYPE`](#enum-index_geotype)
* [Enum `INDEX_OPTION`](#enum-index_option)
* [Enum `INDEX_ORDER`](#enum-index_order) - Indexing.
* [Enum `INSERT_FLAG`](#enum-insert_flag)
* [Enum `INSERT_OPTION`](#enum-insert_option)
* [Enum `QUERY_FLAG`](#enum-query_flag)
* [Enum `QUERY_OPTION`](#enum-query_option)
* [Enum `QuerySpec`](#enum-queryspec)
* [Enum `REPLY_FLAG`](#enum-reply_flag) - Reply flags, but user shouldn't deal with them directly.
* [Enum `UPDATE_FLAG`](#enum-update_flag) - CRUD option flags
* [Enum `UPDATE_OPTION`](#enum-update_option)
* [Enum `WRITE_CONCERN`](#enum-write_concern)
* [Struct `MongoErr`](#struct-mongoerr) - Utility module for use internal and external to crate
* [Implementation ` for MongoErr`](#implementation-for-mongoerr) - MongoErr to propagate errors; would be called Err except that's  taken by Rust...
* [Implementation ` of ToStr for MongoErr`](#implementation-of-tostr-for-mongoerr)

</div>

## Freeze `LITTLE_ENDIAN_TRUE`

~~~ {.rust}
bool
~~~

Misc

## Freeze `MONGO_DEFAULT_PORT`

~~~ {.rust}
uint
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
Special collections for database operations, but users should not
access directly.

## Freeze `SYSTEM_PROFILE`

~~~ {.rust}
&'static str
~~~

## Freeze `SYSTEM_USER`

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

## Enum `INDEX_FIELD`

#### Variants


* `NORMAL(~[(~str, INDEX_ORDER)])`

* `HASHED(~str)`

* `GEOSPATIAL(~str, INDEX_GEOTYPE)`

* `GEOHAYSTACK(~str, ~str, uint)`

## Enum `INDEX_FLAG`

#### Variants


* `BACKGROUND = 1 << 0`

* `UNIQUE = 1 << 1`

* `DROP_DUPS = 1 << 2`

* `SPARSE = 1 << 3`

## Enum `INDEX_GEOTYPE`

#### Variants


* `SPHERICAL`

* `FLAT`

## Enum `INDEX_OPTION`

#### Variants


* `INDEX_NAME(~str)`

* `EXPIRE_AFTER_SEC(int)`

* `VERS(int)`

## Enum `INDEX_ORDER`

Indexing.

#### Variants


* `ASC = 1`

* `DESC = -1`

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

## Implementation for `MongoErr`

MongoErr to propagate errors; would be called Err except that's
taken by Rust...

### Method `new`

~~~ {.rust}
fn new(typ: ~str, name: ~str, msg: ~str) -> MongoErr
~~~

Create a new MongoErr of given type (e.g. "connection", "query"),
name (more specific error), and msg (description of error).

## Implementation of `ToStr` for `MongoErr`

### Method `to_str`

~~~ {.rust}
fn to_str(&self) -> ~str
~~~

Print a MongoErr to string in a standard format.

