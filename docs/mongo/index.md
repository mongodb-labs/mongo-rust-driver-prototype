% Module index

<div class='index'>

* [Enum `INDEX_FLAG`](#enum-index_flag)
* [Enum `INDEX_GEOTYPE`](#enum-index_geotype)
* [Enum `INDEX_OPTION`](#enum-index_option)
* [Enum `INDEX_ORDER`](#enum-index_order) - Indexing.
* [Enum `INDEX_TYPE`](#enum-index_type)
* [Enum `MongoIndexSpec`](#enum-mongoindexspec)
* [Struct `MongoIndex`](#struct-mongoindex)
* [Implementation ` of ::std::clone::Clone for MongoIndexSpec`](#implementation-of-stdcloneclone-for-mongoindexspec) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for MongoIndexSpec`](#implementation-of-stdcmpeq-for-mongoindexspec) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for MongoIndex`](#implementation-of-stdcloneclone-for-mongoindex) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for MongoIndex`](#implementation-of-stdcmpeq-for-mongoindex) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for INDEX_ORDER`](#implementation-of-stdcloneclone-for-index_order) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for INDEX_ORDER`](#implementation-of-stdcmpeq-for-index_order) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for INDEX_FLAG`](#implementation-of-stdcloneclone-for-index_flag) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for INDEX_FLAG`](#implementation-of-stdcmpeq-for-index_flag) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for INDEX_OPTION`](#implementation-of-stdcloneclone-for-index_option) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for INDEX_OPTION`](#implementation-of-stdcmpeq-for-index_option) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for INDEX_GEOTYPE`](#implementation-of-stdcloneclone-for-index_geotype) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for INDEX_GEOTYPE`](#implementation-of-stdcmpeq-for-index_geotype) - Automatically derived.
* [Implementation ` of ::std::clone::Clone for INDEX_TYPE`](#implementation-of-stdcloneclone-for-index_type) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for INDEX_TYPE`](#implementation-of-stdcmpeq-for-index_type) - Automatically derived.
* [Implementation ` of BsonFormattable for INDEX_TYPE`](#implementation-of-bsonformattable-for-index_type)
* [Implementation ` of BsonFormattable for MongoIndex`](#implementation-of-bsonformattable-for-mongoindex)
* [Implementation ` for MongoIndexSpec`](#implementation-for-mongoindexspec)

</div>

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

* `DEFAULT_LANG(~str)`

* `LANG_OVERRIDE(~str)`

## Enum `INDEX_ORDER`

Indexing.

#### Variants


* `ASC = 1`

* `DESC = -1`

## Enum `INDEX_TYPE`

#### Variants


* `NORMAL(~[(~str, INDEX_ORDER)])`

* `HASHED(~str)`

* `GEOSPATIAL(~str, INDEX_GEOTYPE)`

* `GEOHAYSTACK(~str, ~str, uint)`

## Enum `MongoIndexSpec`

#### Variants


* `MongoIndexName(~str)`

* `MongoIndexFields(~[INDEX_TYPE])`

* `MongoIndex(MongoIndex)`

## Struct `MongoIndex`

~~~ {.rust}
pub struct MongoIndex {
    version: int,
    keys: ~[INDEX_TYPE],
    ns: ~str,
    name: ~str,
    flags: Option<~[INDEX_FLAG]>,
    options: Option<~[INDEX_OPTION]>,
}
~~~

## Implementation of `::std::clone::Clone` for `MongoIndexSpec`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> MongoIndexSpec
~~~

## Implementation of `::std::cmp::Eq` for `MongoIndexSpec`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &MongoIndexSpec) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &MongoIndexSpec) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `MongoIndex`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> MongoIndex
~~~

## Implementation of `::std::cmp::Eq` for `MongoIndex`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &MongoIndex) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &MongoIndex) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `INDEX_ORDER`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> INDEX_ORDER
~~~

## Implementation of `::std::cmp::Eq` for `INDEX_ORDER`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &INDEX_ORDER) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &INDEX_ORDER) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `INDEX_FLAG`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> INDEX_FLAG
~~~

## Implementation of `::std::cmp::Eq` for `INDEX_FLAG`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &INDEX_FLAG) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &INDEX_FLAG) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `INDEX_OPTION`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> INDEX_OPTION
~~~

## Implementation of `::std::cmp::Eq` for `INDEX_OPTION`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &INDEX_OPTION) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &INDEX_OPTION) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `INDEX_GEOTYPE`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> INDEX_GEOTYPE
~~~

## Implementation of `::std::cmp::Eq` for `INDEX_GEOTYPE`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &INDEX_GEOTYPE) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &INDEX_GEOTYPE) -> ::bool
~~~

## Implementation of `::std::clone::Clone` for `INDEX_TYPE`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> INDEX_TYPE
~~~

## Implementation of `::std::cmp::Eq` for `INDEX_TYPE`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &INDEX_TYPE) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &INDEX_TYPE) -> ::bool
~~~

## Implementation of `BsonFormattable` for `INDEX_TYPE`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(_: &Document) -> Result<INDEX_TYPE, ~str>
~~~

## Implementation of `BsonFormattable` for `MongoIndex`

### Method `to_bson_t`

~~~ {.rust}
fn to_bson_t(&self) -> Document
~~~

### Method `from_bson_t`

~~~ {.rust}
fn from_bson_t(doc: &Document) -> Result<MongoIndex, ~str>
~~~

## Implementation for `MongoIndexSpec`

### Method `process_index_opts`

~~~ {.rust}
fn process_index_opts(flags: i32, options: Option<~[INDEX_OPTION]>) ->
 (Option<~str>, ~[~str])
~~~

### Method `process_index_fields`

~~~ {.rust}
fn process_index_fields(index_arr: ~[INDEX_TYPE], index_opts: &mut ~[~str],
                        get_name: bool) -> (~str, ~[~str])
~~~

### Method `get_name`

~~~ {.rust}
fn get_name(&self) -> ~str
~~~

From either `~str` or full specification of index, gets name.

#### Returns

name of index (string passed in if `MongoIndexName` passed,
default index name if `MongoIndexFields` passed, string as returned
from database if `MongoIndex` passed)

