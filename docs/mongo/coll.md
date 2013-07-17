% Module coll

<div class='index'>

* [Enum `MongoIndex`](#enum-mongoindex)
* [Struct `Collection`](#struct-collection)
* [Implementation ` for MongoIndex`](#implementation-for-mongoindex)
* [Implementation ` for Collection`](#implementation-for-collection)

</div>

## Enum `MongoIndex`

#### Variants


* `MongoIndexName(~str)`

* `MongoIndexFields(~[INDEX_FIELD])`

## Struct `Collection`

~~~ {.rust}
pub struct Collection {
    db: ~str,
    name: ~str,
    priv client: @Client,
}
~~~

## Implementation for `MongoIndex`

### Method `get_name`

~~~ {.rust}
fn get_name(&self) -> ~str
~~~

From either `~str` or full specification of index, gets name.

#### Returns

name of index (string passed in if `MongoIndexName` passed),
default index name if `MongoIndexFields` passed)

## Implementation for `Collection`

Having created a `Client` and connected as desired
to a server or cluster, users may interact with
collections by creating `Collection` handles to those
collections.

### Method `new`

~~~ {.rust}
fn new(db: ~str, name: ~str, client: @Client) -> Collection
~~~

Creates a new handle to the given collection.
Alternative to `client.get_collection(db, collection)`.

#### Arguments

* `db` - name of database
* `coll` - name of collection to get
* `client` - name of client associated with `Collection`

#### Returns

handle to given collection

### Method `get_db`

~~~ {.rust}
fn get_db(&self) -> DB
~~~

Gets `DB` containing this `Collection`.

#### Returns

handle to database containing this `Collection`

### Method `to_capped`

~~~ {.rust}
fn to_capped(&self, options: ~[COLLECTION_OPTION]) -> Result<(), MongoErr>
~~~

Converts this collection to a capped collection.

#### Arguments

* `options` - array of options with which to create capped
                 collection

#### Returns

() on success, `MongoErr` on failure

### Method `insert`

~~~ {.rust}
fn insert<U: BsonFormattable>(&self, doc: U, wc: Option<~[WRITE_CONCERN]>) ->
 Result<(), MongoErr>
~~~

CRUD ops.

Different methods rather than enum of arguments
since complexity not decreased with enum (for
both users and developers), and CRUD oeprations
assumed reasonably stable.

Moreover, basic operations still do take enums
for flexibility; easy to wrap for syntactic sugar.
INSERT OPS
Inserts given document with given write concern into collection.

#### Arguments

* `doc`- `BsonFormattable` to input
* `wc` - write concern with which to insert (`None` for default of 1,
         `Some` for finer specification)

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* invalid document to insert
* network

### Method `insert_batch`

~~~ {.rust}
fn insert_batch<U: BsonFormattable>(&self, docs: ~[U],
                                    flag_array: Option<~[INSERT_FLAG]>,
                                    option_array: Option<~[INSERT_OPTION]>,
                                    wc: Option<~[WRITE_CONCERN]>) ->
 Result<(), MongoErr>
~~~

Inserts given batch of documents with given write concern and options
into collection.

#### Arguments

* `docs`- array of `BsonFormattable`s to input
* `flag_array` - `CONT_ON_ERR`
* `option_array` - [none yet]
* `wc` - write concern with which to insert (`None` for default of 1,
         `Some` for finer specification)

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* invalid document to insert (e.g. not proper format or
     duplicate `_id`)
* network

### Method `save`

~~~ {.rust}
fn save<U: BsonFormattable>(&self, doc: U, wc: Option<~[WRITE_CONCERN]>) ->
 Result<(), MongoErr>
~~~

### Method `update`

~~~ {.rust}
fn update(&self, query: QuerySpec, update_spec: QuerySpec,
          flag_array: Option<~[UPDATE_FLAG]>,
          option_array: Option<~[UPDATE_OPTION]>,
          wc: Option<~[WRITE_CONCERN]>) -> Result<(), MongoErr>
~~~

UPDATE OPS
Updates documents satisfying given query with given update
specification and write concern.

#### Arguments

* `query` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying documents to update
* `update_spec` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying update to documents
* `flag_array` - `UPSERT`, `MULTI`
* `option_array` - [nothing yet]
* `wc` - write concern with which to update documents

#### Returns

() on success, `MongoErr` on failure

#### Failure Types

* invalid query or update specification
* getLastError
* network

### Method `find`

~~~ {.rust}
fn find(&self, query: Option<QuerySpec>, proj: Option<QuerySpec>,
        flag_array: Option<~[QUERY_FLAG]>) -> Result<Cursor, MongoErr>
~~~

Returns Cursor over given projection from queried documents.

#### Arguments

* `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying documents to query
* `proj` -  optioal `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying projection from queried documents
* `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
                 `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
                 `PARTIAL`

#### Returns

initialized (unqueried) Cursor on success, `MongoErr` on failure

### Method `find_one`

~~~ {.rust}
fn find_one(&self, query: Option<QuerySpec>, proj: Option<QuerySpec>,
            flag_array: Option<~[QUERY_FLAG]>) ->
 Result<~BsonDocument, MongoErr>
~~~

Returns pointer to first Bson from queried documents.

#### Arguments

* `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying documents to query
* `proj` -  optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying projection from queried documents
* `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
                 `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
                 `PARTIAL`

#### Returns

~BsonDocument of first result on success, MongoErr on failure

### Method `remove`

~~~ {.rust}
fn remove(&self, query: Option<QuerySpec>, flag_array: Option<~[DELETE_FLAG]>,
          option_array: Option<~[DELETE_OPTION]>,
          wc: Option<~[WRITE_CONCERN]>) -> Result<(), MongoErr>
~~~

Removes specified documents from collection.

#### Arguments

* `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
             specifying documents to query
* `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
                 `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
                 `PARTIAL`
* `option_array` - [nothing yet]
* `wc` - write concern with which to perform remove

#### Returns

() on success, `MongoErr` on failure

### Method `create_index`

~~~ {.rust}
fn create_index(&self, index_arr: ~[INDEX_FIELD],
                flag_array: Option<~[INDEX_FLAG]>,
                option_array: Option<~[INDEX_OPTION]>) ->
 Result<MongoIndex, MongoErr>
~~~

INDICES (or "Indexes")
Creates index by specifying a vector of the different elements
that can form an index (e.g. (field,order) pairs, geographical
options, etc.)

#### Arguments

* `index_arr` - vector of index elements
                 (`NORMAL(vector of (field, order) pairs)`,
                 `HASHED(field)`,
                 `GEOSPATIAL(field, type)`,
                 `GEOHAYSTACK(loc, field, bucket)')
* `flag_array` - optional vector of index-creating flags:
                 `BACKGROUND`,
                 `UNIQUE`,
                 `DROP_DUPS`,
                 `SPARSE`
* `option_array` - optional vector of index-creating options:
                 `INDEX_NAME(name)`,
                 `EXPIRE_AFTER_SEC(nsecs)`,
                 `VERS(version no)`

#### Returns

name of index as `MongoIndexName` (in enum `MongoIndex`) on success,
`MongoErr` on failure

### Method `ensure_index`

~~~ {.rust}
fn ensure_index(&self, index_arr: ~[INDEX_FIELD],
                flag_array: Option<~[INDEX_FLAG]>,
                option_array: Option<~[INDEX_OPTION]>) ->
 Result<MongoIndex, MongoErr>
~~~

### Method `get_indexes`

~~~ {.rust}
fn get_indexes(&self) -> Result<~[~str], MongoErr>
~~~

### Method `drop_index`

~~~ {.rust}
fn drop_index(&self, index: MongoIndex) -> Result<(), MongoErr>
~~~

Drops specified index.

#### Arguments

* `index` - `MongoIndex` to drop specified either by explicit name
             or fields

#### Returns

() on success, `MongoErr` on failure

### Method `validate`

~~~ {.rust}
fn validate(&self, full: bool, scandata: bool) ->
 Result<~BsonDocument, MongoErr>
~~~

Validate a collection.

