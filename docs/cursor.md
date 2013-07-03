% Module cursor

<div class='index'>

* [Struct `Cursor`](#struct-cursor) - Structure representing a cursor
* [Implementation ` of Iterator<~BsonDocument> for Cursor`](#implementation-of-iteratorbsondocument-for-cursor) - Iterator implementation, opens access to powerful functions like collect, advance, map, etc.
* [Implementation ` for Cursor`](#implementation-for-cursor) - Cursor API

</div>

## Struct `Cursor`

~~~ {.rust}
pub struct Cursor {
    priv id: Option<i64>,
    priv collection: @Collection,
    flags: i32,
    batch_size: i32,
    query_spec: BsonDocument,
    open: bool,
    iter_err: Option<MongoErr>,
    priv retrieved: i32,
    priv proj_spec: Option<BsonDocument>,
    priv skip: i32,
    priv limit: i32,
    priv data: ~[~BsonDocument],
    priv i: i32,
}
~~~

Structure representing a cursor

## Implementation of `Iterator<~BsonDocument>` for `Cursor`

Iterator implementation, opens access to powerful functions like collect, advance, map, etc.

### Method `next`

~~~ {.rust}
fn next(&mut self) -> Option<~BsonDocument>
~~~

Returns pointer to next BsonDocument.

Pointers passed to avoid excessive copying. Any errors
are stored in Cursor's iter_err field.

#### Returns

`Some(~BsonDocument)` if there are more BsonDocuments,
`None` otherwise

## Implementation for `Cursor`

Cursor API

### Method `new`

~~~ {.rust}
fn new(query: BsonDocument, proj: Option<BsonDocument>,
       collection: @Collection, flags: i32) -> Cursor
~~~

Initialize cursor with query, projection, collection, flags,
and skip and limit, but don't query yet (i.e. constructed
cursors are empty).

#### Arguments

* `query` - query associated with this Cursor
* `proj` - projection of query associated with this Cursor
* `collection` - collection associated with this Cursor
* `flags` -  `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
             `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
             `PARTIAL`

#### Returns

Cursor

### Method `skip`

~~~ {.rust}
fn skip(&mut self, skip: i32) -> Result<(), MongoErr>
~~~

CURSOR OPTIONS (must be specified pre-querying)
Skip specified amount before starting to iterate.

#### Arguments

* `skip` - amount to skip

#### Returns

() on success, MongoErr on failure

#### Failure Types

* Cursor already iterated over

### Method `limit`

~~~ {.rust}
fn limit(&mut self, limit: i32) -> Result<(), MongoErr>
~~~

Limit amount to return from Cursor.

#### Arguments

* `limit` - total amount to return

#### Returns

() on success, MongoErr on failure

#### Failure Types

* Cursor already iterated over

### Method `explain`

~~~ {.rust}
fn explain(&mut self) -> Result<~BsonDocument, MongoErr>
~~~

QUERY MODIFICATIONS
Explain the query.
Copies the cursor and runs the query to gather information.

#### Returns

~BsonDocument explaining query on success, MongoErr on failure

### Method `hint`

~~~ {.rust}
fn hint(&mut self, index: MongoIndex)
~~~

Hints an index (name or fields+order) to use while querying.

#### Arguments

* `index` -  `MongoIndexName(name)` of index to use (if named),
             `MongoIndexFields(~[INDEX_FIELD])` to fully specify
                 index from scratch

### Method `sort`

~~~ {.rust}
fn sort(&mut self, orderby: INDEX_FIELD) -> Result<(), MongoErr>
~~~

Sorts results from cursor given fields and their direction.

#### Arguments

* `orderby` - `NORMAL(~[(field, direction)])` where `field`s are
                 `~str` and `direction` are `ASC` or `DESC`

#### Returns

() on success, MongoErr on failure

#### Failure Types

* invalid sorting specification (`orderby`)

### Method `add_flags`

~~~ {.rust}
fn add_flags(&mut self, flags: ~[QUERY_FLAG])
~~~

Adds flags to Cursor.

#### Arguments

* `flags` - array of `QUERY_FLAGS` (specified above), each
             of which to add

### Method `remove_flags`

~~~ {.rust}
fn remove_flags(&mut self, flags: ~[QUERY_FLAG])
~~~

Removes flags from Cursor.

#### Arguments

* `flags` - array of `QUERY_FLAGS` (specified above), each
             of which to remove

### Method `batch_size`

~~~ {.rust}
fn batch_size(&mut self, sz: i32)
~~~

Modify size of next batch to fetch on Cursor refresh.

#### Arguments

* `sz` - size of next batch to fetch on Cursor refresh (`QUERY`
         or `GET_MORE`)

### Method `has_next`

~~~ {.rust}
fn has_next(&self) -> bool
~~~

OTHER USEFUL FUNCTIONS
 Returns whether Cursor has a next `~BsonDocument`.
 Considers the last element of a Cursor to be `None`, hence
 returns `true` at edge case when Cursor exhausted naturally.
self.data.is_empty()

### Method `close`

~~~ {.rust}
fn close(&mut self)
~~~

