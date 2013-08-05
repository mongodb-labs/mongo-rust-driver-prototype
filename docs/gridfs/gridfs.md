% Crate gridfs

<div class='index'>

* [Struct `GridFS`](#struct-gridfs)
* [Implementation ` for GridFS`](#implementation-for-gridfs)
* [Module `gridfile`](gridfile.md)

</div>

## Struct `GridFS`

~~~ {.rust}
pub struct GridFS {
    db: @DB,
    files: Collection,
    chunks: Collection,
    last_id: Option<Document>,
}
~~~

## Implementation for `GridFS`

### Method `new`

~~~ {.rust}
fn new(db: @DB) -> GridFS
~~~

Create a new GridFS handle on the given DB.
The GridFS handle uses the collections
"fs.files" and "fs.chunks".

### Method `file_write`

~~~ {.rust}
fn file_write(&self) -> GridWriter
~~~

### Method `put`

~~~ {.rust}
fn put(&mut self, data: ~[u8]) -> Result<(), MongoErr>
~~~

### Method `delete`

~~~ {.rust}
fn delete(&self, id: Document) -> Result<(), MongoErr>
~~~

### Method `file_read`

~~~ {.rust}
fn file_read(&self, id: Document) -> GridReader
~~~

