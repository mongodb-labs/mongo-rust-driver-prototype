% Module gridfile

<div class='index'>

* [Struct `GridReader`](#struct-gridreader) - Struct for reading from GridFS
* [Struct `GridWriter`](#struct-gridwriter) - Struct for writing to GridFS
* [Implementation ` of rtio::Writer for GridWriter`](#implementation-of-rtiowriter-for-gridwriter)
* [Implementation ` for GridWriter`](#implementation-for-gridwriter)
* [Implementation ` of rtio::Reader for GridReader`](#implementation-of-rtioreader-for-gridreader)
* [Implementation ` for GridReader`](#implementation-for-gridreader)

</div>

## Struct `GridReader`

~~~ {.rust}
pub struct GridReader {
    chunks: Collection,
    files: Collection,
    length: uint,
    position: uint,
    file_id: Document,
    buf: ~[u8],
}
~~~

Struct for reading from GridFS. Currently
it always uses a base collection called "fs".

## Struct `GridWriter`

~~~ {.rust}
pub struct GridWriter {
    chunks: Collection,
    files: Collection,
    closed: bool,
    chunk_size: uint,
    chunk_num: uint,
    file_id: Option<Document>,
    position: uint,
}
~~~

Struct for writing to GridFS. Currently
it always uses a base collection called "fs".

## Implementation of `rtio::Writer` for `GridWriter`

### Method `write`

~~~ {.rust}
fn write(&mut self, d: &[u8])
~~~

Write the given data to the fs.chunks collection.

### Method `flush`

~~~ {.rust}
fn flush(&mut self)
~~~

Complete a write of a document.
Calling this causes document metadata
to be written to the fs.files collection.

## Implementation for `GridWriter`

### Method `new`

~~~ {.rust}
fn new(db: &DB) -> GridWriter
~~~

Create a new GridWriter for the given database.

### Method `close`

~~~ {.rust}
fn close(&mut self) -> Result<(), MongoErr>
~~~

Close this GridWriter.
Closing a GridWriter causes it to flush,
and a closed writer cannot be written to.

## Implementation of `rtio::Reader` for `GridReader`

### Method `read`

~~~ {.rust}
fn read(&mut self, buf: &mut [u8]) -> Option<uint>
~~~

Read data into buf.

The data is collected based on the query
`db.fs.chunks.find({file_id: self.file_id})`
(in rough notation).

Returns the number of bytes read.

### Method `eof`

~~~ {.rust}
fn eof(&mut self) -> bool
~~~

Return true if there is more data that can be read.

## Implementation for `GridReader`

### Method `new`

~~~ {.rust}
fn new(db: &DB, file_id: Document) -> GridReader
~~~

Builds a new GridReader.
Fails if the id given does not match
any _id field in the fs.files collection.

