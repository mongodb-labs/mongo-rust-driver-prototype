use bson::{self, Bson, oid};
use bson::spec::BinarySubtype;

use chrono::{DateTime, UTC};
use crypto::digest::Digest;
use crypto::md5::Md5;

use super::db::{Database, ThreadedDatabase};
use super::coll::Collection;
use super::coll::options::FindOptions;
use super::cursor::Cursor;
use super::Error::{self, ArgumentError, OperationError};
use super::Result;

use std::{cmp, io, fs};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

pub const DEFAULT_CHUNK_SIZE: i32 = 255 * 1024;
pub const MEGABYTE: usize = 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum Mode {
    Closed,
    Reading,
    Writing,
}

pub struct StoreInner {
    files: Collection,
    chunks: Collection,
}

pub struct File {
    mutex: Arc<Mutex<()>>,
    mode: Mode,
    gfs: Store,
    chunk: i32,
    offset: i64,
    wpending: i32,
    wbuf: Vec<u8>,
    wsum: String,
    rbuf: Vec<u8>,
    rcache: Option<CachedChunk>,
    doc: GfsFile,
    // err: Error,
}

pub struct GfsFile {
    id: oid::ObjectId,
    chunk_size: i32,
    len: i64,
    md5: String,
    aliases: Vec<String>,
    name: Option<String>,
    upload_date: Option<DateTime<UTC>>,
    content_type: Option<String>,
    metadata: Option<bson::Document>,
}

struct CachedChunk {
    wait: Arc<Mutex<()>>,
    n: i32,
    data: Vec<u8>,
    err: Option<Error>,
}

pub trait ThreadedStore {
    fn with_db(db: Database) -> Store;
    fn with_prefix(db: Database, prefix: String) -> Store;
    fn create(&self, name: String) -> Result<File>;
    fn open(&self, name: String) -> Result<File>;
    fn open_id(&self, id: oid::ObjectId) -> Result<File>;
    fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> Result<Cursor>;
    // TODO: Make a GridCursor wrapper for this?
    fn open_next(&self, cursor: &mut Cursor) -> Result<Option<File>>;
    fn remove(&self, name: String) -> Result<()>;
    fn remove_id(&self, id: oid::ObjectId) -> Result<()>;
    fn put(&self, name: String) -> Result<()>;
    fn get(&self, name: String) -> Result<()>;
}

pub type Store = Arc<StoreInner>;

impl CachedChunk {
    pub fn new(n: i32) -> CachedChunk {
        CachedChunk {
            wait: Arc::new(Mutex::new(())),
            n: n,
            data: Vec::new(),
            err: None,
        }
    }
}

impl GfsFile {
    pub fn to_bson(&self) -> bson::Document {
        let mut doc = doc! {
            "_id" => (self.id.clone()),
            "chunkSize" => (self.chunk_size),
            "length" => (self.len),
            "md5" => (self.md5.to_owned()),
            "uploadDate" => (self.upload_date.as_ref().unwrap().clone())
        };

        if self.name.is_some() {
            doc.insert("filename".to_owned(),
                       Bson::String(self.name.as_ref().unwrap().to_owned()));
        }

        if self.content_type.is_some() {
            doc.insert("contentType".to_owned(),
                       Bson::String(self.content_type.as_ref().unwrap().to_owned()));
        }

        // self.metadata.and_then(

        doc
    }
}

pub struct Chunk {
    /// The unique chunk ObjectId.
    id: oid::ObjectId,
    /// The id of the parent file document.
    files_id: oid::ObjectId,
    /// The sequence number of the chunk, starting from zero.
    n: i64,
    /// The binary chunk payload.
    data: Vec<u8>,
}

impl Chunk {
    pub fn new(id: oid::ObjectId, files_id: oid::ObjectId, n: i64, data: Vec<u8>) -> Chunk {
        Chunk {
            id: id,
            files_id: files_id,
            n: n,
            data: data,
        }
    }
}

impl ThreadedStore for Store {
    fn with_db(db: Database) -> Store {
        Store::with_prefix(db, "fs".to_owned())
    }

    fn with_prefix(db: Database, prefix: String) -> Store {
        Arc::new(StoreInner {
            files: db.collection(&format!("{}.files", prefix)[..]),
            chunks: db.collection(&format!("{}.chunks", prefix)[..]),
        })
    }

    fn create(&self, name: String) -> Result<File> {
        Ok(File::with_name(self.clone(), name, try!(oid::ObjectId::new()), Mode::Writing))
    }

    fn open(&self, name: String) -> Result<File> {
        let mut options = FindOptions::new();
        options.sort = Some(doc!{ "uploadDate" => 1 });

        match try!(self.files.find_one(Some(doc!{ "filename" => name }), Some(options))) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError("File does not exist.".to_owned())),
        }
    }

    fn open_id(&self, id: oid::ObjectId) -> Result<File> {
        match try!(self.files.find_one(Some(doc!{ "_id" => id }), None)) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError("File does not exist.".to_owned())),
        }
    }

    fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> Result<Cursor> {
        self.files.find(filter, options)
    }

    // TODO: Make a GridCursor wrapper for this?
    fn open_next(&self, cursor: &mut Cursor) -> Result<Option<File>> {
        match cursor.next() {
            Some(Ok(bdoc)) => Ok(Some(File::with_doc(self.clone(), bdoc))),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    fn remove(&self, name: String) -> Result<()> {
        let mut options = FindOptions::new();
        options.projection = Some(doc!{ "_id" => 1 });

        let mut cursor = try!(self.find(Some(doc!{ "filename" => name }), Some(options)));
        for res in cursor {
            let doc = try!(res);
            if let Some(&Bson::ObjectId(ref id)) = doc.get("_id") {
                try!(self.remove_id(id.clone()));
            }
        }

        Ok(())
    }

    fn remove_id(&self, id: oid::ObjectId) -> Result<()> {
        try!(self.files.delete_many(doc!{ "_id" => (id.clone()) }, None));
        try!(self.chunks.delete_many(doc!{ "files_id" => (id.clone()) }, None));
        Ok(())
    }

    fn put(&self, name: String) -> Result<()> {
        let mut file = try!(self.create(name.to_owned()));
        let mut f = try!(fs::File::open(name.to_owned()));
        try!(io::copy(&mut f, &mut file));
        file.close();
        Ok(())
    }

    fn get(&self, name: String) -> Result<()> {
        let mut f = try!(fs::File::create(name.to_owned()));
        let mut file = try!(self.open(name.to_owned()));
        try!(io::copy(&mut file, &mut f));
        file.close();
        Ok(())
    }
}

impl File {
    pub fn new(gfs: Store, id: oid::ObjectId, mode: Mode) -> File {
        File {
            mutex: Arc::new(Mutex::new(())),
            mode: mode,
            gfs: gfs,
            chunk: 0,
            offset: 0,
            wpending: 0,
            wbuf: Vec::new(),
            wsum: String::new(),
            rbuf: Vec::new(),
            rcache: None,
            doc: GfsFile::new(id),
        }
    }

    pub fn with_name(gfs: Store, name: String, id: oid::ObjectId, mode: Mode) -> File {
        File {
            mutex: Arc::new(Mutex::new(())),
            mode: mode,
            gfs: gfs,
            chunk: 0,
            offset: 0,
            wpending: 0,
            wbuf: Vec::new(),
            wsum: String::new(),
            rbuf: Vec::new(),
            rcache: None,
            doc: GfsFile::with_name(name, id),
        }
    }

    pub fn with_doc(gfs: Store, doc: bson::Document) -> File {
        File {
            mutex: Arc::new(Mutex::new(())),
            mode: Mode::Reading,
            gfs: gfs,
            chunk: 0,
            offset: 0,
            wpending: 0,
            wbuf: Vec::new(),
            wsum: String::new(),
            rbuf: Vec::new(),
            rcache: None,
            doc: GfsFile::with_doc(doc),
        }
    }

    pub fn assert_mode(&self, mode: Mode) -> Result<()> {
        if self.mode != mode {
            match self.mode {
                Mode::Reading => Err(ArgumentError("File is open for reading.".to_owned())),
                Mode::Writing => Err(ArgumentError("File is open for writing.".to_owned())),
                Mode::Closed => Err(ArgumentError("File is closed.".to_owned())),
            }
        } else {
            Ok(())
        }
    }

    pub fn id(&self) -> oid::ObjectId {
        self.doc.id.clone()
    }
    /*
    pub fn set_chunk_size(&mut self, size: i32) {
    try!(self.assert_mode(Mode::Writing));
    self.chunk_size = size;
}

    pub fn set_id(id: Bson::ObjectId) {
    try!(self.assert_mode(Mode::Writing));
    self.id = id;
}

    pub fn set_name(name: String) -> Result<()> {
    try!(self.assert_mode(Mode::Writing));
    self.name = name;
}*/

    //    pub fn set_content_type set_metadata ...

    pub fn close(&mut self) -> Result<()> {
        try!(self.mutex.lock());
        if self.mode == Mode::Writing {
            if self.wbuf.len() > 0 /* && self.err.is_none() */ {
                let chunk = self.wbuf.clone();
                self.insert_chunk(&chunk);
                self.wbuf.clear();
            }
            self.complete_write();
        } /*else if self.mode == Mode::Reading && self.rcache {
        rcache wait lock; set nil
    }*/
        self.mode = Mode::Closed;
        Ok(())
    }

    fn complete_write(&mut self) -> Result<()> {
        /*while self.wpending > 0 {
        wait on cond
    }*/

        // if self.err == nil {
        // let hexsum = ;
        if self.doc.upload_date.is_none() {
            self.doc.upload_date = Some(UTC::now());
        }
        // self.doc.md5 = hexsum;
        self.gfs.files.insert_one(self.doc.to_bson(), None);
        //self.gfs.chunks.ensure_index_key("files_id", "n");
        Ok(())
    }

    fn insert_chunk(&mut self, buf: &[u8]) -> Result<()> {
        let n = self.chunk;
        self.chunk += 1;
        //self.wsum.write(buf)
        //while doc.chunk_size * self.wpending >= MEGABYTE {
        // Pending MB
        // self.cond.wait
        // if err return
        //}

        self.wpending += 1;
        let mut vec_buf = Vec::with_capacity(buf.len());
        vec_buf.extend(buf.iter().cloned());

        let mut document = doc! {
            "_id" => (try!(oid::ObjectId::new())),
            "files_id" => (self.doc.id.clone()),
            "n" => n,
            "data" => (BinarySubtype::Generic, vec_buf)
        };

        //thread::spawn(move || {
        let result = self.gfs.chunks.insert_one(document, None);
        //self.lock
        self.wpending -= 1;
        //    if result.is
        //});
        Ok(())
    }

    fn get_chunk(&mut self) -> Result<Vec<u8>> {
        let data = match self.rcache.take() {
            Some(cache) => {
                if cache.n == self.chunk {
                    try!(cache.wait.lock());
                    cache.data
                } else {
                    match try!(self.gfs.chunks.find_one(
                        Some(doc!{"files_id" => (self.doc.id.clone()), "n" => (self.chunk)}),
                        None)) {
                        Some(doc) => match doc.get("data") {
                            Some(&Bson::Binary(_, ref buf)) => buf.clone(),
                            _ => return Err(OperationError("Chunk contained no data".to_owned())),
                        },
                        None => return Err(OperationError("Chunk not found".to_owned())),
                    }
                }
            },
            None => {
                match try!(
                    self.gfs.chunks.find_one(
                        Some(doc!{"files_id" => (self.doc.id.clone()), "n" => (self.chunk)}),
                        None)) {
                    Some(doc) => match doc.get("data") {
                        Some(&Bson::Binary(_, ref buf)) => buf.clone(),
                        _ => return Err(OperationError("Chunk contained no data".to_owned())),
                    },
                    None => return Err(OperationError("Chunk not found".to_owned())),
                }
            }
        };

        self.chunk += 1;

        if (self.chunk as i64) * (self.doc.chunk_size as i64) < self.doc.len {
            let mut cache = CachedChunk::new(self.chunk);

            cache.wait.lock();
            //thread stuff
            let result = self.gfs.chunks.find_one(
                Some(doc!{"files_id" => (self.doc.id.clone()), "n" => (self.chunk)}),
                None);

            match result {
                Ok(Some(doc)) => match doc.get("data") {
                    Some(&Bson::Binary(_, ref buf)) => cache.data = buf.clone(),
                    _ => cache.err = Some(OperationError("Chunk contained no data.".to_owned())),
                },
                Ok(None) => cache.err = Some(OperationError("Chunk not found.".to_owned())),
                Err(err) => cache.err = Some(err),
            }
            // end thread stuff
            self.rcache = Some(cache);
        }

        Ok(data)
    }
}

impl io::Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.assert_mode(Mode::Writing);
        let _ = match self.mutex.lock() {
            Ok(doc) => (),
            Err(_) => return Err(io::Error::new(
                io::ErrorKind::BrokenPipe, ArgumentError("test".to_owned()))),
        };

        let mut data = buf;

        // if file.err.is_some()

        let n = data.len();
        let chunk_size = self.doc.chunk_size as usize;
        self.doc.len += data.len() as i64;

        if self.wbuf.len() + data.len() < chunk_size {
            self.wbuf.extend(data.iter().cloned());
            return Ok(n);
        }

        if self.wbuf.len() > 0 {
            let missing = cmp::min(chunk_size - self.wbuf.len(), data.len());
            let (part1, part2) = data.split_at(missing);

            self.wbuf.extend(part1.iter().cloned());
            data = part2;
            let mut chunk = self.wbuf.clone();
            let result = self.insert_chunk(&mut chunk);
            self.wbuf.clear();
        }

        while data.len() > chunk_size as usize {
            let size = cmp::min(chunk_size, data.len());
            let (part1, part2) = data.split_at(size);
            let result = self.insert_chunk(part1);
            data = part2;
        }

        self.wbuf.extend(data.iter().cloned());
        return Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!()
    }
}

impl io::Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.assert_mode(Mode::Reading);
        let _ = match self.mutex.lock() {
            Ok(doc) => (),
            Err(_) => return Err(io::Error::new(
                io::ErrorKind::Other, ArgumentError("test".to_owned()))),
        };

        if self.offset == self.doc.len {
            return Ok(0);//Err(io::Error::new(
                //io::ErrorKind::Other, ArgumentError("EOF".to_owned())));
        }

        let mut err: Option<Error> = None;
        let mut n = 0;

        let mut written = 0;
        while err.is_none() {
            let i = try!((&mut *buf).write(&mut self.rbuf));
            n += i;
            self.offset += i as i64;

            let mut new_rbuf = Vec::with_capacity(self.rbuf.len() - i);
            {
                let (p1, p2) = self.rbuf.split_at(i);
                let b: Vec<u8> = p2.iter().map(|&i| i).collect();
                new_rbuf.extend(b);
            }
            self.rbuf = new_rbuf;

            written += i;
            if written >= buf.len() || self.offset == self.doc.len {
                break;
            }

            self.rbuf = match self.get_chunk() {
                Ok(buf) => buf,
                Err(err) => return Err(io::Error::new(
                    io::ErrorKind::Other, OperationError("Unable to retrieve chunk.".to_owned()))),
            };
        }

        Ok(n)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        self.close();
    }
}

impl GfsFile {
    pub fn new(id: oid::ObjectId) -> GfsFile {
        GfsFile {
            id: id,
            chunk_size: DEFAULT_CHUNK_SIZE,
            name: None,
            len: 0,
            md5: String::new(),
            aliases: Vec::new(),
            upload_date: None,
            content_type: None,
            metadata: None,
        }
    }

    pub fn with_name(name: String, id: oid::ObjectId) -> GfsFile {
        GfsFile {
            id: id,
            chunk_size: DEFAULT_CHUNK_SIZE,
            name: Some(name),
            len: 0,
            md5: String::new(),
            aliases: Vec::new(),
            upload_date: None,
            content_type: None,
            metadata: None,
        }
    }

    pub fn with_doc(doc: bson::Document) -> GfsFile {
        let mut file: GfsFile;

        if let Some(&Bson::ObjectId(ref id)) = doc.get("_id") {
            file = GfsFile::new(id.clone())
        } else {
            panic!("Document has no _id!");
        }

        if let Some(&Bson::String(ref name)) = doc.get("filename") {
            file.name = Some(name.to_owned());
        }

        if let Some(&Bson::I32(ref chunk_size)) = doc.get("chunkSize") {
            file.chunk_size = *chunk_size;
        }

        if let Some(&Bson::UtcDatetime(ref datetime)) = doc.get("uploadDate") {
            file.upload_date = Some(datetime.to_owned());
        }

        if let Some(&Bson::I64(ref length)) = doc.get("length") {            
            file.len = *length;
        }

        if let Some(&Bson::String(ref hash)) = doc.get("md5") {
            file.md5 = hash.to_owned();
        }

        if let Some(&Bson::String(ref content_type)) = doc.get("contentType") {
            file.content_type = Some(content_type.to_owned());
        }

        if let Some(&Bson::Document(ref metadata)) = doc.get("metadata") {
            file.metadata = Some(metadata.clone());
        }

        file
    }
}
