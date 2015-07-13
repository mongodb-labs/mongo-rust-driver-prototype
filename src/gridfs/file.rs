use bson::{self, Bson, oid};
use bson::spec::BinarySubtype;

use chrono::{DateTime, UTC};
use crypto::digest::Digest;
use crypto::md5::Md5;

use Error::{self, ArgumentError, OperationError, PoisonLockError};
use Result;

use super::Store;

use std::{cmp, io, thread};
use std::error::Error as ErrorTrait;
use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicIsize, ATOMIC_ISIZE_INIT, Ordering};

pub const DEFAULT_CHUNK_SIZE: i32 = 255 * 1024;
pub const MEGABYTE: usize = 1024 * 1024;

/// File modes.
#[derive(Debug, PartialEq, Eq)]
pub enum Mode {
    Closed,
    Reading,
    Writing,
}

pub struct File {
    mutex: Arc<Mutex<()>>,
    condvar: Arc<Condvar>,
    gfs: Store,
    chunk: i32,
    offset: i64,
    wpending: Arc<AtomicIsize>,
    wbuf: Vec<u8>,
    wsum: Md5,
    rbuf: Vec<u8>,
    rcache: Option<Arc<Mutex<CachedChunk>>>,
    pub mode: Mode,
    pub doc: GfsFile,
    pub err: Arc<Option<Error>>,
}

pub struct GfsFile {
    len: i64,
    md5: String,
    pub id: oid::ObjectId,
    pub chunk_size: i32,
    pub aliases: Vec<String>,
    pub name: Option<String>,
    pub upload_date: Option<DateTime<UTC>>,
    pub content_type: Option<String>,
    pub metadata: Option<Vec<u8>>,
}

struct CachedChunk {
    n: i32,
    data: Vec<u8>,
    err: Option<Error>,
}

impl Deref for File {
    type Target = GfsFile;

    fn deref<'a>(&'a self) -> &'a Self::Target {
        &self.doc
    }
}

impl DerefMut for File {
    fn deref_mut<'a>(&'a mut self) -> &'a mut Self::Target {
        &mut self.doc
    }
}

impl File {
    pub fn new(gfs: Store, id: oid::ObjectId, mode: Mode) -> File {
        File::with_gfs_file(gfs, GfsFile::new(id), mode)
    }

    pub fn with_name(gfs: Store, name: String, id: oid::ObjectId, mode: Mode) -> File {
        File::with_gfs_file(gfs, GfsFile::with_name(name, id), mode)
    }

    pub fn with_doc(gfs: Store, doc: bson::Document) -> File {
        File::with_gfs_file(gfs, GfsFile::with_doc(doc), Mode::Reading)
    }

    fn with_gfs_file(gfs: Store, file: GfsFile, mode: Mode) -> File {
        File {
            mutex: Arc::new(Mutex::new(())),
            condvar: Arc::new(Condvar::new()),
            mode: mode,
            gfs: gfs,
            chunk: 0,
            offset: 0,
            wpending: Arc::new(ATOMIC_ISIZE_INIT),
            wbuf: Vec::new(),
            wsum: Md5::new(),
            rbuf: Vec::new(),
            rcache: None,
            doc: file,
            err: Arc::new(None),
        }
    }

    pub fn assert_mode(&self, mode: Mode) -> Result<()> {
        if self.mode != mode {
            return match self.mode {
                Mode::Reading => Err(ArgumentError("File is open for reading.".to_owned())),
                Mode::Writing => Err(ArgumentError("File is open for writing.".to_owned())),
                Mode::Closed => Err(ArgumentError("File is closed.".to_owned())),
            }
        }
        Ok(())
    }

    /// Completes writing or reading and closes the file. This will be called when the
    /// file is dropped, but errors will be ignored. Therefore, this method should
    /// be called manually.
    pub fn close(&mut self) -> Result<()> {
        if self.mode == Mode::Writing {
            try!(self.flush());
        }

        let _ = try!(self.mutex.lock());
        if self.mode  == Mode::Writing {
            // Complete write
            if self.err.is_none() {
                if self.doc.upload_date.is_none() {
                    self.doc.upload_date = Some(UTC::now());
                }
                self.doc.md5 = self.wsum.result_str();
                try!(self.gfs.files.insert_one(self.doc.to_bson(), None));
                //self.gfs.chunks.ensure_index_key("files_id", "n");
            } else {
                try!(self.gfs.chunks.delete_many(doc!{ "files_id" => (self.doc.id.clone()) }, None));
            }
        }

        if self.mode == Mode::Reading && self.rcache.is_some() {
            {
                let cache = self.rcache.as_ref().unwrap();
                let _ = try!(cache.lock());
            }
            self.rcache = None;
        }

        self.mode = Mode::Closed;

        if self.err.is_some() {
            Err(OperationError(self.err.as_ref().unwrap().description().to_owned()))
        } else {
            Ok(())
        }
    }

    fn insert_chunk(&self, n: i32, buf: &[u8]) -> Result<()> {
        self.wpending.fetch_add(1, Ordering::SeqCst);
        let mut vec_buf = Vec::with_capacity(buf.len());
        vec_buf.extend(buf.iter().cloned());

        let document = doc! {
            "_id" => (try!(oid::ObjectId::new())),
            "files_id" => (self.doc.id.clone()),
            "n" => n,
            "data" => (BinarySubtype::Generic, vec_buf)
        };

        let arc_gfs = self.gfs.clone();
        let arc_mutex = self.mutex.clone();
        let arc_wpending = self.wpending.clone();
        let cvar = self.condvar.clone();
        let mut err = self.err.clone();

        thread::spawn(move || {
            let result = arc_gfs.chunks.insert_one(document, None);
            let _ = arc_mutex.lock();
            arc_wpending.fetch_sub(1, Ordering::SeqCst);
            if result.is_err() {
                err = Arc::new(Some(result.err().unwrap()));
            }
            cvar.notify_all();
        });

        Ok(())
    }

    fn find_chunk(&mut self, id: oid::ObjectId, chunk: i32) -> Result<Vec<u8>> {
        match try!(self.gfs.chunks.find_one(
            Some(doc!{"files_id" => id, "n" => chunk }),
            None)) {
            Some(doc) => match doc.get("data") {
                Some(&Bson::Binary(_, ref buf)) => Ok(buf.clone()),
                _ => return Err(OperationError("Chunk contained no data".to_owned())),
            },
            None => return Err(OperationError("Chunk not found".to_owned())),
        }
    }

    fn get_chunk(&mut self) -> Result<Vec<u8>> {
        let id = self.doc.id.clone();
        let chunk = self.chunk;

        let data = if let Some(lock) = self.rcache.take() {
            let cache = try!(lock.lock());
            if cache.n == self.chunk {
                cache.data.clone()
            } else {
                try!(self.find_chunk(id, chunk))
            }
        } else {
            try!(self.find_chunk(id, chunk))
        };

        self.chunk += 1;

        if (self.chunk as i64) * (self.doc.chunk_size as i64) < self.doc.len {
            let cache = Arc::new(Mutex::new(CachedChunk::new(self.chunk)));

            let arc_cache = cache.clone();
            let arc_gfs = self.gfs.clone();
            let id = self.doc.id.clone();
            let chunk = self.chunk;

            thread::spawn(move || {
                let mut cache = match arc_cache.lock() {
                    Ok(cache) => cache,
                    Err(_) => {
                        // Cache lock is poisoned; abandon caching mechanism.
                        return
                    }
                };

                let result = arc_gfs.chunks.find_one(
                    Some(doc!{"files_id" => (id), "n" => (chunk)}),
                    None);

                match result {
                    Ok(Some(doc)) => match doc.get("data") {
                        Some(&Bson::Binary(_, ref buf)) => cache.data = buf.clone(),
                        _ => cache.err = Some(OperationError("Chunk contained no data.".to_owned())),
                    },
                    Ok(None) => cache.err = Some(OperationError("Chunk not found.".to_owned())),
                    Err(err) => cache.err = Some(err),
                }
            });

            self.rcache = Some(cache);
        }

        Ok(data)
    }
}

impl io::Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        try!(self.assert_mode(Mode::Writing));

        let mut guard = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(io::Error::new(
                io::ErrorKind::Other, PoisonLockError)),
        };

        let mut data = buf;

        if self.err.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                OperationError(self.err.as_ref().unwrap().description().to_owned())));
        }

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

            let n = self.chunk;
            self.chunk += 1;
            self.wsum.input(buf);
            while self.doc.chunk_size * self.wpending.load(Ordering::SeqCst) as i32 >= MEGABYTE as i32 {
                // Pending MB
                guard = match self.condvar.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return Err(io::Error::new(
                        io::ErrorKind::Other, ArgumentError("test".to_owned())))
                };

                if self.err.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        OperationError(self.err.as_ref().unwrap().description().to_owned())))
                }
            }

            try!(self.insert_chunk(n, &mut chunk));
            self.wbuf.clear();
        }

        while data.len() > chunk_size as usize {
            let size = cmp::min(chunk_size, data.len());
            let (part1, part2) = data.split_at(size);

            let n = self.chunk;
            self.chunk += 1;
            self.wsum.input(buf);
            while self.doc.chunk_size * self.wpending.load(Ordering::SeqCst) as i32 >= MEGABYTE as i32 {
                // Pending MB
                guard = match self.condvar.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return Err(io::Error::new(
                        io::ErrorKind::Other,
                        PoisonLockError
                            ))
                };

                if self.err.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        OperationError(self.err.as_ref().unwrap().description().to_owned())))
                }
            }

            try!(self.insert_chunk(n, part1));
            data = part2;
        }

        self.wbuf.extend(data.iter().cloned());
        return Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        try!(self.assert_mode(Mode::Writing));

        let mut guard = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(io::Error::new(
                io::ErrorKind::Other, PoisonLockError)),
        };

        if self.wbuf.len() > 0  && self.err.is_none() {
            let chunk = self.wbuf.clone();
            let n = self.chunk;
            self.chunk += 1;
            self.wsum.input(&self.wbuf);

            while self.doc.chunk_size * self.wpending.load(Ordering::SeqCst) as i32 >= MEGABYTE as i32 {
                // Pending MB
                guard = match self.condvar.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return Err(io::Error::new(
                        io::ErrorKind::Other, PoisonLockError)),
                }
            }

            if self.err.is_none() {
                try!(self.insert_chunk(n, &chunk));
                self.wbuf.clear();
            }
        }

        while self.wpending.load(Ordering::SeqCst) > 0 {
            guard = match self.condvar.wait(guard) {
                Ok(guard) => guard,
                Err(_) => return Err(io::Error::new(
                    io::ErrorKind::Other, PoisonLockError)),
            }
        }

        Ok(())
    }
}

impl io::Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        try!(self.assert_mode(Mode::Reading));

        let _ = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(io::Error::new(
                io::ErrorKind::Other, PoisonLockError)),
        };

        if self.offset == self.doc.len {
            return Ok(0);
        }

        while self.rbuf.len() < buf.len() {
            let chunk = try!(self.get_chunk());
            self.rbuf.extend(chunk);
        }

        let i = try!((&mut *buf).write(&mut self.rbuf));
        self.offset += i as i64;

        let mut new_rbuf = Vec::with_capacity(self.rbuf.len() - i);
        {
            let (_, p2) = self.rbuf.split_at(i);
            let b: Vec<u8> = p2.iter().map(|&i| i).collect();
            new_rbuf.extend(b);
        }

        self.rbuf = new_rbuf;

        Ok(i)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        // This ignores errors during closing; should close explicitly and
        // handle errors manually.
        let _ = self.close();
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

        if let Some(&Bson::Binary(_, ref metadata)) = doc.get("metadata") {
            file.metadata = Some(metadata.clone());
        }

        file
    }

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

        if self.metadata.is_some() {
            doc.insert("metadata".to_owned(),
                       Bson::Binary(BinarySubtype::Generic,
                                    self.metadata.as_ref().unwrap().clone()));
        }

        doc
    }
}

impl CachedChunk {
    pub fn new(n: i32) -> CachedChunk {
        CachedChunk {
            n: n,
            data: Vec::new(),
            err: None,
        }
    }
}
