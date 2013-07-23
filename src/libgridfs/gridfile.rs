/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io::{Writer,BytesWriter};

use bson::encode::*;

use mongo::coll::*;
use mongo::util::*;

pub struct GridIn {
    chunks: @Collection,
    files: @Collection,
    closed: bool,
    buf: BytesWriter,
    chunk_size: uint,
    chunk_num: uint,
    file_id: Option<Document>,
}

pub struct GridOut {
    chunks: @Collection,
    files: @Collection
}

impl GridIn {
    pub fn new(chunks: @Collection, files: @Collection) -> GridIn {
        GridIn {
            chunks: chunks,
            files: files,
            closed: false,
            buf: BytesWriter::new(),
            chunk_size: 256 * 1024,
            chunk_num: 0,
            file_id: None,
        }
    }

    pub fn write(&mut self, d: ~[u8]) -> Result<(), MongoErr> {
        if self.closed {
            return Err(MongoErr::new(
                    ~"gridfile::write",
                    ~"cannot write to a closed file",
                    ~"closed files can no longer be written"))
        }
        let mut data: ~[u8] = d;
        if self.buf.tell() > 0 {
            let space = self.chunk_size - self.buf.tell();
            let to_write = data.iter().transform(|x| *x).take_(space).collect::<~[u8]>();
            if space > 0 {
                self.buf.write(to_write);
            }
            match self.flush_data(to_write) {
                Ok(_) => (),
                Err(e) => return Err(e)
            }
            self.buf = BytesWriter::new();
        }
        let mut to_write: ~[u8] = data.iter().transform(|x| *x).take_(self.chunk_size).collect();
        data = data.iter().skip(self.chunk_size).transform(|x| *x).collect();
        while to_write.len() == self.chunk_size {
            match self.flush_data(to_write) {
                Ok(_) => (),
                Err(e) => return Err(e)
            }
            data = data.iter().skip(self.chunk_size).transform(|x| *x).collect();
            to_write = data.iter().take_(self.chunk_size).transform(|x| *x).collect();
        }
        self.buf.write(to_write);
        Ok(())
    }

    pub fn close(&mut self) {
        if !self.closed {
            self.close_buf();
            self.closed = true;
        }
    }

    fn flush_data(&mut self, data: &[u8]) -> Result<(), MongoErr> {
        let mut chunk = BsonDocument::new();
        chunk.put(~"n", Int32(self.chunk_num as i32));
        chunk.put(~"data", Binary(0u8, data.clone().to_owned()));
        match self.file_id {
            Some(ObjectId(ref v)) => chunk.put(~"_id", ObjectId(v.clone())),
            _ => ()
        }
        match self.chunks.insert(chunk.clone(), None) {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        if self.file_id.is_none() {
            match self.chunks.find_one(Some(SpecObj(chunk)), None, None) {
                Ok(d) => match d.find(~"_id") {
                    Some(id) => self.file_id = Some(id.clone()),
                    _ => return Err(MongoErr::new(
                            ~"gridfile::flush_data",
                            ~"error creating _id for chunk",
                            ~"could not find an oid for a chunk"))
                },
                _ => return Err(MongoErr::new(
                        ~"gridfile::flush_data",
                        ~"error creating _id for chunk",
                        ~"could not find an oid for a chunk"))
            }
        }
        self.chunk_num += 1;
        Ok(())
    }

    fn close_buf(&self) {
        self.buf.flush();
        let db = self.chunks.get_db();

        let mut oid = ~"";
        match self.file_id {
            Some(Binary(_, ref v)) => {
                for v.iter().advance |b| {
                    let mut byte = b.to_str();
                    if byte.len() == 1 {
                        byte = "0".append(byte)
                    }
                    oid.push_str(byte);
                }
            }
            _ => ()
        }

        //TODO: generate metadata
        let md5 = match db.run_command(SpecNotation(
    }
}
