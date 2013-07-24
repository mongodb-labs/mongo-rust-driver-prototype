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

use bson::encode::*;

use mongo::db::*;
use mongo::coll::*;
use mongo::util::*;

use gridfile::*;

pub struct GridFS {
    db: @DB,
    files: ~Collection,
    chunks: ~Collection,
}

impl GridFS {
    /**
     * Create a new GridFS handle on the given DB.
     * The GridFS handle uses the collections
     * "fs.files" and "fs.chunks".
     */
    pub fn new(db: @DB) -> GridFS {
        GridFS {
            db: db,
            files: ~(db.get_collection(~"fs.files")),
            chunks: ~(db.get_collection(~"fs.chunks"))
        }
    }

    pub fn new_file<'a>(&'a self) -> GridIn<'a> {
        GridIn::new(&'a *self.chunks, &'a *self.files)
    }

    pub fn put(&self, data: ~[u8]) -> Result<(), MongoErr> {
        use std::rt::io::io_error;

        let mut res = Ok(());
        let mut file = self.new_file();
        do io_error::cond.trap(|c| {
            res = Err(MongoErr::new(
                ~"grid::put",
                c.desc.to_owned(),
                if c.detail.is_some() {c.detail.unwrap()}
                else {~"method returned without error detail"}));
        }).in {
            file.write(data);
            file.close();
        }
        res
    }

    pub fn delete(&self, id: Document) -> Result<(), MongoErr> {
        let mut file_doc = BsonDocument::new();
        let mut chunk_doc = BsonDocument::new();
        file_doc.put(~"_id", id.clone());
        chunk_doc.put(~"files_id", id);
        result_and(
            self.files.remove(Some(SpecObj(file_doc)), None, None, None),
            self.chunks.remove(Some(SpecObj(chunk_doc)), None, None, None)
        )
    }
}

priv fn result_and<T,U>(r1: Result<T,U>, r2: Result<T,U>) -> Result<T,U> {
    match r1 {
        Ok(k) => match r2 {
            Ok(_) => Ok(k),
            Err(e) => return Err(e)
        },
        Err(e) => Err(e)
    }
}
