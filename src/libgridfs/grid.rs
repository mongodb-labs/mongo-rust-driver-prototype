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

use mongo::db::*;
use mongo::coll::*;
use mongo::util::*;

use gridfile::*;

pub struct GridFS {
    db: @DB,
    files: @Collection,
    chunks: @Collection,
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
            files: @(db.get_collection(~"fs.files")),
            chunks: @(db.get_collection(~"fs.chunks"))
        }
    }

    pub fn new_file(&self) -> GridIn {
        GridIn::new(self.chunks, self.files)
    }

    pub fn put(&self, data: ~[u8]) -> Result<(), MongoErr> {
        let mut file = self.new_file();
        file.write(data)
    }
}
