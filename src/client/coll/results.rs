use bson;
use bson::Bson;

use std::collections::BTreeMap;
use client::coll::error::{BulkWriteException, WriteException};
use client::coll::options::WriteModel;

/// Results for a bulk write operation.
#[derive(Clone)]
pub struct BulkWriteResult {
    pub acknowledged: bool,
    pub inserted_count: i32,
    pub inserted_ids: BTreeMap<i64, Bson>,
    pub matched_count: i32,
    pub modified_count: i32,
    pub deleted_count: i32,
    pub upserted_count: i32,
    pub upserted_ids: BTreeMap<i64, Bson>,
    pub bulk_write_exception: Option<BulkWriteException>,
}

/// Results for a bulk delete operation.
#[derive(Clone)]
pub struct BulkDeleteResult {
    pub acknowledged: bool,
    pub deleted_count: i32,
    pub write_exception: Option<BulkWriteException>,
}

/// Results for a bulk update operation.
#[derive(Clone)]
pub struct BulkUpdateResult {
    pub acknowledged: bool,
    pub matched_count: i32,
    pub modified_count: i32,
    pub upserted_id: Option<Bson>,
    pub write_exception: Option<BulkWriteException>,
}

/// Results for an insertOne operation.
#[derive(Clone)]
pub struct InsertOneResult {
    pub acknowledged: bool,
    pub inserted_id: Option<Bson>,
    pub write_exception: Option<WriteException>,
}

/// Results for an insertMany operation.
#[derive(Clone)]
pub struct InsertManyResult {
    pub acknowledged: bool,
    pub inserted_ids: Option<BTreeMap<i64, Bson>>,
    pub bulk_write_exception: Option<BulkWriteException>,
}

/// Results for a deletion operation.
#[derive(Clone)]
pub struct DeleteResult {
    pub acknowledged: bool,
    pub deleted_count: i32,
    pub write_exception: Option<WriteException>,
}

/// Results for an update operation.
#[derive(Clone)]
pub struct UpdateResult {
    pub acknowledged: bool,
    pub matched_count: i32,
    pub modified_count: i32,
    pub upserted_id: Option<Bson>,
    pub write_exception: Option<WriteException>,
}

impl BulkWriteResult {
    /// Extracts server reply information into a result.
    pub fn new() -> BulkWriteResult {
        BulkWriteResult {
            acknowledged: true,
            inserted_ids: BTreeMap::new(),
            inserted_count: 0,
            matched_count: 0,
            modified_count: 0,
            deleted_count: 0,
            upserted_count: 0,
            upserted_ids: BTreeMap::new(),
            bulk_write_exception: None,
        }
    }

    pub fn process_insert_one_result(&mut self, result: InsertOneResult, i: i64,
                                     req: WriteModel,
                                     exception: &mut BulkWriteException) {
        match result.write_exception {
            Some(write_exception) =>
                exception.add_write_exception(write_exception, i as i32, req),
            None => {
                let id = result.inserted_id.expect("`inserted_id` should not be `None` \
                                                    if there is no WriteException");
                self.inserted_ids.insert(i, id);
                self.inserted_count += 1;

                exception.processed_requests.push(req)
            }
        };
    }

    pub fn process_insert_many_result(&mut self, result: InsertManyResult,
                                      models: Vec<WriteModel>,
                                      exception: &mut BulkWriteException) {
        match result.bulk_write_exception {
            Some(new_exception) =>
                exception.add_bulk_write_exception(new_exception),
            None => for model in models {
                exception.processed_requests.push(model);
            }
        }

        if let Some(ids) = result.inserted_ids {
            for (i, id) in ids {
                self.inserted_ids.insert(i, id);
            }
        }
    }
}

impl BulkDeleteResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<BulkWriteException>) -> BulkDeleteResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        BulkDeleteResult {
            acknowledged: true,
            deleted_count: n,
            write_exception: exception,
        }
    }
}

impl BulkUpdateResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<BulkWriteException>) -> BulkUpdateResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        let (n_upserted, id) = match doc.get("upserted") {
            Some(&Bson::Array(ref arr)) => (arr.len() as i32, Some(arr[0].clone())),
            _ => (0, None)
        };

        let n_matched = n - n_upserted;

        let n_modified = match doc.get("nModified") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        BulkUpdateResult {
            acknowledged: true,
            matched_count: n_matched,
            modified_count: n_modified,
            upserted_id: id,
            write_exception: exception,
        }
    }
}

impl InsertOneResult {
    /// Extracts server reply information into a result.
    pub fn new(inserted_id: Option<Bson>, exception: Option<WriteException>) -> InsertOneResult {
        InsertOneResult {
            acknowledged: true,
            inserted_id: inserted_id,
            write_exception: exception,
        }
    }
}

impl InsertManyResult {
    /// Extracts server reply information into a result.
    pub fn new(inserted_ids: Option<BTreeMap<i64, Bson>>, exception: Option<BulkWriteException>) -> InsertManyResult {
        InsertManyResult {
            acknowledged: true,
            inserted_ids: inserted_ids,
            bulk_write_exception: exception,
        }
    }
}

impl DeleteResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<WriteException>) -> DeleteResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        DeleteResult {
            acknowledged: true,
            deleted_count: n,
            write_exception: exception,
        }
    }

    pub fn with_bulk_result(result: BulkDeleteResult) -> DeleteResult {
        let exception = match result.write_exception {
            Some(bulk_exception) => Some(WriteException::with_bulk_exception(bulk_exception)),
            None => None,
        };

        DeleteResult {
            acknowledged: result.acknowledged,
            deleted_count: result.deleted_count,
            write_exception: exception,
        }
    }
}

impl UpdateResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<WriteException>) -> UpdateResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        let (n_upserted, id) = match doc.get("upserted") {
            Some(&Bson::Array(ref arr)) => (arr.len() as i32, Some(arr[0].clone())),
            _ => (0, None)
        };

        let n_matched = n - n_upserted;

        let n_modified = match doc.get("nModified") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        UpdateResult {
            acknowledged: true,
            matched_count: n_matched,
            modified_count: n_modified,
            upserted_id: id,
            write_exception: exception,
        }
    }

    pub fn with_bulk_result(result: BulkUpdateResult) -> UpdateResult {
        let exception = match result.write_exception {
            Some(bulk_exception) => Some(WriteException::with_bulk_exception(bulk_exception)),
            None => None,
        };
        
        UpdateResult {
            acknowledged: result.acknowledged,
            matched_count: result.matched_count,
            modified_count: result.modified_count,
            upserted_id: result.upserted_id,
            write_exception: exception,
        }
    }
}
