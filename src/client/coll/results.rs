use bson;
use bson::Bson;

use std::collections::BTreeMap;

/// Results for a bulk write operation.
#[derive(Clone)]
pub struct BulkWriteResult {
    pub acknowledged: bool,
    pub inserted_count: i64,
    pub inserted_ids: Option<BTreeMap<i64, Bson>>,
    pub matched_count: i64,
    pub modified_count: i64,
    pub deleted_count: i64,
    pub upserted_count: i64,
    pub upserted_ids: BTreeMap<i64, Bson>,
}

/// Results for an insertOne operation.
#[derive(Clone)]
pub struct InsertOneResult {
    pub acknowledged: bool,
    pub inserted_id: Option<Bson>,
}

/// Results for an insertMany operation.
#[derive(Clone)]
pub struct InsertManyResult {
    pub acknowledged: bool,
    pub inserted_ids: Option<BTreeMap<i64, Bson>>,
}

/// Results for a deletion operation.
#[derive(Clone)]
pub struct DeleteResult {
    pub acknowledged: bool,
    pub deleted_count: i64,
}

/// Results for an update operation.
#[derive(Clone)]
pub struct UpdateResult {
    pub acknowledged: bool,
    pub matched_count: i64,
    pub modified_count: i64,
    pub upserted_id: Option<Bson>,
}

impl InsertOneResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document) -> InsertOneResult {
        InsertOneResult {
            acknowledged: true,
            inserted_id: None,
        }
    }
}

impl InsertManyResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document) -> InsertManyResult {
        InsertManyResult {
            acknowledged: true,
            inserted_ids: None,
        }
    }
}

impl DeleteResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document) -> DeleteResult {
        let n = match doc.get("n") {
            Some(&Bson::I64(n)) => n,
            _ => 0,
        };

        DeleteResult {
            acknowledged: true,
            deleted_count: n,
        }
    }
}

impl UpdateResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document) -> UpdateResult {
        let n = match doc.get("n") {
            Some(&Bson::I64(n)) => n,
            _ => 0,
        };

        let n_modified = match doc.get("nModified") {
            Some(&Bson::I64(n)) => n,
            _ => 0,
        };

        UpdateResult {
            acknowledged: true,
            matched_count: n,
            modified_count: n_modified,
            upserted_id: None,
        }
    }
}
