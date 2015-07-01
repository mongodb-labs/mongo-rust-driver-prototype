use bson::Document;
use client::coll::options::WriteModel;
use client::coll::results::InsertManyResult;

struct UpdateModel {
    pub filter: Document,
    pub update: Document,
    pub upsert: bool,
    pub multi: bool,
}

struct DeleteModel {
    pub filter: Document,
    pub multi: bool,
}

pub enum Batch {
    Insert {
        documents: Vec<Document>,
    }
}
