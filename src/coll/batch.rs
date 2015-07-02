use bson::Document;
use super::options::WriteModel;
use super::results::InsertManyResult;

pub struct UpdateModel {
    pub filter: Document,
    pub update: Document,
    pub upsert: bool,
    pub multi: bool,
}

pub struct DeleteModel {
    pub filter: Document,
    pub multi: bool,
}

impl UpdateModel {
    pub fn new(filter: Document, update: Document, upsert: bool, multi: bool) -> UpdateModel {
        UpdateModel {
            filter: filter,
            update: update,
            upsert: upsert,
            multi: multi,
        }
    }
}

impl DeleteModel {
    pub fn new(filter: Document, multi: bool) -> DeleteModel {
        DeleteModel {
            filter: filter,
            multi: multi,
        }
    }
}

pub enum Batch {
    Insert {
        documents: Vec<Document>,
    }
}
