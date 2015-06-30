use bson::Document;
use client::coll::options::WriteModel;
use client::coll::results::InsertManyResult;

pub enum Batch {
    Insert {
        documents: Vec<Document>,
    }
}
