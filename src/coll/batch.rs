use super::options::WriteModel;

use bson::Document;
use std::convert::From;

#[derive(Debug)]
pub struct DeleteModel {
    pub filter: Document,
    pub multi: bool,
}

#[derive(Debug)]
pub struct UpdateModel {
    pub filter: Document,
    pub update: Document,
    pub upsert: bool,
    pub multi: bool,
    pub is_replace: bool,
}

impl UpdateModel {
    pub fn new(filter: Document, update: Document, upsert: bool, multi: bool) -> UpdateModel {
        UpdateModel {
            filter: filter,
            update: update,
            upsert: upsert,
            multi: multi,
            is_replace: false,
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

#[derive(Debug)]
pub enum Batch {
    Insert(Vec<Document>),
    Delete(Vec<DeleteModel>),
    Update(Vec<UpdateModel>),
}

impl From<WriteModel> for Batch {
    fn from(model: WriteModel) -> Batch {
        match model {
            WriteModel::InsertOne { document } => Batch::Insert(vec![document]),
            WriteModel::DeleteOne { filter } =>
                Batch::Delete(vec![DeleteModel { filter: filter, multi: false }]),
            WriteModel::DeleteMany { filter } =>
                Batch::Delete(vec![DeleteModel { filter: filter, multi: true }]),
            WriteModel::ReplaceOne { filter, replacement: update, upsert } =>
                Batch::Update(vec![UpdateModel { filter: filter,
                                                 update: update,
                                                 upsert: upsert, multi: false,
                                                 is_replace: true }]),
            WriteModel::UpdateOne { filter, update, upsert } =>
                Batch::Update(vec![UpdateModel { filter: filter,
                                                 update: update,
                                                 upsert: upsert, multi: false,
                                                 is_replace: false }]),
            WriteModel::UpdateMany { filter, update, upsert } =>
                Batch::Update(vec![UpdateModel { filter: filter,
                                                 update: update,
                                                 upsert: upsert, multi: true,
                                                 is_replace: false }]),
        }
    }
}

impl Batch {
    pub fn len(&self) -> i64 {
        let length = match self {
            &Batch::Insert(ref v) => v.len(),
            &Batch::Delete(ref v) => v.len(),
            &Batch::Update(ref v) => v.len(),
        };

        length as i64
    }

    /// Attempts to merge another model into this batch.
    ///
    /// # Arguments
    ///
    ///  `model` - The model to try to merge.
    ///
    /// # Return value
    ///
    /// Returns `None` on success, or the model that couldn't be merged on
    /// failure.
    pub fn merge_model(&mut self, model: WriteModel) -> Option<WriteModel> {
        match self {
            &mut Batch::Insert(ref mut docs) => {
                match model {
                    WriteModel::InsertOne { document } => docs.push(document),
                    _ => return Some(model)
                }
            },
            &mut Batch::Delete(ref mut models) => {
                match model {
                    WriteModel::DeleteOne { filter} =>
                        models.push(DeleteModel { filter: filter, multi: false}),
                    WriteModel::DeleteMany { filter } =>
                        models.push(DeleteModel { filter: filter, multi: true}),
                    _ => return Some(model)
                }
            },
            &mut Batch::Update(ref mut models) => {
                match model {
                    WriteModel::ReplaceOne { filter, replacement: update,
                                             upsert } =>
                        models.push(UpdateModel { filter: filter,
                                                  update: update,
                                                  upsert: upsert, multi: false,
                                                  is_replace: true }),
                    WriteModel::UpdateOne { filter, update, upsert } =>
                        models.push(UpdateModel { filter: filter,
                                                  update: update,
                                                  upsert: upsert, multi: false,
                                                  is_replace: false }),
                    WriteModel::UpdateMany { filter, update, upsert } =>
                        models.push(UpdateModel { filter: filter,
                                                  update: update,
                                                  upsert: upsert, multi: true,
                                                  is_replace: false }),
                    _ => return Some(model)
                }
            }
        }

        None
    }
}
