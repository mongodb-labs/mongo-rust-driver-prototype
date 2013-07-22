use bson::encode::*;

use mongo::db::*;

pub struct GridFS {
    db: ~DB
}
