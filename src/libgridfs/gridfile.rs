use bson::encode::*;

use mongo::coll::*;

pub struct GridIn {
    collection: @Collection,
    closed: bool
}

impl GridIn {
    pub fn new(coll: @Collection) -> GridIn {
        GridIn {
            collection: coll,
            closed: false
        }
    }


}
