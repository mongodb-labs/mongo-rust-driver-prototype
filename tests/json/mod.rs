#[macro_use]
mod macros;

pub mod crud;
pub mod eq;
pub mod sdam;
pub mod server_selection;

use rustc_serialize::json::Object;

pub trait FromJson {
    fn from_json(object: &Object) -> Self;
}

pub trait FromJsonResult {
    fn from_json(object: &Object) -> Result<Self, String>;
}
