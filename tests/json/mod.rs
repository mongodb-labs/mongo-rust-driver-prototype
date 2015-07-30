#[macro_use]
mod macros;

pub mod crud;
pub mod eq;
pub mod sdam;

use rustc_serialize::json::Object;

pub trait FromJson {
    fn from_json(object: &Object) -> Self;
}
