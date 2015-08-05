#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate rand;
extern crate rustc_serialize;
extern crate nalgebra;

mod apm;
mod client;
mod json;
mod sdam;
mod server_selection;
