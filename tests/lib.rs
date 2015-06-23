extern crate bson;

#[macro_use(add_to_doc, doc)]
extern crate mongodb;
extern crate rustc_serialize;
extern crate nalgebra;

mod client;
mod json;
mod macros;
