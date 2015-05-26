extern crate libc;
extern crate mongodb;

use libc::size_t;

#[test]
fn it_works() {
    let doc = mongodb::get_sample_bson_doc();
    unsafe {
        assert_eq!((*doc).len, 14);
    }
    let s = mongodb::bson_to_json(doc);
    assert_eq!(s, "{ \"foo\" : 42 }");
    assert!(true);
}
