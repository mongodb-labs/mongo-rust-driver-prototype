extern crate libc;
use libc::size_t;

#[repr(C)]
struct BSONDocument {
    flags: u32,
    len: u32,
    padding: [u8;120],
}

#[link(name = "bson-1.0")]
extern {
    fn bson_get_major_version() -> i32;
    fn bson_new() -> *mut BSONDocument;
    fn bson_append_int32(bson: *const BSONDocument, key: *const u8, key_length: i32, value: i32) -> bool;
    fn bson_as_json(bson: *const BSONDocument, length: *mut size_t) -> *mut u8;
}

fn main() {
    let x = unsafe { bson_get_major_version() };
    println!("BSON major version is: {}", x);
    unsafe {
        let y = bson_new();
        println!("Y length is {}", (*y).len);

        let mut size = 0 as size_t;
        let s = bson_as_json(y, &mut size);
        println!("As JSON initial: {}", String::from_raw_parts(s, size as usize, size as usize));

        bson_append_int32(y, (b"foo").as_ptr(), 3, 42 as i32);
        println!("Y length is {} after adding foo=42", (*y).len);

        let mut size = 0 as size_t;
        let s = bson_as_json(y, &mut size);
        println!("As JSON: {}", String::from_raw_parts(s, size as usize, size as usize));
    }
}
