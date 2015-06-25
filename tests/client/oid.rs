use mongodb::client::oid;
use rustc_serialize::hex::ToHex;

#[test]
fn deserialize() {
    let bytes: [u8; 12] = [
        0xDEu8,
        0xADu8,
        0xBEu8,
        0xEFu8,  // timestamp is 3735928559
        0xEFu8,
        0xCDu8,
        0xABu8,  // machine_id is 11259375
        0xFAu8,
        0x29u8,  // process_id is 10746
        0x11u8,
        0x22u8,
        0x33u8,  // increment is 1122867
        ];

    assert_eq!(3735928559 as u32, oid::get_timestamp(bytes));
    assert_eq!(11259375 as u32, oid::get_machine_id(bytes));
    assert_eq!(10746 as u16, oid::get_pid(bytes));
    assert_eq!(1122867 as u32, oid::get_counter(bytes));
}

#[test]
fn timestamp() {
    let time: u32 = 2000000;
    let oid = oid::with_timestamp(time);
    let timestamp = oid::get_timestamp(oid);
    assert_eq!(time, timestamp);
}

#[test]
fn string_oid() {
    let s = "123456789012123456789012";
    let oid_res = oid::with_string(s);
    assert!(oid_res.is_ok());
    let actual_s = oid_res.unwrap().to_hex();
    assert_eq!(s.to_owned(), actual_s);
}

#[test]
fn byte_string_oid() {
    let s = "541b1a00e8a23afa832b218e";
    let oid_res = oid::with_string(s);
    assert!(oid_res.is_ok());
    let oid = oid_res.unwrap();
    let bytes: [u8; 12] = [0x54u8, 0x1Bu8, 0x1Au8, 0x00u8,
                           0xE8u8, 0xA2u8, 0x3Au8, 0xFAu8,
                           0x83u8, 0x2Bu8, 0x21u8, 0x8Eu8];

    for i in 0..12 {
        assert_eq!(bytes[i], oid[i]);
    }
}
