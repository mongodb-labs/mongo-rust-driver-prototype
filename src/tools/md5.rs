/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::int::range;
use std::libc::c_int;
use std::ptr::to_unsafe_ptr;
use std::to_bytes::*;

static L_END: bool = true;

#[link_args = "-lmd5"]
extern {
    fn md5_init(pms: *MD5State);
    fn md5_append(pms: *MD5State, data: *const u8, nbytes: c_int);
    fn md5_finish(pms: *MD5State, digest: *[u8,..16]);
}

priv struct MD5State {
    count: [u32,..2],
    abcd: [u32,..4],
    buf: [u8,..64]
}

impl MD5State {
    fn new(len: u64) -> MD5State {
        let mut c: [u32,..2] = [0u32,0];
        let l = len.to_bytes(L_END);
        c[0] |= l[0] as u32;
        c[0] |= (l[1] << 8) as u32;
        c[0] |= (l[2] << 16) as u32;
        c[0] |= (l[3] << 24) as u32;
        c[1] |= l[4] as u32;
        c[1] |= (l[5] << 8) as u32;
        c[1] |= (l[6] << 16) as u32;
        c[1] |= (l[7] << 24) as u32;

        MD5State {
            count: c,
            abcd: [0u32,0,0,0],
            buf: [
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0
                ]
        }
    }
}

priv fn md5(msg: &str) -> ~str {
    let msg_bytes = msg.to_bytes(L_END);
    let m = MD5State::new(msg_bytes.len() as u64);
    let digest: [u8,..16] = [
        0,0,0,0,
        0,0,0,0,
        0,0,0,0,
        0,0,0,0
    ];

    unsafe {
        md5_init(to_unsafe_ptr(&m));
        md5_append(to_unsafe_ptr(&m), to_unsafe_ptr(&(msg_bytes[0])), msg_bytes.len() as i32);
        md5_finish(to_unsafe_ptr(&m), to_unsafe_ptr(&digest));
    }

    let mut result: ~str = ~"";
    for range(0, 16) |i| {
        let mut byte = fmt!("%x", digest[i] as uint);
        if byte.len() == 1 {
            byte = (~"0").append(byte);
        }
        result.push_str(byte);
    }
    result
}

#[cfg(test)]
#[test]
fn md5_test() {
    assert_eq!(md5(~"hello"), ~"5d41402abc4b2a76b9719d911017c592");
    assert_eq!(md5(~"asdfasdfasdf"), ~"a95c530a7af5f492a74499e70578d150");
}
