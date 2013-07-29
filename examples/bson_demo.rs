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

extern mod bson;

use bson::encode::*;
use bson::decode::*;
use bson::formattable::*;

fn main() {
    println(fmt!("5 as a bson repr: %s", 5f.to_bson_t().to_str()));
    println(fmt!("5 as a bson string: %s", 5f.to_bson_t().to_bson().to_str()));
    println("");
    println(fmt!("[1,2,3] as a bson repr: %s", (~[1u16,2,3]).to_bson_t().to_str()));
    println(fmt!("[1,2,3] as a bson string: %s", (~[1u16,2,3]).to_bson_t().to_bson().to_str()));
    println("");
    println(fmt!("'foo' as a bson repr: %s", (~"foo").to_bson_t().to_str()));
    println(fmt!("'foo' as a bson string: %s", (~"foo").to_bson_t().to_bson().to_str()));
    println("");
    println(fmt!("{foo: Timestamp(500,300)} as a bson rep: %s", (~"{ 'foo': { '$timestamp': { 't': 500, 'i': 300 } } }").to_bson_t().to_str()));
    println(fmt!("{foo: Timestamp(500,300)} as a bson string: %s", (~"{ 'foo': { '$timestamp': { 't': 500, 'i': 300 } } }").to_bson_t().to_bson().to_str()));
    println("");
    println(fmt!("New FooStruct as a bson repr: %s", (FooStruct::new()).to_bson_t().to_str()));
    println(fmt!("New FooStruct as a bson string: %s", (FooStruct::new()).to_bson_t().to_bson().to_str()));
    println("");
    let foo_struct = FooStruct::new();
    let foo_bson_repr = (FooStruct::new()).to_bson_t();
    let foo_bson_str = (FooStruct::new()).to_bson_t().to_bson();
    println(fmt!("Roundtripping representations: %? -> %s -> %?",
        foo_struct, foo_bson_repr.to_str(),
        BsonFormattable::from_bson_t::<FooStruct>(&foo_bson_repr)
        ));
    println(fmt!("Roundtripping strings: %? -> %s -> %?",
        FooStruct::new(), foo_bson_str.to_str(),
        (BsonFormattable::from_bson_t::<FooStruct>(&Embedded(~decode(foo_bson_str).unwrap()))).unwrap()
        ));
}

#[deriving(ToStr)]
impl FooStruct {
    fn new() -> FooStruct {
        FooStruct { flag: true, widget: false, value: 0 }
    }
}

impl BsonFormattable for FooStruct {
    fn to_bson_t(&self) -> Document {
        let mut doc = BsonDocument::new();
        doc.put(~"flag", Bool(self.flag));
        doc.put(~"widget", Bool(self.widget));
        doc.put(~"value", self.value.to_bson_t());
        Embedded(~doc)
    }

    fn from_bson_t(doc: &Document) -> Result<FooStruct, ~str> {
        match *doc {
            Embedded(ref d) => {
                let mut s = FooStruct::new();
                if d.contains_key(~"flag") {
                    s.flag = match d.find(~"flag").unwrap() {
                        &Bool(b) => b,
                        _ => return Err(~"flag must be boolean")
                    }
                }
                if d.contains_key(~"widget") {
                    s.widget = match d.find(~"widget").unwrap() {
                        &Bool(b) => b,
                        _ => return Err(~"widget must be boolean")
                    }
                }
                if d.contains_key(~"value") {
                    s.value = match d.find(~"value").unwrap() {
                        &Int32(i) => i as uint,
                        &Int64(i) => i as uint,
                        &Double(f) => f as uint,
                        _ => return Err(~"value must be numeric")
                    }
                }
                return Ok(s);
            },
            _ => fail!("can only format Embedded as FooStruct")
        }
    }
}

struct FooStruct {
    flag: bool,
    widget: bool,
    value: uint
}
