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

use std::rand::*;
use std::rand::RngUtil;

use bson::encode::*;
use bson::formattable::*;

static MAX_LEN : uint = 16;

pub struct Person {
    _id : Option<Document>,
    ind : i32,
    name : ~str,
    id_str : ~str,
    val : i32,
    point : (f64, f64),
}

impl BsonFormattable for Person {
    pub fn to_bson_t(&self) -> Document {
        let mut bson_doc = BsonDocument::new();
        bson_doc.put(~"ind", Int32(self.ind));
        bson_doc.put(~"name", UString(copy self.name));
        bson_doc.put(~"id_str", UString(copy self.id_str));
        bson_doc.put(~"val", Int32(self.val));
        let mut sub_bson_doc = BsonDocument::new();
        let (x, y) = self.point;
        sub_bson_doc.put(~"x", Double(x));
        sub_bson_doc.put(~"y", Double(y));
        bson_doc.put(~"point", Embedded(~sub_bson_doc));

        if self._id.is_some() {
            bson_doc.put(~"_id", (copy self._id).unwrap());
        }

        Embedded(~bson_doc)
    }

    pub fn from_bson_t(doc : Document) -> Result<Person, ~str> {
        let bson_doc = match doc {
            Embedded(bson) => *bson,
            _ => return Err(~"not Person struct (not Embedded BsonDocument)"),
        };

        let _id = match bson_doc.find(~"_id") {
            None => None,
            Some(d) => Some(copy *d),
        };

        let ind = match bson_doc.find(~"ind") {
            None => return Err(~"not Person struct (no ind field)"),
            Some(d) => match *d {
                Int32(i) => i,
                _ => return Err(~"not Person struct (ind field not Int32)"),
            }
        };

        let name = match bson_doc.find(~"name") {
            None => return Err(~"not Person struct (no name field)"),
            Some(d) => match (copy *d) {
                UString(i) => i,
                _ => return Err(~"not Person struct (name field not UString)"),
            }
        };

        let id_str = match bson_doc.find(~"id_str") {
            None => return Err(~"not Person struct (no id_str field)"),
            Some(d) => match (copy *d) {
                UString(i) => i,
                _ => return Err(~"not Person struct (id_str field not UString)"),
            }
        };

        let val = match bson_doc.find(~"val") {
            None => return Err(~"not Person struct (no val field)"),
            Some(d) => match (copy *d) {
                Int32(i) => i,
                _ => return Err(~"not Person struct (val field not Int32)"),
            }
        };

        let point_doc = match bson_doc.find(~"point") {
            None => return Err(~"not Person struct (no point field)"),
            Some(d) => match (copy *d) {
                Embedded(i) => i,
                _ => return Err(~"not Person struct (point field not Embedded BsonDocument)"),
            }
        };

        let x = match point_doc.find(~"x") {
            None => return Err(~"not Person struct (no x field in point)"),
            Some(d) => match (copy *d) {
                Double(i) => i,
                _ => return Err(~"not Person struct (x field in point not Double)"),
            }
        };

        let y = match point_doc.find(~"y") {
            None => return Err(~"not Person struct (no y field in point)"),
            Some(d) => match *d {
                Double(i) => i,
                _ => return Err(~"not Person struct (y field in point not Double)"),
            }
        };

        Ok(Person { _id : _id, ind : ind, name : name, id_str : id_str, val : val, point : (x,y) })
    }
}

impl ToStr for Person {
    pub fn to_str(&self) -> ~str {
        fmt!("Person {
                \t_id:\t%?,
                \tind:\t%?,
                \tname:\t%s,
                \tid_str:\t%s,
                \tval:\t%?,
                \tpoint:\t%? }",
                self._id, self.ind, self.name, self.id_str, self.val, self.point)
    }
}

impl Person {
    pub fn new(_id : Option<Document>, ind : i32, name : ~str, rng : &mut IsaacRng) -> Person {
        let mut silly_id = ~"";
        let mut val = 0;
        for MAX_LEN.times {
            if rng.gen() {
                silly_id.push_char(rng.gen_char_from("abcdefghijklmnopqrstuvwxyz"));
                val += 1;
            }
        }

        Person {
            _id : _id,
            ind : ind,
            name : name,
            id_str : silly_id,
            val : val,
            point : (rng.gen(), rng.gen()),
        }
    }

    pub fn make_mob(mob_sz : uint) -> ~[Person] {
        let mut rng = rng();

        let mut i = 0;
        let mut people = ~[];
        for mob_sz.times {
            people.push(Person::new(None, i, fmt!("Person %?", i), &mut rng));
            i += 1;
        }

        people
    }
}

fn main() {
    let mini_mob = Person::make_mob(10);

    for mini_mob.iter().advance |&person| {
        println(person.to_str());
    }
}
