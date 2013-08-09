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

#[deriving(Clone,Eq)]
pub struct Person {
    _id : Option<Document>,
    ind : Option<i32>,
    name : Option<~str>,
    id_str : Option<~str>,
    val : Option<i32>,
    point : Option<(f64, f64)>,
}

impl BsonFormattable for Person {
    pub fn to_bson_t(&self) -> Document {
        let mut bson_doc = BsonDocument::new();
        if self.ind.is_some() {
            bson_doc.put(~"ind", Int32(self.ind.clone().unwrap()));
        }
        if self.name.is_some() {
            bson_doc.put(~"name", UString(self.name.clone().unwrap()));
        }
        if self.id_str.is_some() {
             bson_doc.put(~"id_str", UString(self.id_str.clone().unwrap()));
        }
        if self.val.is_some() {
            bson_doc.put(~"val", Int32(self.val.clone().unwrap()));
        }
        let mut sub_bson_doc = BsonDocument::new();
        if self.point.is_some() {
            let (x, y) = self.point.clone().unwrap();
            sub_bson_doc.put(~"x", Double(x));
            sub_bson_doc.put(~"y", Double(y));
            bson_doc.put(~"point", Embedded(~sub_bson_doc));
        }

        if self._id.is_some() {
            bson_doc.put(~"_id", self._id.clone().unwrap());
        }

        Embedded(~bson_doc)
    }

    pub fn from_bson_t(doc : &Document) -> Result<Person, ~str> {
        let bson_doc = match *doc {
            Embedded(ref bson) => bson.clone(),
            _ => return Err(~"not Person struct (not Embedded BsonDocument)"),
        };

        let _id = match bson_doc.find(~"_id") {
            None => None,
            Some(d) => Some(d.clone()),
        };

        let ind = match bson_doc.find(~"ind") {
            None => None,
            Some(d) => match d {
                &Int32(ref i) => Some(*i),
                _ => return Err(~"not Person struct (ind field not Int32)"),
            }
        };

        let name = match bson_doc.find(~"name") {
            None => None,
            Some(d) => match d {
                &UString(ref i) => Some(i.clone()),
                _ => return Err(~"not Person struct (name field not UString)"),
            }
        };

        let id_str = match bson_doc.find(~"id_str") {
            None => None,
            Some(d) => match d {
                &UString(ref i) => Some(i.clone()),
                _ => return Err(~"not Person struct (id_str field not UString)"),
            }
        };

        let val = match bson_doc.find(~"val") {
            None => None,
            Some(d) => match d {
                &Int32(ref i) => Some(*i),
                _ => return Err(~"not Person struct (val field not Int32)"),
            }
        };

        let point_doc = match bson_doc.find(~"point") {
            None => None,
            Some(d) => match d {
                &Embedded(ref i) => Some(i.clone()),
                _ => return Err(~"not Person struct (point field not Embedded BsonDocument)"),
            }
        };

        let point = if point_doc.is_some() {
            let tmp = point_doc.clone().unwrap();
            let x = match tmp.find(~"x") {
                None => return Err(~"not Person struct (no x field in point)"),
                Some(d) => match d {
                    &Double(ref i) => *i,
                    _ => return Err(~"not Person struct (x field in point not Double)"),
                }
            };

            let y = match tmp.find(~"y") {
                None => return Err(~"not Person struct (no y field in point)"),
                Some(d) => match *d {
                    Double(i) => i,
                    _ => return Err(~"not Person struct (y field in point not Double)"),
                }
            };

            Some((x,y))
        } else { None };

        Ok(Person { _id : _id, ind : ind, name : name, id_str : id_str, val : val, point : point })
    }
}

impl ToStr for Person {
    pub fn to_str(&self) -> ~str {
        fmt!("Person {
                \t_id:\t%?,
                \tind:\t%?,
                \tname:\t%?,
                \tid_str:\t%?,
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
            ind : Some(ind),
            name : Some(name),
            id_str : Some(silly_id),
            val : Some(val),
            point : Some((rng.gen(), rng.gen())),
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
