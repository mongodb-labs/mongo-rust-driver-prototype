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

extern mod extra;
extern mod mongo;
extern mod bson;

pub mod fill_coll;

mod good_insert_single;
mod good_insert_batch_small;
mod good_insert_batch_big;
mod bad_insert_no_cont;
mod bad_insert_cont;
mod indices;
mod get_collections;
mod drop_collection;
mod sort;
mod limit_and_skip;
mod update;
mod capped_coll;
mod drop_db;
mod add_user;
mod authenticate;
mod logout;
mod validate;

#[cfg(shard)]
mod shard_dbs;

#[cfg(rs)]
// run in isolation unless there are 7 replica sets already set up
//      (or ready to be set up for rs_initiate), and the code
//      has been altered appropriately
mod rs_stepdown;            // 27018-27020 seed
mod rs_reconfig_tags;       // 37018 seed
mod rs_reconfig_priorities; // 37018 seed
mod rs_manual;              // 27018-27022 seed
mod rs_initiate;            // 37018-37022 hosts (uninitiated)
mod rs_members;             // 37018-37022 seed
mod rs_read_pref;           // 27018 seed
