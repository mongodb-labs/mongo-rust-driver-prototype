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
extern mod mongo;

use mongo::client::*;
use mongo::db::*;

#[test]
fn test_uri_rs_parse() {
    let mut uris_pass;
    let mut uris_fail;
    let mut err_str = ~"";
    let client = @Client::new();

    // ***RS_SIMPLE***
    // for rs containing 27018 and 27019
    uris_pass = ~[
        "mongodb://localhost:27018,localhost:27019",
        "mongodb://localhost:27018,localhost:27019/",
        "mongodb://localhost:27018,localhost:27019/?",
        "mongodb://127.0.0.1:27018,localhost:27019",
        "mongodb://127.0.0.1:27018,localhost:27019/",
        "mongodb://127.0.0.1:27018,localhost:27019/?",
    ];
    expect_pass(client, &mut err_str, uris_pass);

    // ***RS_LOGIN***
    // set up authentication and tags
    match client.connect_to_rs([(~"127.0.0.1", 27018)]) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str()),
    }
    let mut db = client.get_admin();
    match db.add_user(~"admin_user", ~"admin_pwd", ~[]) {
        Ok(_) => (),
        Err(e) => debug!(e.to_str()),
    }
    db = DB::new(~"uri_node_login_db", client);
    match db.add_user(~"db_user", ~"db_pwd", ~[]) {
        Ok(_) => (),
        Err(e) => debug!(e.to_str()),
    }
    client.disconnect();

    uris_pass = ~[
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/",
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/?",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/?",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db?",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?",
    ];
    expect_pass(client, &mut err_str, uris_pass);

    // ***RS_OPTIONS***
    uris_pass = ~[
        "mongodb://localhost:27018,localhost:27019/?readPreference=secondary",
        "mongodb://127.0.0.1:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://localhost:27018,localhost:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://localhost:27018,localhost:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreference=secondary&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://localhost:27018,localhost:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://127.0.0.1:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary",
        "mongodb://localhost:27018,localhost:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
        "mongodb://127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
        "mongodb://admin_user:admin_pwd@localhost:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27018,localhost:27019/?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
        "mongodb://db_user:db_pwd@localhost:27018,localhost:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
        "mongodb://db_user:db_pwd@127.0.0.1:27018,127.0.0.1:27019/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2&readPreference=secondary&readPreferenceTags=&readPreferenceTags=tag3:val3",
    ];
    expect_pass(client, &mut err_str, uris_pass);

    uris_fail = ~[
        "mongodb://localhost:27018,localhost:27019/?readPreference=secondary&readPreference=primary",
    ];
    expect_fail(client, &mut err_str, uris_fail);

    if err_str.len() > 0 { fail!(err_str); }
}

fn expect_pass(client : @Client, err_str : &mut ~str, uris : &[&str]) {
    for uris.iter().advance |&uri| {
        match client.connect_with_uri(uri) {
            Ok(_) => debug!(fmt!("%s: success", uri)),
            Err(e) => err_str.push_str(fmt!("%s: FAILED: %s\n", uri, e.to_str())),
        }
        client.disconnect();
    }
}

fn expect_fail(client : @Client, err_str : &mut ~str, uris : &[&str]) {
    for uris.iter().advance |&uri| {
        match client.connect_with_uri(uri) {
            Ok(_) => err_str.push_str(fmt!("%s: UNEXPECTED SUCCESS\n", uri)),
            Err(e) => debug!(fmt!("%s", e.to_str())),
        }
        client.disconnect();
    }
}
