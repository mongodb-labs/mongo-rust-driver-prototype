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

use mongo::client::*;
use mongo::db::*;

#[test]
fn test_uri_node_parse() {
    let mut uris_pass;
    let mut uris_fail;
    let mut err_str = ~"";
    let client = @Client::new();

    // ***NODE_SIMPLE***
    uris_pass = ~[
        "mongodb://localhost",
        "mongodb://localhost/",
        "mongodb://localhost/?",
        "mongodb://localhost:27017",
        "mongodb://localhost:27017/",
        "mongodb://localhost:27017/?",
        "mongodb://127.0.0.1",
        "mongodb://127.0.0.1/",
        "mongodb://127.0.0.1/?",
        "mongodb://127.0.0.1:27017",
        "mongodb://127.0.0.1:27017/",
        "mongodb://127.0.0.1:27017/?"
    ];
    expect_pass(client, &mut err_str, uris_pass);

    uris_fail = ~[
        "monogdb://localhost",
        "mongodb:/localhost",
        "mongodb://loaclhost",
        "mongodb://localhost?",
        "mongodb://loclhost/?"
    ];
    expect_fail(client, &mut err_str, uris_fail);

    // ***NODE_LOGIN***
    // set up authentication
    match client.connect(~"127.0.0.1", 27017) {
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
        //"mongodb://admin_user:admin_pwd@localhost",   // Rust URL parser
        "mongodb://admin_user:admin_pwd@localhost/",
        "mongodb://admin_user:admin_pwd@localhost/?",
        //"mongodb://admin_user:admin_pwd@localhost:27017",
        "mongodb://admin_user:admin_pwd@localhost:27017/",
        "mongodb://admin_user:admin_pwd@localhost:27017/?",
        //"mongodb://admin_user:admin_pwd@127.0.0.1",
        "mongodb://admin_user:admin_pwd@127.0.0.1/",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?",
        //"mongodb://admin_user:admin_pwd@127.0.0.1:27017",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?"
    ];
    expect_pass(client, &mut err_str, uris_pass);

    uris_fail = ~[
        // incorrect credentials format
        //"mongodb://admin_user-admin_pwd@localhost",   // Rust URL parser rejets
        "mongodb://admin_user-admin_pwd@localhost/",
        "mongodb://admin_user-admin_pwd@localhost/?",
        //"mongodb://admin_user-admin_pwd@localhost:27017",
        "mongodb://admin_user-admin_pwd@localhost:27017/",
        "mongodb://admin_user-admin_pwd@localhost:27017/?",
        //"mongodb://admin_user-admin_pwd@127.0.0.1",
        "mongodb://admin_user-admin_pwd@127.0.0.1/",
        "mongodb://admin_user-admin_pwd@127.0.0.1/?",
        //"mongodb://admin_user-admin_pwd@127.0.0.1:27017",
        "mongodb://admin_user-admin_pwd@127.0.0.1:27017/",
        "mongodb://admin_user-admin_pwd@127.0.0.1:27017/?",
        "mongodb://db_user-db_pwd@localhost/uri_node_login_db",
        "mongodb://db_user-db_pwd@localhost/uri_node_login_db?",
        "mongodb://db_user-db_pwd@localhost:27017/uri_node_login_db",
        "mongodb://db_user-db_pwd@localhost:27017/uri_node_login_db?",
        "mongodb://db_user-db_pwd@127.0.0.1/uri_node_login_db",
        "mongodb://db_user-db_pwd@127.0.0.1/uri_node_login_db?",
        "mongodb://db_user-db_pwd@127.0.0.1:27017/uri_node_login_db",
        "mongodb://db_user-db_pwd@127.0.0.1:27017/uri_node_login_db?",
        // incorrect credentials
        //"mongodb://admin_user:admi_pwd@localhost",
        "mongodb://admin_user:admi_pwd@localhost/",
        "mongodb://admin_user:admi_pwd@localhost/?",
        //"mongodb://admin_user:admi_pwd@localhost:27017",
        "mongodb://admin_user:admi_pwd@localhost:27017/",
        "mongodb://admin_user:admi_pwd@localhost:27017/?",
        //"mongodb://admin_user:admi_pwd@127.0.0.1",
        "mongodb://admin_user:admi_pwd@127.0.0.1/",
        "mongodb://admin_user:admi_pwd@127.0.0.1/?",
        //"mongodb://admin_user:admi_pwd@127.0.0.1:27017",
        "mongodb://admin_user:admi_pwd@127.0.0.1:27017/",
        "mongodb://admin_user:admi_pwd@127.0.0.1:27017/?",
        "mongodb://db_user:d_pwd@localhost/uri_node_login_db",
        "mongodb://db_user:d_pwd@localhost/uri_node_login_db?",
        "mongodb://db_user:d_pwd@localhost:27017/uri_node_login_db",
        "mongodb://db_user:d_pwd@localhost:27017/uri_node_login_db?",
        "mongodb://db_user:d_pwd@127.0.0.1/uri_node_login_db",
        "mongodb://db_user:d_pwd@127.0.0.1/uri_node_login_db?",
        "mongodb://db_user:d_pwd@127.0.0.1:27017/uri_node_login_db",
        "mongodb://db_user:d_pwd@127.0.0.1:27017/uri_node_login_db?",
        /*// db but no credentials
        "mongodb://localhost/uri_node_login_db",
        "mongodb://localhost/uri_node_login_db?",
        "mongodb://localhost:27017/uri_node_login_db",
        "mongodb://localhost:27017/uri_node_login_db?",
        "mongodb://127.0.0.1/uri_node_login_db",
        "mongodb://127.0.0.1/uri_node_login_db?",
        "mongodb://127.0.0.1:27017/uri_node_login_db",
        "mongodb://127.0.0.1:27017/uri_node_login_db?",*/
    ];
    expect_fail(client, &mut err_str, uris_fail);

    // ***NODE_OPTIONS***
    // these options should all parse correctly, even if in some cases
    //      the option is meaningless
    uris_pass = ~[
        "mongodb://localhost/?w=0",
        "mongodb://localhost/?w=majority",
        "mongodb://localhost/?w=tag1:val1",
        "mongodb://localhost/?w=tag1:val1,tag2:val2",
        "mongodb://localhost/?wtimeoutMS=10",
        "mongodb://localhost/?w=0&wtimeoutMS=10",
        "mongodb://localhost/?w=majority&wtimeoutMS=10",
        "mongodb://localhost/?w=tag1:val1&wtimeoutMS=10",
        "mongodb://localhost/?w=tag1:val1,tag2:val2&wtimeoutMS=10",
        "mongodb://localhost/?w=tag1:val1,tag2:val2&wtimeoutMS=10&journal=true",
        "mongodb://localhost:27017/?w=0",
        "mongodb://localhost:27017/?w=majority",
        "mongodb://localhost:27017/?w=tag1:val1",
        "mongodb://localhost:27017/?w=tag1:val1,tag2:val2",
        "mongodb://localhost:27017/?wtimeoutMS=10",
        "mongodb://localhost:27017/?w=0&wtimeoutMS=10",
        "mongodb://localhost:27017/?w=majority&wtimeoutMS=10",
        "mongodb://localhost:27017/?w=tag1:val1&wtimeoutMS=10",
        "mongodb://localhost:27017/?w=tag1:val1,tag2:val2&wtimeoutMS=10",
        "mongodb://localhost:27017/?w=tag1:val1,tag2:val2&wtimeoutMS=10&journal=true",
        "mongodb://admin_user:admin_pwd@localhost/?w=0",
        "mongodb://admin_user:admin_pwd@localhost/?w=majority",
        "mongodb://admin_user:admin_pwd@localhost/?w=tag1:val1",
        "mongodb://admin_user:admin_pwd@localhost/?w=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost/?wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost/?w=0&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost/?w=majority&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost/?w=tag1:val1&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost/?w=tag1:val1,tag2:val2&wtimeoutMS=10&journal=false",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=0",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=majority",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=tag1:val1",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost:27017/?wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=0&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=majority&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=tag1:val1&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=tag1:val1,tag2:val2&wtimeoutMS=10&journal=false",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?w=0",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?w=tag1:val:1",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?w=tag1:val:1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?w=tag1:val:1,tag2:val2&wtimeoutMS=10",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?w=0",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?w=tag1:val:1",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?w=tag1:val:1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?w=tag1:val:1,tag2:val2&wtimeoutMS=10",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?w=0",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?w=tag1:val:1",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?w=tag1:val:1,tag2:val2",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?w=tag1:val:1,tag2:val2&wtimeoutMS=10&journal=true",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?w=0",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?w=tag1:val:1",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?w=tag1:val:1,tag2:val2",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?w=tag1:val:1,tag2:val2&wtimeoutMS=10&journal=false",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?w=0",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?w=tag1:val:1",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?w=tag1:val:1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?w=tag1:val:1,tag2:val2&wtimeoutMS=10&journal=true",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=0",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=tag1:val:1",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=tag1:val:1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=tag1:val:1,tag2:val2&wtimeoutMS=10&journal=false",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=tag1:val:1,tag2:val2;wtimeoutMS=10;journal=false",
    ];
    expect_pass(client, &mut err_str, uris_pass);

    uris_fail= ~[
        "mongodb://localhost/?invalid",
        "mongodb://localhost/?invalid=",
        "mongodb://localhost/?invalid=unknown",
        "mongodb://localhost:27017/?invalid",
        "mongodb://localhost:27017/?invalid=",
        "mongodb://localhost:27017/?invalid=unknown",
        "mongodb://127.0.0.1/?invalid",
        "mongodb://127.0.0.1/?invalid=",
        "mongodb://127.0.0.1/?invalid=unknown",
        "mongodb://127.0.0.1:27017/?invalid",
        "mongodb://127.0.0.1:27017/?invalid=",
        "mongodb://127.0.0.1:27017/?invalid=unknown",
        "mongodb://localhost/?journal=no",
        "mongodb://localhost/?wtimeoutMS=ten",
        "mongodb://localhost/?w=tag1:val1,tag2oops",
        "mongodb://localhost:27017/?journal=no",
        "mongodb://localhost:27017/?wtimeoutMS=ten",
        "mongodb://localhost:27017/?w=tag1:val1,tag2oops",
        "mongodb://127.0.0.1/?journal=no",
        "mongodb://127.0.0.1/?wtimeoutMS=ten",
        "mongodb://127.0.0.1/?w=tag1:val1,tag2oops",
        "mongodb://127.0.0.1:27017/?journal=no",
        "mongodb://127.0.0.1:27017/?wtimeoutMS=ten",
        "mongodb://127.0.0.1:27017/?w=tag1:val1,tag2oops",
        "mongodb://localhost/?readPreference=primary",
        "mongodb://127.0.0.1/?readPreference=primary",
        "mongodb://localhost:27017/?readPreference=primary",
        "mongodb://127.0.0.1:27017/?readPreference=primary",
        "mongodb://localhost/?readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1/?readPreferenceTags=tag1:val1",
        "mongodb://localhost:27017/?readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1:27017/?readPreferenceTags=tag1:val",
        "mongodb://localhost/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://localhost:27017/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1:27017/?readPreferenceTags=tag1:val,tag2:val2",
        "mongodb://localhost/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://localhost:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://127.0.0.1:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val",
        "mongodb://localhost/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://localhost:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://127.0.0.1:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost/?invalid",
        "mongodb://admin_user:admin_pwd@localhost/?invalid=",
        "mongodb://admin_user:admin_pwd@localhost/?invalid=unknown",
        "mongodb://admin_user:admin_pwd@localhost:27017/?invalid",
        "mongodb://admin_user:admin_pwd@localhost:27017/?invalid=",
        "mongodb://admin_user:admin_pwd@localhost:27017/?invalid=unknown",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?invalid",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?invalid=",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?invalid=unknown",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?invalid",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?invalid=",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?invalid=unknown",
        "mongodb://admin_user:admin_pwd@localhost/?journal=no",
        "mongodb://admin_user:admin_pwd@localhost/?wtimeoutMS=ten",
        "mongodb://admin_user:admin_pwd@localhost/?w=tag1:val1,tag2oops",
        "mongodb://admin_user:admin_pwd@localhost:27017/?journal=no",
        "mongodb://admin_user:admin_pwd@localhost:27017/?wtimeoutMS=ten",
        "mongodb://admin_user:admin_pwd@localhost:27017/?w=tag1:val1,tag2oops",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?journal=no",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?wtimeoutMS=ten",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?w=tag1:val1,tag2oops",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?journal=no",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?wtimeoutMS=ten",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?w=tag1:val1,tag2oops",
        "mongodb://admin_user:admin_pwd@localhost/?readPreference=primary",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?readPreference=primary",
        "mongodb://admin_user:admin_pwd@localhost:27017/?readPreference=primary",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?readPreference=primary",
        "mongodb://admin_user:admin_pwd@localhost/?readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@localhost:27017/?readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?readPreferenceTags=tag1:val",
        "mongodb://admin_user:admin_pwd@localhost/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost:27017/?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?readPreferenceTags=tag1:val,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@localhost:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val",
        "mongodb://admin_user:admin_pwd@localhost/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@localhost:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://admin_user:admin_pwd@127.0.0.1:27017/?readPreference=primaryPreferred&readPreferenceTags=tag1:val,tag2:val2",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?invalid",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?invalid=",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?invalid=unknown",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?invalid",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?invalid=",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?invalid=unknown",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?invalid",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?invalid=",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?invalid=unknown",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?invalid",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?invalid=",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?invalid=unknown",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?journal=no",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?wtimeoutMS=ten",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?w=tag1:val1,tag2oops",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?journal=no",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?wtimeoutMS=ten",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?w=tag1:val1,tag2oops",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?journal=no",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?wtimeoutMS=ten",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?w=tag1:val1,tag2oops",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?journal=no",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?wtimeoutMS=ten",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?w=tag1:val1,tag2oops",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?readPreference=primary",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?readPreference=primary",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?readPreference=primary",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?readPreference=primary",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?readPreferenceTags=tag1:val",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?readPreferenceTags=tag1:val,tag2:val2",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val",
        "mongodb://db_user:db_pwd@localhost/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@localhost:27017/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val1,tag2:val2",
        "mongodb://db_user:db_pwd@127.0.0.1:27017/uri_node_login_db?readPreference=primaryPreferred&readPreferenceTags=tag1:val,tag2:val2",
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
