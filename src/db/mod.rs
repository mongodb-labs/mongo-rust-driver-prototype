pub mod options;
pub mod roles;

use auth::Authenticator;
use bson;
use bson::Bson;
use {Client, CommandType, ThreadedClient, Result};
use Error::{CursorNotFoundError, OperationError};
use coll::Collection;
use coll::options::FindOptions;
use common::{ReadPreference, WriteConcern};
use cursor::{Cursor, DEFAULT_BATCH_SIZE};
use self::options::{CreateCollectionOptions, CreateUserOptions, UserInfoOptions};
use self::roles::Role;
use std::sync::Arc;

/// Interfaces with a MongoDB database.
pub struct DatabaseInner {
    pub name: String,
    pub client: Client,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
}

pub type Database = Arc<DatabaseInner>;

pub trait ThreadedDatabase {
    /// Creates a database representation with optional read and write controls.
    fn open(client: Client, name: &str, read_preference: Option<ReadPreference>,
            write_concern: Option<WriteConcern>) -> Database;
    fn auth(&self, user: &str, password: &str) -> Result<()>;
    fn collection(&self, coll_name: &str) -> Collection;
    fn collection_with_prefs(&self, coll_name: &str, create: bool,
                             read_preference: Option<ReadPreference>,
                             write_concern: Option<WriteConcern>) -> Collection;
    fn get_req_id(&self) -> i32;
    fn command_cursor(&self, spec: bson::Document, cmd_type: CommandType,
                      read_pref: ReadPreference) -> Result<Cursor>;
    fn command(&self, spec: bson::Document, cmd_type: CommandType,
               read_preference: Option<ReadPreference>) -> Result<bson::Document>;
    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor>;
    fn list_collections_with_batch_size(&self, filter: Option<bson::Document>,
                                        batch_size: i32) -> Result<Cursor>;
    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>>;
    fn create_collection(&self, name: &str,
                         options: Option<CreateCollectionOptions>) -> Result<()>;
    fn create_user(&self, name: &str, password: &str,
                   options: Option<CreateUserOptions>) -> Result<()>;
    fn drop_all_users(&self, write_concern: Option<WriteConcern>) -> Result<(i32)>;
    fn drop_collection(&self, name: &str) -> Result<()>;
    fn drop_database(&self) -> Result<()>;
    fn drop_user(&self, name: &str, Option<WriteConcern>) -> Result<()>;
    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>>;
    fn get_user(&self, user: &str,
                options: Option<UserInfoOptions>) -> Result<bson::Document>;
    fn get_users(&self, users: Vec<&str>,
                 options: Option<UserInfoOptions>) -> Result<Vec<bson::Document>>;
}

impl ThreadedDatabase for Database {
    /// Creates a database representation with optional read and write controls.
    fn open(client: Client, name: &str, read_preference: Option<ReadPreference>,
            write_concern: Option<WriteConcern>) -> Database {
        let rp = read_preference.unwrap_or(client.read_preference.to_owned());
        let wc = write_concern.unwrap_or(client.write_concern.to_owned());

        Arc::new(DatabaseInner {
            name: name.to_owned(),
            client: client,
            read_preference: rp,
            write_concern: wc,
        })
    }

    /// Logs in a user using the SCRAM-SHA-1 mechanism
    fn auth(&self, user: &str, password: &str) -> Result<()> {
        let authenticator = Authenticator::new(self.clone());
        authenticator.auth(user, password)
    }

    /// Creates a collection representation with inherited read and write controls.
    fn collection(&self, coll_name: &str) -> Collection {
        Collection::new(self.clone(), coll_name, false, Some(self.read_preference.to_owned()), Some(self.write_concern.to_owned()))
    }

    /// Creates a collection representation with custom read and write controls.
    fn collection_with_prefs(&self, coll_name: &str, create: bool,
                             read_preference: Option<ReadPreference>,
                             write_concern: Option<WriteConcern>) -> Collection {
        Collection::new(self.clone(), coll_name, create, read_preference, write_concern)
    }

    /// Return a unique operational request id.
    fn get_req_id(&self) -> i32 {
        self.client.get_req_id()
    }

    /// Generates a cursor for a relevant operational command.
    fn command_cursor(&self, spec: bson::Document, cmd_type: CommandType,
                      read_pref: ReadPreference) -> Result<Cursor> {
        Cursor::command_cursor(self.client.clone(), &self.name[..], spec, cmd_type, read_pref)
    }

    /// Sends an administrative command over find_one.
    fn command(&self, spec: bson::Document, cmd_type: CommandType,
               read_preference: Option<ReadPreference>) -> Result<bson::Document> {

        let coll = self.collection("$cmd");
        let mut options = FindOptions::new();
        options.batch_size = 1;
        options.read_preference = read_preference;
        let res = try!(coll.find_one_with_command_type(Some(spec.clone()), Some(options),
                                                       cmd_type));
        res.ok_or(OperationError(format!("Failed to execute command with spec {:?}.", spec)))
    }

    /// Returns a list of collections within the database.
    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor> {
        self.list_collections_with_batch_size(filter, DEFAULT_BATCH_SIZE)
    }

    fn list_collections_with_batch_size(&self, filter: Option<bson::Document>,
                                        batch_size: i32) -> Result<Cursor> {

        let mut spec = bson::Document::new();
        let mut cursor = bson::Document::new();

        cursor.insert("batchSize".to_owned(), Bson::I32(batch_size));
        spec.insert("listCollections".to_owned(), Bson::I32(1));
        spec.insert("cursor".to_owned(), Bson::Document(cursor));
        if filter.is_some() {
            spec.insert("filter".to_owned(), Bson::Document(filter.unwrap()));
        }

        self.command_cursor(spec, CommandType::ListCollections, self.read_preference.to_owned())
    }


    /// Returns a list of collection names within the database.
    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>> {
        let mut cursor = try!(self.list_collections(filter));
        let mut results = vec![];
        loop {
            match cursor.next() {
                Some(Ok(doc)) => if let Some(&Bson::String(ref name)) = doc.get("name") {
                    results.push(name.to_owned());
                },
                Some(Err(err)) => return Err(err),
                None => return Ok(results),
            }
        }
    }

    /// Creates a new collection.
    ///
    /// Note that due to the implicit creation of collections during insertion, this
    /// method should only be used to instantiate capped collections.
    fn create_collection(&self, name: &str,
                         options: Option<CreateCollectionOptions>) -> Result<()> {
        let coll_options = options.unwrap_or(CreateCollectionOptions::new());
        let mut doc = doc! {
            "create" => name,
            "capped" => (coll_options.capped),
            "auto_index_id" => (coll_options.auto_index_id)
        };

        if let Some(i) = coll_options.size {
            doc.insert("size".to_owned(), Bson::I64(i));
        }

        if let Some(i) = coll_options.max {
            doc.insert("max".to_owned(), Bson::I64(i));
        }

        let flag_one = if coll_options.use_power_of_two_sizes { 1 } else { 0 };
        let flag_two = if coll_options.no_padding { 2 } else { 0 };

        doc.insert("flags".to_owned(), Bson::I32(flag_one + flag_two));

        self.command(doc, CommandType::CreateCollection, None).map(|_| ())
    }

    /// Creates a new user.
    fn create_user(&self, name: &str, password: &str,
                   options: Option<CreateUserOptions>) -> Result<()> {
        let user_options = options.unwrap_or(CreateUserOptions::new());
        let mut doc = doc! {
            "createUser" => name,
            "pwd" => password
        };

        if let Some(data) = user_options.custom_data {
            doc.insert("customData".to_owned(), Bson::Document(data));
        }

        doc.insert("roles".to_owned(), Role::to_bson_array(user_options.roles));

        if let Some(concern) = user_options.write_concern {
            doc.insert("writeConcern".to_owned(), Bson::Document(concern.to_bson()));
        }

        self.command(doc, CommandType::CreateUser, None).map(|_| ())
    }

    /// Permanently deletes all users from the database.
    fn drop_all_users(&self, write_concern: Option<WriteConcern>) -> Result<(i32)> {
        let mut doc = doc! { "dropAllUsersFromDatabase" => 1 };

        if let Some(concern) = write_concern {
            doc.insert("writeConcern".to_owned(), Bson::Document(concern.to_bson()));
        }

        let response = try!(self.command(doc, CommandType::DropAllUsers, None));

        match response.get("n") {
            Some(&Bson::I32(i)) => Ok(i),
            Some(&Bson::I64(i)) => Ok(i as i32),
            _ => Err(CursorNotFoundError)
        }
    }

    /// Permanently deletes the collection from the database.
    fn drop_collection(&self, name: &str) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("drop".to_owned(), Bson::String(name.to_owned()));
        try!(self.command(spec, CommandType::DropCollection, None));
        Ok(())
    }


    /// Permanently deletes the database from the server.
    fn drop_database(&self) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("dropDatabase".to_owned(), Bson::I32(1));
        try!(self.command(spec, CommandType::DropDatabase, None));
        Ok(())
    }

    /// Permanently deletes the user from the database.
    fn drop_user(&self, name: &str, write_concern: Option<WriteConcern>) -> Result<()> {
        let mut doc = doc! { "dropUser" => name };

        if let Some(concern) = write_concern {
            doc.insert("writeConcern".to_owned(), Bson::Document(concern.to_bson()));
        }

        self.command(doc, CommandType::DropUser, None).map(|_| ())
    }

    /// Retrieves information about all users in the database.
    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>> {
        let doc = doc! {
            "usersInfo" => 1,
            "showCredentials" => show_credentials
        };

        let out = try!(self.command(doc, CommandType::GetUsers, None));
        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError)
        };

        let mut users = vec![];

        for bson in vec {
            match bson {
                Bson::Document(doc) => users.push(doc),
                _ => return Err(CursorNotFoundError)
            };
        }

        Ok(users)
    }

    /// Retrives information about a given user from the database.
    fn get_user(&self, user: &str,
                options: Option<UserInfoOptions>) -> Result<bson::Document> {
        let info_options = options.unwrap_or(UserInfoOptions::new());

        let doc = doc! {
            "usersInfo" => { "user" => user, "db" => (Bson::String(self.name.to_owned())) },
            "showCredentials" => (info_options.show_credentials),
            "showPrivileges" => (info_options.show_privileges)
        };

        let out = match self.command(doc, CommandType::GetUser, None) {
            Ok(doc) => doc,
            Err(e) => return Err(e)
        };

        let users = match out.get("users") {
            Some(&Bson::Array(ref v)) => v.clone(),
            _ => return Err(CursorNotFoundError)
        };

        match users.first() {
            Some(&Bson::Document(ref doc)) => Ok(doc.clone()),
            _ => Err(CursorNotFoundError)
        }
    }

    /// Retrives information about a given set of users from the database.
    fn get_users(&self, users: Vec<&str>,
                 options: Option<UserInfoOptions>) -> Result<Vec<bson::Document>> {
        let info_options = options.unwrap_or(UserInfoOptions::new());
        let vec = users.into_iter().map(|user| {
            let doc = doc! { "user" => user, "db" => (Bson::String(self.name.to_owned())) };
            Bson::Document(doc)
        }).collect();

        let doc = doc! {
            "usersInfo" => (Bson::Array(vec)),
            "showCredentials" => (info_options.show_credentials),
            "showPrivileges" => (info_options.show_privileges)
        };

        let out = try!(self.command(doc, CommandType::GetUsers, None));
        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError)
        };

        let mut users = vec![];

        for bson in vec {
            match bson {
                Bson::Document(doc) => users.push(doc),
                _ => return Err(CursorNotFoundError)
            };
        }

        Ok(users)
    }
}
