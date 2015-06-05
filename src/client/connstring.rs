use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::vec::Vec;

pub const DEFAULT_PORT: u16 = 27017;
pub const URI_SCHEME: &'static str = "mongodb://";

/// MongoDB connection types.
pub enum ConnectionType {
    Invalid,
    Master,
    Pair,
    Set,
}

/// Encapsulates the hostname and port of a host.
pub struct Host {
    pub host_name: String,
    pub port: u16,
}

impl Host {
    /// Creates a new Host struct.
    pub fn new(host_name: String, port: u16) -> Host {
        Host {
            host_name: host_name,
            port: port,
        }
    }
}

/// Encapsulates the options and read preference tags of a MongoDB connection.
pub struct ConnectionOptions {
    pub options: HashMap<String, String>,
    pub read_pref_tags: Vec<String>,
}

impl ConnectionOptions {
    /// Creates a new ConnectionOptions struct.
    pub fn new(options: HashMap<String, String>, read_pref_tags: Vec<String>) -> ConnectionOptions {
        ConnectionOptions {
            options: options,
            read_pref_tags: read_pref_tags,
        }
    }

    /// Retrieves an option using a borrowed String
    pub fn get(&self, key: &String) -> Option<&String> {
        self.options.get(key)
    }

    /// Retrieves an option using a borrowed str
    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.options.get(&key.to_owned()) {
            Some(val) => Some(&val),
            None => None,
        }
    }
}

/// Encapsulates information for connection to a single MongoDB host or replicated set.
pub struct ConnectionString {
    ctype: ConnectionType,
    pub hosts: Vec<Host>,
    pub string: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub collection: Option<String>,
    pub options: Option<ConnectionOptions>,
}

impl ConnectionString {
    /// Creates a new ConnectionString for a single, unreplicated host.
    pub fn new(host_name: &str, port: u16) -> ConnectionString {
        let host = Host::new(host_name.to_owned(), port);
        ConnectionString::new_from_host(host)
    }

    fn new_from_host(host: Host) -> ConnectionString {
        ConnectionString {
            ctype: ConnectionType::Master,
            hosts: vec![host],
            string: None,
            user: None,
            password: None,
            database: Some("test".to_owned()),
            collection: None,
            options: None,
        }
    }

    /// Returns the connection type as an owned string.
    pub fn type_to_string(ctype: ConnectionType) -> String {
        let string = match ctype {
            ConnectionType::Invalid => "invalid",
            ConnectionType::Master => "master",
            ConnectionType::Pair => "pair",
            ConnectionType::Set => "set",
        };
        string.to_owned()
    }
}

/// Parses a MongoDB connection string URI as defined by
/// [the manual](http://docs.mongodb.org/manual/reference/connection-string/).
pub fn parse(address: &str) -> Result<ConnectionString, &str> {
    if !address.starts_with(URI_SCHEME) {
        return Err("MongoDB connection string must start with 'mongodb://'")
    }

    // Remove scheme
    let addr = &address[URI_SCHEME.len()..];

    let mut ctype: ConnectionType;
    let mut hosts: Vec<Host>;
    let mut user: Option<String> = None;
    let mut password: Option<String> = None;
    let mut database: Option<String> = Some("test".to_owned());
    let mut collection: Option<String> = None;
    let mut options: Option<ConnectionOptions> = None;

    // Split on host/path
    let (host_str, path_str) = match addr.contains(".sock") {
        true => {
            let (host_part, path_part) = rpartition(addr, "/");
            let host_test_uri = &format!("{}{}", URI_SCHEME, host_part);
            match parse(host_test_uri) {
                Ok(_) => (host_part, path_part),
                Err(_) => (addr, ""),
            }
        },
        false => partition(addr, "/")
    };

    if path_str.len() == 0 && host_str.contains("?") {
        return Err("A '/' is required between the host list and any options.");
    }

    // Split on authentication and hosts
    if host_str.contains("@") {
        let (user_info, host_string) = rpartition(host_str, "@");
        let (u,p) = try!(parse_user_info(user_info));
        user = Some(u.to_owned());
        password = Some(p.to_owned());
        hosts = try!(split_hosts(host_string));
    } else {
        hosts = try!(split_hosts(host_str));
    }

    // Match connection type
    ctype = match hosts.len() {
        1 => ConnectionType::Master,
        2 =>  ConnectionType::Pair,
        n => {
            if n > 2 {
                ConnectionType::Set
            } else {
                ConnectionType::Invalid
            }
        }
    };

    let mut opts = "";

    // Split on database name, collection, and options
    if path_str.len() > 0 {
        if path_str.starts_with("?") {
            opts = &path_str[1..];
        } else {
            let (dbase, options) = partition(path_str, "?");
            let (dbase_new, coll) = partition(dbase, ".");
            database = Some(dbase_new.to_owned());
            collection = Some(coll.to_owned());
            opts = options;
        }
    }

    // Collect options if any exist
    if opts.len() > 0 {
        options = Some(split_options(opts).unwrap());
    }

    Ok(ConnectionString {
        ctype: ctype,
        hosts: hosts,
        string: Some(address.to_owned()),
        user: user,
        password: password,
        database: database,
        collection: collection,
        options: options,
    })
}

// Parse user information of the form user:password
fn parse_user_info(user_info: &str) -> Result<(&str, &str), &str> {
    let (user, password) = rpartition(user_info, ":");
    if user_info.contains("@") || user.contains(":") {
        return Err("':' or '@' characters in a username or password must be escaped according to RFC 2396.")
    }
    if user.len() == 0 {
        return Err("The empty string is not a valid username.")
    }
    Ok((user, password))
}

// Parses a literal IPv6 literal host entity of the form [host] or [host]:port
fn parse_ipv6_literal_host(entity: &str) -> Result<Host, &str> {
    match entity.find("]") {
        Some(_) => {
            match entity.find("]:") {
                Some(idx) => {
                    let port = &entity[idx+2..];
                    match port.parse::<u16>() {
                        Ok(val) => Ok(Host::new(entity[1..idx].to_ascii_lowercase(), val)),
                        Err(_) => Err("Port must be an integer."),
                    }
                },
                None => Ok(Host::new(entity[1..].to_ascii_lowercase(), DEFAULT_PORT)),
            }
        }
        None => Err("An IPv6 address must be enclosed in '[' and ']' according to RFC 2732."),
    }
}

// Parses a host entity of the form host or host:port, and redirects IPv6 entities.
// All host names are lowercased.
fn parse_host(entity: &str) -> Result<Host, &str> {
    if entity.starts_with("[") {
        parse_ipv6_literal_host(entity)
    } else if entity.contains(":") {
        let (host, port) = partition(entity, ":");
        if port.contains(":") {
            return Err("Reserved characters such as ':' must
                        be escaped according to RFC 2396. An IPv6 address literal
                        must be enclosed in '[' and according to RFC 2732.");
        }
        match port.parse::<u16>() {
            Ok(val) => Ok(Host::new(host.to_ascii_lowercase(), val)),
            Err(_) => Err("Port must be an integer"),
        }
    } else {
        Ok(Host::new(entity.to_ascii_lowercase(), DEFAULT_PORT))
    }
}

// Splits and parses comma-separated hosts.
fn split_hosts(host_str: &str) -> Result<Vec<Host>, &str> {
    let mut hosts: Vec<Host> = Vec::new();
    for entity in host_str.split(",") {
        if entity.len() == 0 {
            return Err("Empty host, or extra comma in host list.");
        }
        let host = try!(parse_host(entity));
        hosts.push(host);
    }
    Ok(hosts)
}

// Parses the delimited string into its options and Read Preference Tags.
fn parse_options(opts: &str, delim: Option<&str>) -> ConnectionOptions {
    let mut options: HashMap<String, String> = HashMap::new();
    let mut read_pref_tags: Vec<String> = Vec::new();

    // Split and collect options into a vec
    let opt_list = match delim {
        Some(delim) => opts.split(delim).collect(),
        None => vec!(opts)
    };

    // Build the map and tag vec
    for opt in opt_list {
        let (key, val) = partition(opt, "=");
        if key.to_ascii_lowercase() == "readpreferencetags" {
            read_pref_tags.push(val.to_owned());
        } else {
            options.insert(key.to_owned(), val.to_owned());
        }
    }

    ConnectionOptions::new(options, read_pref_tags)
}

// Determines the option delimiter and offloads parsing to parse_options.
fn split_options(opts: &str) -> Result<ConnectionOptions, &str> {
    let and_idx = opts.find("&");
    let semi_idx = opts.find(";");
    let mut delim = None;

    if and_idx != None && semi_idx != None {
        return Err("Cannot mix '&' and ';' for option separators.");
    } else if and_idx != None {
        delim = Some("&");
    } else if semi_idx != None {
        delim = Some(";");
    } else if opts.find("=") == None {
        return Err("InvalidURI: MongoDB URI options are key=value pairs.");
    }
    let options = parse_options(opts, delim);
    Ok(options)
}

// Partitions a string around the left-most occurrence of the separator, if it exists.
fn partition<'a>(string: &'a str, sep: &str) -> (&'a str, &'a str) {
    return match string.find(sep) {
        Some(idx) => (&string[..idx], &string[idx+1..]),
        None => (string, ""),
    }
}

// Partitions a string around the right-most occurrence of the separator, if it exists.
fn rpartition<'a>(string: &'a str, sep: &str) -> (&'a str, &'a str) {
    return match string.rfind(sep) {
        Some(idx) => (&string[..idx], &string[idx+1..]),
        None => (string, ""),
    }
}
