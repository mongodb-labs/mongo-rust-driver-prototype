use super::framework::run_suite;

use std::fs;
use std::path::Path;

#[test]
fn server_selection_replica_set_no_primary_read() {
    let paths = fs::read_dir(&Path::new("tests/json/data/specs/source/server-selection/tests/server_selection/ReplicaSetNoPrimary/read")).unwrap();

    for path in paths {
        let path2 = path.unwrap().path();
        let filename = path2.to_string_lossy();
        if filename.ends_with(".json") {
            println!("running suite {}", &filename);
            run_suite(&filename)
        }
    }
}

#[test]
fn server_selection_replica_set_no_primary_write() {
    let paths = fs::read_dir(&Path::new("tests/json/data/specs/source/server-selection/tests/server_selection/ReplicaSetNoPrimary/write")).unwrap();

    for path in paths {
        let path2 = path.unwrap().path();
        let filename = path2.to_string_lossy();
        if filename.ends_with(".json") {
            println!("running suite {}", &filename);
            run_suite(&filename)
        }
    }
}
