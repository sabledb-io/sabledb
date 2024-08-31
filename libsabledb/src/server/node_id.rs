use crate::{file, ServerOptions};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref NODE_ID: RwLock<String> = RwLock::<String>::default();
}

pub struct NodeId {}

impl NodeId {
    const NODE_FILE: &'static str = "NODE_ID";

    /// Return the current node ID
    pub fn current() -> String {
        NODE_ID.read().expect("NODE_ID current").clone()
    }

    /// Initialise the node ID by loading or creating it
    pub fn initialise(options: &ServerOptions) {
        let replication_conf = Self::file_path_from_dir(options);
        if let Some(node_id) = file::read_file_content(&replication_conf) {
            // update the global
            *NODE_ID.write().expect("NODE_ID write lock") = node_id;
        } else {
            // Create new UUID and update the file + the global
            let node_id = uuid::Uuid::new_v4().to_string();
            file::write_file_content(&replication_conf, &node_id);
            *NODE_ID.write().expect("NODE_ID write lock") = node_id;
        }
    }

    fn file_path_from_dir(options: &ServerOptions) -> PathBuf {
        let configuration_dir = options.general_settings.config_dir.as_deref();
        let mut replication_conf = match configuration_dir {
            Some(p) => p.to_path_buf(),
            None => match std::env::current_dir() {
                Ok(wd) => wd,
                Err(_) => Path::new(".").to_path_buf(),
            },
        };

        replication_conf.push(Self::NODE_FILE);
        replication_conf
    }
}
