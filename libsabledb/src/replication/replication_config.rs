use crate::SableError;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref FILE_LOCK: RwLock<u8> = RwLock::new(0);
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub enum ServerRole {
    #[default]
    Primary,
    Replica,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Server role ("primary", "replica")
    pub role: ServerRole,
    /// If the role is `ServerRole::Replica`, this property holds
    /// the IP of the primary server.
    /// If the role is `Server::Primary`, this property holds the
    /// the IP on which this server accepts new replication clients
    pub ip: String,
    /// When the role is `ServerRole::Replica`, this property holds
    /// the IP of the primary server
    /// If the role is `Server::Primary`, this property holds the
    /// the port on which this server accepts new replication clients
    pub port: u16,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            role: ServerRole::Primary,
            ip: "127.0.0.1".to_string(),
            port: 7379,
        }
    }
}

impl ReplicationConfig {
    const REPLICATION_CONF: &'static str = "replication.json";

    pub fn primary_config(ip: String, port: u16) -> Self {
        ReplicationConfig {
            role: ServerRole::Primary,
            ip,
            port,
        }
    }

    /// Read replication configuration file from a given directory
    pub fn from_dir(
        configuration_dir: Option<&Path>,
        default_listen_ip: String,
        default_port: u16,
    ) -> Self {
        let _guard = FILE_LOCK.read();
        let replication_conf = Self::file_path_from_dir(configuration_dir);

        let conf = if replication_conf.exists() {
            let path_buf = replication_conf.to_path_buf();
            match std::fs::read_to_string(path_buf.clone()) {
                Ok(content) => {
                    tracing::info!("Parsing replication config: {}", content);
                    match serde_json::from_str(&content) {
                        Ok(conf) => conf,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to de-serialize replication config from file {}. {:?}",
                                replication_conf.display(),
                                e
                            );
                            ReplicationConfig::primary_config(default_listen_ip, default_port)
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    ReplicationConfig::primary_config(default_listen_ip, default_port)
                }
                Err(e) => {
                    tracing::warn!("Could not read file: {}. {:?}", path_buf.display(), e);
                    ReplicationConfig::primary_config(default_listen_ip, default_port)
                }
            }
        } else {
            ReplicationConfig::primary_config(default_listen_ip, default_port)
        };
        let _ = Self::write_file_internal(&conf, configuration_dir);
        conf
    }

    /// Write `repl_config` to a file overriding previous content
    // (uses write lock)
    pub fn write_file(
        repl_config: &ReplicationConfig,
        configuration_dir: Option<&Path>,
    ) -> Result<(), SableError> {
        let _guard = FILE_LOCK.write();
        Self::write_file_internal(repl_config, configuration_dir)
    }

    /// Write `repl_config` to a file overriding previous content
    // (no locking in this version)
    fn write_file_internal(
        repl_config: &ReplicationConfig,
        configuration_dir: Option<&Path>,
    ) -> Result<(), SableError> {
        let replication_conf = Self::file_path_from_dir(configuration_dir);
        tracing::debug!("Updating {}", replication_conf.display());
        let json = match serde_json::to_string_pretty(repl_config) {
            Err(e) => {
                let errmsg = format!(
                    "Failed to convert struct {:?} into JSON. {:?}",
                    repl_config, e
                );
                return Err(SableError::OtherError(errmsg));
            }
            Ok(json) => json,
        };

        let mut file = std::fs::File::create(replication_conf.clone())?;
        file.write_all(json.as_bytes())?;
        tracing::debug!(
            "{} updated successfully with content:\n{}",
            replication_conf.display(),
            json
        );
        Ok(())
    }

    fn file_path_from_dir(configuration_dir: Option<&Path>) -> PathBuf {
        let mut replication_conf = match configuration_dir {
            Some(p) => p.to_path_buf(),
            None => match std::env::current_dir() {
                Ok(wd) => wd,
                Err(_) => Path::new(".").to_path_buf(),
            },
        };

        replication_conf.push(Self::REPLICATION_CONF);
        replication_conf
    }
}
