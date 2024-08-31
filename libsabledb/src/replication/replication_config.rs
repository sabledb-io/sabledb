use crate::SableError;
use ini::Ini;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref FILE_LOCK: RwLock<u8> = RwLock::new(0);
}

#[derive(Default, Debug, Clone)]
pub enum ServerRole {
    #[default]
    Primary,
    Replica,
}

impl FromStr for ServerRole {
    type Err = SableError;
    fn from_str(s: &str) -> Result<Self, SableError> {
        match s.to_lowercase().as_str() {
            "replica" => Ok(ServerRole::Replica),
            _ => Ok(ServerRole::Primary),
        }
    }
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ServerRole::Primary => write!(f, "primary"),
            ServerRole::Replica => write!(f, "replica"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    /// Server role ("primary", "replica")
    pub role: ServerRole,
    /// If the role is `ServerRole::Replica`, this property holds
    /// the private address of the primary server.
    /// If the role is `Server::Primary`, this property holds the
    /// the private address on which this server accepts new replication clients
    pub address: String,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            role: ServerRole::Primary,
            address: "127.0.0.1:7379".to_string(),
        }
    }
}

impl ReplicationConfig {
    const REPLICATION_CONF: &'static str = "replication.ini";

    pub fn primary_config(address: String) -> Self {
        ReplicationConfig {
            role: ServerRole::Primary,
            address,
        }
    }

    /// Read replication configuration file from a given directory
    pub fn from_dir(configuration_dir: Option<&Path>, default_address: String) -> Self {
        let _guard = FILE_LOCK.read();
        let replication_conf = Self::file_path_from_dir(configuration_dir);

        // The replication configuration file is using an INI format
        let Ok(ini_file) = Ini::load_from_file(&replication_conf) else {
            return ReplicationConfig::primary_config(default_address);
        };

        let Some(replication) = ini_file.section(Some("replication")) else {
            tracing::warn!(
                "Replication configuration file {} does not contain a 'replication' section",
                replication_conf.display()
            );
            return ReplicationConfig::primary_config(default_address);
        };

        // read the role
        let role = if let Some(role) = replication.get("role") {
            ServerRole::from_str(role).expect("can't fail") // can not fail, see `from_str` impl above
        } else {
            ServerRole::Primary
        };

        // read the role
        let address = if let Some(address) = replication.get("address") {
            address.to_string()
        } else {
            default_address
        };
        ReplicationConfig { role, address }
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
        let mut ini = ini::Ini::default();
        ini.with_section(Some("replication"))
            .set("role", format!("{}", repl_config.role))
            .set("address", repl_config.address.clone());
        ini.write_to_file(&replication_conf)?;
        tracing::info!("Successfully updated file: {}", replication_conf.display());
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
