use crate::{SableError, ServerOptions};
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
    const REPLICATION_CONF: &'static str = "REPLICATION";

    pub fn new_replica(primary_address: String) -> Self {
        ReplicationConfig {
            role: ServerRole::Replica,
            address: primary_address,
        }
    }

    pub fn new_primary(listen_address: String) -> Self {
        ReplicationConfig {
            role: ServerRole::Primary,
            address: listen_address,
        }
    }

    /// Construct `ReplicationConfig` from configuration file
    pub fn load(options: &ServerOptions) -> Self {
        let _guard = FILE_LOCK.read();
        let replication_conf = Self::file_path_from_dir(options);

        let private_address = options.general_settings.private_address.clone();
        // The replication configuration file is using an INI format
        let Ok(ini_file) = Ini::load_from_file(&replication_conf) else {
            return ReplicationConfig::new_primary(private_address);
        };

        let Some(replication) = ini_file.section(Some("replication")) else {
            tracing::warn!(
                "Replication configuration file {} does not contain a 'replication' section",
                replication_conf.display()
            );
            return ReplicationConfig::new_primary(private_address);
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
            private_address
        };

        ReplicationConfig { role, address }
    }

    /// Persist the configuration to the disk.
    /// (uses write lock)
    pub fn save(&self, options: &ServerOptions) -> Result<(), SableError> {
        let _guard = FILE_LOCK.write();
        self.write_file_internal(options)
    }

    /// Write `repl_config` to a file overriding previous content
    // (no locking in this version)
    fn write_file_internal(&self, options: &ServerOptions) -> Result<(), SableError> {
        let filepath = Self::file_path_from_dir(options);
        let mut ini = ini::Ini::default();
        ini.with_section(Some("replication"))
            .set("role", format!("{}", self.role))
            .set("address", self.address.clone());
        ini.write_to_file(&filepath)?;
        tracing::info!("Successfully updated file: {}", filepath.display());
        Ok(())
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

        replication_conf.push(Self::REPLICATION_CONF);
        replication_conf
    }
}
