use crate::{file_utils, replication::ServerRole, ServerOptions};
use ini::Ini;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    RwLock,
};

macro_rules! ini_read {
    ($ini_file:expr, $section:expr, $prop:expr, $default_value:expr) => {{
        let val = if let Some(sect) = $ini_file.section(Some($section)) {
            if let Some(prop) = sect.get($prop) {
                prop.to_string()
            } else {
                $default_value
            }
        } else {
            $default_value
        };
        val
    }};
}

#[derive(Default)]
pub struct ServerPersistentState {
    node_id: RwLock<String>,
    primary_node_id: RwLock<String>,
    role: AtomicU8,
    private_primary_address: RwLock<String>,
    config_file: RwLock<String>,
}

const ROLE_PRIMARY: u8 = 0;
const ROLE_REPLICA: u8 = 1;

impl ServerPersistentState {
    pub fn new() -> Self {
        ServerPersistentState {
            node_id: RwLock::new(uuid::Uuid::new_v4().to_string()),
            primary_node_id: RwLock::<String>::default(),
            role: AtomicU8::new(ROLE_PRIMARY),
            private_primary_address: RwLock::<String>::default(),
            config_file: RwLock::<String>::default(),
        }
    }

    const NODE_FILE: &'static str = "NODE";

    /// Return the current node ID
    #[inline]
    pub fn id(&self) -> String {
        self.node_id.read().expect("poisoned mutex").clone()
    }

    /// Return the current node ID
    #[inline]
    pub fn set_id(&self, node_id: String) {
        *self.node_id.write().expect("poisoned mutex") = node_id;
    }

    /// Set the node's role to either replica or primary
    #[inline]
    pub fn set_role(&self, role: ServerRole) {
        self.role.store(
            if role == ServerRole::Primary {
                ROLE_PRIMARY
            } else {
                ROLE_REPLICA
            },
            Ordering::Relaxed,
        );
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        self.role.load(Ordering::Relaxed) == ROLE_REPLICA
    }

    #[inline]
    pub fn is_primary(&self) -> bool {
        !self.is_replica()
    }

    /// Return the current's node role
    #[inline]
    pub fn role(&self) -> ServerRole {
        if self.role.load(Ordering::Relaxed) == ROLE_REPLICA {
            ServerRole::Replica
        } else {
            ServerRole::Primary
        }
    }

    /// Sets the remote address of the primary. This method also changes the role
    /// of this node to "Replica"
    #[inline]
    pub fn set_primary_address(&self, address: String) {
        *self
            .private_primary_address
            .write()
            .expect("poisoned mutex") = address;
        self.set_role(ServerRole::Replica);
    }

    /// Return the address of the primary
    #[inline]
    pub fn primary_address(&self) -> String {
        self.private_primary_address
            .read()
            .expect("poisoned mutex")
            .clone()
    }

    /// Set this node's primary node ID. If the provided `primary_node_id.is_some()`
    /// this function also changes the role to `Replica`. Otherwise (i.e. `primary_node_id.is_none()`)
    /// it clears the current node's primary node ID and sets the role to `Primary`
    #[inline]
    pub fn set_primary_node_id(&self, primary_node_id: Option<String>) {
        if primary_node_id.is_some() {
            self.set_role(ServerRole::Replica);
            *self.primary_node_id.write().expect("poisoned mutex") =
                primary_node_id.unwrap_or_default();
        } else {
            self.set_role(ServerRole::Primary);
            self.primary_node_id
                .write()
                .expect("poisoned mutex")
                .clear();
        }
    }

    /// Return the current's node role
    #[inline]
    pub fn primary_node_id(&self) -> String {
        self.primary_node_id.read().expect("poisoned mutex").clone()
    }

    /// Initialise the node ID by loading or creating it
    pub fn initialise(&self, options: &ServerOptions) {
        let file_path = Self::file_path_from_dir(options);
        *self.config_file.write().expect("poisoned mutex") =
            file_path.to_string_lossy().to_string();

        let Some(content) = file_utils::read_file_content(&file_path) else {
            self.save();
            return;
        };

        // The replication configuration file is using an INI format
        let Ok(ini_file) = Ini::load_from_str(&content) else {
            tracing::warn!("Failed to construct INI file from\n {}", content);
            self.save();
            return;
        };

        // read the values from the config file
        {
            // replace defaults with values from the disk
            let node_id = ini_read!(
                ini_file,
                "general",
                "node_id",
                uuid::Uuid::new_v4().to_string()
            );
            self.set_id(node_id);

            let role = ini_read!(
                ini_file,
                "replication",
                "role",
                format!("{}", ServerRole::Primary)
            );
            self.set_role(ServerRole::from_str(&role).unwrap()); // can't fail

            match self.role() {
                ServerRole::Primary => {
                    // the role is Primary. So we don't have an address to connect to
                    self.set_primary_node_id(None);
                }
                ServerRole::Replica => {
                    // The role is Replica,
                    let address = ini_read!(ini_file, "replication", "address", String::new());

                    if address.is_empty() {
                        tracing::warn!("Missing primary address in configuration file. Setting role back to Primary");
                        self.set_primary_node_id(None);
                        self.set_role(ServerRole::Primary);
                    } else {
                        self.set_primary_address(address); // this also sets the role to replica
                    }
                }
            }
        }

        self.save();
    }

    pub fn save(&self) {
        if self.id().is_empty() {
            // create new node ID if needed
            self.set_id(uuid::Uuid::new_v4().to_string());
        }

        let filepath = self.config_file.read().expect("poisoned mutex").clone();
        let mut ini = ini::Ini::default();
        ini.with_section(Some("general")).set("node_id", self.id());

        ini.with_section(Some("replication"))
            .set("address", self.primary_address())
            .set("role", format!("{}", self.role()));

        if let Err(e) = ini.write_to_file(&filepath) {
            tracing::error!("Failed to write INI file. {:?}", e);
            return;
        }

        tracing::info!("Successfully updated file: {}", filepath);
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
