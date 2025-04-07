use crate::{file_utils, replication::ServerRole, server::SlotBitmap, ServerOptions};
use ini::Ini;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

const OPTIONS_LOCK_ERR: &str = "Failed to obtain read lock on ServerOptions";

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

#[allow(dead_code)]
#[derive(Default)]
struct NodeInfo {
    /// The node's private address
    private_address: String,
    /// The node's public address (this is the address on which clients are connected to)
    public_address: String,
    /// The slots owned by this node
    slots: Option<SlotBitmap>,
}

impl From<&crate::replication::Node> for NodeInfo {
    fn from(n: &crate::replication::Node) -> Self {
        NodeInfo {
            private_address: n.private_address().clone(),
            public_address: n.public_address().clone(),
            slots: SlotBitmap::from_str(n.slots().as_str()).ok(),
        }
    }
}
/// This struct represents the content of the `NODE` configuration file
#[derive(Default)]
struct ServerPersistentStateInner {
    node_id: String,
    primary_node_id: String,
    role: u8,
    private_primary_address: String,
    config_file: String,
    shard_name: String,
    cluster_name: String,
    cluster_nodes: Vec<NodeInfo>,
}

#[derive(Clone, Default)]
pub struct ServerPersistentState {
    inner: Arc<RwLock<ServerPersistentStateInner>>,
    slots: SlotBitmap,
}

const ROLE_PRIMARY: u8 = 0;
const ROLE_REPLICA: u8 = 1;
const POISONED_MUTEX: &str = "poisoned mutex";

impl ServerPersistentState {
    pub fn new() -> Self {
        let slots = SlotBitmap::default();
        slots.set_all(); // by default, this node owns all the slots
        ServerPersistentState {
            inner: Arc::new(RwLock::new(ServerPersistentStateInner {
                node_id: uuid::Uuid::new_v4().to_string(),
                primary_node_id: String::default(),
                role: ROLE_PRIMARY,
                private_primary_address: String::default(),
                config_file: String::default(),
                shard_name: String::default(),
                cluster_name: String::default(),
                cluster_nodes: Vec::<NodeInfo>::default(),
            })),
            slots,
        }
    }

    const NODE_FILE: &'static str = "NODE";

    /// Return the current node ID
    #[inline]
    pub fn id(&self) -> String {
        self.inner.read().expect(POISONED_MUTEX).node_id.clone()
    }

    /// Return the current node ID
    #[inline]
    pub fn set_id(&self, node_id: String) {
        self.inner.write().expect(POISONED_MUTEX).node_id = node_id;
    }

    /// Set the node's role to either replica or primary
    #[inline]
    pub fn set_role(&self, role: ServerRole) {
        let mut inner = self.inner.write().expect(POISONED_MUTEX);
        inner.role = if role == ServerRole::Primary {
            ROLE_PRIMARY
        } else {
            ROLE_REPLICA
        };

        if role == ServerRole::Primary {
            // Clear replica related values
            inner.private_primary_address.clear();
            inner.primary_node_id.clear();
        }
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        self.inner.read().expect(POISONED_MUTEX).role == ROLE_REPLICA
    }

    #[inline]
    pub fn is_primary(&self) -> bool {
        !self.is_replica()
    }

    /// Return the current's node role
    #[inline]
    pub fn role(&self) -> ServerRole {
        if self.is_replica() {
            ServerRole::Replica
        } else {
            ServerRole::Primary
        }
    }

    /// Return the node's slots
    #[inline]
    pub fn slots(&self) -> &SlotBitmap {
        &self.slots
    }

    #[inline]
    pub fn set_slots(&self, slots: &str) -> Result<(), crate::SableError> {
        self.slots.from_string(slots)
    }

    /// Sets the remote address of the primary. This method also changes the role
    /// of this node to "Replica"
    #[inline]
    pub fn set_primary_address(&self, address: String) {
        self.inner
            .write()
            .expect(POISONED_MUTEX)
            .private_primary_address = address;
        self.set_role(ServerRole::Replica);
    }

    /// Return the address of the primary
    #[inline]
    pub fn primary_address(&self) -> String {
        self.inner
            .read()
            .expect(POISONED_MUTEX)
            .private_primary_address
            .clone()
    }

    /// Set this node's primary node ID. If the provided `primary_node_id.is_some()`
    /// this function also changes the role to `Replica`. Otherwise (i.e. `primary_node_id.is_none()`)
    /// it clears the current node's primary node ID and sets the role to `Primary`
    #[inline]
    pub fn set_primary_node_id(&self, primary_node_id: Option<String>) {
        if let Some(primary_node_id) = primary_node_id {
            self.set_role(ServerRole::Replica);
            self.inner.write().expect(POISONED_MUTEX).primary_node_id = primary_node_id;
        } else {
            self.set_role(ServerRole::Primary);
            self.inner
                .write()
                .expect(POISONED_MUTEX)
                .primary_node_id
                .clear();
            self.inner
                .write()
                .expect(POISONED_MUTEX)
                .private_primary_address
                .clear();
        }
    }

    /// Return the current's node role
    #[inline]
    pub fn primary_node_id(&self) -> String {
        self.inner
            .read()
            .expect(POISONED_MUTEX)
            .primary_node_id
            .clone()
    }

    #[inline]
    pub fn set_shard_name(&self, shard_name: String) {
        self.inner.write().expect(POISONED_MUTEX).shard_name = shard_name;
    }

    #[inline]
    pub fn shard_name(&self) -> String {
        self.inner.read().expect(POISONED_MUTEX).shard_name.clone()
    }

    #[inline]
    pub fn set_cluster_name(&self, cluster_name: String) {
        self.inner.write().expect(POISONED_MUTEX).cluster_name = cluster_name;
    }

    #[inline]
    pub fn cluster_name(&self) -> String {
        self.inner
            .read()
            .expect(POISONED_MUTEX)
            .cluster_name
            .clone()
    }

    #[inline]
    pub fn in_cluster(&self) -> bool {
        self.inner
            .read()
            .expect(POISONED_MUTEX)
            .cluster_name
            .is_empty()
    }

    /// Find and return the owner of `slot`
    pub fn node_owner_for_slot(
        &self,
        slot: u16,
    ) -> Result<Option<(String, u16)>, crate::SableError> {
        let state = self.inner.read().expect(POISONED_MUTEX);
        for node_info in &state.cluster_nodes {
            if let Some(slots) = &node_info.slots {
                if slots.is_set(slot)? {
                    let Ok(socket) = node_info.public_address.parse::<SocketAddr>() else {
                        return Err(super::SableError::InvalidArgument(
                            "Invalid public address".into(),
                        ));
                    };
                    return Ok(Some((socket.ip().to_string(), socket.port())));
                }
            }
        }
        // Could not find the slot owner
        Ok(None)
    }

    /// Initialise the node ID by loading or creating it
    pub fn initialise(&self, options: Arc<RwLock<ServerOptions>>) {
        let file_path = Self::file_path_from_dir(options);
        self.inner.write().expect(POISONED_MUTEX).config_file =
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

            // slots ownership
            let slots_str = ini_read!(ini_file, "general", "slots", "0-16383".to_string());
            if let Err(e) = self.slots.from_string(slots_str.as_str()) {
                tracing::warn!("Failed to load slot range from string: {}", e);
            }

            // read the shard / cluster name
            let shard_name = ini_read!(ini_file, "general", "shard", String::default());
            self.set_shard_name(shard_name);

            let cluster_name = ini_read!(ini_file, "general", "cluster", String::default());
            self.set_cluster_name(cluster_name);

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

        let filepath = self.inner.read().expect(POISONED_MUTEX).config_file.clone();
        let mut ini = ini::Ini::default();

        ini.with_section(Some("general"))
            .set("node_id", self.id())
            .set("slots", self.slots.to_string())
            .set("shard", self.shard_name())
            .set("cluster", self.cluster_name());

        ini.with_section(Some("replication"))
            .set("address", self.primary_address())
            .set("role", format!("{}", self.role()));

        if let Err(e) = ini.write_to_file(&filepath) {
            tracing::debug!("Failed to write INI file `{}`. {:?}", filepath, e);
            return;
        }

        tracing::info!("Successfully updated file: {}", filepath);
    }

    fn file_path_from_dir(options: Arc<RwLock<ServerOptions>>) -> PathBuf {
        let options_ref = options.read().expect(OPTIONS_LOCK_ERR);
        let configuration_dir = options_ref.general_settings.config_dir.as_deref();

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
