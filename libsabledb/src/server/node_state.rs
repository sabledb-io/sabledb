use crate::{
    file_utils,
    replication::{Node, ServerRole},
    server::SlotBitmap,
    utils, CommandLineArgs, SableError, ServerOptions,
};
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

#[derive(Default, Clone)]
pub struct NodeExt {
    pub inner: Node,
    pub slots: Option<SlotBitmap>,
}

impl NodeExt {
    pub fn inner(&self) -> &Node {
        &self.inner
    }
}

impl std::fmt::Debug for NodeExt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("NodeExt")
            .field("node", &self.inner)
            .finish()
    }
}

impl From<&Node> for NodeExt {
    fn from(n: &Node) -> Self {
        NodeExt {
            inner: n.clone(),
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
    cluster_primary_nodes: Vec<NodeExt>,
    cluster_all_nodes: Vec<NodeExt>,
}

impl ServerPersistentStateInner {
    /// Prepare detailed information about the cluster nodes, suitable for producing output
    /// for the "CLUSTER NODES" command
    /// id: The node ID, a 40-character globally unique string generated when a node is created and never changed again (unless CLUSTER RESET HARD is used).
    /// ip:port@cport: The node address that clients should contact to run queries, along with the used cluster bus port. :0@0 can be expected when the address is no longer known for this node ID, hence flagged with noaddr.
    /// hostname: A human readable string that can be configured via the cluster-annouce-hostname setting. The max length of the string is 256 characters, excluding the null terminator. The name can contain ASCII alphanumeric characters, '-', and '.' only.
    /// flags: A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, nofailover, noflags. Flags are explained below.
    /// master: If the node is a replica, and the primary is known, the primary node ID, otherwise the "-" character.
    /// ping-sent: Unix time at which the currently active ping was sent, or zero if there are no pending pings, in milliseconds.
    /// pong-recv: Unix time the last pong was received, in milliseconds.
    /// config-epoch: The configuration epoch (or version) of the current node (or of the current primary if the node is a replica). Each time there is a failover, a new, unique, monotonically increasing configuration epoch is created. If multiple nodes claim to serve the same hash slots, the one with the higher configuration epoch wins.
    /// link-state: The state of the link used for the node-to-node cluster bus. Use this link to communicate with the node. Can be connected or disconnected.
    /// slot: A hash slot number or range. Starting from argument number 9, but there may be up to 16384 entries in total (limit never reached). This is the list of hash slots served by this node. If the entry is just a number, it is parsed as such. If it is a range, it is in the form start-end, and means that the node is responsible for all the hash slots from start to end including the start and end values.
    pub fn cluster_nodes_lines(&self) -> Vec<Vec<(String, String)>> {
        let mut result =
            Vec::<Vec<(String, String)>>::with_capacity(self.cluster_primary_nodes.len());
        for node in &self.cluster_all_nodes {
            let mut node_fields = Vec::<(String, String)>::default();
            node_fields.push(("id".into(), node.inner().node_id().clone()));

            let private_port = node
                .inner()
                .private_address()
                .split(':')
                .map(|s| s.into())
                .collect::<Vec<String>>()
                .get(1)
                .unwrap_or(&String::default())
                .parse::<u16>()
                .unwrap_or(0);

            let ip_port_cport = format!("{}@{}", &node.inner().public_address(), private_port);
            node_fields.push(("ip:port@cport,hostname".into(), ip_port_cport));

            let master = if node.inner().is_replica() {
                node.inner().primary_node_id().clone()
            } else {
                "-".into()
            };

            node_fields.push(("flags".into(), self.node_flags(node)));
            node_fields.push(("master".into(), master));
            node_fields.push(("ping-sent".into(), "0".into()));
            node_fields.push(("pong-recv".into(), "0".into()));
            node_fields.push((
                "config-epoch".into(),
                node.inner().last_txn_id().to_string(),
            ));
            node_fields.push(("link-state".into(), "connected".into()));

            let slots = node
                .inner()
                .slots()
                .replace(",", " ")
                .trim_end()
                .to_string();
            node_fields.push(("slots".into(), slots));
            result.push(node_fields);
        }
        result
    }

    /// A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, nofailover, noflags.
    fn node_flags(&self, node: &NodeExt) -> String {
        let current_node_id = &self.node_id;
        let mut flags: String = if current_node_id.eq(node.inner().node_id()) {
            "myself,".into()
        } else {
            "".into()
        };

        if node.inner().is_replica() {
            flags.push_str("slave");
        } else {
            flags.push_str("master");
        }
        flags
    }
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
                node_id: utils::create_uuid(),
                primary_node_id: String::default(),
                role: ROLE_PRIMARY,
                private_primary_address: String::default(),
                config_file: String::default(),
                shard_name: String::default(),
                cluster_name: String::default(),
                cluster_primary_nodes: Vec::<NodeExt>::default(),
                cluster_all_nodes: Vec::<NodeExt>::default(),
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
    pub fn set_slots(&self, slots: &str) -> Result<(), SableError> {
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
        !self
            .inner
            .read()
            .expect(POISONED_MUTEX)
            .cluster_name
            .is_empty()
    }

    /// Find and return the owner of `slot`
    pub fn node_owner_for_slot(&self, slot: u16) -> Result<Option<(String, u16)>, SableError> {
        let state = self.inner.read().expect(POISONED_MUTEX);
        for node_info in &state.cluster_primary_nodes {
            if let Some(slots) = &node_info.slots {
                if slots.is_set(slot)? {
                    let Ok(socket) = node_info.inner().public_address().parse::<SocketAddr>()
                    else {
                        return Err(SableError::InvalidArgument("Invalid public address".into()));
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
            let node_id = ini_read!(ini_file, "general", "node_id", utils::create_uuid());
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

    /// Initialise the node ID by loading or creating it
    pub fn update_from_commandline_args(&self, args: &CommandLineArgs) -> Result<(), SableError> {
        if let Some(shard_name) = args.shard_name() {
            self.set_shard_name(shard_name);
        }
        if let Some(cluster_name) = args.cluster_name() {
            self.set_cluster_name(cluster_name);
        }
        if let Some(slots) = args.slots() {
            self.set_slots(slots.as_str())?;
        }
        self.save();
        Ok(())
    }

    pub fn save(&self) {
        if self.id().is_empty() {
            // create new node ID if needed
            self.set_id(utils::create_uuid());
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

    /// Update the cluster nodes (this will update both primaries & all nodes)
    pub fn set_cluster_nodes(&self, nodes: Vec<NodeExt>) {
        let primary_nodes: Vec<NodeExt> = nodes
            .iter()
            .filter(|n| n.inner.is_primary())
            .cloned()
            .collect();

        tracing::debug!("Cluster primary nodes: {:#?}", primary_nodes);

        self.inner
            .write()
            .expect(POISONED_MUTEX)
            .cluster_primary_nodes = primary_nodes;

        tracing::debug!("All cluster nodes: {:#?}", nodes);
        self.inner.write().expect(POISONED_MUTEX).cluster_all_nodes = nodes;
    }

    /// Prepare detailed information about the cluster nodes, suitable for producing output
    /// for the "CLUSTER NODES" command
    pub fn cluster_nodes_lines(&self) -> Vec<Vec<(String, String)>> {
        self.inner
            .read()
            .expect(POISONED_MUTEX)
            .cluster_nodes_lines()
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
