use crate::{
    impl_builder_with_fn, replication::ServerRole, utils::TimeUtils, SableError, Server,
    ServerOptions,
};
use redis::Client as ValkeyClient;
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;

const POISONED_MUTEX: &str = "Poisoned Mutex";

thread_local! {
    static DB_CONN: RefCell<DbClient> = RefCell::<DbClient>::default();
}

struct ConnectionGuard {
    reset_connection: bool,
}

impl ConnectionGuard {
    pub fn mark_success(&mut self) {
        self.reset_connection = false;
    }
}

impl Default for ConnectionGuard {
    fn default() -> Self {
        ConnectionGuard {
            reset_connection: true,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if self.reset_connection {
            DB_CONN.with_borrow_mut(|db_client| {
                db_client.is_completed = false;
                db_client.conn = None;
            });
        }
    }
}

#[derive(Default)]
struct DbClient {
    client: Option<ValkeyClient>,
    conn: Option<redis::Connection>,
    is_completed: bool,
}

impl DbClient {
    pub fn connection(&mut self) -> Option<&mut redis::Connection> {
        let Some(ref mut conn) = &mut self.conn else {
            return None;
        };
        Some(conn)
    }

    /// Attempt to open redis connection. If a connection is already opened,
    /// do nothing
    fn open_connection_if_needed(&mut self, options: Arc<StdRwLock<ServerOptions>>) {
        if self.is_completed {
            return;
        }

        let Some(cluster_address) = options.read().expect(POISONED_MUTEX).get_cluster_address()
        else {
            // No cluster address
            tracing::debug!("Cluster database is not configured");
            self.is_completed = true;
            return;
        };

        tracing::debug!(
            "Initializing connection to redis cluster database {} for PID: {}/{:?}",
            cluster_address,
            std::process::id(),
            std::thread::current().id(),
        );

        // Build the connection string
        let cluster_address = if cluster_address.starts_with("tls://") {
            format!(
                "{}/#insecure",
                cluster_address.replace("tls://", "rediss://")
            )
        } else {
            format!("redis://{}", cluster_address)
        };

        tracing::info!("Calling open for {}...", cluster_address);
        let client = match redis::Client::open(cluster_address.as_str()) {
            Err(e) => {
                tracing::warn!(
                    "Failed to open connection to cluster database at: {cluster_address}. {:?}",
                    e
                );
                return;
            }
            Ok(client) => client,
        };
        match client.get_connection_with_timeout(std::time::Duration::from_secs(3)) {
            Ok(con) => {
                self.client = Some(client);
                self.conn = Some(con);
                self.is_completed = true;
            }
            Err(e) if e.kind() == redis::ErrorKind::IoError => {
                tracing::error!("Could not connect to cluster database. (IoError): {e}");
                return;
            }
            Err(e) => {
                tracing::error!("Could not connect to cluster database. {e}");
                self.is_completed = true;
                return;
            }
        }
        tracing::info!("Success");
    }
}

/// There are 3 main entities that we keep the in the cluster database:
/// - `Node` - represents a single SableDB instance
/// - `Shard` - represents a group of SableDB instances. In a shard there is one primary and the remaining nodes
///   are replicas, this implies that all members of the shard are serving the same set of slots
/// - `Cluster` - a group of shards
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Node {
    /// The node ID
    node_id: String,
    /// The node ID of the primary (will be empty for primary node)
    primary_node_id: String,
    /// The name of the shard this node belongs to
    shard_name: String,
    /// Last timestamp for this node. In microseconds since epoch
    last_updated: u64,
    /// The role (primary / replica)
    role: ServerRole,
    /// The node's private address
    private_address: String,
    /// The node's public address (this is the address on which clients are connected to)
    public_address: String,
    /// The last txn ID stored on this node's database
    last_txn_id: u64,
    /// The slots owned by this node
    slots: String,
}

impl Default for Node {
    fn default() -> Self {
        let server_state = Server::state();
        let pstate = server_state.persistent_state();

        Node {
            node_id: pstate.id(),
            shard_name: pstate.shard_name(),
            last_updated: TimeUtils::epoch_micros().unwrap_or(0),
            role: pstate.role(),
            primary_node_id: pstate.primary_node_id(),
            slots: pstate.slots().to_string(),
            private_address: server_state
                .options()
                .read()
                .expect(POISONED_MUTEX)
                .general_settings
                .private_address
                .clone(),
            public_address: server_state
                .options()
                .read()
                .expect(POISONED_MUTEX)
                .general_settings
                .public_address
                .clone(),
            last_txn_id: 0,
        }
    }
}

impl Node {
    pub fn set_last_txn_id(&mut self, txn_id: u64) -> &mut Self {
        self.last_txn_id = txn_id;
        self
    }

    pub fn set_role(&mut self, role: ServerRole) -> &mut Self {
        self.role = role;
        self
    }

    pub fn shard_name(&self) -> &String {
        &self.shard_name
    }

    pub fn node_id(&self) -> &String {
        &self.node_id
    }

    pub fn slots(&self) -> &String {
        &self.slots
    }

    pub fn set_slots(&mut self, slots: String) {
        self.slots = slots;
    }

    pub fn set_primary_node_id(&mut self, primary_node_id: String) {
        self.primary_node_id = primary_node_id;
    }

    pub fn primary_node_id(&self) -> &String {
        &self.primary_node_id
    }

    pub fn is_primary(&self) -> bool {
        self.role == ServerRole::Primary
    }

    pub fn is_replica(&self) -> bool {
        self.role == ServerRole::Replica
    }

    pub fn last_updated(&self) -> u64 {
        self.last_updated
    }

    pub fn last_txn_id(&self) -> u64 {
        self.last_txn_id
    }

    pub fn private_address(&self) -> String {
        self.private_address.to_string()
    }

    pub fn public_address(&self) -> String {
        self.public_address.to_string()
    }

    /// Return the queue name for this node
    pub fn queue_name(&self) -> String {
        format!("{}.QUEUE.{}", self.shard_name, self.node_id)
    }
}

pub struct NodeBuilder {
    node_id: String,
    shard_name: String,
    last_updated: u64,
    role: ServerRole,
    private_address: String,
    public_address: String,
    last_txn_id: u64,
    slots: String,
    primary_node_id: String,
}

impl Default for NodeBuilder {
    fn default() -> NodeBuilder {
        let node = Node::default();
        NodeBuilder {
            node_id: node.node_id,
            shard_name: node.shard_name,
            last_updated: node.last_updated,
            role: node.role,
            private_address: node.private_address,
            public_address: node.public_address,
            last_txn_id: node.last_txn_id,
            slots: node.slots,
            primary_node_id: node.primary_node_id,
        }
    }
}

impl NodeBuilder {
    impl_builder_with_fn!(node_id, String);
    impl_builder_with_fn!(shard_name, String);
    impl_builder_with_fn!(last_updated, u64);
    impl_builder_with_fn!(role, ServerRole);
    impl_builder_with_fn!(private_address, String);
    impl_builder_with_fn!(public_address, String);
    impl_builder_with_fn!(last_txn_id, u64);
    impl_builder_with_fn!(slots, String);
    impl_builder_with_fn!(primary_node_id, String);

    pub fn build(self) -> Node {
        Node {
            node_id: self.node_id,
            shard_name: self.shard_name,
            last_updated: self.last_updated,
            role: self.role,
            private_address: self.private_address,
            public_address: self.public_address,
            last_txn_id: self.last_txn_id,
            slots: self.slots,
            primary_node_id: self.primary_node_id,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Shard {
    /// The shard's name
    name: String,
    /// List of node IDs belong to this shard
    nodes: HashSet<String>,
    /// The cluster name
    cluster_name: String,
    /// The slots owned by this shard
    slots: String,
}

#[derive(Default)]
pub struct ShardBuilder {
    name: String,
    nodes: HashSet<String>,
    cluster_name: String,
    slots: String,
}

impl ShardBuilder {
    pub fn with_nodes(mut self, nodes: &[&Node]) -> Self {
        for node in nodes {
            self.nodes.insert(node.node_id.clone());
        }
        self
    }

    impl_builder_with_fn!(name, String);
    impl_builder_with_fn!(cluster_name, String);
    impl_builder_with_fn!(slots, String);

    pub fn build(self) -> Shard {
        Shard {
            name: self.name,
            nodes: self.nodes,
            cluster_name: self.cluster_name,
            slots: self.slots,
        }
    }
}

impl Shard {
    pub fn with_name(name: &str) -> Self {
        Shard {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn add_node(&mut self, node: &Node) {
        self.nodes.insert(node.node_id.clone());
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Cluster {
    /// List of shards belong to this cluster
    shards: HashSet<String>,
    /// The cluster name
    name: String,
}

#[allow(dead_code)]
pub struct Persistence {
    options: Arc<StdRwLock<ServerOptions>>,
}

#[macro_export]
macro_rules! impl_persistence_get_for {
    ($func_name:ident, $type_name:ident) => {
        // The macro will expand into the contents of this block.
        paste::item! {
            /// Read record from the database by `key`
            pub fn $func_name(&self, key: &String) -> Result<Option<$type_name>, SableError>
            {
                let mut cg = ConnectionGuard::default();
                DB_CONN.with_borrow_mut(|db_client| {
                    let Some(conn) = db_client.connection() else {
                        return Err(SableError::ConnectionNotOpened);
                    };

                    let redis::Value::BulkString(json) = conn.get(key)? else {
                        return Ok(None);
                    };

                    let s = String::from_utf8_lossy(&json).to_string();
                    let v: $type_name = serde_json::from_str(&s).map_err(|e| {
                        SableError::OtherError(format!("Failed to convert JSON to Object. {e}"))
                    })?;

                    Ok(Some(v))
                }).inspect(|_| {
                    cg.mark_success();
                })
            }
        }
    };
}

#[derive(Debug, Clone)]
pub enum ShardPrimaryResult {
    /// Managed to find primary
    Ok(Node),
    /// Shard has multiple primaries
    MultiplePrimaries,
    /// Shard has no primary
    NoPrimary,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ShardIsStableResult {
    /// The shard is stable
    Ok,
    /// Shard has multiple primaries
    MultiplePrimaries,
    /// Shard has no primary
    NoPrimary,
    /// Members of the shard do not share the same slot range
    MultipleSlotRange,
    /// A replica node has no primary node ID set
    ReplicaIsMissingPrimary,
}

impl PartialEq for ShardPrimaryResult {
    fn eq(&self, other: &ShardPrimaryResult) -> bool {
        matches!(
            (self, other),
            (Self::Ok(_), Self::Ok(_))
                | (Self::MultiplePrimaries, Self::MultiplePrimaries)
                | (Self::NoPrimary, Self::NoPrimary)
        )
    }
}

impl ShardPrimaryResult {
    /// Convert `ShardPrimaryResult::Ok(node)` -> `Some(node)`, anything else is converted into `None`
    pub fn ok(self) -> Option<Node> {
        match self {
            Self::Ok(node) => Some(node),
            _ => None,
        }
    }
}

impl std::fmt::Display for ShardPrimaryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Ok(_) => write!(f, "Ok(..)"),
            Self::MultiplePrimaries => write!(f, "MultiplePrimaries"),
            Self::NoPrimary => write!(f, "NoPrimary"),
        }
    }
}

impl Persistence {
    pub fn with_options(options: Arc<StdRwLock<ServerOptions>>) -> Self {
        DB_CONN.with_borrow_mut(|db_client| {
            db_client.open_connection_if_needed(options.clone());
        });
        Persistence { options }
    }

    /// Mainly used by tests
    pub fn with_connection(
        options: Arc<StdRwLock<ServerOptions>>,
        conn: redis::Connection,
    ) -> Self {
        DB_CONN.with_borrow_mut(|db_client| {
            db_client.is_completed = true;
            db_client.conn = Some(conn);
        });
        Persistence { options }
    }

    /// Insert or replace Node record into the database
    pub fn put_node(&self, node: &Node) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        let key = format!("{}.{}", node.shard_name, node.node_id);
        self.put(&key, node).inspect(|_| {
            cg.mark_success();
        })
    }

    /// Insert or replace Shard record into the database
    pub fn put_shard(&self, shard: &Shard) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        self.put(&shard.name, shard).inspect(|_| {
            cg.mark_success();
        })
    }

    /// Insert or replace Shard record into the database
    pub fn put_cluster(&self, cluster: &Cluster) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        self.put(&cluster.name, cluster).inspect(|_| {
            cg.mark_success();
        })
    }

    /// Delete item from the database by its ID
    pub fn delete(&self, item_id: &String) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };
                let _: redis::Value = conn.del(item_id)?;
                Ok(())
            })
            .inspect(|_| {
                cg.mark_success();
            })
    }

    impl_persistence_get_for!(get_node, Node);
    impl_persistence_get_for!(get_shard, Shard);
    impl_persistence_get_for!(get_cluster, Cluster);

    pub fn lock(&self, lock_name: &String, timeout_ms: u64) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        DB_CONN
            .with_borrow_mut(|db_client| {
                if lock_name.is_empty() {
                    return Err(SableError::InvalidArgument(
                        "Unable to construct LOCK. Empty lock name provided".to_string(),
                    ));
                }

                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };

                let res: redis::Value = redis::cmd("LOCK")
                    .arg(lock_name)
                    .arg(timeout_ms)
                    .query(conn)?;
                match res {
                    redis::Value::Okay => {
                        // we got the lock
                        Ok(())
                    }
                    other => Err(SableError::OtherError(format!(
                        "Failed to lock: {lock_name}. {:?}",
                        other
                    ))),
                }
            })
            .inspect(|_| {
                cg.mark_success();
            })
    }

    pub fn unlock(&self, lock_name: &String) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };

                let res: redis::Value = redis::cmd("UNLOCK").arg(lock_name).query(conn)?;
                match res {
                    redis::Value::Okay => {
                        // we got the lock
                        Ok(())
                    }
                    other => Err(SableError::OtherError(format!(
                        "Failed to unlock: {lock_name}. {:?}",
                        other
                    ))),
                }
            })
            .inspect(|_| {
                cg.mark_success();
            })
    }

    /// Retrieve the nodes of a given shard from the database
    pub fn shard_nodes(&self, shard: &Shard) -> Result<Vec<Node>, SableError> {
        let mut cg = ConnectionGuard::default();
        let mut result = Vec::<Node>::default();
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };

                // the nodes are kept in the form of "<shard>.<node-id>"
                let node_keys: Vec<String> = shard
                    .nodes
                    .iter()
                    .map(|node_id| format!("{}.{}", &shard.name, node_id))
                    .collect();

                let res = redis::cmd("MGET").arg(&node_keys).query(conn)?;
                match res {
                    redis::Value::Array(arr) => {
                        let it = arr.iter();
                        for json in it {
                            match json {
                                redis::Value::Nil => { /* not found */ }
                                redis::Value::BulkString(json) => {
                                    let s = String::from_utf8_lossy(json).to_string();
                                    let v: Node = serde_json::from_str(&s).map_err(|e| {
                                        SableError::OtherError(format!(
                                            "Failed to convert JSON to Node. {e}"
                                        ))
                                    })?;
                                    result.push(v);
                                }
                                other => {
                                    return Err(SableError::Corrupted(format!(
                                        "Expected BulkString value. Found: {:?}",
                                        other
                                    )));
                                }
                            }
                        }
                        Ok(())
                    }
                    other => Err(SableError::OtherError(format!(
                        "Failed to get shard: '{}' nodes. {:?}",
                        shard.name, other
                    ))),
                }
            })
            .inspect(|_| {
                cg.mark_success();
            })?;
        Ok(result)
    }

    /// Find the primary node of a shard
    pub fn shard_primary(&self, shard: &Shard) -> Result<ShardPrimaryResult, SableError> {
        let mut cg = ConnectionGuard::default();
        let nodes = self.shard_nodes(shard)?;
        let nodes: Vec<&Node> = nodes.iter().collect();
        Self::find_primary_node(&nodes).inspect(|_| {
            cg.mark_success();
        })
    }

    /// Given a list of nodes, check to see if they form a shard. In case of true, return the
    /// primary node
    pub fn is_shard_stable(&self, nodes: &[&Node]) -> Result<ShardIsStableResult, SableError> {
        // The primary node ID for this shard
        let mut primary_node: Option<&Node> = None;

        // Confirm exactly 1 primary node and find it
        for node in nodes {
            if node.role == ServerRole::Primary {
                if primary_node.is_none() {
                    primary_node = Some(node);
                } else {
                    // 2 primaries?
                    return Ok(ShardIsStableResult::MultiplePrimaries);
                }
            }
        }

        if primary_node.is_none() {
            return Ok(ShardIsStableResult::NoPrimary);
        }

        let Some(primary_node) = primary_node else {
            return Err(SableError::InternalError("Unexpected None!".into()));
        };

        // Confirm that all nodes have the same slots and all replicas are pointing to the same primary
        for node in nodes {
            if node.slots != primary_node.slots {
                return Ok(ShardIsStableResult::MultipleSlotRange);
            }

            if node.role == ServerRole::Replica {
                if node.primary_node_id.is_empty() {
                    return Ok(ShardIsStableResult::ReplicaIsMissingPrimary);
                }

                // A replica that does not point to the correct primary node
                if node.primary_node_id != primary_node.node_id {
                    return Ok(ShardIsStableResult::MultiplePrimaries);
                }
            }
        }
        Ok(ShardIsStableResult::Ok)
    }

    /// Push a command to the node-id
    pub fn queue_push_command(&self, node: &Node, command: &String) -> Result<(), SableError> {
        let mut cg = ConnectionGuard::default();
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };

                tracing::info!(
                    "Sending command '{}' to queue '{}'",
                    command,
                    &node.queue_name()
                );
                conn.lpush::<&String, &String, redis::Value>(&node.queue_name(), command)?;
                tracing::info!("Success");
                Ok(())
            })
            .inspect(|_| {
                cg.mark_success();
            })
    }

    /// Pop a command from the top of the queue with a timeout
    pub fn queue_pop_command_with_timeout(
        &self,
        node: &Node,
        timeout_secs: f64,
    ) -> Result<Option<String>, SableError> {
        let mut cg = ConnectionGuard::default();
        let mut val = Option::<String>::None;
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };
                if let redis::Value::Array(arr) =
                    conn.brpop::<&String, redis::Value>(&node.queue_name(), timeout_secs)?
                {
                    if let Some(redis::Value::BulkString(cmd)) = arr.get(1) {
                        val = Some(String::from_utf8_lossy(cmd).to_string());
                    }
                }
                Ok(())
            })
            .inspect(|_| {
                cg.mark_success();
            })?;
        Ok(val)
    }

    /// Return the length of node's queue
    pub fn queue_len(&self, node: &Node) -> Result<usize, SableError> {
        let mut cg = ConnectionGuard::default();
        let mut qlen = 0usize;
        DB_CONN
            .with_borrow_mut(|db_client| {
                let Some(conn) = db_client.connection() else {
                    return Err(SableError::ConnectionNotOpened);
                };
                if let redis::Value::Int(len) =
                    conn.llen::<&String, redis::Value>(&node.queue_name())?
                {
                    qlen = len.try_into().unwrap_or(0);
                }
                Ok(())
            })
            .inspect(|_| {
                cg.mark_success();
            })?;
        Ok(qlen)
    }

    /// Insert or replace item into the database
    fn put<T>(&self, key: &String, value: &T) -> Result<(), SableError>
    where
        T: ?Sized + Serialize,
    {
        DB_CONN.with_borrow_mut(|db_client| {
            let Some(conn) = db_client.connection() else {
                return Err(SableError::ConnectionNotOpened);
            };

            let value = serde_json::to_string(value).map_err(|e| {
                SableError::OtherError(format!("Failed to convert object to JSON. {e}"))
            })?;
            let _: redis::Value = conn.set(key, value)?;
            Ok(())
        })
    }

    /// Given a list of nodes, check to see if they form a shard. In case of true, return the
    /// primary node
    fn find_primary_node(nodes: &[&Node]) -> Result<ShardPrimaryResult, SableError> {
        // The primary node ID for this shard
        let mut primary_node: Option<&Node> = None;

        // Confirm exactly 1 primary node and find it
        for node in nodes {
            if node.role == ServerRole::Primary {
                if primary_node.is_none() {
                    primary_node = Some(node);
                } else {
                    // 2 primaries?
                    return Ok(ShardPrimaryResult::MultiplePrimaries);
                }
            }
        }

        if primary_node.is_none() {
            return Ok(ShardPrimaryResult::NoPrimary);
        }

        let Some(primary_node) = primary_node else {
            return Err(SableError::InternalError("Unexpected None!".into()));
        };
        Ok(ShardPrimaryResult::Ok(primary_node.clone()))
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_seriializaton() {
        let mut node = Node::default();
        node.set_last_txn_id(1234).set_role(ServerRole::Replica);
        let s = serde_json::to_string(&node).unwrap();
        println!("Node: {s}");

        let de_node: Node = serde_json::from_str(s.as_str()).unwrap();
        assert_eq!(node, de_node);
    }

    #[test]
    fn test_find_primary() {
        let mut node1 = NodeBuilder::default()
            .with_node_id("1".to_string())
            .with_slots("0-100".to_string())
            .with_role(ServerRole::Replica)
            .build();
        assert_eq!(node1.role, ServerRole::Replica);

        let mut node2 = NodeBuilder::default()
            .with_node_id("2".to_string())
            .with_slots("100-200".to_string())
            .with_role(ServerRole::Replica)
            .build();
        assert_eq!(node1.role, ServerRole::Replica);

        // should fail: 2 replicas
        assert_eq!(
            ShardPrimaryResult::NoPrimary,
            Persistence::find_primary_node(&[&node1, &node2]).unwrap()
        );

        node1.set_role(ServerRole::Primary);
        assert_eq!(node1.role, ServerRole::Primary);
        assert_eq!(node2.role, ServerRole::Replica);

        // Fix the slot range issue
        node2.set_slots(node1.slots().clone());

        node2.set_primary_node_id(node1.node_id.clone());
        assert_eq!(
            ShardPrimaryResult::Ok(Node::default()),
            Persistence::find_primary_node(&[&node1, &node2]).unwrap()
        );

        node2.set_role(ServerRole::Primary); // we now have 2 primaries

        // Should fail: 2 primaries
        assert_eq!(
            ShardPrimaryResult::MultiplePrimaries,
            Persistence::find_primary_node(&[&node1, &node2]).unwrap()
        );
    }
}
