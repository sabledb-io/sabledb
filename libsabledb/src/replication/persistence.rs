use crate::{replication::ServerRole, utils::TimeUtils, SableError, Server, ServerOptions};
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

#[derive(Default)]
struct DbClient {
    client: Option<ValkeyClient>,
    is_completed: bool,
}

impl DbClient {
    pub fn connection(
        &mut self,
        options: Arc<StdRwLock<ServerOptions>>,
    ) -> Option<redis::Connection> {
        self.open_connection_if_needed(options.clone());
        let Some(client) = &self.client else {
            return None;
        };
        match client.get_connection_with_timeout(std::time::Duration::from_secs(1)) {
            Ok(con) => Some(con),
            Err(e) => {
                tracing::error!("Could not connect to cluster database. {e}");
                None
            }
        }
    }

    /// Attempt to open redis connection. If a connection is already opened,
    /// do nothing
    fn open_connection_if_needed(&mut self, options: Arc<StdRwLock<ServerOptions>>) {
        if self.is_completed {
            return;
        }

        self.is_completed = true;
        let Some(cluster_address) = options.read().expect(POISONED_MUTEX).get_cluster_address()
        else {
            // No cluster address
            tracing::debug!("Cluster database is not configured");
            return;
        };

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
        self.client = Some(client);
        tracing::trace!("Success");
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
}

impl Default for Node {
    fn default() -> Self {
        Node {
            node_id: Server::state().persistent_state().id(),
            shard_name: Server::state().persistent_state().shard_name(),
            last_updated: TimeUtils::epoch_micros().unwrap_or(0),
            role: Server::state().persistent_state().role(),
            private_address: Server::state()
                .options()
                .read()
                .expect(POISONED_MUTEX)
                .general_settings
                .private_address
                .clone(),
            public_address: Server::state()
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Shard {
    /// The shard's name
    name: String,
    /// List of node IDs belong to this shard
    nodes: HashSet<String>,
    /// The shard's primary ID
    primary_node_id: String,
    /// The cluster name
    cluster_name: String,
    /// The slots owned by this shard
    slots: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Cluster {
    /// List of shards belong to this cluster
    shards: HashSet<String>,
    /// The cluster name
    name: String,
}

pub struct Persistence {
    options: Arc<StdRwLock<ServerOptions>>,
}

impl Persistence {
    pub fn with_options(options: Arc<StdRwLock<ServerOptions>>) -> Self {
        Persistence { options }
    }

    /// Insert or replace Node record in the database
    pub fn put_node(&self, node: &Node) -> Result<(), SableError> {
        DB_CONN.with_borrow_mut(|db_client| {
            let Some(mut conn) = db_client.connection(self.options.clone()) else {
                return Ok(());
            };

            let value = serde_json::to_string(&node).map_err(|e| {
                SableError::OtherError(format!("Failed to convert object to JSON. {e}"))
            })?;
            let _: redis::Value = conn.set(&node.node_id, value)?;
            Ok(())
        })
    }

    /// Delete Node from the database by its ID
    pub fn delete_node(&self, node_id: &String) -> Result<(), SableError> {
        DB_CONN.with_borrow_mut(|db_client| {
            let Some(mut conn) = db_client.connection(self.options.clone()) else {
                return Ok(());
            };

            let _: redis::Value = conn.del(node_id)?;
            Ok(())
        })
    }

    /// Read node from the database by its ID
    pub fn get_node(&self, node_id: &String) -> Result<Option<Node>, SableError> {
        DB_CONN.with_borrow_mut(|db_client| {
            let Some(mut conn) = db_client.connection(self.options.clone()) else {
                return Ok(None);
            };

            let redis::Value::BulkString(json) = conn.get(node_id)? else {
                return Ok(None);
            };

            let node: Node =
                serde_json::from_str(&String::from_utf8_lossy(&json)).map_err(|e| {
                    SableError::OtherError(format!("Failed to convert JSON to Node. {e}"))
                })?;
            Ok(Some(node))
        })
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
}
