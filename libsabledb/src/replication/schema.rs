use crate::{replication::ServerRole, utils::TimeUtils, SableError, Server};
#[allow(unused_imports)]
use redis::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const POISONED_MUTEX: &str = "Poisoned Mutex";

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
    /// List of node IDs belong to this shard
    nodes: HashSet<String>,
    /// The shard's name
    name: String,
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

pub struct Persistence {}

impl Persistence {
    pub fn put_node(_conn: &mut redis::Connection, _node: &Node) -> Result<(), SableError> {
        Ok(())
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
