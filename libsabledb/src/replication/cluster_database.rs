use crate::{replication::NodeProperties, SableError, Server, ServerOptions};
use redis::Commands;
use redis::Value;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

#[allow(unused_imports)]
use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};

const MUTEX_ERR: &str = "poisoned mutex";
const CLUSTER_PRIMARIES: &str = "CLUSTER_PRIMARIES";

lazy_static::lazy_static! {
    static ref CM_CONN: StdRwLock<Option<redis::Client>> = StdRwLock::<Option<redis::Client>>::default();
}

#[derive(Debug, PartialEq, Clone)]
pub enum LockResult {
    /// Lock created successfully
    Ok,
    /// A lock already exists. Return the lock value (which is the owner node-id)
    AlreadyExist(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum UnLockResult {
    /// Lock unlocked successfully
    Ok,
    /// Can't unlock it - the lock was not created by this instance
    NotOwner(String),
    /// Could not find a lock with this name
    NoSuchLock,
}

/// Attempt to open redis connection. If a connection is already opened,
/// do nothing
fn open_connection(options: Arc<StdRwLock<ServerOptions>>) {
    tracing::trace!("Setting up connection with cluster database");
    {
        // we already have a connection opened, nothing more to be done here
        let conn = CM_CONN.read().expect(MUTEX_ERR);
        if conn.is_some() {
            tracing::trace!("Connection is already opened");
            return;
        }
    }

    let Some(cluster_address) = &options
        .read()
        .expect("read error")
        .general_settings
        .cluster_address
    else {
        // No cluster address
        tracing::trace!("Cluster database is not configured");
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

    // Successfully opened the connection
    *CM_CONN.write().expect(MUTEX_ERR) = Some(client);
    tracing::info!("Success");
}

/// Close the current connection, making sure that the next call to `open_connection`
/// will re-open it
#[allow(dead_code)]
fn close_connection() {
    *CM_CONN.write().expect(MUTEX_ERR) = None;
}

fn get_conn_and_run<F>(
    options: Arc<StdRwLock<ServerOptions>>,
    mut code: F,
) -> Result<(), SableError>
where
    F: FnMut(&mut redis::Connection) -> Result<(), SableError>,
{
    open_connection(options.clone());
    tracing::trace!("Locking connection");
    let mut conn = CM_CONN.write().expect(MUTEX_ERR);
    let Some(client) = &mut *conn else {
        return Ok(());
    };
    tracing::trace!("Done");

    tracing::trace!("Opening connection");
    let mut conn = client.get_connection_with_timeout(Duration::from_secs(1))?;
    tracing::trace!("Opening connection..done");
    code(&mut conn)
}

pub struct ClusterDB {
    options: Arc<StdRwLock<ServerOptions>>,
}

impl ClusterDB {
    pub fn with_options(options: Arc<StdRwLock<ServerOptions>>) -> Self {
        ClusterDB { options }
    }

    /// Update this node with the server manager database. If no database is configured, do nothing
    /// If an entry for the same node already exists, it is overridden (the key is the node-id)
    pub fn put_node_properties(&self, node_info: &NodeProperties) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), |client| node_info.put(client))
    }

    /// Detach this node from its primary
    pub fn remove_this_from_primary(&self) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), move |client| {
            let cur_node_id = Server::state().persistent_state().id();
            let primary_node_id = self.node_primary_id()?;
            tracing::info!(
                "Disassociating self({}) from Primary({})",
                cur_node_id,
                primary_node_id,
            );
            let key = format!("{}_replicas", primary_node_id);
            client.srem::<String, &String, redis::Value>(key, &cur_node_id)?;
            tracing::info!("Success");
            Ok(())
        })
    }

    /// Add node to the CLUSTER_PRIMARIES set
    pub fn cluster_add(&self, node_id: &String) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), move |client| {
            let _: redis::Value = client.sadd(CLUSTER_PRIMARIES, node_id)?;
            Ok(())
        })
    }

    /// Delete node to the CLUSTER_PRIMARIES set
    pub fn cluster_del(&self, node_id: &String) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), move |client| {
            let _: redis::Value = client.srem(CLUSTER_PRIMARIES, node_id)?;
            Ok(())
        })
    }

    /// Delete this node from the database
    pub fn delete_self(&self) -> Result<(), SableError> {
        self.delete_node(Server::state().persistent_state().id())
    }

    /// Delete node identified by `node_id` from the database
    pub fn delete_node(&self, node_id: String) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), move |client| {
            let _: redis::Value = client.del(&node_id)?;
            Ok(())
        })
    }

    /// Return the "last_updated" property for this node's primary
    pub fn primary_last_updated(&self) -> Result<Option<u64>, SableError> {
        let primary_node_id = self.node_primary_id()?;
        self.node_last_updated(primary_node_id)
    }

    /// Return the "last_updated" property of
    pub fn node_last_updated(&self, node_id: String) -> Result<Option<u64>, SableError> {
        let mut node_heartbeat_ts: Option<u64> = None;
        get_conn_and_run(self.options.clone(), |client| {
            let res = client.hget(&node_id, PROP_LAST_UPDATED)?;
            node_heartbeat_ts = match res {
                Value::BulkString(val) => {
                    let Ok(val) = String::from_utf8_lossy(&val).parse::<u64>() else {
                        return Err(SableError::OtherError(
                            "Failed to parse last_updated field to u64".into(),
                        ));
                    };
                    Some(val)
                }
                Value::Nil => {
                    // Not found
                    None
                }
                _ => {
                    return Err(SableError::OtherError(
                        "Failed to parse last_updated field to u64. Expected BulkString value"
                            .into(),
                    ));
                }
            };
            Ok(())
        })?;

        Ok(node_heartbeat_ts)
    }

    /// Add `replica_node_id` to the set identified by the key `<primary_node_id>_replicas`
    pub fn update_replicas_set(
        &self,
        primary_node_id: &String,
        replica_node_id: &String,
    ) -> Result<(), SableError> {
        get_conn_and_run(self.options.clone(), |client| {
            tracing::debug!(
                "Adding replica {} to node {}",
                replica_node_id,
                primary_node_id
            );
            let key = format!("{}_replicas", primary_node_id);
            client.sadd::<String, &String, redis::Value>(key, replica_node_id)?;
            tracing::debug!("Success");
            Ok(())
        })
    }

    /// Return list of replicas for the current node's primary + their last updated txn ID
    pub fn list_replicas(&self) -> Result<Vec<(String, u64)>, SableError> {
        let mut result = Vec::<(String, u64)>::new();
        get_conn_and_run(self.options.clone(), |client| {
            let primary_node_id = self.node_primary_id()?;
            let shard_set_key = format!("{}_replicas", primary_node_id);
            let Value::Array(members) = client.smembers(&shard_set_key)? else {
                return Err(SableError::OtherError(
                    "List replicas error. Invalid return value. (Expected Array)".into(),
                ));
            };
            tracing::debug!("SMEMBERS for {} -> {:#?}", shard_set_key, members);
            let replica_ids: Vec<String> = members
                .iter()
                .filter_map(|v| match v {
                    Value::BulkString(s) => {
                        // read the node's last_updated field
                        Some(String::from_utf8_lossy(s).to_string())
                    }
                    _ => None,
                })
                .collect();

            // for each member, retrieve its last updated property
            for node_id in &replica_ids {
                if let Value::BulkString(last_updated) = client.hget(node_id, PROP_LAST_TXN_ID)? {
                    let last_updated = String::from_utf8_lossy(&last_updated)
                        .parse::<u64>()
                        .unwrap_or(0);
                    result.push((node_id.clone(), last_updated));
                } else {
                    result.push((node_id.clone(), 0));
                }
            }
            Ok(())
        })?;
        Ok(result)
    }

    /// Return the node address from the database
    pub fn node_address(&self, node_id: &String) -> Result<String, SableError> {
        let mut address_mut = String::default();
        get_conn_and_run(self.options.clone(), |client| {
            let Value::BulkString(address) = client.hget(node_id, PROP_NODE_ADDRESS)? else {
                return Err(SableError::ClsuterDbError(format!(
                    "Failed to read node {} address. Expected BulkString",
                    node_id
                )));
            };
            address_mut = String::from_utf8_lossy(&address).to_string();
            Ok(())
        })?;
        Ok(address_mut)
    }

    /// ========---------------------------------------------
    /// Internal API
    /// ========---------------------------------------------
    fn node_primary_id(&self) -> Result<String, SableError> {
        let primary_node_id = Server::state().persistent_state().primary_node_id();
        if primary_node_id.is_empty() {
            return Err(SableError::OtherError(
                "Could not find primary ID. No known primary at this point in time".into(),
            ));
        }
        Ok(primary_node_id)
    }
}
