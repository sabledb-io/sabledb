use crate::{replication::NodeProperties, utils::TimeUtils, SableError, Server, ServerOptions};
use redis::Commands;
use redis::Value;
use std::sync::RwLock;
use std::time::Duration;

#[allow(unused_imports)]
use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};

const MUTEX_ERR: &str = "poisoned mutex";

lazy_static::lazy_static! {
    static ref CM_CONN: RwLock<Option<redis::Client>> = RwLock::<Option<redis::Client>>::default();
}

#[derive(Debug, PartialEq, Clone)]
pub enum LockResult {
    /// Lock created successfully
    Ok,
    /// A lock already exists. Return the lock value (which is the owner node-id)
    AlreadyExist(String),
}

/// Attempt to open redis connection. If a connection is already opened,
/// do nothing
fn open_connection(options: &ServerOptions) {
    tracing::trace!("Setting up connection with cluster database");
    {
        // we already have a connection opened, nothing more to be done here
        let conn = CM_CONN.read().expect(MUTEX_ERR);
        if conn.is_some() {
            tracing::trace!("Connection is already opened");
            return;
        }
    }

    let Some(cluster_address) = &options.general_settings.cluster_address else {
        // No cluster address
        tracing::trace!("Cluster database is not configured");
        return;
    };

    // run initialization here
    let cluster_address = format!("rediss://{}/#insecure", cluster_address);
    tracing::info!("Opening...");
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
    tracing::trace!("Success");
}

/// Close the current connection, making sure that the next call to `open_connection`
/// will re-open it
#[allow(dead_code)]
fn close_connection() {
    *CM_CONN.write().expect(MUTEX_ERR) = None;
}

fn get_conn_and_run<F>(options: &ServerOptions, mut code: F) -> Result<(), SableError>
where
    F: FnMut(&mut redis::Connection) -> Result<(), SableError>,
{
    open_connection(options);
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

pub struct ClusterDB<'a> {
    options: &'a ServerOptions,
}

impl<'a> ClusterDB<'a> {
    pub fn with_options(options: &'a ServerOptions) -> Self {
        ClusterDB { options }
    }

    /// Update this node with the server manager database. If no database is configured, do nothing
    /// If an entry for the same node already exists, it is overridden (the key is the node-id)
    pub fn put_node_properties(&self, node_info: &NodeProperties) -> Result<(), SableError> {
        get_conn_and_run(self.options, |client| node_info.put(client))
    }

    /// Update the "last_updated" field for this node
    pub fn put_last_updated(&self) -> Result<(), SableError> {
        get_conn_and_run(self.options, move |client| {
            let cur_node_id = Server::state().persistent_state().id();
            let curts = TimeUtils::epoch_micros().unwrap_or_default();
            tracing::trace!("Updating heartbeat for node {} -> {}", &cur_node_id, curts);
            client.hset(cur_node_id, PROP_LAST_UPDATED, curts)?;
            tracing::trace!("Success");
            Ok(())
        })
    }

    /// Associate node identified by `replica_node_id` with the current node
    pub fn add_replica(&self, replica_node_id: String) -> Result<(), SableError> {
        get_conn_and_run(self.options, move |client| {
            if !Server::state().persistent_state().is_primary() {
                return Err(SableError::OtherError(
                    "Can't add replica. Node is not a Primary".into(),
                ));
            }

            let cur_node_id = Server::state().persistent_state().id();
            tracing::debug!(
                "Adding replica {} to node {}",
                &replica_node_id,
                cur_node_id
            );
            let key = format!("{}_replicas", cur_node_id);
            client.sadd(key, &replica_node_id)?;
            tracing::debug!("Success");
            Ok(())
        })
    }

    /// Remove replica identified by `replica_node_id` from the current node
    pub fn remove_replica_from_this(&self, replica_node_id: String) -> Result<(), SableError> {
        get_conn_and_run(self.options, move |client| {
            let cur_node_id = Server::state().persistent_state().id();
            tracing::debug!(
                "Disassociating node({}) from primary ({})",
                replica_node_id,
                cur_node_id,
            );
            let key = format!("{}_replicas", cur_node_id);
            client.srem(key, &replica_node_id)?;
            tracing::debug!("Success");
            Ok(())
        })
    }

    /// Detach this node from the primary
    pub fn remove_this_from_primary(&self) -> Result<(), SableError> {
        get_conn_and_run(self.options, move |client| {
            let cur_node_id = Server::state().persistent_state().id();
            let primary_node_id = self.node_primary_id()?;
            tracing::info!(
                "Disassociating self({}) from primary ({})",
                cur_node_id,
                primary_node_id,
            );
            let key = format!("{}_replicas", primary_node_id);
            client.srem(key, &cur_node_id)?;
            tracing::info!("Success");
            Ok(())
        })
    }

    /// Delete this node from the database
    pub fn delete_self(&self) -> Result<(), SableError> {
        self.delete_node(Server::state().persistent_state().id())
    }

    /// Delete node identified by `node_id` from the database
    pub fn delete_node(&self, node_id: String) -> Result<(), SableError> {
        get_conn_and_run(self.options, move |client| {
            client.del(&node_id)?;
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
        get_conn_and_run(self.options, |client| {
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

    /// Attempt to create a lock for this node's primary
    pub fn lock_primary(&self) -> Result<LockResult, SableError> {
        let primary_node_id = self.node_primary_id()?;
        let current_node_id = Server::state().persistent_state().id();
        let lock_key = format!("{}_LOCK", primary_node_id);

        let mut result = LockResult::Ok;
        get_conn_and_run(self.options, |client| {
            let set_options = redis::SetOptions::default()
                .with_expiration(redis::SetExpiry::EX(60))
                .conditional_set(redis::ExistenceCheck::NX);
            let res = client.set_options(&lock_key, &current_node_id, set_options)?;
            match res {
                Value::Nil => {
                    let lock_value = if let Value::BulkString(s) = client.get(&lock_key)? {
                        String::from_utf8_lossy(&s).to_string()
                    } else {
                        String::default()
                    };
                    result = LockResult::AlreadyExist(lock_value);
                    Ok(())
                }
                Value::Okay => {
                    result = LockResult::Ok;
                    Ok(())
                }
                other => Err(SableError::OtherError(format!(
                    "Create lock error. Invalid return value. {:?}",
                    other
                ))),
            }
        })?;
        Ok(result)
    }

    /// Return list of replicas for the current node's primary + their last updated txn ID
    pub fn list_replicas(&self) -> Result<Vec<(String, u64)>, SableError> {
        let mut result = Vec::<(String, u64)>::new();
        get_conn_and_run(self.options, |client| {
            let primary_node_id = self.node_primary_id()?;
            let shard_set_key = format!("{}_replicas", primary_node_id);
            let Value::Array(members) = client.smembers(&shard_set_key)? else {
                return Err(SableError::OtherError(
                    "List replicas error. Invalid return value. (Expected Array)".into(),
                ));
            };
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
                }
            }
            Ok(())
        })?;
        Ok(result)
    }

    /// Delete primary from the database
    /// - Delete the primary record from the database
    /// - Delete `<primary_id>_replicas` set from the database
    pub fn delete_primary(&self) -> Result<(), SableError> {
        let primary_id = self.node_primary_id()?;
        get_conn_and_run(self.options, |client| {
            client.del(&primary_id)?;
            let replicas_set = format!("{}_replicas", primary_id);
            client.del(&replicas_set)?;
            Ok(())
        })
    }

    /// Return the node address from the database
    pub fn node_address(&self, node_id: &String) -> Result<String, SableError> {
        let mut address_mut = String::default();
        get_conn_and_run(self.options, |client| {
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

    /// Push a command to the node-id
    pub fn push_command(&self, node_id: &String, command: &String) -> Result<(), SableError> {
        get_conn_and_run(self.options, |client| {
            let queue_name = format!("{}_QUEUE", node_id);
            tracing::info!("Sending command '{}' to queue '{}'", command, queue_name);
            client.lpush(&queue_name, command)?;
            tracing::info!("Success");
            Ok(())
        })
    }

    /// Pop a command from the top of the queue with a timeout
    pub fn pop_command_with_timeout(
        &self,
        node_id: &String,
        timeout_secs: f64,
    ) -> Result<Option<String>, SableError> {
        let mut val = Option::<String>::None;
        get_conn_and_run(self.options, |client| {
            let queue_name = format!("{}_QUEUE", node_id);
            if let Value::Array(arr) = client.brpop(&queue_name, timeout_secs)? {
                if let Some(Value::BulkString(cmd)) = arr.get(1) {
                    val = Some(String::from_utf8_lossy(cmd).to_string());
                }
            }
            Ok(())
        })?;
        Ok(val)
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
