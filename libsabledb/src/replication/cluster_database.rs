use crate::{replication::NodeProperties, utils::TimeUtils, SableError, Server, ServerOptions};
use redis::Commands;
use std::sync::RwLock;

#[allow(unused_imports)]
use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};

const MUTEX_ERR: &str = "poisoned mutex";

lazy_static::lazy_static! {
    static ref CM_CONN: RwLock<Option<redis::Client>> = RwLock::<Option<redis::Client>>::default();
}

/// Attempt to open redis connection. If a connection is already opened,
/// do nothing
fn open_connection(options: &ServerOptions) {
    {
        // we already have a connection opened, nothing more to be done here
        let conn = CM_CONN.read().expect(MUTEX_ERR);
        if conn.is_some() {
            return;
        }
    }

    let Some(cluster_address) = &options.general_settings.cluster_address else {
        // No cluster address
        return;
    };

    // run initialization here
    let cluster_address = format!("rediss://{}/#insecure", cluster_address);
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
}

/// Close the current connection, making sure that the next call to `open_connection`
/// will re-open it
fn close_connection() {
    *CM_CONN.write().expect(MUTEX_ERR) = None;
}

fn get_conn_and_run<F>(options: &ServerOptions, mut code: F) -> Result<(), SableError>
where
    F: FnMut(&mut redis::Client) -> Result<(), SableError>,
{
    open_connection(options);
    let mut conn = CM_CONN.write().expect(MUTEX_ERR);
    let Some(client) = &mut *conn else {
        return Ok(());
    };

    if let Err(e) = code(client) {
        // In case of an error, close the connection
        close_connection();
        Err(e)
    } else {
        Ok(())
    }
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
        get_conn_and_run(&self.options, |client| node_info.put(client))
    }

    /// Update the "last_updated" field for this node
    pub fn put_last_updated(&self) -> Result<(), SableError> {
        get_conn_and_run(&self.options, move |client| {
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
        get_conn_and_run(&self.options, move |client| {
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
        get_conn_and_run(&self.options, move |client| {
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
        get_conn_and_run(&self.options, move |client| {
            let cur_node_id = Server::state().persistent_state().id();
            let primary_node_id = Server::state().persistent_state().primary_node_id();
            if primary_node_id.is_empty() {
                return Err(SableError::OtherError(
                    "Can't remove self from primary - no known primary".into(),
                ));
            }

            tracing::debug!(
                "Disassociating self({}) from primary ({})",
                cur_node_id,
                primary_node_id,
            );
            let key = format!("{}_replicas", primary_node_id);
            client.srem(key, &cur_node_id)?;
            tracing::debug!("Success");
            Ok(())
        })
    }

    /// Return the "last_updated" property for this node's primary
    pub fn primary_last_updated(&self) -> Result<u64, SableError> {
        let primary_node_id = Server::state().persistent_state().primary_node_id();
        if primary_node_id.is_empty() {
            return Err(SableError::OtherError(
                "Unable to read primary heartbeat property. No known primary".into(),
            ));
        }

        let mut primary_heartbeat_ts = 0u64;
        get_conn_and_run(&self.options, |client| {
            let res = client.hget(&primary_node_id, PROP_LAST_UPDATED)?;
            primary_heartbeat_ts = match res {
                redis::Value::BulkString(val) => {
                    let Ok(val) = String::from_utf8_lossy(&val).parse::<u64>() else {
                        return Err(SableError::OtherError(
                            "Failed to parse last_updated field to u64".into(),
                        ));
                    };
                    val
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

        Ok(primary_heartbeat_ts)
    }
}
