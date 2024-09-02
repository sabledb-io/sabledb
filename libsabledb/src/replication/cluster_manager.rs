use crate::{replication::ServerRole, utils::TimeUtils, SableError, Server, ServerOptions};
use redis::Commands;
use std::collections::BTreeMap;
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref CM_CONN: RwLock<Connection> = RwLock::<Connection>::default();
}

thread_local! {
    pub static LAST_UPDATED_TS: std::cell::RefCell<u64> = const { std::cell::RefCell::new(0u64) };
}

const PROP_NODE_ID: &str = "node_id";
const PROP_NODE_ADDRESS: &str = "node_address";
const PROP_ROLE: &str = "role";
const PROP_LAST_UPDATED: &str = "last_updated";
const PROP_LAST_TXN_ID: &str = "last_txn_id";
const PROP_PRIMARY_NODE_ID: &str = "primary_node_id";

#[derive(Default)]
pub struct NodeProperties {
    properties: BTreeMap<&'static str, String>,
}

impl NodeProperties {
    pub fn current(options: &ServerOptions) -> Self {
        let mut properties = BTreeMap::<&str, String>::new();
        properties.insert(PROP_NODE_ID, Server::state().persistent_state().id());
        properties.insert(
            PROP_NODE_ADDRESS,
            options.general_settings.private_address.clone(),
        );
        properties.insert(
            PROP_ROLE,
            format!("{}", Server::state().persistent_state().role()),
        );
        properties.insert(
            PROP_LAST_UPDATED,
            format!("{}", TimeUtils::epoch_micros().unwrap_or_default()),
        );
        properties.insert(PROP_LAST_TXN_ID, "0".to_string());
        properties.insert(PROP_PRIMARY_NODE_ID, String::default());
        NodeProperties { properties }
    }

    /// Write this object to the cluster manager database
    pub fn put(&self, conn: &mut redis::Client) -> Result<(), SableError> {
        let cur_node_id = Server::state().persistent_state().id();
        let props: Vec<(String, String)> = self
            .properties
            .iter()
            .map(|(field_name, field_value)| (field_name.to_string(), field_value.to_string()))
            .collect();
        tracing::debug!("Writing node object in cluster manager: {:?}", props);
        let mut conn = conn.get_connection()?;
        conn.hset_multiple(cur_node_id, &props)?;
        tracing::debug!("Success");
        update_heartbeat_reported_ts();
        Ok(())
    }

    pub fn with_last_txn_id(mut self, last_txn_id: u64) -> Self {
        self.properties
            .insert(PROP_LAST_TXN_ID, format!("{}", last_txn_id));
        self
    }

    pub fn with_role_replica(mut self) -> Self {
        self.properties
            .insert(PROP_ROLE, format!("{}", ServerRole::Replica));
        self
    }

    pub fn with_role_primary(mut self) -> Self {
        self.properties
            .insert(PROP_ROLE, format!("{}", ServerRole::Primary));
        self
    }

    pub fn with_primary_node_id(mut self, primary_node_id: String) -> Self {
        self.properties
            .insert(PROP_PRIMARY_NODE_ID, primary_node_id);
        self
    }
}

/// Connection to the cluster manager database
#[derive(Default)]
struct Connection {
    pub client: Option<redis::Client>,
}

impl Connection {
    pub fn open(&mut self, options: &ServerOptions) -> Result<(), SableError> {
        let Some(cluster_address) = &options.general_settings.cluster_address else {
            return Ok(());
        };

        self.client = Some(redis::Client::open(format!(
            "rediss://{}/#insecure",
            cluster_address
        ))?);
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }
}

/// Connect to the cluster database
fn connect(options: &ServerOptions) -> Result<(), SableError> {
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    if conn.is_connected() {
        return Ok(());
    }
    conn.open(options)
}

/// Update this node with the server manager database. If no database is configured, do nothing
/// If an entry for the same node already exists, it is overridden (the key is the node-id)
pub fn put_node_properties(
    options: &ServerOptions,
    node_info: &NodeProperties,
) -> Result<(), SableError> {
    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(());
    };
    node_info.put(client)
}

/// Update the "last updated" field for this node in the cluster manager database
pub fn put_last_updated(options: &ServerOptions) -> Result<(), SableError> {
    if !can_update_heartbeat() {
        return Ok(());
    }

    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(());
    };

    let cur_node_id = Server::state().persistent_state().id();
    let curts = TimeUtils::epoch_micros().unwrap_or_default();
    tracing::debug!(
        "Updating cluster manager property: '{}:({} => {})'",
        cur_node_id,
        PROP_LAST_UPDATED,
        curts
    );
    let mut conn = client.get_connection()?;
    conn.hset(cur_node_id, PROP_LAST_UPDATED, curts)?;
    update_heartbeat_reported_ts();
    tracing::debug!("Success");
    Ok(())
}

/// Associate node identified by `replica_node_id` with the current node
pub fn add_replica(options: &ServerOptions, replica_node_id: String) -> Result<(), SableError> {
    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(());
    };
    let cur_node_id = Server::state().persistent_state().id();
    let mut conn = client.get_connection()?;
    tracing::debug!(
        "Associating node({}) as replica for ({})",
        replica_node_id,
        cur_node_id,
    );
    let key = format!("{}_replicas", cur_node_id);
    conn.sadd(key, replica_node_id)?;
    tracing::debug!("Success");
    Ok(())
}

#[allow(dead_code)]
/// Remove replica identified by `replica_node_id` from the current node
pub fn remove_replica(options: &ServerOptions, replica_node_id: String) -> Result<(), SableError> {
    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(());
    };
    let cur_node_id = Server::state().persistent_state().id();
    let mut conn = client.get_connection()?;
    tracing::debug!(
        "Disassociating node({}) from primary ({})",
        replica_node_id,
        cur_node_id,
    );
    let key = format!("{}_replicas", cur_node_id);
    conn.srem(key, replica_node_id)?;
    tracing::debug!("Success");
    Ok(())
}

/// Return true if 1000 milliseconds passed since the last time we updated the cluster manager database
fn can_update_heartbeat() -> bool {
    let current_time =
        crate::utils::current_time(crate::utils::CurrentTimeResolution::Milliseconds);
    LAST_UPDATED_TS.with(|last_ts| {
        let last_logged_ts = *last_ts.borrow();
        if current_time - last_logged_ts >= 1000 {
            *last_ts.borrow_mut() = current_time;
            true
        } else {
            false
        }
    })
}

/// Return true if 1000 milliseconds passed since the last time we updated the cluster manager database
fn update_heartbeat_reported_ts() {
    let current_time =
        crate::utils::current_time(crate::utils::CurrentTimeResolution::Milliseconds);
    LAST_UPDATED_TS.with(|last_ts| {
        *last_ts.borrow_mut() = current_time;
    })
}