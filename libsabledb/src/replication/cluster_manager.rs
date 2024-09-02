use crate::{replication::ServerRole, utils::TimeUtils, SableError, Server, ServerOptions};
use num_format::{Locale, ToFormattedString};
use rand::Rng;
use redis::Commands;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

lazy_static::lazy_static! {
    static ref CM_CONN: RwLock<Connection> = RwLock::<Connection>::default();
    static ref LAST_UPDATED_TS: AtomicU64 = AtomicU64::default();
    static ref LAST_HTBT_CHECKED_TS: AtomicU64 = AtomicU64::default();
    static ref CHECK_PRIMARY_ALIVE_INTERVAL: AtomicU64 = AtomicU64::default();
    static ref NOT_RESPONDING_COUNTER: AtomicU64 = AtomicU64::default();
}

macro_rules! check_us_passed_since {
    ($counter:expr, $interval_us:expr) => {{
        let current_time = $crate::TimeUtils::epoch_micros().unwrap_or_default();
        let last_logged_ts = $counter.load(Ordering::Relaxed);
        if current_time - last_logged_ts >= $interval_us {
            $counter.store(current_time, Ordering::Relaxed);
            true
        } else {
            false
        }
    }};
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

/// Initialise the cluster manager API
pub fn initialise(_options: &ServerOptions) {
    let mut rng = rand::thread_rng();
    let us = rng.gen_range(5000000..10_000000);
    CHECK_PRIMARY_ALIVE_INTERVAL.store(us, Ordering::Relaxed);
    tracing::info!(
        "Check primary interval is set to {} microseconds",
        us.to_formatted_string(&Locale::en)
    );
    NOT_RESPONDING_COUNTER.store(0, Ordering::Relaxed);
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
    if !check_us_passed_since!(LAST_UPDATED_TS, 1_000_000) {
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

/// Check and perform failover is needed
pub fn fail_over_if_needed(options: &ServerOptions) -> Result<(), SableError> {
    if check_is_alive(options)? {
        return Ok(());
    }

    // TODO: do failover here
    Ok(())
}

///===------------------------------
/// Private API calls
///===------------------------------

/// Check that the primary is alive
fn check_is_alive(options: &ServerOptions) -> Result<bool, SableError> {
    if !check_us_passed_since!(LAST_HTBT_CHECKED_TS, 1_500_000) {
        return Ok(true);
    }

    // Only replica should run this test
    if !Server::state().persistent_state().is_replica() {
        return Ok(true);
    }

    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(true);
    };

    let primary_node_id = Server::state().persistent_state().primary_node_id();
    if primary_node_id.is_empty() {
        tracing::debug!("Replica has no primary node defined");
        return Ok(true);
    }
    let mut conn = client.get_connection()?;

    let res = conn.hget(&primary_node_id, PROP_LAST_UPDATED)?;
    let primary_heartbeat_ts = match res {
        redis::Value::BulkString(val) => {
            let Ok(val) = String::from_utf8_lossy(&val).parse::<u64>() else {
                tracing::warn!("Failed to parse last_updated field to u64");
                return Ok(true);
            };
            val
        }
        _ => {
            tracing::warn!("Expected BulkString value");
            return Ok(true);
        }
    };

    let curr_ts = TimeUtils::epoch_micros().unwrap_or(0);
    if curr_ts.saturating_sub(primary_heartbeat_ts)
        > CHECK_PRIMARY_ALIVE_INTERVAL.load(Ordering::Relaxed)
    {
        // Primary is not responding!
        if NOT_RESPONDING_COUNTER.load(Ordering::Relaxed) >= 3 {
            tracing::info!("Primary {primary_node_id} is not available");
            return Ok(false);
        } else {
            // increase the error counter
            tracing::debug!(
                "Primary {primary_node_id} is not responding. Retry counter={}",
                NOT_RESPONDING_COUNTER.load(Ordering::Relaxed)
            );
            NOT_RESPONDING_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
    } else {
        if NOT_RESPONDING_COUNTER.load(Ordering::Relaxed) >= 3 {
            tracing::info!("Primary {primary_node_id} is alive again!");
        }
        // clear the error counter
        NOT_RESPONDING_COUNTER.store(0, Ordering::Relaxed);
    }
    Ok(true)
}

/// Return true if 1000 milliseconds passed since the last time we updated the cluster manager database
fn update_heartbeat_reported_ts() {
    let current_time =
        crate::utils::current_time(crate::utils::CurrentTimeResolution::Microseconds);
    LAST_UPDATED_TS.store(current_time, Ordering::Relaxed);
}
