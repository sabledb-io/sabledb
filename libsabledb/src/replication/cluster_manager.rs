use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};
use crate::{
    replication::{cluster_database::LockResult, ClusterDB, ServerRole},
    utils::{RedisObject, RespResponseParserV2, ResponseParseResult, TimeUtils},
    RedisCommand, SableError, Server, ServerOptions, StorageAdapter,
};
use num_format::{Locale, ToFormattedString};
use rand::Rng;
use redis::Commands;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

lazy_static::lazy_static! {
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
    let db = ClusterDB::with_options(options);
    db.put_node_properties(node_info)
}

/// Update the "last updated" field for this node in the cluster manager database
pub fn put_last_updated(options: &ServerOptions) -> Result<(), SableError> {
    if !check_us_passed_since!(LAST_UPDATED_TS, 1_000_000) {
        return Ok(());
    }

    let db = ClusterDB::with_options(options);
    db.put_last_updated()?;
    update_heartbeat_reported_ts();
    Ok(())
}

/// Associate node identified by `replica_node_id` with the current node
pub fn add_replica(options: &ServerOptions, replica_node_id: String) -> Result<(), SableError> {
    let db = ClusterDB::with_options(options);
    db.add_replica(replica_node_id)
}

#[allow(dead_code)]
/// Remove replica identified by `replica_node_id` from the current node
pub fn remove_replica_from_this(
    options: &ServerOptions,
    replica_node_id: String,
) -> Result<(), SableError> {
    let db = ClusterDB::with_options(options);
    db.remove_replica_from_this(replica_node_id)
}

#[allow(dead_code)]
/// Remove the current from its primary
pub fn remove_this_from_primary(options: &ServerOptions) -> Result<(), SableError> {
    let db = ClusterDB::with_options(options);
    db.remove_this_from_primary()
}

/// Update the cluster database that this node is a primary
pub fn delete_self(options: &ServerOptions, _store: &StorageAdapter) -> Result<(), SableError> {
    let current_node_id = Server::state().persistent_state().id();
    tracing::info!(
        "Deleting node: {} from the cluster database",
        current_node_id
    );
    let db = ClusterDB::with_options(options);

    // if we are associated with a primary -> remove ourself
    let _ = db.remove_this_from_primary();
    db.delete_self()
}

/// Check and perform failover is needed
pub async fn fail_over_if_needed(
    options: &ServerOptions,
    store: &StorageAdapter,
) -> Result<(), SableError> {
    if is_primary_alive(options)? {
        return Ok(());
    }

    let db = ClusterDB::with_options(options);

    // Try to lock the primary, if we succeeded in doing this,
    // this `lock_primary` returns the value of the lock which
    // is used later to delete the lock
    tracing::info!("Trying to perform a failover...");
    let lock_created = match db.lock_primary()? {
        LockResult::Ok => {
            tracing::info!("Successfully created lock");
            true
        }
        LockResult::AlreadyExist(node_id) => {
            tracing::info!("Failover already started by Node {node_id}");
            false
        }
    };

    if lock_created {
        // This instance will be the orchestrator of the failover
        tracing::info!("Listing replicas...");
        let members = db.list_replicas()?;
        tracing::info!("Found {:?}", members);

        let current_node_id = Server::state().persistent_state().id();

        // filter this instance
        let members: Vec<(&String, &u64)> = members
            .iter()
            .filter_map(|(node_id, last_updated)| {
                if node_id.ne(&current_node_id) {
                    Some((node_id, last_updated))
                } else {
                    None
                }
            })
            .collect();

        tracing::info!(
            "Remaining replicas after removing self from the list: {:?}",
            members
        );

        if members.is_empty() {
            // No other replicas, disconnect this node from the primary, and switch to primary node
            tracing::info!("This node is the single replica. Switching to primary mode");
            if !process_command_internal(
                options,
                store,
                "REPLICAOF NO ONE",
                RedisObject::Status("OK".into()),
            )
            .await?
            {
                return Err(SableError::OtherError(
                    "Failed to execute internal command: REPLICAOF NO ONE".into(),
                ));
            }

            // Perform a database cleanup and change this node to Primary
            tracing::info!("Deleting Primary from the cluster database");
            db.delete_primary()?;
            tracing::info!("Success");
        } else {
            // TODO: we have more replicas, choose the best replica and make it the primary
        }
    } else {
        // Some other instance is coordinating the failover
        // TODO: wait for the command for the coordinator replica
    }
    Ok(())
}

///===------------------------------
/// Private API calls
///===------------------------------
async fn process_command_internal(
    _options: &ServerOptions,
    store: &StorageAdapter,
    command: &str,
    expected_output: RedisObject,
) -> Result<bool, SableError> {
    let Ok(cmd) = RedisCommand::from_str(command) else {
        return Err(SableError::InternalError(
            "Failed to construct command".into(),
        ));
    };

    // Instruct this instance to switch role to primary
    let response = match RespResponseParserV2::parse_response(
        &Server::process_internal_command(Rc::new(cmd), store).await?,
    )? {
        ResponseParseResult::Ok((_, response)) => response,
        ResponseParseResult::NeedMoreData => {
            return Err(SableError::ParseError(
                "Failed to process internal command (NeedMoreData)".into(),
            ));
        }
    };
    Ok(response == expected_output)
}

/// Check that the primary is alive
fn is_primary_alive(options: &ServerOptions) -> Result<bool, SableError> {
    if !check_us_passed_since!(LAST_HTBT_CHECKED_TS, 1_500_000) {
        return Ok(true);
    }

    // Only replica should run this test
    if !Server::state().persistent_state().is_replica() {
        return Ok(true);
    }
    let db = ClusterDB::with_options(options);

    // get the primary last updated timestamp from the database
    let primary_heartbeat_ts = db.primary_last_updated()?.unwrap_or(0);
    let primary_node_id = Server::state().persistent_state().primary_node_id();
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
