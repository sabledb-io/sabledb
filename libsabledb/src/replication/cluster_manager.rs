use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};
use crate::{
    replication::{ClusterDB, Lock, PrimaryLock, ServerRole},
    utils::{RespResponseParserV2, ResponseParseResult, TimeUtils, ValkeyObject},
    SableError, Server, ServerOptions, StorageAdapter, ValkeyCommand,
};
use num_format::{Locale, ToFormattedString};
use rand::Rng;
use redis::Commands;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock as StdRwLock,
};

lazy_static::lazy_static! {
    static ref LAST_UPDATED_TS: AtomicU64 = AtomicU64::default();
    static ref LAST_HTBT_CHECKED_TS: AtomicU64 = AtomicU64::default();
    static ref CHECK_PRIMARY_ALIVE_INTERVAL: AtomicU64 = AtomicU64::default();
    static ref NOT_RESPONDING_COUNTER: AtomicU64 = AtomicU64::default();
}

enum ProcessCommandQueueResult {
    Done,
    NoCommands,
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

/// Check whether we have a cluster DB set in our configuration file
/// return if not
macro_rules! check_cluster_db_or {
    ($options:expr, $ret_val:expr) => {{
        if $options
            .read()
            .expect("read error")
            .general_settings
            .cluster_address
            .is_none()
        {
            return $ret_val;
        }
    }};
}

#[derive(Default, Debug)]
pub struct NodeProperties {
    properties: BTreeMap<&'static str, String>,
}

impl NodeProperties {
    pub fn current(options: Arc<StdRwLock<ServerOptions>>) -> Self {
        let mut properties = BTreeMap::<&str, String>::new();
        properties.insert(PROP_NODE_ID, Server::state().persistent_state().id());
        properties.insert(
            PROP_NODE_ADDRESS,
            options
                .read()
                .expect("read error")
                .general_settings
                .private_address
                .clone(),
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
    pub fn put(&self, conn: &mut redis::Connection) -> Result<(), SableError> {
        let cur_node_id = Server::state().persistent_state().id();
        let props: Vec<(String, String)> = self
            .properties
            .iter()
            .map(|(field_name, field_value)| (field_name.to_string(), field_value.to_string()))
            .collect();
        tracing::debug!("Writing node object in cluster manager: {:?}", props);
        let _: redis::Value = conn.hset_multiple(cur_node_id, &props)?;
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
pub fn initialise(_options: Arc<StdRwLock<ServerOptions>>) {
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
    options: Arc<StdRwLock<ServerOptions>>,
    node_info: &NodeProperties,
) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));

    let db = ClusterDB::with_options(options);
    db.put_node_properties(node_info)?;

    if Server::state().persistent_state().is_primary() {
        // Add ourself to the CLUSTER_PRIMARIES set
        db.cluster_add(&Server::state().persistent_state().id())?;
    }
    Ok(())
}

/// Associate current node as
pub fn update_replicas_set(options: Arc<StdRwLock<ServerOptions>>) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));

    if !Server::state().persistent_state().is_replica() {
        return Ok(());
    }

    let primary_node_id = Server::state().persistent_state().primary_node_id();
    let current_node_id = Server::state().persistent_state().id();
    if primary_node_id.is_empty() {
        tracing::warn!(
            "Can't associate Replica({}) with Primary({})",
            current_node_id,
            primary_node_id
        );
        return Ok(());
    }

    let db = ClusterDB::with_options(options);
    db.update_replicas_set(&primary_node_id, &current_node_id)
}

/// Update the cluster database that this node is a primary
pub fn delete_self(options: Arc<StdRwLock<ServerOptions>>) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));
    let current_node_id = Server::state().persistent_state().id();
    tracing::info!(
        "Deleting node (self): {} from the cluster database",
        current_node_id
    );
    let db = ClusterDB::with_options(options);

    // if we are associated with a primary -> remove ourself
    let _ = db.remove_this_from_primary();
    db.delete_self()
}

/// If there are commands on this node's queue, process one
pub async fn check_node_queue(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));
    if is_commands_queue_empty(options.clone()).await? {
        return Ok(());
    }

    // Process one command from the queue
    process_commands_queue(options.clone(), store, 1).await
}

/// Check and perform failover is needed
pub async fn fail_over_if_needed(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));

    // In order to be able to perform a failover, this instance needs to have a valid primary node ID
    if Server::state()
        .persistent_state()
        .primary_node_id()
        .is_empty()
        && Server::state().persistent_state().is_replica()
    {
        tracing::debug!("No primary is set yet, fail-over is ignored");
        return Ok(());
    }

    // Synchronized the operations by using the shard lock
    let mut primary_lock = PrimaryLock::new(options.clone())?;
    primary_lock.lock()?;

    if !is_commands_queue_empty(options.clone()).await? {
        // Got commands to process, do it now
        // This can happen if a fail-over is in process
        return process_commands_queue(options.clone(), store, 1).await;
    }

    if is_primary_alive(options.clone())? {
        return Ok(());
    }

    tracing::info!("Starting fail-over");
    let db = ClusterDB::with_options(options.clone());
    let current_node_id = Server::state().persistent_state().id();

    tracing::info!("Trying to perform a failover...");

    // This instance will be the orchestrator of the failover
    tracing::info!("Listing replicas...");
    let mut all_replicas = db.list_replicas()?;
    tracing::info!("Found {:?}", all_replicas);

    // We have multiple replicas, choose the best replica and make it the primary
    let Some((new_primary_id, _last_txn_id)) = all_replicas
        .iter()
        .max_by_key(|(_node_id, last_updated_txn)| last_updated_txn)
    else {
        return Err(SableError::AutoFailOverError("No replicas found".into()));
    };

    let old_primary_id = Server::state().persistent_state().primary_node_id();
    tracing::info!(
        "New primary: {}. Old primary {}",
        new_primary_id,
        old_primary_id
    );
    let new_primary_id = new_primary_id.clone();

    // Delete the old primary from the "CLUSTER_PRIMARIES" table
    db.cluster_del(&old_primary_id)?;

    // Add the old primary to the list of replicas. This way we also broadcast a "REPLICAOF <IP> <PORT>"
    // to the old primary's queue
    all_replicas.push((old_primary_id, 0));
    broadcast_failover(options.clone(), &all_replicas, &new_primary_id).await?;

    if current_node_id.eq(&new_primary_id) {
        // If this node is the new primary, don't wait until next iteration, switch to primary node now
        process_commands_queue(options, store, 1).await?;
    }
    Ok(())
}

//===------------------------------
// Private API calls
//===------------------------------

/// Broadcast all members of this shard that a failover is taking place
async fn broadcast_failover(
    options: Arc<StdRwLock<ServerOptions>>,
    all_replicas: &Vec<(String, u64)>,
    new_primary_id: &String,
) -> Result<(), SableError> {
    // get the new primary port + IP
    let db = ClusterDB::with_options(options);
    let address = db.node_address(new_primary_id)?;
    let address = address.replace(':', " ");
    let command = format!("REPLICAOF {}", address);
    let command_primary = String::from("REPLICAOF NO ONE");

    tracing::info!("Sending commands to replicas...");
    for (node_id, _) in all_replicas {
        if node_id.eq(new_primary_id) {
            tracing::info!(
                "Sending command: '{}' to new primary {}",
                &command_primary,
                node_id
            );
            db.push_command(node_id, &command_primary)?;
        } else {
            tracing::info!("Sending command: '{}' to node {}", &command, node_id);
            db.push_command(node_id, &command)?;
        }
    }
    tracing::info!("Success");
    Ok(())
}

/// Return whether the current node's command queue is empty
async fn is_commands_queue_empty(
    options: Arc<StdRwLock<ServerOptions>>,
) -> Result<bool, SableError> {
    let db = ClusterDB::with_options(options);
    let current_node_id = Server::state().persistent_state().id();
    Ok(db.command_queue_len(&current_node_id)? == 0)
}

/// Process `count` commands from the node's commands queue
async fn process_commands_queue(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
    mut count: u32,
) -> Result<(), SableError> {
    // wait for the command and run it
    loop {
        if let ProcessCommandQueueResult::Done =
            try_process_commands_queue(options.clone(), store).await?
        {
            count = count.saturating_sub(1);
            if count == 0 {
                return Ok(());
            }
        }
    }
}

/// Check the current node's command queue and process a single command from it
async fn try_process_commands_queue(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<ProcessCommandQueueResult, SableError> {
    let db = ClusterDB::with_options(options.clone());
    let current_node_id = Server::state().persistent_state().id();

    // wait for the command and run it
    tracing::info!("Waiting for command on queue {}_QUEUE...", current_node_id);
    let Some(cmd) = db.pop_command_with_timeout(&current_node_id, 1.0)? else {
        return Ok(ProcessCommandQueueResult::NoCommands);
    };

    tracing::info!("Node {} running command '{}'", current_node_id, cmd);
    process_command_internal(
        options,
        store,
        cmd.as_str(),
        ValkeyObject::Status("OK".into()),
    )
    .await?;
    Ok(ProcessCommandQueueResult::Done)
}

async fn process_command_internal(
    _options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
    command: &str,
    expected_output: ValkeyObject,
) -> Result<bool, SableError> {
    let Ok(cmd) = ValkeyCommand::from_str(command) else {
        return Err(SableError::AutoFailOverError(
            "Failed to construct command".into(),
        ));
    };

    // Instruct this instance to switch role to primary
    let response = match RespResponseParserV2::parse_response(
        &Server::process_internal_command(Rc::new(cmd), store).await?,
    )? {
        ResponseParseResult::Ok((_, response)) => response,
        ResponseParseResult::NeedMoreData => {
            return Err(SableError::AutoFailOverError(
                "Failed to process internal command (NeedMoreData)".into(),
            ));
        }
    };
    Ok(response == expected_output)
}

/// Check that the primary is alive
fn is_primary_alive(options: Arc<StdRwLock<ServerOptions>>) -> Result<bool, SableError> {
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
