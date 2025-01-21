use crate::replication::{
    PROP_LAST_TXN_ID, PROP_LAST_UPDATED, PROP_NODE_ADDRESS, PROP_NODE_ID, PROP_PRIMARY_NODE_ID,
    PROP_ROLE,
};
#[allow(unused_imports)]
use crate::{
    replication::{
        BlockingLock, ClusterDB, Lock, Node, NodeBuilder, Persistence, ServerRole, ShardBuilder,
        ShardPrimaryResult,
    },
    utils::{RespResponseParserV2, ResponseParseResult, TimeUtils, ValkeyObject},
    SableError, Server, ServerOptions, StorageAdapter, ValkeyCommand,
};
use num_format::{Locale, ToFormattedString};
use rand::Rng;
use redis::Commands;
use std::collections::{BTreeMap, HashMap};
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

/// Update this node with the server manager database. A node is always associated with a shard
/// If the shard does not exist, this function will also create it
///
/// If update was done successfully, return the updated node
pub fn put_node(
    options: Arc<StdRwLock<ServerOptions>>,
    mut node: Node,
) -> Result<Option<Node>, SableError> {
    check_cluster_db_or!(options, Ok(None));
    if node.shard_name().is_empty() {
        tracing::warn!(
            "No shard name is provided. Can not add node '{}' to cluster database",
            node.node_id(),
        );
        return Ok(None);
    }

    let db = Persistence::with_options(options);

    // Lock the shard
    let mut lk = BlockingLock::with_db(&db, node.shard_name().to_string());
    lk.lock()?;
    // Make sure that this node appears in the Shard information
    let (shard, primary_node) = if let Some(mut shard) = db.get_shard(node.shard_name())? {
        shard.add_node(&node);
        let primary_node = db.shard_primary(&shard)?.ok();
        (shard, primary_node)
    } else {
        let server_state = Server::state();
        let server_state = server_state.persistent_state();

        // first time, create the shard entry and put it in the database
        (
            ShardBuilder::default()
                .with_name(node.shard_name().clone())
                .with_nodes(&[&node])
                .with_slots(server_state.slots().to_string())
                .with_cluster_name(server_state.cluster_name())
                .build(),
            None,
        )
    };

    if let Some(primary_node) = primary_node {
        node.set_slots(primary_node.slots().to_string());
        node.set_primary_node_id(primary_node.node_id().to_string());
    }

    db.put_shard(&shard)?;
    db.put_node(&node)?;
    Ok(Some(node))
}

/// If there are commands on this node's queue, process one
pub async fn check_node_queue(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));

    let db = Persistence::with_options(options.clone());
    if is_commands_queue_empty(&db).await? {
        return Ok(());
    }

    // Process one command from the queue
    process_commands_queue(&db, options, store, 1).await
}

/// Check and perform failover is needed
pub async fn fail_over_if_needed(
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<(), SableError> {
    check_cluster_db_or!(options, Ok(()));

    // In order to be able to perform a failover, this instance needs to be a replica
    if !Server::state().persistent_state().is_replica() {
        tracing::debug!("Only replica can trigger a failover");
        return Ok(());
    }

    let db = Persistence::with_options(options.clone());
    let shard_name = Server::state().persistent_state().shard_name();

    // Synchronized the operations by using the shard lock
    let mut lk = BlockingLock::with_db(&db, shard_name.to_string());
    lk.lock()?;

    if !is_commands_queue_empty(&db).await? {
        // Got commands to process, do it now, This can happen if a fail-over is already in progress by another process
        return process_commands_queue(&db, options.clone(), store, 1).await;
    }

    let Some(shard) = db.get_shard(&shard_name)? else {
        tracing::warn!("Could not load shard {} from the database", shard_name);
        return Ok(());
    };

    let ShardPrimaryResult::Ok(mut old_primary) = db.shard_primary(&shard)? else {
        tracing::warn!("Could not locate shard '{}' primary", shard_name);
        return Ok(());
    };

    if is_node_alive(&old_primary)? {
        return Ok(());
    }

    tracing::info!("Starting fail-over");
    tracing::info!("Trying to perform a failover...");

    // This instance will be the orchestrator of the failover
    tracing::info!("Listing replicas...");
    let mut all_replicas = db.shard_nodes(&shard)?;

    // Keep only replicas
    all_replicas.retain(|node| node.is_replica());
    tracing::info!("Found {:#?}", all_replicas);

    // Choose the best replica to use
    let Some(mut new_primary) = all_replicas
        .iter()
        .max_by_key(|node| node.last_txn_id())
        .cloned()
    else {
        return Err(SableError::AutoFailOverError("No replicas found".into()));
    };

    // Keep all nodes that their node ID is not equal to the new primary ID
    all_replicas.retain(|node| node.node_id().ne(new_primary.node_id()));

    // Convert the vector of replicas into HashMap
    let mut all_replicas: HashMap<String, Node> = all_replicas
        .iter()
        .map(|node| (node.node_id().to_string(), (*node).clone()))
        .collect();

    tracing::info!(
        "Changing roles. New primary: {}. Old primary {}",
        new_primary.node_id(),
        old_primary.node_id()
    );

    // switch the roles
    new_primary.set_role(ServerRole::Primary);
    old_primary.set_role(ServerRole::Replica);

    // reflect the role change in the database
    db.put_node(&old_primary)?;
    db.put_node(&new_primary)?;

    // Add the old primary to the list of replicas. This way we also broadcast a "REPLICAOF <IP> <PORT>"
    // to the old primary's queue
    all_replicas.insert(old_primary.node_id().to_string(), old_primary);
    broadcast_failover(&db, &all_replicas, &new_primary).await?;

    let current_node_id = Server::state().persistent_state().id();

    // If this node is the new primary, don't wait until next iteration, switch to primary node now
    if current_node_id.eq(new_primary.node_id()) {
        process_commands_queue(&db, options, store, 1).await?;
    }
    Ok(())
}

//===------------------------------
// Private API calls
//===------------------------------

/// Broadcast all members of this shard that a failover is taking place
async fn broadcast_failover(
    db: &Persistence,
    all_replicas: &HashMap<String, Node>,
    new_primary: &Node,
) -> Result<(), SableError> {
    // get the new primary port + IP
    let address = new_primary.private_address();

    // the node address is in the format of "IP:PORT", change it to "IP PORT"
    let address = address.replace(':', " ");
    let command_for_replicas = format!("REPLICAOF {}", address);
    let command_for_primary = String::from("REPLICAOF NO ONE");

    tracing::info!("Sending commands to replicas...");
    for replica in all_replicas.values() {
        db.queue_push_command(replica, &command_for_replicas)?;
        tracing::info!(
            "Sent command: '{}' on queue: {}",
            &command_for_replicas,
            &replica.queue_name()
        );
    }
    // Push command for the new primary
    db.queue_push_command(new_primary, &command_for_primary)?;
    tracing::info!(
        "Sent command: '{}' on queue: {}",
        &command_for_primary,
        &new_primary.queue_name()
    );
    tracing::info!("Success");
    Ok(())
}

/// Return whether the current node's command queue is empty
async fn is_commands_queue_empty(db: &Persistence) -> Result<bool, SableError> {
    let current_node_id = Server::state().persistent_state().id();
    let node = NodeBuilder::default()
        .with_node_id(current_node_id)
        .with_shard_name(Server::state().persistent_state().shard_name())
        .build();
    Ok(db.queue_len(&node)? == 0)
}

/// Process `count` commands from the node's commands queue
async fn process_commands_queue(
    db: &Persistence,
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
    mut count: u32,
) -> Result<(), SableError> {
    // wait for the command and run it
    loop {
        if let ProcessCommandQueueResult::Done =
            try_process_commands_queue(db, options.clone(), store).await?
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
    db: &Persistence,
    options: Arc<StdRwLock<ServerOptions>>,
    store: &StorageAdapter,
) -> Result<ProcessCommandQueueResult, SableError> {
    let current_node_id = Server::state().persistent_state().id();

    // We just need to get the queue name, for this we don't need to load the node from the database
    let node = NodeBuilder::default()
        .with_node_id(current_node_id)
        .with_shard_name(Server::state().persistent_state().shard_name())
        .build();

    // wait for the command and run it
    tracing::info!("Waiting for command on queue '{}'...", &node.queue_name());
    let Some(cmd) = db.queue_pop_command_with_timeout(&node, 1.0)? else {
        return Ok(ProcessCommandQueueResult::NoCommands);
    };

    tracing::info!("Running command '{}'", cmd);
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

/// Check that `node` is alive
fn is_node_alive(node: &Node) -> Result<bool, SableError> {
    if !check_us_passed_since!(LAST_HTBT_CHECKED_TS, 1_500_000) {
        return Ok(true);
    }

    // get the primary last updated timestamp from the database
    let node_last_updated_ts = node.last_updated();
    let curr_ts = TimeUtils::epoch_micros().unwrap_or(0);

    if curr_ts.saturating_sub(node_last_updated_ts)
        > CHECK_PRIMARY_ALIVE_INTERVAL.load(Ordering::Relaxed)
    {
        // Primary is not responding!
        if NOT_RESPONDING_COUNTER.load(Ordering::Relaxed) >= 3 {
            tracing::info!("Node {} seems to be offline", node.node_id());
            return Ok(false);
        } else {
            // increase the error counter
            tracing::debug!(
                "Node {} seems to be offline. Retry counter={}",
                node.node_id(),
                NOT_RESPONDING_COUNTER.load(Ordering::Relaxed)
            );
            NOT_RESPONDING_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
    } else {
        if NOT_RESPONDING_COUNTER.load(Ordering::Relaxed) >= 3 {
            tracing::info!("Node {} is online!", node.node_id());
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
