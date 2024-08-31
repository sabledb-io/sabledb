use crate::{
    replication::{ReplicationConfig, ServerRole},
    utils::TimeUtils,
    NodeId, SableError, ServerOptions,
};
use redis::Commands;
use std::sync::RwLock;
use struct_iterable::Iterable;
lazy_static::lazy_static! {
    static ref CM_CONN: RwLock<Connection> = RwLock::<Connection>::default();
}

/// Convert Any type into String
macro_rules! any_to_string {
    ($val:expr) => {
        if let Some(string) = $val.downcast_ref::<String>() {
            string.clone()
        } else if let Some(number) = $val.downcast_ref::<u64>() {
            format!("{}", number)
        } else if let Some(role) = $val.downcast_ref::<ServerRole>() {
            format!("{}", role)
        } else {
            String::new()
        }
    };
}

#[derive(Default, Iterable)]
pub struct NodeInfo {
    node_id: String,
    node_address: String,
    role: ServerRole,
    last_updated: u64,
    last_txn_id: u64,
    /// When role is a Replica, this holds the primary node-id
    primary_node_id: String,
}

impl NodeInfo {
    pub fn new(options: &ServerOptions) -> Self {
        let repl_info = ReplicationConfig::load(options);
        NodeInfo {
            node_id: NodeId::current(),
            node_address: options.general_settings.private_address.clone(),
            role: repl_info.role.clone(),
            last_updated: TimeUtils::epoch_micros().unwrap_or_default(),
            ..Default::default()
        }
    }

    /// Write this object to the cluster manager database
    pub fn put(&self, conn: &mut redis::Client) -> Result<(), SableError> {
        let props: Vec<(String, String)> = self
            .iter()
            .map(|(field_name, field_value)| {
                let val = any_to_string!(field_value);
                (
                    format!("{}:{}", self.node_id, field_name),
                    format!("{:?}", val),
                )
            })
            .collect();
        tracing::info!("Writing node object in cluster manager: {:?}", props);
        conn.mset(&props)?;
        Ok(())
    }

    pub fn with_last_txn_id(mut self, last_txn_id: u64) -> Self {
        self.last_txn_id = last_txn_id;
        self
    }

    pub fn with_role_replica(mut self) -> Self {
        self.role = ServerRole::Replica;
        self
    }

    pub fn with_role_primary(mut self) -> Self {
        self.role = ServerRole::Primary;
        self
    }

    pub fn with_primary_node_id(mut self, primary_node_id: String) -> Self {
        self.primary_node_id = primary_node_id;
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
pub fn put_node_info(options: &ServerOptions, node_info: &NodeInfo) -> Result<(), SableError> {
    connect(options)?;
    let mut conn = CM_CONN.write().expect("poisoned mutex");
    let Some(client) = &mut conn.client else {
        return Ok(());
    };
    node_info.put(client)
}
