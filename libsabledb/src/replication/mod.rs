mod cluster_database;
mod cluster_lock;
pub mod cluster_manager;
mod persistence;
mod replication_client;
mod replication_config;
mod replication_messages;
mod replication_server;
mod replication_traits;
mod replicator;
mod storage_updates;

pub use crate::SableError;
pub use persistence::Persistence;

pub use cluster_database::{ClusterDB, LockResult, UnLockResult};
pub use cluster_lock::{BlockingLock, Lock, PrimaryLock};
pub use cluster_manager::NodeProperties;

pub use replication_client::{ReplClientCommand, ReplicationClient};
pub use replication_config::ServerRole;
pub use replication_messages::{
    ReplicationRequest, ReplicationResponse, RequestCommon, ResponseCommon, ResponseReason,
};
pub use replication_server::{replication_thread_stop_all, ReplicationServer};
pub use storage_updates::{DeleteRecord, PutRecord, StorageUpdates, StorageUpdatesIterItem};

pub use replication_traits::{
    BytesReader, BytesWriter, TcpStreamBytesReader, TcpStreamBytesWriter,
};
pub use replicator::{ReplicationWorkerMessage, Replicator, ReplicatorContext};

/// Prepare the socket by setting time-out and disabling delay
pub fn prepare_std_socket(socket: &std::net::TcpStream) -> Result<(), SableError> {
    // tokio sockets are non-blocking. We need to change this
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(std::time::Duration::from_millis(250)))?;
    socket.set_write_timeout(Some(std::time::Duration::from_millis(250)))?;
    let _ = socket.set_nodelay(true);
    Ok(())
}

const PROP_NODE_ID: &str = "node_id";
const PROP_NODE_ADDRESS: &str = "node_address";
const PROP_ROLE: &str = "role";
const PROP_LAST_UPDATED: &str = "last_updated";
const PROP_LAST_TXN_ID: &str = "last_txn_id";
const PROP_PRIMARY_NODE_ID: &str = "primary_node_id";
