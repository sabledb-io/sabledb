mod cluster_lock;
mod cluster_manager;
mod persistence;
mod replication_client;
mod replication_config;
mod replication_messages;
mod replication_server;
mod replication_traits;
mod replicator;
mod storage_updates;

pub use crate::SableError;
pub use cluster_manager::*;
pub use persistence::*;

pub use cluster_lock::{BlockingLock, Lock};

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

#[macro_export]
macro_rules! bincode_to_bytesmut_or {
    ($value:expr, $err:expr) => {{
        let Ok(buffer) = bincode::serialize(&$value) else {
            error!("bincode::serialize error");
            return $err;
        };
        bytes::BytesMut::from(buffer.as_slice())
    }};
}

#[macro_export]
macro_rules! bincode_to_bytesmut {
    ($value:expr) => {{
        BytesMut::from(bincode::serialize(&$value)?.as_slice())
    }};
}
