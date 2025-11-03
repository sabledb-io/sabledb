mod client_replication_loop;
mod cluster_lock;
mod cluster_manager;
mod messages;
mod node_talk_client;
mod node_talk_server;
mod persistence;
mod replication_config;
mod replication_traits;
mod replicator;
mod storage_updates;

pub use crate::SableError;
pub use cluster_manager::*;
pub use persistence::*;

pub use client_replication_loop::*;
pub use cluster_lock::{BlockingLock, Lock};

pub use client_replication_loop::NodeTalkCommand;
pub use messages::{NodeResponse, NodeTalkRequest, RequestCommon, ResponseCommon, ResponseReason};
pub use node_talk_client::*;
pub use node_talk_server::*;
pub use replication_config::ServerRole;
pub use storage_updates::{StorageUpdates, StorageUpdatesRecord};

pub use replication_traits::{
    BytesReader, BytesWriter, TcpStreamBytesReader, TcpStreamBytesWriter,
};
pub use replicator::{ReplicationWorkerMessage, Replicator, ReplicatorContext};

/// Prepare the socket by setting time-out and disabling delay
pub fn socket_set_timeout(socket: &std::net::TcpStream) -> Result<(), SableError> {
    // tokio sockets are non-blocking. We need to change this
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(std::time::Duration::from_millis(250)))?;
    socket.set_write_timeout(Some(std::time::Duration::from_millis(250)))?;
    let _ = socket.set_nodelay(true);
    Ok(())
}

/// Prepare the socket by setting time-out and disabling delay
pub fn socket_make_blocking(socket: &std::net::TcpStream) -> Result<(), SableError> {
    socket.set_nonblocking(false)?;
    socket.set_write_timeout(None)?;
    socket.set_read_timeout(None)?;
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
