use crate::replication::{cluster_manager, prepare_std_socket};
use crate::server::ServerOptions;
use crate::server::{ReplicaTelemetry, ReplicationTelemetry};
use crate::storage::GetChangesLimits;
use std::rc::Rc;

use futures::future;

use crate::utils;
#[allow(unused_imports)]
use crate::{
    io::Archive,
    replication::{
        BytesReader, BytesWriter, ReplicationRequest, ReplicationResponse, ResponseCommon,
        ResponseReason, TcpStreamBytesReader, TcpStreamBytesWriter,
    },
    SableError, Server, StorageAdapter,
};

use num_format::{Locale, ToFormattedString};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, RwLock as StdRwLock,
};
use tokio::net::TcpListener;

const OPTIONS_LOCK_ERR: &str = "Failed to obtain read lock on ServerOptions";

#[derive(Debug)]
enum HandleRequestResult {
    NodeJoined(String),
    Success,
    NetError(String),
    Shutdown(String),
}

#[cfg(not(test))]
use tracing::{debug, error, info};

#[cfg(test)]
use std::{println as info, println as debug, println as error};

#[derive(Default)]
pub struct ReplicationServer {}

lazy_static::lazy_static! {
    static ref REPLICATION_THREADS: AtomicUsize = AtomicUsize::new(0);
    static ref STOP_FLAG: AtomicBool = AtomicBool::new(false);
}

/// Notify the replication threads to stop and wait for them to terminate
pub async fn replication_thread_stop_all() {
    info!("Notifying all replication threads to shutdown");
    STOP_FLAG.store(true, Ordering::Relaxed);
    loop {
        let running_threads = REPLICATION_THREADS.load(Ordering::Relaxed);
        if running_threads == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
    STOP_FLAG.store(false, Ordering::Relaxed);
    info!("All replication threads have stopped");
}

/// Increment the number of running replication threads by 1
fn replication_thread_incr() {
    let _ = REPLICATION_THREADS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the number of running replication threads by 1
fn replication_thread_decr() {
    let _ = REPLICATION_THREADS.fetch_sub(1, Ordering::Relaxed);
}

/// Is the shutdown flag set?
fn replication_thread_is_going_down() -> bool {
    debug!("Checking for STOP_FLAG flag");
    STOP_FLAG.load(Ordering::Relaxed)
}

/// Helper struct for marking a replication thread
/// as running and mark it as "off" when this helper
/// goes out of scope
struct ReplicationThreadMarker {
    address: Option<String>,
}

impl ReplicationThreadMarker {
    pub fn new() -> Self {
        replication_thread_incr();
        ReplicationThreadMarker { address: None }
    }

    pub fn set_node_id(&mut self, node_id: &String) {
        self.address = Some(node_id.to_string());
        ReplicationTelemetry::update_replica_info(node_id.to_string(), ReplicaTelemetry::default());
    }
}

impl Drop for ReplicationThreadMarker {
    fn drop(&mut self) {
        replication_thread_decr();
        if let Some(address) = &self.address {
            ReplicationTelemetry::remove_replica(address);
        }
    }
}

impl ReplicationServer {
    fn read_request(
        reader: &mut dyn BytesReader,
    ) -> Result<Option<ReplicationRequest>, SableError> {
        // Read the request (this is a fixed size request)
        let result = reader.read_message()?;
        match result {
            None => Ok(None),
            Some(bytes) => Ok(ReplicationRequest::from_bytes(&bytes)),
        }
    }

    /// A checkpoint is a database snapshot in this given point in time
    /// a replication starts by sending a checkpoint to the replica
    /// after the replica accepts the checkpoints, it start replicate the
    /// changes
    fn send_checkpoint(
        store: &StorageAdapter,
        _options: Arc<StdRwLock<ServerOptions>>,
        replica_addr: &String,
        stream: &mut std::net::TcpStream,
    ) -> Result<u64, SableError> {
        // async sockets are non-blocking. Make it blocking for sending the file
        stream.set_nonblocking(false)?;
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(None)?;

        let ts = utils::current_time(utils::CurrentTimeResolution::Microseconds).to_string();
        info!("Preparing checkpoint for replica {}", replica_addr);
        let backup_db_path = std::path::PathBuf::from(format!(
            "{}.checkpoint.{}",
            store.open_params().db_path.display(),
            ts
        ));
        let changes_count = store.create_checkpoint(&backup_db_path)?;
        info!(
            "Checkpoint {} created successfully with {} changes",
            backup_db_path.display(),
            changes_count
        );

        let archiver = Archive::default();
        let tar_file = archiver.create(&backup_db_path)?;

        // Send the file size first
        let mut file = std::fs::File::open(&tar_file)?;
        let file_len = file.metadata()?.len() as usize;
        info!(
            "Sending tar file: {}, Len: {} bytes",
            tar_file.display(),
            file_len.to_formatted_string(&Locale::en)
        );

        let mut file_len = crate::BytesMutUtils::from_usize(&file_len);
        crate::io::write_bytes(stream, &mut file_len)?;

        // Now send the content
        std::io::copy(&mut file, stream)?;
        info!("Sending tar file {} completed", tar_file.display());
        prepare_std_socket(stream)?;

        // Delete the tar + folder
        let _ = std::fs::remove_file(&tar_file);
        let _ = std::fs::remove_dir_all(&backup_db_path);
        Ok(changes_count)
    }

    /// Write `response` to `writer`. Return `true` on success, `false` otherwise
    fn write_response(writer: &mut impl BytesWriter, response: &ReplicationResponse) -> bool {
        let mut response_mut = response.to_bytes();
        if let Err(e) = writer.write_message(&mut response_mut) {
            error!("Failed to send response: '{}'. {:?}", response, e);
            false
        } else {
            debug!("Successfully send message '{}'", response);
            true
        }
    }

    /// The main replication request -> reply flow is happening here.
    /// This function reads a single replication request and responds with the proper response.
    fn handle_single_request(
        store: &StorageAdapter,
        options: Arc<StdRwLock<ServerOptions>>,
        stream: &mut std::net::TcpStream,
        replica_node_id: &String,
    ) -> HandleRequestResult {
        let mut reader = TcpStreamBytesReader::new(stream);
        let mut writer = TcpStreamBytesWriter::new(stream);

        debug!("Waiting for replication request..");
        let req = loop {
            let result = Self::read_request(&mut reader);
            match result {
                Err(e) => {
                    return HandleRequestResult::NetError(format!(
                        "Failed to read request. {:?}",
                        e
                    ));
                }
                // ❤ Rust...
                Ok(Some(req)) => break req,
                Ok(None) => {
                    // timeout
                    if replication_thread_is_going_down() {
                        return HandleRequestResult::Shutdown(
                            "Received request to shutdown - bye".into(),
                        );
                    }
                    Self::update_primary_info(options.clone(), store);
                    continue;
                }
            };
        };
        debug!("Received replication request {:?}", req);

        match req {
            ReplicationRequest::JoinShard(common) => {
                info!("Received request: JoinShard({})", common);
                let shard_name = if Server::state().persistent_state().shard_name().is_empty() {
                    Server::state().persistent_state().id()
                } else {
                    Server::state().persistent_state().shard_name()
                };

                let response_ok = ReplicationResponse::Ok(
                    ResponseCommon::new(&common).with_context(shard_name.clone()),
                );
                if !Self::write_response(&mut writer, &response_ok) {
                    return HandleRequestResult::NetError("Failed to write response".into());
                }
                info!(
                    "Replica: {} joined the shard: {}",
                    common.node_id(),
                    shard_name
                );
                return HandleRequestResult::NodeJoined(common.node_id().to_string());
            }
            ReplicationRequest::FullSync(common) => {
                debug!("Received request {}", common);
                let response_ok = ReplicationResponse::Ok(ResponseCommon::new(&common));

                // Response with an ACK followed by the file
                debug!("Sending replication response: {}", response_ok);
                if !Self::write_response(&mut writer, &response_ok) {
                    return HandleRequestResult::NetError("Failed to write response".into());
                }
                let changes_count =
                    match Self::send_checkpoint(store, options.clone(), replica_node_id, stream) {
                        Err(e) => {
                            return HandleRequestResult::NetError(format!(
                                "Failed sending db checkpoint to replica {}. {:?}",
                                replica_node_id, e
                            ));
                        }
                        Ok(count) => count,
                    };

                let replinfo = ReplicaTelemetry {
                    last_change_sequence_number: changes_count,
                    distance_from_primary: 0,
                };
                ReplicationTelemetry::update_replica_info(replica_node_id.to_string(), replinfo);

                // Update the current node info in the cluster manager database as primary
                Self::update_primary_info(options.clone(), store);
            }
            ReplicationRequest::GetUpdatesSince((common, seq)) => {
                debug!(
                    "Received request GetUpdatesSince({}, ChangesSince:{})",
                    common, seq
                );
                debug!(
                    "Replica {} is requesting changes since: {}",
                    replica_node_id, seq
                );

                if common.request_id() == 0 {
                    // This is the first request - force a fullsync
                    let response_not_ok = ReplicationResponse::NotOk(
                        ResponseCommon::new(&common).with_reason(ResponseReason::NoFullSyncDone),
                    );

                    debug!("Sending replication response: {}", response_not_ok);
                    if Self::write_response(&mut writer, &response_not_ok) {
                        return HandleRequestResult::Success;
                    } else {
                        return HandleRequestResult::NetError("Failed to write response".into());
                    }
                }

                // Get the changes from the database. If no changes available
                // hold the request until we have some changes to send over
                let mut retries = 5usize;
                let limits = Rc::new(
                    GetChangesLimits::builder()
                        .with_memory(
                            options
                                .read()
                                .expect(OPTIONS_LOCK_ERR)
                                .replication_limits
                                .single_update_buffer_size as u64,
                        )
                        .with_max_changes_count(
                            options
                                .read()
                                .expect(OPTIONS_LOCK_ERR)
                                .replication_limits
                                .num_updates_per_message as u64,
                        )
                        .build(),
                );

                let storage_updates = loop {
                    let storage_updates = match store.storage_updates_since(seq, limits.clone()) {
                        Err(e) => {
                            let msg =
                                format!("Failed to construct 'changes since' message. {:?}", e);
                            tracing::warn!(msg);

                            // build the response + the error code and send it back
                            let response_not_ok = ReplicationResponse::NotOk(
                                ResponseCommon::new(&common)
                                    .with_reason(ResponseReason::CreatingUpdatesSinceError),
                            );
                            if Self::write_response(&mut writer, &response_not_ok) {
                                return HandleRequestResult::Success;
                            } else {
                                return HandleRequestResult::NetError(
                                    "Failed to write response".into(),
                                );
                            }
                        }
                        Ok(changes_since) => changes_since,
                    };

                    if storage_updates.is_empty() {
                        // No changes, stall the request
                        retries = retries.saturating_sub(1);
                        if retries == 0 {
                            break None;
                        }

                        // Nothing to send, suspend ourselves for a bit
                        // until we have something to send
                        std::thread::sleep(std::time::Duration::from_millis(
                            options
                                .read()
                                .unwrap()
                                .replication_limits
                                .check_for_updates_interval_ms as u64,
                        ));
                        continue;
                    } else {
                        break Some(storage_updates);
                    }
                };

                let Some(storage_updates) = storage_updates else {
                    // No changes available to send
                    let response_not_ok = ReplicationResponse::NotOk(
                        ResponseCommon::new(&common)
                            .with_reason(ResponseReason::NoChangesAvailable),
                    );
                    if Self::write_response(&mut writer, &response_not_ok) {
                        return HandleRequestResult::Success;
                    } else {
                        return HandleRequestResult::NetError("Failed to write response".into());
                    }
                };

                info!("Sending replication update: {}", storage_updates);

                // Send a `ReplicationResponse::Ok` message, followed by the data
                let response_ok = ReplicationResponse::Ok(ResponseCommon::new(&common));
                if !Self::write_response(&mut writer, &response_ok) {
                    return HandleRequestResult::NetError("Failed to write response".into());
                }

                // Send the data
                let mut buffer = storage_updates.to_bytes();
                match writer.write_message(&mut buffer) {
                    Err(SableError::BrokenPipe) => {
                        return HandleRequestResult::NetError(
                            "Failed to send changes to replica (broken pipe)".into(),
                        );
                    }
                    Err(e) => {
                        return HandleRequestResult::NetError(format!(
                            "Failed to send changes to replica. {:?}",
                            e
                        ));
                    }
                    _ => {
                        let replinfo = ReplicaTelemetry {
                            last_change_sequence_number: storage_updates.end_seq_number,
                            distance_from_primary: 0,
                        };
                        ReplicationTelemetry::update_replica_info(
                            replica_node_id.to_string(),
                            replinfo,
                        );
                    }
                }
            }
        }
        HandleRequestResult::Success
    }

    // Primary node main loop: wait for a new replica connection
    // and launch a thread to handle it
    pub async fn run(
        &self,
        options: Arc<StdRwLock<ServerOptions>>,
        store: StorageAdapter,
    ) -> Result<(), SableError> {
        let private_address = options
            .read()
            .expect(OPTIONS_LOCK_ERR)
            .general_settings
            .private_address
            .clone();

        let listener = TcpListener::bind(private_address)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "failed to bind address {}",
                    &options
                        .read()
                        .expect(OPTIONS_LOCK_ERR)
                        .general_settings
                        .private_address
                )
            });
        info!(
            "Accepting replicas @{}",
            &options
                .read()
                .expect(OPTIONS_LOCK_ERR)
                .general_settings
                .private_address
        );

        loop {
            // Accept with timeout
            let accept_fut = listener.accept();
            let timeout_fut = tokio::time::sleep(tokio::time::Duration::from_secs(1));

            futures::pin_mut!(accept_fut);
            futures::pin_mut!(timeout_fut);

            match future::select(accept_fut, timeout_fut).await {
                future::Either::Left((value, _)) => {
                    let (socket, addr) = value?;
                    self.handle_new_connection(options.clone(), store.clone(), socket, addr)?;
                }
                future::Either::Right(_) => {
                    // TimeOut, do a tick operation here
                    tracing::trace!("Checking node's queue...");
                    if let Err(e) = cluster_manager::check_node_queue(options.clone(), &store).await
                    {
                        tracing::warn!("Failed to process node command queue. {:?}", e);
                    }
                }
            }
        }
    }

    /// Create a new thread that will handle the newly connected replica
    fn handle_new_connection(
        &self,
        options: Arc<StdRwLock<ServerOptions>>,
        store: StorageAdapter,
        socket: tokio::net::TcpStream,
        addr: std::net::SocketAddr,
    ) -> Result<(), SableError> {
        info!("Accepted new connection from: {:?}", addr);
        let mut stream = match socket.into_std() {
            Ok(socket) => socket,
            Err(e) => {
                error!("Failed to convert async socket -> std socket!. {:?}", e);
                return Ok(());
            }
        };

        let store_clone = store.clone();
        let server_options_clone = options.clone();

        // spawn a thread to so we could move to sync api
        // this will allow us to write directly from the storage -> network
        // without building buffers in the memory
        let _handle = std::thread::spawn(move || {
            info!("Replication thread started for connection {:?}", addr);
            let mut guard = ReplicationThreadMarker::new();
            let mut replica_name = String::default();

            // we now work in a simple request/reply mode:
            // the replica sends a request requesting changes since
            // the Nth update and this primary sends back the changes

            // First, prepare the socket
            if let Err(e) = prepare_std_socket(&stream) {
                error!("Failed to prepare socket. {:?}", e);
                let _ = stream.shutdown(std::net::Shutdown::Both);
                return;
            }

            loop {
                match Self::handle_single_request(
                    &store_clone,
                    server_options_clone.clone(),
                    &mut stream,
                    &replica_name,
                ) {
                    HandleRequestResult::NodeJoined(replica_id) => {
                        guard.set_node_id(&replica_id);
                        // keep the node-id
                        replica_name = replica_id;
                    }
                    HandleRequestResult::Success => {}
                    HandleRequestResult::NetError(msg) => {
                        tracing::warn!("Network error with replica {replica_name}. {msg}");
                        info!("Closing connection with replica: {:?}", stream);
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                    HandleRequestResult::Shutdown(msg) => {
                        info!("{msg}");
                        info!("Closing connection with replica: {:?}", stream);
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                }
                Self::update_primary_info(server_options_clone.clone(), &store_clone);
            }
        });
        Ok(())
    }

    fn update_primary_info(options: Arc<StdRwLock<ServerOptions>>, store: &StorageAdapter) {
        // Update the current node info in the cluster manager database as primary
        let node = crate::replication::NodeBuilder::default()
            .with_last_txn_id(store.latest_sequence_number().unwrap_or_default())
            .build();
        if let Err(e) = cluster_manager::put_node(options.clone(), node.clone()) {
            crate::warn_with_throttling!(
                10,
                "Error while updating self as Primary({:?}) in the cluster database. {:?}",
                node,
                e
            );
        }
    }
}
