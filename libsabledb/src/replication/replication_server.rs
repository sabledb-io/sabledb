use crate::replication::prepare_std_socket;
use crate::server_options::ServerOptions;
use crate::telemetry::{ReplicaTelemetry, ReplicationTelemetry};

#[allow(unused_imports)]
use crate::{
    replication::{
        BytesReader, BytesWriter, ReplRequest, TcpStreamBytesReader, TcpStreamBytesWriter,
    },
    SableError, StorageAdapter,
};
#[allow(unused_imports)]
use bytes::BytesMut;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::net::TcpListener;

#[derive(Default)]
pub struct ReplicationServer {}

lazy_static::lazy_static! {
    static ref REPLICATION_THREADS: AtomicUsize = AtomicUsize::new(0);
    static ref STOP_FLAG: AtomicBool = AtomicBool::new(false);
}

/// Notify the replication threads to stop and wait for them to terminate
pub async fn replication_thread_stop_all() {
    tracing::info!("Notifying all replication threads to shutdown");
    STOP_FLAG.store(true, Ordering::Relaxed);
    loop {
        let running_threads = REPLICATION_THREADS.load(Ordering::Relaxed);
        if running_threads == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    STOP_FLAG.store(false, Ordering::Relaxed);
    tracing::info!("All replication threads have stopped");
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
    STOP_FLAG.load(Ordering::Relaxed)
}

/// Helper struct for marking a replication thread
/// as running and mark it as "off" when this helper
/// goes out of scope
struct ReplicationThreadMarker {
    address: String,
}

impl ReplicationThreadMarker {
    pub fn new(address: String) -> Self {
        ReplicationTelemetry::update_replica_info(address.clone(), ReplicaTelemetry::default());
        replication_thread_incr();
        ReplicationThreadMarker { address }
    }
}

impl Drop for ReplicationThreadMarker {
    fn drop(&mut self) {
        replication_thread_decr();
        ReplicationTelemetry::remove_replica(&self.address);
    }
}

impl ReplicationServer {
    fn read_request(reader: &mut dyn BytesReader) -> Result<Option<ReplRequest>, SableError> {
        // Read the request (this is a fixed size request)
        let result = reader.read_message()?;
        match result {
            None => Ok(None),
            Some(bytes) => Ok(ReplRequest::from_bytes(&bytes)),
        }
    }

    /// The main replication request -> reply flow is happening
    /// This function reads a single replication request
    /// and responds with the proper response.
    fn handle_single_request(
        store: &StorageAdapter,
        options: &ServerOptions,
        reader: &mut impl BytesReader,
        writer: &mut impl BytesWriter,
    ) -> bool {
        tracing::debug!("Waiting for replication request..");
        let req = loop {
            let result = Self::read_request(reader);
            match result {
                Err(e) => {
                    tracing::error!("Failed to read request. {:?}", e);
                    return false;
                }
                // And this is why I ❤ Rust...
                Ok(Some(req)) => break req,
                Ok(None) => {
                    // timeout
                    if replication_thread_is_going_down() {
                        tracing::info!("Received request to shutdown - bye");
                        return false;
                    }
                    continue;
                }
            };
        };
        tracing::debug!("Received replication request {:?}", req);

        match req.req_type {
            ReplRequest::GET_UPDATES_SINCE => {
                tracing::debug!("Replica is requesting changes since: {}", req.payload);
                let storage_updates = loop {
                    let storage_updates = match store.storage_updates_since(
                        req.payload,
                        Some(options.replication_limits.single_update_buffer_size as u64),
                        Some(options.replication_limits.num_updates_per_message as u64),
                    ) {
                        Err(e) => {
                            tracing::error!("Failed to construct 'changes since' message. {:?}", e);
                            return false;
                        }
                        Ok(changes_since) => changes_since,
                    };

                    if storage_updates.is_empty() {
                        // Nothing to send, suspend ourselves for a bit
                        // until we have something to send
                        std::thread::sleep(std::time::Duration::from_millis(
                            options.replication_limits.check_for_updates_interval_ms as u64,
                        ));
                        continue;
                    } else {
                        break storage_updates;
                    }
                };

                // Serialise the data
                tracing::info!("Sending replication update: {}", storage_updates);
                let mut buffer = storage_updates.to_bytes();
                match writer.write_message(&mut buffer) {
                    Err(SableError::BrokenPipe) => {
                        tracing::warn!("Failed to send changes to replica (broken pipe)");
                        return false;
                    }
                    Err(e) => {
                        tracing::error!("Failed to send changes to replica. {:?}", e);
                        return false;
                    }
                    _ => {}
                }
            }
            _ => {
                tracing::error!(
                    "Replication protocol error. Unknown replication request with type {}",
                    req.req_type
                );
                return false;
            }
        }
        true
    }

    pub async fn run(
        &self,
        options: ServerOptions,
        store: StorageAdapter,
    ) -> Result<(), SableError> {
        let repl_config = options.load_replication_config();
        let address = format!("{}:{}", repl_config.ip, repl_config.port);

        let listener = TcpListener::bind(address.clone())
            .await
            .unwrap_or_else(|_| panic!("failed to bind address {}", address));
        tracing::info!("Replication server started on address: {}", address);
        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::info!("Accepted new connection from replica: {:?}", addr);
            let stream = match socket.into_std() {
                Ok(socket) => socket,
                Err(e) => {
                    tracing::error!("Failed to convert async socket -> std socket!. {:?}", e);
                    continue;
                }
            };

            let store_clone = store.clone();
            let server_options_clone = options.clone();
            // spawn a thread to so we could move to sync api
            // this will allow us to write directly from the storage -> network
            // without building buffers in the memory
            let _handle = std::thread::spawn(move || {
                tracing::info!("Replication thread started for connection {:?}", addr);
                let _guard = ReplicationThreadMarker::new(addr.to_string());

                // we now work in a simple request/reply mode:
                // the replica sends a request requesting changes since
                // the Nth update and this primary sends back the changes

                // First, prepare the socket
                if let Err(e) = prepare_std_socket(&stream) {
                    tracing::error!("Failed to prepare socket. {:?}", e);
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    return;
                }

                // Read the request
                let mut reader = TcpStreamBytesReader::new(&stream);
                let mut writer = TcpStreamBytesWriter::new(&stream);
                loop {
                    if !Self::handle_single_request(
                        &store_clone,
                        &server_options_clone,
                        &mut reader,
                        &mut writer,
                    ) {
                        tracing::info!("Closing connection with replica: {:?}", stream);
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                }
            });
            // let the handle drop to make the thread detached
        }

        #[allow(unreachable_code)]
        Ok::<(), SableError>(())
    }
}
