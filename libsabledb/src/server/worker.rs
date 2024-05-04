use crate::{Client, SableError, ServerState, StorageAdapter, Telemetry, TimeUtils, WorkerHandle};
use rand::Rng;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
#[allow(unused_imports)]
use tracing::log::{log_enabled, Level};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace};

#[derive(Debug)]
pub enum WorkerMessage {
    NewConnection(TcpStream),
    Shutdown,
    BroadcastMessage(BroadcastMessageType),
}

#[derive(Debug, Clone, Copy)]
/// Define the message types that can be broadcast between the different workers
/// using the `ServerState::broadcast_msg()` method
pub enum BroadcastMessageType {
    /// Notify all workers that a client was killed.
    /// The owner worker should terminate that client
    KillClient(u128),
}

pub type WorkerSender = tokio::sync::mpsc::Sender<WorkerMessage>;
pub type WorkerReceiver = tokio::sync::mpsc::Receiver<WorkerMessage>;

#[allow(dead_code)]
pub struct Worker {
    /// Shared server state
    server_state: Arc<ServerState>,
    /// The channel on which this worker accepts commands from the main thread
    rx_channel: WorkerReceiver,
    /// The store
    store: StorageAdapter,
    /// Statistics merge interval
    stats_merge_interval: u64,
}

#[derive(Clone, Debug)]
/// The `WorkerContext` allows the acceptor thread to communicate with the workers
/// Over a dedicated channel
pub struct WorkerContext {
    pub runtime_handle: WorkerHandle,
    pub worker_send_channel: WorkerSender,
    pub thread_id: std::thread::ThreadId,
}

impl WorkerContext {
    /// Send message to the worker
    pub fn send(&self, message: WorkerMessage) -> Result<(), SableError> {
        // before using the message, enter the worker's context
        let _guard = self.runtime_handle.enter();
        self.runtime_handle.block_on(async {
            let _ = self.worker_send_channel.send(message).await;
        });
        Ok(())
    }
}

impl Worker {
    /// Create a new worker instance, run it inside
    /// a separate thread using a dedicated `tokio` Runtime
    /// and return a `WorkerContext` object on success.
    /// The worker context can be used to communicate with the
    /// worker over a channel
    ///
    /// ----
    /// ## Arguments
    ///
    /// - `telemetry` the global telemetry object
    pub fn run(
        server_state: Arc<ServerState>,
        store: StorageAdapter,
    ) -> Result<WorkerContext, SableError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<WorkerMessage>(1000); // channel with back-pressure of 1000
        let (handle_sender, handle_receiver) = std::sync::mpsc::channel();
        let tx_clone = tx.clone();
        let _ = std::thread::Builder::new()
            .name("Worker".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("Worker")
                    .build()
                    .unwrap_or_else(|e| {
                        panic!("failed to create tokio runtime. {:?}", e);
                    });

                // Register this worker thread with the server
                let thread_id = std::thread::current().id();
                server_state.add_worker_tx_channel(thread_id, tx_clone);

                // send the current runtime handle to the calling thread
                // this error is non-recoverable, so call `panic!` here
                handle_sender
                    .send((thread_id, rt.handle().clone()))
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to send tokio runtime handle to caller thread!. {:?}",
                            e
                        );
                    });
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    let mut worker = Worker::new(rx, server_state.clone(), store.clone());
                    worker.main_loop().await;
                });
            });

        let (thread_id, thread_runtime_handle) = handle_receiver.recv().unwrap_or_else(|e| {
            panic!("failed to recv tokio runtime handle from thread. {:?}", e);
        });

        Ok(WorkerContext {
            runtime_handle: thread_runtime_handle.clone(),
            worker_send_channel: tx,
            thread_id,
        })
    }

    /// Create a new worker instance
    fn new(rx: WorkerReceiver, server_state: Arc<ServerState>, store: StorageAdapter) -> Self {
        Worker {
            rx_channel: rx,
            server_state,
            store,
            stats_merge_interval: 0,
        }
    }

    /// The worker's main loop
    async fn main_loop(&mut self) {
        debug!("Worker ready to handle connection");

        // pick reporting interval for this worker to avoid all workers
        // contesting for the same lock
        let mut rng = rand::thread_rng();
        let secs = rng.gen_range(1..3);
        let nanos = rng.gen_range(0..u32::MAX);

        // create the TLS acceptor for this thread
        let acceptor = if self.server_state.options().use_tls() {
            let cert = &self
                .server_state
                .options()
                .general_settings
                .cert
                .clone()
                .expect("None certificate file");
            let key = &self
                .server_state
                .options()
                .general_settings
                .key
                .clone()
                .expect("None key file");
            let tls_acceptor = crate::net::create_tls_acceptor(cert, key);

            if let Ok(tls_acceptor) = tls_acceptor {
                Some(Rc::new(tls_acceptor))
            } else {
                error!(
                    "Failed to create TLS acceptor with cert {:?} and key {:?}",
                    cert, key
                );
                None
            }
        } else {
            None
        };

        info!(
            "This worker will update its statistics every: {}.{} seconds",
            secs, nanos
        );

        self.stats_merge_interval = tokio::time::Duration::new(secs, nanos)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);

        let mut last_flush = TimeUtils::epoch_ms().expect("failed to get timestamp!");
        let mut last_merge = last_flush;

        loop {
            tokio::select! {
                msg = self.rx_channel.recv() => {
                    debug!("Received command: {:?}", msg);
                    match msg {
                        Some(WorkerMessage::NewConnection(stream)) => {
                            let acceptor = acceptor.as_ref().cloned();
                            if let Err(e) = self.handle_new_connection(stream, acceptor).await {
                                error!("Failed to handle new connection. {:?}", e);
                            } else {
                                debug!("Task created successfully");
                            }
                        }
                        Some(WorkerMessage::Shutdown) => {
                            info!("Shutting down");
                            break;
                        }
                        Some(WorkerMessage::BroadcastMessage(BroadcastMessageType::KillClient(
                            client_id,
                        ))) => {
                            // Terminate client. If `client_id` is owned by this worker
                            // it will be marked as "terminated", otherwise this function
                            // does nothing
                            Client::terminate_client(client_id);
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    self.tick(&mut last_flush, &mut last_merge);
                }
            }
        }
    }

    /// Handle idle event
    fn tick(&self, last_flush: &mut u64, last_merge: &mut u64) {
        let Ok(cur_ts) = TimeUtils::epoch_ms() else {
            crate::error_with_throttling!(300, "unable to get timestamp from system");
            return;
        };

        let flush_interval = self
            .server_state
            .options()
            .open_params
            .rocksdb
            .manual_wal_flush_interval_ms as u64;

        // update this worker telemetry
        if cur_ts - *last_merge > self.stats_merge_interval {
            *last_merge = cur_ts;
            self.server_state
                .shared_telemetry()
                .lock()
                .expect("mutex")
                .merge_worker_telemetry(Telemetry::clone());
            Telemetry::clear();
        }

        if cur_ts - *last_flush > flush_interval {
            if let Err(e) = self.store.flush_wal() {
                crate::error_with_throttling!(300, "Failed to flush WAL. {:?}", e);
            }
        }
    }

    /// Handle a new connection
    async fn handle_new_connection(
        &self,
        stream: TcpStream,
        tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    ) -> Result<(), SableError> {
        // Create new connection and spawn it on a dedicated task
        let server_state_clone = self.server_state.clone();
        let store_clone = self.store.clone();
        tokio::task::spawn_local(async move {
            debug!("Handling new connection");
            let mut client = Client::new(server_state_clone, store_clone, tls_acceptor);
            client.run(stream).await?;
            Ok::<(), SableError>(())
        });

        Ok(())
    }
}
