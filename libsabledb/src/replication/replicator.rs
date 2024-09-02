use crate::replication::ReplicationServer;

#[allow(unused_imports)]
use crate::{
    replication::{replication_thread_stop_all, ReplClientCommand, ReplicationClient, ServerRole},
    server::{Client, SableError, ServerOptions, Telemetry, WorkerContext},
    storage::StorageAdapter,
    Server,
};

use tokio::sync::mpsc::Receiver as TokioReciever;
use tokio::sync::mpsc::Sender as TokioSender;

pub type ReplicatorSender = TokioSender<ReplicationWorkerMessage>;
pub type ReplicatorReceiver = TokioReciever<ReplicationWorkerMessage>;
pub type WorkerHandle = tokio::runtime::Handle;

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ServerRoleChanged {
    /// The server role has changed to Replica
    Replica {
        /// The primary address
        primary_address: String,
    },
    /// The server is now a Primary
    Primary {
        /// The address on which this server accepts new replicas
        listen_address: String,
    },
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub enum ReplicationWorkerMessage {
    /// Disconnect from the current primary
    #[default]
    PrimaryMode,
    /// Connect to a remote server
    ConnectToPrimary((String, u16)),
    /// Shutdown the replicator thread
    Shutdown,
}

#[derive(Default, Debug)]
#[allow(dead_code)]
enum ReplicatorStateResult {
    /// Disconnect from the current primary
    #[default]
    PrimaryMode,
    /// Connect to a remote server
    ConnectToPrimary((String, u16)),
    /// Shutdown the replicator thread
    Shutdown,
}

#[allow(dead_code)]
pub struct Replicator {
    /// Shared server state
    server_options: ServerOptions,
    /// The channel on which this worker accepts commands
    rx_channel: ReplicatorReceiver,
    /// The store
    store: StorageAdapter,
}

#[derive(Clone, Debug)]
/// The `ReplicatorContext` allows other threads to communicate with the replicator
/// thread using a dedicated channel
pub struct ReplicatorContext {
    runtime_handle: WorkerHandle,
    worker_send_channel: ReplicatorSender,
}

#[allow(unsafe_code)]
unsafe impl Send for ReplicatorContext {}

#[allow(dead_code)]
impl ReplicatorContext {
    /// Send message to the worker
    pub async fn send(&self, message: ReplicationWorkerMessage) -> Result<(), SableError> {
        // before using the message, enter the worker's context
        let _guard = self.runtime_handle.enter();
        let _ = self.worker_send_channel.send(message).await;
        Ok(())
    }

    /// Send message to the worker (non async)
    pub fn send_sync(&self, message: ReplicationWorkerMessage) -> Result<(), SableError> {
        if let Err(e) = self.worker_send_channel.try_send(message) {
            return Err(SableError::OtherError(format!("{:?}", e)));
        }
        Ok(())
    }
}

#[allow(dead_code)]
impl Replicator {
    /// Private method: create a new replicator instance
    async fn new(
        rx: ReplicatorReceiver,
        server_options: ServerOptions,
        store: StorageAdapter,
    ) -> Self {
        Replicator {
            rx_channel: rx,
            server_options,
            store,
        }
    }

    /// Spawn the replication thread returning a a context for the caller
    /// The context can be used to communicate with the replicator
    pub fn run(
        server_options: ServerOptions,
        store: StorageAdapter,
    ) -> Result<ReplicatorContext, SableError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<ReplicationWorkerMessage>(100);
        let (handle_sender, handle_receiver) = std::sync::mpsc::channel();
        let _ = std::thread::Builder::new()
            .name("Replicator".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("Replicator")
                    .build()
                    .unwrap_or_else(|e| {
                        panic!("failed to create tokio runtime. {:?}", e);
                    });

                // send the current runtime handle to the calling thread
                // this error is non-recoverable, so call `panic!` here
                handle_sender.send(rt.handle().clone()).unwrap_or_else(|e| {
                    panic!(
                        "failed to send tokio runtime handle to caller thread!. {:?}",
                        e
                    );
                });

                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    // The replicator tasks:
                    // - The replication main loop (exchanging data)
                    // - Heartbeat task (using UDP)
                    let server_options_cloned = server_options.clone();
                    let replicator_handle = tokio::task::spawn_local(async move {
                        let mut replicator =
                            Replicator::new(rx, server_options_cloned, store.clone()).await;
                        if let Err(e) = replicator.main_loop().await {
                            tracing::error!("replicator error. {:?}", e);
                        }
                    });
                    let _ = replicator_handle.await;
                });
            });

        let thread_runtime_handle = handle_receiver.recv().unwrap_or_else(|e| {
            panic!(
                "failed to recv tokio runtime handle from replicator thread. {:?}",
                e
            );
        });

        Ok(ReplicatorContext {
            runtime_handle: thread_runtime_handle.clone(),
            worker_send_channel: tx,
        })
    }

    /// Create a new replication thread and wait for it exit.
    /// Replication thread can exit in 2 ways only:
    /// - The server terminated
    /// - User issued a "replicaof no one" command which basically changes the server role to primary
    async fn replica_loop(&mut self) -> Result<ReplicatorStateResult, SableError> {
        tracing::info!(
            "Connecting to primary at: {}",
            Server::state().persistent_state().primary_address()
        );

        let replication_client = ReplicationClient::default();
        // Launch the replication client on a dedicated thread
        // and return immediately
        let tx = replication_client
            .run(self.server_options.clone(), self.store.clone())
            .await?;

        loop {
            match self.rx_channel.recv().await {
                Some(ReplicationWorkerMessage::ConnectToPrimary((primary_ip, primary_port))) => {
                    // switching primary
                    tracing::info!(
                        "Connecting to primary server: {}:{}",
                        primary_ip,
                        primary_port
                    );

                    // terminate the current replication
                    let _ = tx.send(ReplClientCommand::Shutdown).await;

                    // start a new one
                    return Ok(ReplicatorStateResult::ConnectToPrimary((
                        primary_ip,
                        primary_port,
                    )));
                }
                Some(ReplicationWorkerMessage::PrimaryMode) => {
                    // switching into primary mode, tell our replication thread to terminate itself
                    tracing::info!("Before switching to primary - stopping the replication thread");
                    let _ = tx.send(ReplClientCommand::Shutdown).await;
                    return Ok(ReplicatorStateResult::PrimaryMode);
                }
                Some(ReplicationWorkerMessage::Shutdown) => {
                    let _ = tx.send(ReplClientCommand::Shutdown).await;
                    return Ok(ReplicatorStateResult::Shutdown);
                }
                None => {}
            }
        }
    }

    /// Run this loop when the server is running a "primary" mode
    async fn primary_loop(&mut self) -> Result<ReplicatorStateResult, SableError> {
        tracing::info!("Running primary loop");
        let server = ReplicationServer::default();

        // Change the node state to primary and clear any primary node ID
        Server::state().persistent_state().set_primary_node_id(None);

        loop {
            tokio::select! {
                cmd = self.rx_channel.recv() => {
                    match cmd {
                        Some(ReplicationWorkerMessage::ConnectToPrimary((primary_ip, primary_port))) => {
                            tracing::info!(
                                "Switching to replica mode. Connecting to primary at {}:{}",
                                primary_ip,
                                primary_port,
                            );
                            replication_thread_stop_all().await;
                            return Ok(ReplicatorStateResult::ConnectToPrimary((primary_ip, primary_port)));
                        }
                        Some(ReplicationWorkerMessage::PrimaryMode) => {
                            tracing::info!("Already in Primary mode");
                        }
                        Some(ReplicationWorkerMessage::Shutdown) => {
                            return Ok(ReplicatorStateResult::Shutdown);
                        }
                        None => {}
                    }
                }
                _ = server.run(self.server_options.clone(), self.store.clone()) => {}
            }
        }
    }

    /// The replicator's main loop
    async fn main_loop(&mut self) -> Result<(), SableError> {
        tracing::info!("Started");

        let mut result = match Server::state().persistent_state().role() {
            ServerRole::Primary => self.primary_loop().await?,
            ServerRole::Replica => self.replica_loop().await?,
        };

        loop {
            result = match result {
                ReplicatorStateResult::ConnectToPrimary((ip, port)) => {
                    Server::state()
                        .persistent_state()
                        .set_primary_address(format!("{}:{}", ip, port));
                    Server::state().persistent_state().save();
                    self.replica_loop().await?
                }
                ReplicatorStateResult::PrimaryMode => {
                    tracing::info!("Switching to primary mode");
                    self.primary_loop().await?
                }
                ReplicatorStateResult::Shutdown => {
                    tracing::info!("Exiting");
                    break;
                }
            }
        }
        Ok(())
    }
}
