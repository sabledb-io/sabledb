use crate::replication::ReplicationServer;
use futures_intrusive::sync::ManualResetEvent;

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
use std::sync::{Arc, RwLock as StdRwLock};

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
    /// Initialisation is done, start the replicator
    InitDone,
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

pub struct Replicator {
    /// Shared server state
    server_options: Arc<StdRwLock<ServerOptions>>,
    /// The channel on which this worker accepts commands
    rx_channel: ReplicatorReceiver,
    /// The store
    store: StorageAdapter,
    /// Events for running replication tasks (usually, there should be only one in the list)
    events: Vec<Arc<ManualResetEvent>>,
}

#[derive(Clone, Debug)]
/// The `ReplicatorContext` allows other threads to communicate with the replicator
/// thread using a dedicated channel
pub struct ReplicatorContext {
    runtime_handle: WorkerHandle,
    worker_send_channel: ReplicatorSender,
}

unsafe impl Send for ReplicatorContext {}

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

impl Replicator {
    /// Private method: create a new replicator instance
    async fn new(
        rx: ReplicatorReceiver,
        server_options: Arc<StdRwLock<ServerOptions>>,
        store: StorageAdapter,
    ) -> Self {
        Replicator {
            rx_channel: rx,
            server_options,
            store,
            events: Vec::<Arc<ManualResetEvent>>::default(),
        }
    }

    /// Loop over the events queue and set them all. Clear the queue afterwards
    fn set_all_events(&mut self) {
        for evt in &self.events {
            evt.set();
        }
        self.events.clear();
    }

    /// Create a new manual reset event and a clone of it. A clone is also kept internally
    fn new_event(&mut self) -> Arc<ManualResetEvent> {
        let event = Arc::new(ManualResetEvent::new(false));
        self.events.push(event.clone());
        event
    }

    /// Spawn the replication thread returning a a context for the caller
    /// The context can be used to communicate with the replicator
    pub fn run(
        server_options: Arc<StdRwLock<ServerOptions>>,
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

    /// Create a new replication thread and wait for it exit. There can be only one!
    /// Replication thread can exit in 2 ways only:
    /// - The server terminated
    /// - User issued a "replicaof no one" command which basically changes the server role to primary
    async fn replica_loop(
        server_options: Arc<StdRwLock<ServerOptions>>,
        store: StorageAdapter,
        event: Arc<ManualResetEvent>,
    ) -> Result<(), SableError> {
        tracing::info!("Entering replica loop");

        let replication_client = ReplicationClient::default();
        // Launch the replication client on a dedicated thread
        // and return immediately
        let Ok(tx) = replication_client
            .run(server_options, store, event.clone())
            .await
        else {
            tracing::error!("Failed to start replication client. Replication client task exiting");
            return Ok(());
        };

        // Wait for signal to terminate
        tracing::info!("Waiting for termination event");
        event.wait().await;
        tracing::info!("Got event to terminate replication loop");

        // Notify the replication client thread to terminate
        let _ = tx.send(ReplClientCommand::Shutdown).await;

        // Clear the event
        event.reset();
        Ok(())
    }

    /// Change the node state to primary and clear any primary node ID
    fn change_state_to_primary(&self) {
        Server::state().persistent_state().set_primary_node_id(None);
        Server::state().persistent_state().save();
        tracing::info!("Success");
    }

    /// The replicator's main loop
    async fn main_loop(&mut self) -> Result<(), SableError> {
        tracing::info!("Waiting for initialisation complete event..");
        let Some(ReplicationWorkerMessage::InitDone) = self.rx_channel.recv().await else {
            return Err(SableError::InternalError("Expected 'Start' command".into()));
        };
        tracing::info!("Started");

        if Server::state().persistent_state().is_primary() {
            self.change_state_to_primary();
        }

        // Start the replication server loop
        let server_options_clone = self.server_options.clone();
        let store_clone = self.store.clone();
        tokio::task::spawn_local(async {
            tracing::info!("Running replication server");
            let server = ReplicationServer::default();
            server.run(server_options_clone, store_clone).await
        });

        while let Some(cmd) = self.rx_channel.recv().await {
            match cmd {
                ReplicationWorkerMessage::ConnectToPrimary((primary_ip, primary_port)) => {
                    // Notify the replication task to exit
                    self.set_all_events();

                    tracing::info!("Connecting to primary at {}:{}", primary_ip, primary_port);
                    replication_thread_stop_all().await;
                    let address = format!("{}:{}", primary_ip, primary_port);
                    Server::state()
                        .persistent_state()
                        .set_primary_address(address);

                    let event = self.new_event();
                    // This node is a replica. Start a replication task
                    tokio::task::spawn_local(Self::replica_loop(
                        self.server_options.clone(),
                        self.store.clone(),
                        event,
                    ));
                }
                ReplicationWorkerMessage::PrimaryMode => {
                    // Notify the replication task to exit
                    self.set_all_events();
                    tracing::info!("Switching to primary role");
                    replication_thread_stop_all().await;
                    self.change_state_to_primary();
                }
                ReplicationWorkerMessage::Shutdown => {
                    tracing::info!("Received notification to shutdown");
                    self.set_all_events();
                    replication_thread_stop_all().await;
                    return Ok(());
                }
                ReplicationWorkerMessage::InitDone => {
                    return Err(SableError::InternalError("Invalid command 'Start'".into()));
                }
            }
        }
        Ok(())
    }
}
