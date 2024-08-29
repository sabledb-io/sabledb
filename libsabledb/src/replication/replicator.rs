use crate::replication::ReplicationServer;
use crate::server::ReplicationTelemetry;

#[allow(unused_imports)]
use crate::{
    replication::{replication_thread_stop_all, ReplClientCommand, ReplicationClient, ServerRole},
    server::{Client, SableError, ServerOptions, Telemetry, WorkerContext},
    storage::StorageAdapter,
};

use tokio::sync::mpsc::Receiver as TokioReciever;
use tokio::sync::mpsc::Sender as TokioSender;

pub type ReplicatorSender = TokioSender<ReplicationWorkerMessage>;
pub type ReplicatorReceiver = TokioReciever<ReplicationWorkerMessage>;
pub type WorkerHandle = tokio::runtime::Handle;

#[derive(Debug, Clone)]
enum ServerRoleChanged {
    /// The server role has changed to Replica
    Replica {
        /// The primary IP address
        primary_ip: String,
        /// The primary **replication** port
        primary_port: u16,
    },
    /// The server is now a Primary
    Primary {
        /// The IP on which this server accepts new replicas
        ip: String,
        /// The port on which this server accepts replicas
        port: u16,
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
                    let (role_changed_tx, role_changed_rx) =
                        tokio::sync::mpsc::channel::<ServerRoleChanged>(10);
                    // The replicator tasks:
                    // - The replication main loop (exchanging data)
                    // - Heartbeat task (using UDP)
                    let server_options_cloned = server_options.clone();
                    let replicator_handle = tokio::task::spawn_local(async move {
                        let mut replicator =
                            Replicator::new(rx, server_options_cloned, store.clone()).await;
                        if let Err(e) = replicator.main_loop(role_changed_tx).await {
                            tracing::error!("replicator error. {:?}", e);
                        }
                    });

                    let heartbeat_handle = tokio::task::spawn_local(async move {
                        if let Err(e) =
                            Self::heartbeat_loop(server_options.clone(), role_changed_rx).await
                        {
                            tracing::error!("heartbeat_loop error. {:?}", e);
                        }
                    });

                    tokio::select! {
                        _ = replicator_handle => {}
                        _ = heartbeat_handle => {}
                    }
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

    /// Depends on the role of the server, perform heartbeat action over UDP.
    /// If the server is Replica -> wait for heartbeat from the primary
    /// If the server is Primary -> send a heartbeat to our replicas
    async fn heartbeat_loop(
        server_options: ServerOptions,
        mut role_changed_rx: TokioReciever<ServerRoleChanged>,
    ) -> Result<(), SableError> {
        let Some(role) = role_changed_rx.recv().await else {
            return Err(SableError::OtherError(
                "Failed to read from role_changed_rx channel!".into(),
            ));
        };
        let mut heartbeat_task_handle =
            Self::run_heartbeat_task(server_options.clone(), role).await?;
        while let Some(role) = role_changed_rx.recv().await {
            // cancel the current heartbeat task and start a new one based on the new role
            heartbeat_task_handle.abort();
            heartbeat_task_handle = Self::run_heartbeat_task(server_options.clone(), role).await?;
        }

        Ok(())
    }

    /// Based on the new role, start the heartbeat task. Return its handle to the caller
    async fn run_heartbeat_task(
        _server_options: ServerOptions,
        role: ServerRoleChanged,
    ) -> Result<tokio::task::JoinHandle<()>, SableError> {
        let handle = match role {
            ServerRoleChanged::Replica {
                primary_ip,
                primary_port,
            } => {
                // The broadcast used is replication port +1
                let primary_port = primary_port.saturating_add(1);
                tokio::task::spawn_local(async move {
                    tracing::info!(
                        "Server role is: Replica. Starting heartbeat UDP receiver from Primary({}:{})",
                        primary_ip,
                        primary_port
                    );
                    // TODO: for now, just sleep
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                })
            }
            ServerRoleChanged::Primary { ip, port } => {
                // The broadcast used is replication port +1
                let port = port.saturating_add(1);
                tokio::task::spawn_local(async move {
                    tracing::info!(
                        "Server role is: Primary. Starting heartbeat broadcaster ({}:{})",
                        ip,
                        port
                    );
                    // TODO: for now, just sleep
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                })
            }
        };
        Ok(handle)
    }

    /// Create a new replication thread and wait for it exit.
    /// Replication thread can exit in 2 ways only:
    /// - The server terminated
    /// - User issued a "replicaof no one" command which basically changes the server role to primary
    async fn replica_loop(
        &mut self,
        role_changed_tx: &mut TokioSender<ServerRoleChanged>,
        primary_addr: Option<(String, u16)>,
    ) -> Result<ReplicatorStateResult, SableError> {
        let mut replication_config = self.server_options.load_replication_config();
        if let Some((primary_ip, primary_port)) = primary_addr {
            replication_config.ip = primary_ip;
            replication_config.port = primary_port;
        }

        tracing::info!(
            "Running replica loop using config: {:#?}",
            replication_config
        );

        let replication_client = ReplicationClient::default();
        // Launch the replication client on a dedicated thread
        // and return immediately
        let tx = replication_client
            .run(self.server_options.clone(), self.store.clone())
            .await?;
        ReplicationTelemetry::set_role(ServerRole::Replica);

        // Notify that our role has changed
        if let Err(e) = role_changed_tx
            .send(ServerRoleChanged::Replica {
                primary_ip: replication_config.ip.clone(),
                primary_port: replication_config.port,
            })
            .await
        {
            tracing::warn!(
                "Failed to notify role changed event (new role: Replica). {:?}",
                e
            );
        }

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
    async fn primary_loop(
        &mut self,
        role_changed_tx: &mut TokioSender<ServerRoleChanged>,
    ) -> Result<ReplicatorStateResult, SableError> {
        let replication_config = self.server_options.load_replication_config();
        tracing::info!(
            "Running primary loop using config: {:?}",
            replication_config
        );
        let server = ReplicationServer::default();
        ReplicationTelemetry::set_role(ServerRole::Primary);

        // Notify that our role has changed
        if let Err(e) = role_changed_tx
            .send(ServerRoleChanged::Primary {
                ip: replication_config.ip.clone(),
                port: replication_config.port,
            })
            .await
        {
            tracing::warn!(
                "Failed to notify role changed event (new role: Primary). {:?}",
                e
            );
        }
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
    async fn main_loop(
        &mut self,
        mut role_changed_tx: TokioSender<ServerRoleChanged>,
    ) -> Result<(), SableError> {
        tracing::info!("Started");

        let replication_config = self.server_options.load_replication_config();
        let mut result = match &replication_config.role {
            ServerRole::Primary => self.primary_loop(&mut role_changed_tx).await?,
            ServerRole::Replica => self.replica_loop(&mut role_changed_tx, None).await?,
        };

        loop {
            result = match result {
                ReplicatorStateResult::ConnectToPrimary((ip, port)) => {
                    self.replica_loop(&mut role_changed_tx, Some((ip, port)))
                        .await?
                }
                ReplicatorStateResult::PrimaryMode => {
                    tracing::info!("Switching to primary mode");
                    self.primary_loop(&mut role_changed_tx).await?
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
