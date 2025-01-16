use crate::server::{
    BroadcastMessageType, Client, ClientState, SableError, ServerOptions, SlotBitmap, Telemetry,
    WorkerContext, WorkerManager, WorkerMessage, WorkerSender,
};
use crate::{
    commands::ClientNextAction,
    replication::{ReplicationWorkerMessage, Replicator, ReplicatorContext},
    Cron, CronContext, CronMessage, ServerPersistentState, StorageAdapter, ValkeyCommand,
};
use bytes::BytesMut;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::RwLock as StdRwLock;
use std::sync::{
    //atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc::Receiver as TokioReceiver;
use tokio::sync::mpsc::Sender as TokioSender;
use tokio::sync::RwLock as TokioRwLock;

lazy_static::lazy_static! {
    static ref SERVER_STATE: StdRwLock<Arc<ServerState>> = StdRwLock::new(Arc::new(ServerState::default()));
}

#[derive(Default)]
struct BlockedClients {
    /// Maps between client id and the keys it is blocking on
    clients: HashMap<u128, Vec<BytesMut>>,
    /// Mapes between a key and list of clients pending
    keys_map: HashMap<BytesMut, VecDeque<(u128, TokioSender<u8>)>>,
}

impl BlockedClients {
    pub fn contains_key(&self, key: &BytesMut) -> bool {
        self.keys_map.contains_key(key)
    }

    pub fn get_mut(&mut self, key: &BytesMut) -> Option<&mut VecDeque<(u128, TokioSender<u8>)>> {
        self.keys_map.get_mut(key)
    }

    pub fn is_empty(&self) -> bool {
        self.keys_map.is_empty()
    }

    pub fn delete_key(&mut self, key: &BytesMut) {
        let _ = self.keys_map.remove(key);
    }

    pub fn add_client(&mut self, client_id: u128, keys: Vec<BytesMut>) {
        self.clients.insert(client_id, keys);
    }

    /// Remove a client from the blocking clients tracking lists
    /// Visit all keys marked for notification and remove all the
    /// references to `client_id` and finally, remove the `client_id`
    /// from the blocked clients list
    pub fn remove_client(&mut self, client_id: &u128) {
        // fast path checking:
        let Some(blocking_keys) = self.clients.get(client_id) else {
            tracing::trace!("BlockedClients::remove_client(): nothing to be done");
            return;
        };

        tracing::trace!("Removing client {} for blocking lists", client_id);
        // visit the keys that this client was blocking on and remove the registered
        // entry for `client_id`
        for k in blocking_keys {
            let Some(list) = self.keys_map.get_mut(k) else {
                continue;
            };

            list.retain(|(id, _)| id.ne(client_id));
            if list.is_empty() {
                // no more blocked clients for this key, remove this entire reocrd
                let _ = self.keys_map.remove(k);
            }
        }

        // and finally, remove the client id from the `clients` map
        let _ = self.clients.remove(client_id);
        tracing::trace!("Success");
    }
}

/// Possible output for block_client function
#[derive(Debug)]
pub enum BlockClientResult {
    /// Client successfully blocked
    Blocked(TokioReceiver<u8>),
    /// Transaction is active, can't block
    TxnActive,
}

pub struct ServerState {
    blocked_clients: TokioRwLock<BlockedClients>,
    telemetry: Arc<StdRwLock<Telemetry>>,
    opts: Arc<StdRwLock<ServerOptions>>,
    replicator_context: Option<Arc<ReplicatorContext>>,
    evictor_context: Option<Arc<CronContext>>,
    worker_tx_channels: DashMap<std::thread::ThreadId, WorkerSender>,
    /// This state is persisted to the disk
    persistent_state: ServerPersistentState,
}

pub struct Server {
    worker_manager: WorkerManager,
}

impl Default for ServerState {
    fn default() -> Self {
        ServerState::new()
    }
}

impl ServerState {
    pub fn new() -> Self {
        ServerState {
            telemetry: Arc::new(StdRwLock::<Telemetry>::default()),
            blocked_clients: TokioRwLock::<BlockedClients>::default(),
            opts: Arc::new(StdRwLock::<ServerOptions>::default()),
            replicator_context: None,
            evictor_context: None,
            worker_tx_channels: DashMap::<std::thread::ThreadId, WorkerSender>::new(),
            persistent_state: ServerPersistentState::new(),
        }
    }

    pub fn add_worker_tx_channel(&self, worker_id: std::thread::ThreadId, tx: WorkerSender) {
        self.worker_tx_channels.insert(worker_id, tx);
    }

    /// Broadcast a message to all the workers
    pub async fn broadcast_msg(&self, message: BroadcastMessageType) -> Result<(), SableError> {
        for item in &self.worker_tx_channels {
            item.value()
                .send(WorkerMessage::BroadcastMessage(message))
                .await?;
        }
        Ok(())
    }

    pub fn set_server_options(mut self, opts: Arc<StdRwLock<ServerOptions>>) -> Self {
        self.opts = opts;
        self
    }

    pub fn set_replication_context(mut self, replication_context: ReplicatorContext) -> Self {
        self.replicator_context = Some(Arc::new(replication_context));
        self
    }

    pub fn set_evictor_context(mut self, evictor_context: CronContext) -> Self {
        self.evictor_context = Some(Arc::new(evictor_context));
        self
    }

    /// Mark client as "terminated"
    pub async fn terminate_client(&self, client_id: u128) -> Result<(), SableError> {
        // first, try to local thread, if this fails, broadcast the message to other threads
        if !Client::terminate_client(client_id) {
            self.broadcast_msg(BroadcastMessageType::KillClient(client_id))
                .await?;
        }
        Ok(())
    }

    pub fn shared_telemetry(&self) -> Arc<StdRwLock<Telemetry>> {
        self.telemetry.clone()
    }

    pub fn options(&self) -> Arc<StdRwLock<ServerOptions>> {
        self.opts.clone()
    }

    /// Remove `client_id` from the blocking list queues
    pub async fn remove_blocked_client(&self, client_id: &u128) {
        let mut blocked_clients = self.blocked_clients.write().await;
        blocked_clients.remove_client(client_id);
    }

    /// If we have blocked clients waiting for `key` -> wake them up now
    pub async fn wakeup_clients(&self, key: &BytesMut, mut num_clients: usize) {
        tracing::trace!("Waking up {} clients for {:?}", num_clients, key);
        {
            // Fast path:
            // Obtain a read lock and check if there are any clients that needs to be waked up
            let blocked_clients = self.blocked_clients.read().await;
            if blocked_clients.is_empty() || !blocked_clients.contains_key(key) {
                tracing::trace!("there are no blocked clients for key {:?}", key);
                return;
            }
        }

        // need to get a write lock
        let mut blocked_clients = self.blocked_clients.write().await;

        // Since a client might register for more than one key, we need to
        // keep track of the clients that were notified and remove them from the blocking list
        let mut notified_clients = Vec::<u128>::new();

        //==>
        // Some other thread might have updated the table here, so double check the table before continuing
        // but this time do this under a write-lock
        //==>

        // double check the blocking client table now
        if blocked_clients.is_empty() || !blocked_clients.contains_key(key) {
            return;
        }

        // based on num_clients, wakeup all the clients that are blocked by
        // this key
        let remove_key = if let Some(channel_queue) = blocked_clients.get_mut(key) {
            while num_clients > 0 {
                if let Some((client_id, client_channel)) = channel_queue.pop_front() {
                    if let Err(e) = client_channel.send(0u8).await {
                        tracing::debug!(
                            "error while sending wakeup bit. client already timed out or terminated. {:?}",
                            e
                        );
                    } else {
                        tracing::debug!("Client {} was notified!", client_id);
                        notified_clients.push(client_id);
                        num_clients = num_clients.saturating_sub(1);
                    }
                } else {
                    // No more clients in the queue
                    break;
                }
            }
            channel_queue.is_empty()
        } else {
            false
        };

        if remove_key {
            // we no longer have clients blocked by this key
            blocked_clients.delete_key(key);
        }

        for cid in &notified_clients {
            // remove this client from the tables
            blocked_clients.remove_client(cid);
        }
    }

    /// Block the current client for the provided keys
    pub async fn block_client(
        &self,
        client_id: u128,
        keys: &[BytesMut],
        client_state: Rc<ClientState>,
    ) -> BlockClientResult {
        tracing::debug!("blocking client {} for keys {:?}", client_id, keys);
        if client_state.is_txn_state_exec() {
            return BlockClientResult::TxnActive;
        }

        // need to get a write lock
        let mut blocked_clients = self.blocked_clients.write().await;

        // Keep track of this client
        let keys_to_block_on: Vec<BytesMut> = keys.to_vec();
        blocked_clients.add_client(client_id, keys_to_block_on);

        let (tx, rx) = tokio::sync::mpsc::channel(keys.len());
        for key in keys.iter() {
            if let Some(channel_queue) = blocked_clients.keys_map.get_mut(key) {
                channel_queue.push_back((client_id, tx.clone()));
            } else {
                // first time
                let mut channel_queue = VecDeque::<(u128, TokioSender<u8>)>::new();
                channel_queue.push_back((client_id, tx.clone()));
                blocked_clients.keys_map.insert(key.clone(), channel_queue);
            };
        }
        BlockClientResult::Blocked(rx)
    }

    // Notify the replicator that initialisation is done
    pub fn notify_replicator_init_done_sync(&self) -> Result<(), SableError> {
        if let Some(repliction_context) = &self.replicator_context {
            repliction_context.send_sync(ReplicationWorkerMessage::InitDone)?;
        }
        Ok(())
    }

    // Connect to primary instance
    pub async fn connect_to_primary(&self, address: String, port: u16) -> Result<(), SableError> {
        if let Some(repliction_context) = &self.replicator_context {
            // Update the configuration file first
            Server::state()
                .persistent_state()
                .set_primary_address(format!("{}:{}", address, port));
            Server::state().persistent_state().save();
            repliction_context
                .send(ReplicationWorkerMessage::ConnectToPrimary((address, port)))
                .await?;
        }
        Ok(())
    }

    // Connect to primary instance
    pub fn connect_to_primary_sync(&self, address: String, port: u16) -> Result<(), SableError> {
        if let Some(repliction_context) = &self.replicator_context {
            tracing::debug!("Sending request to ConnectToPrimary({}, {})", address, port);
            // Update the configuration file first
            Server::state()
                .persistent_state()
                .set_primary_address(format!("{}:{}", address, port));
            Server::state().persistent_state().save();
            repliction_context
                .send_sync(ReplicationWorkerMessage::ConnectToPrimary((address, port)))?;
            tracing::debug!("Success");
        }
        Ok(())
    }

    // Change the role of this instance to primary
    pub async fn switch_role_to_primary(&self) -> Result<(), SableError> {
        Server::state().persistent_state().set_primary_node_id(None);
        Server::state().persistent_state().save();

        if let Some(repliction_context) = &self.replicator_context {
            repliction_context
                .send(ReplicationWorkerMessage::PrimaryMode)
                .await?;
        }
        Ok(())
    }

    pub fn shutdown(&self) {
        if let Some(replicator_context) = &self.replicator_context {
            tracing::info!("Sending shutdown command to replicator");
            let _ = replicator_context.send_sync(ReplicationWorkerMessage::Shutdown);
        }
        if let Some(evictor_context) = &self.evictor_context {
            tracing::info!("Sending shutdown command to evictor");
            let _ = evictor_context.send_sync(CronMessage::Shutdown);
        }
    }

    // Sending command to the evictor thread using async API
    pub async fn send_evictor(&self, message: CronMessage) -> Result<(), SableError> {
        if let Some(evictor_context) = &self.evictor_context {
            tracing::debug!("Sending {:?} command (async) to evictor", message);
            evictor_context.send(message).await?;
        }
        Ok(())
    }

    // Sending command to the evictor thread using non async API
    pub fn send_evictor_sync(&self, message: CronMessage) -> Result<(), SableError> {
        if let Some(evictor_context) = &self.evictor_context {
            tracing::debug!("Sending {:?} command (sync) to evictor", message);
            evictor_context.send_sync(message)?;
        }
        Ok(())
    }

    pub fn persistent_state(&self) -> &ServerPersistentState {
        &self.persistent_state
    }

    /// Return the slots owned by this instance
    pub fn slots(&self) -> &SlotBitmap {
        self.persistent_state.slots()
    }
}

impl Server {
    pub fn new(
        opts: Arc<StdRwLock<ServerOptions>>,
        store: StorageAdapter,
        workers_count: usize,
    ) -> Result<Self, SableError> {
        let replicator_context = Replicator::run(opts.clone(), store.clone())?;
        let evictor_content = Cron::run(opts.clone(), store.clone())?;
        let state = Arc::new(
            ServerState::new()
                .set_server_options(opts)
                .set_replication_context(replicator_context)
                .set_evictor_context(evictor_content),
        );

        *SERVER_STATE.write().expect("global server state lock") = state.clone();

        let worker_manager = WorkerManager::new(workers_count, store.clone(), state.clone())?;
        Ok(Server { worker_manager })
    }

    pub fn get_worker(&self) -> &WorkerContext {
        self.worker_manager.pick()
    }

    pub fn state() -> Arc<ServerState> {
        SERVER_STATE.read().expect("").clone()
    }

    /// Execute an internal command and returns its output as a raw RESP string.
    /// Note that blocking commands are not allowed.
    pub async fn process_internal_command(
        command: Rc<ValkeyCommand>,
        store: &StorageAdapter,
    ) -> Result<BytesMut, SableError> {
        let state = Self::state();
        let mut sink = crate::io::FileResponseSink::new().await?;
        let client = Client::new(state, store.clone(), None);
        match Client::handle_command(client.inner(), command.clone(), &mut sink.fp).await? {
            ClientNextAction::NoAction => {
                Ok(BytesMut::from(sink.read_all_as_string().await?.as_bytes()))
            }
            ClientNextAction::TerminateConnection => Err(SableError::ConnectionClosed),
            ClientNextAction::SendResponse(buf) => Ok(buf),
            ClientNextAction::Wait((_, _, _)) => Err(SableError::OtherError(
                "Internal blocking commands are not allowed".into(),
            )),
        }
    }
}
