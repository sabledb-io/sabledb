use crate::{
    commands::RedisCommand,
    server::{new_client_id, ServerState},
    storage::{ScanCursor, StorageAdapter},
};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicU16, AtomicU32},
    Arc,
};

pub struct ClientStateFlags {}

impl ClientStateFlags {
    /// The client was killed (either by the user or by SableDb internally)
    pub const KILLED: u32 = (1 << 0);
    /// Pre transaction state. This state is a special state that indicating that
    /// SableDb is preparing for executing a transcation for this client
    pub const TXN_CALC_SLOTS: u32 = (1 << 1);
    /// Pre transaction state. This state is a special state that indicating that
    /// SableDb is preparing for executing a transcation for this client
    pub const TXN_MULTI: u32 = (1 << 2);
    /// Txn is currently in progress. When this state is detected, some operations
    /// are skipped (for example: LockManager will return noop lock, because the lock is already obtained
    /// at the top level command, i.e. `EXEC`)
    pub const TXN_EXEC: u32 = (1 << 3);
}

pub struct ClientState {
    server_state: Arc<ServerState>,
    store: StorageAdapter,
    store_with_cache: StorageAdapter,
    client_id: u128,
    pub tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    db_id: AtomicU16,
    attributes: DashMap<String, String>,
    flags: AtomicU32,
    cursors: DashMap<u64, Rc<ScanCursor>>,
    /// Holds the commands to be executed while in the "MULTI" state
    txn_commands: Arc<SegQueue<Rc<RedisCommand>>>,
}

impl ClientState {
    pub fn new(
        server_state: Arc<ServerState>,
        store: StorageAdapter,
        tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    ) -> Self {
        ClientState {
            server_state,
            store_with_cache: store.transaction(),
            store,
            client_id: new_client_id(),
            tls_acceptor,
            db_id: AtomicU16::new(0),
            attributes: DashMap::<String, String>::default(),
            flags: AtomicU32::new(0),
            cursors: DashMap::<u64, Rc<ScanCursor>>::default(),
            txn_commands: Arc::<SegQueue<Rc<RedisCommand>>>::default(),
        }
    }

    /// Return the queued commands queue
    pub fn txn_commands(&self) -> Arc<SegQueue<Rc<RedisCommand>>> {
        self.txn_commands.clone()
    }

    /// Queue command for later execution
    pub fn txn_queue_command(&self, command: Rc<RedisCommand>) {
        self.txn_commands.push(command);
    }

    /// Return the queued command as a vector. This function consumes the queue
    /// after which `self.queued_commands.is_empty() == true`
    pub fn txn_take_commands(&self) -> Vec<Rc<RedisCommand>> {
        let mut vec_commands = Vec::<Rc<RedisCommand>>::with_capacity(self.txn_commands.len());
        while let Some(cmd) = self.txn_commands.pop() {
            vec_commands.push(cmd);
        }
        vec_commands
    }

    pub fn database(&self) -> &StorageAdapter {
        if self.is_txn_state_exec() {
            &self.store_with_cache
        } else {
            &self.store
        }
    }

    pub fn id(&self) -> u128 {
        self.client_id
    }

    pub fn server_inner_state(&self) -> Arc<ServerState> {
        self.server_state.clone()
    }

    /// Return the client's database ID
    pub fn database_id(&self) -> u16 {
        self.db_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the active database ID for this client
    pub fn set_database_id(&self, id: u16) {
        self.db_id.store(id, std::sync::atomic::Ordering::Relaxed);
    }

    /// Is the client alive? (e.g. was it killed using `client kill` command?)
    pub fn active(&self) -> bool {
        !self.is_flag_enabled(ClientStateFlags::KILLED)
    }

    /// Kill the current client by marking it as non active. The connection will be closed
    /// next time the client will attempt to use it or when a timeout occurs
    pub fn kill(&self) {
        self.enable_client_flag(ClientStateFlags::KILLED, true)
    }

    pub fn is_txn_state_calc_slots(&self) -> bool {
        self.is_flag_enabled(ClientStateFlags::TXN_CALC_SLOTS)
    }

    pub fn set_txn_state_calc_slots(&self, enabled: bool) {
        self.enable_client_flag(ClientStateFlags::TXN_CALC_SLOTS, enabled)
    }

    pub fn set_txn_state_exec(&self, enabled: bool) {
        self.enable_client_flag(ClientStateFlags::TXN_EXEC, enabled)
    }

    pub fn is_txn_state_exec(&self) -> bool {
        self.is_flag_enabled(ClientStateFlags::TXN_EXEC)
    }

    pub fn set_txn_state_multi(&self, enabled: bool) {
        self.enable_client_flag(ClientStateFlags::TXN_MULTI, enabled)
    }

    pub fn discard_transaction(&self) {
        self.enable_client_flag(ClientStateFlags::TXN_MULTI, false);
        self.enable_client_flag(ClientStateFlags::TXN_CALC_SLOTS, false);
        self.enable_client_flag(ClientStateFlags::TXN_EXEC, false);
        let _ = self.txn_take_commands();
    }

    pub fn is_txn_state_multi(&self) -> bool {
        self.is_flag_enabled(ClientStateFlags::TXN_MULTI)
    }

    /// Set a client attribute
    pub fn set_attribute(&self, name: &str, value: &str) {
        self.attributes.insert(name.to_owned(), value.to_owned());
    }

    /// Get a client attribute
    pub fn attribute(&self, name: &String) -> Option<String> {
        self.attributes.get(name).map(|p| p.value().clone())
    }

    pub fn error(&self, msg: &str) {
        tracing::error!("CLNT {}: {}", self.client_id, msg);
    }

    pub fn debug(&self, msg: &str) {
        Self::static_debug(self.client_id, msg)
    }

    pub fn trace(&self, msg: &str) {
        tracing::trace!("CLNT {}: {}", self.client_id, msg);
    }

    pub fn warn(&self, msg: &str) {
        tracing::warn!("CLNT {}: {}", self.client_id, msg);
    }

    /// static version
    pub fn static_debug(client_id: u128, msg: &str) {
        tracing::debug!("CLNT {}: {}", client_id, msg);
    }

    /// Return a cursor by ID or create a new cursor, add it and return it
    pub fn cursor_or<F>(&self, cursor_id: u64, f: F) -> Option<Rc<ScanCursor>>
    where
        F: FnOnce() -> Rc<ScanCursor>,
    {
        if let Some(c) = self.cursors.get(&cursor_id) {
            Some(c.value().clone())
        } else if cursor_id == 0 {
            // create a new cursor and add it (only if cursor_id == 0)
            let cursor = f();
            self.cursors.insert(cursor.id(), cursor.clone());
            Some(cursor)
        } else {
            None
        }
    }

    /// Insert or replace cursor (this method uses `cursor.id()` as the key)
    pub fn set_cursor(&self, cursor: Rc<ScanCursor>) {
        self.cursors.insert(cursor.id(), cursor);
    }

    /// Remove a cursor from this client
    pub fn remove_cursor(&self, cursor_id: u64) {
        let _ = self.cursors.remove(&cursor_id);
    }

    /// return the number of active cursors for this client
    pub fn cursors_count(&self) -> usize {
        self.cursors.len()
    }

    // Helper methods
    fn enable_client_flag(&self, flag: u32, enabled: bool) {
        let mut flags = self.flags.load(std::sync::atomic::Ordering::Relaxed);
        if enabled {
            flags |= flag;
        } else {
            flags &= !flag;
        }
        self.flags
            .store(flags, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_flag_enabled(&self, flag: u32) -> bool {
        let flags = self.flags.load(std::sync::atomic::Ordering::Relaxed);
        flags & flag == flag
    }
}
