use crate::{
    commands::{ClientNextAction, HandleCommandResult, Strings},
    io::RespWriter,
    storage::ScanCursor,
    utils::RespBuilderV2,
    ClientCommands, GenericCommands, HashCommands, ListCommands, ParserError, RedisCommand,
    RedisCommandName, RequestParser, SableError, ServerCommands, ServerState, StorageAdapter,
    StringCommands, Telemetry, TransactionCommands,
};

use bytes::BytesMut;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::{
    atomic::{AtomicU16, AtomicU32},
    Mutex,
};

const PONG: &[u8] = b"+PONG\r\n";

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

#[allow(unused_imports)]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::Receiver as TokioReceiver,
    time::{sleep, Duration},
};

use tracing::log::{log_enabled, Level};

lazy_static::lazy_static! {
    static ref CLIENT_ID_GENERATOR: Mutex<u128> = Mutex::new(0);
}

thread_local! {
    /// Keep track on clients assigned to this worker
    pub static WORKER_CLIENTS: RefCell<HashMap<u128, Rc<ClientState>>>
        = RefCell::new(HashMap::<u128, Rc<ClientState>>::new());
}

/// Generate a new client ID
fn new_client_id() -> u128 {
    let mut value = CLIENT_ID_GENERATOR.lock().expect("poisoned mutex");
    *value += 1;
    *value
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
    queued_commands: Arc<SegQueue<Rc<RedisCommand>>>,
}

#[derive(PartialEq, PartialOrd)]
enum PreHandleCommandResult {
    Continue,
    WriteInReadOnlyReplica,
    /// Client was killed
    ClientKilled,
    /// Running inside a "MULTI" block. This command should be queued
    QueueCommand,
    /// The current txn should be aborted. This can happen for multiple reasons
    /// e.g. a "No transaction" command was passed as part of the MULTI phase
    CmdIsNotValidForTxn,
}

/// Used by the `block_until` return code
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitResult {
    TryAgain,
    Timeout,
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
            queued_commands: Arc::<SegQueue<Rc<RedisCommand>>>::default(),
        }
    }

    /// Return the queued commands queue
    pub fn commands_queue(&self) -> Arc<SegQueue<Rc<RedisCommand>>> {
        self.queued_commands.clone()
    }

    /// Return the queued command as a vector. This function consumes the queue
    /// after which `self.queued_commands.is_empty() == true`
    pub fn take_queued_commands(&self) -> Vec<Rc<RedisCommand>> {
        let mut vec_commands = Vec::<Rc<RedisCommand>>::with_capacity(self.queued_commands.len());
        while let Some(cmd) = self.queued_commands.pop() {
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

pub struct Client {
    state: Rc<ClientState>,
}

impl Client {
    /// Terminate a client by its ID (`client_id`). Return `true` if the client was successfully marked
    /// as terminated
    pub fn terminate_client(client_id: u128) -> bool {
        WORKER_CLIENTS.with(|clients| {
            if let Some(client_state) = clients.borrow().get(&client_id) {
                tracing::info!("Client {} terminated", client_id);
                client_state.kill();
                true
            } else {
                false
            }
        })
    }

    pub fn inner(&self) -> Rc<ClientState> {
        self.state.clone()
    }

    pub fn new(
        server_state: Arc<ServerState>,
        store: StorageAdapter,
        tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    ) -> Self {
        Telemetry::inc_connections_opened();
        let state = Rc::new(ClientState::new(server_state, store, tls_acceptor));
        let state_clone = state.clone();

        // register this client
        WORKER_CLIENTS.with(|clients| {
            clients
                .borrow_mut()
                .insert(state_clone.client_id, state_clone);
        });
        Client { state }
    }

    /// Execute the client's main loop
    pub async fn run(&mut self, stream: std::net::TcpStream) -> Result<(), SableError> {
        self.main_loop(stream).await
    }

    /// The client's main loop
    async fn main_loop(&mut self, stream: std::net::TcpStream) -> Result<(), SableError> {
        let tokio_stream = tokio::net::TcpStream::from_std(stream)?;
        let (channel_tx, channel_rx) = tokio::sync::mpsc::channel(100);

        let (r, w) = if self.state.server_state.options().use_tls() {
            // TLS enabled. Perform the TLS handshake and spawn the tasks
            let Some(tls_acceptor) = &self.inner().tls_acceptor else {
                return Err(SableError::OtherError("No TLS acceptor".to_string()));
            };
            tracing::trace!("Waiting for TLS handshake");
            let tls_stream = tls_acceptor.accept(tokio_stream).await?;
            let (rx, tx) = tokio::io::split(tls_stream);
            let shared_state = self.state.clone();
            let r = tokio::task::spawn_local(async move {
                let _ = Self::reader_loop(rx, channel_tx, shared_state).await;
            });

            let shared_state = self.state.clone();
            let w = tokio::task::spawn_local(async move {
                let _ = Self::writer_loop(tx, channel_rx, shared_state).await;
            });
            (r, w)
        } else {
            // No TLS
            let (rx, tx) = tokio::io::split(tokio_stream);
            let shared_state = self.state.clone();
            let r = tokio::task::spawn_local(async move {
                let _ = Self::reader_loop(rx, channel_tx, shared_state).await;
            });

            let shared_state = self.state.clone();
            let w = tokio::task::spawn_local(async move {
                let _ = Self::writer_loop(tx, channel_rx, shared_state).await;
            });
            (r, w)
        };

        // If any of the tasks (reader - writer) ends,
        // abort the connection
        tokio::select! {
            _ = r => {
                Err(SableError::StdIoError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "reader task ended prematurely. closing connection",
                )))
            },
            _ = w => {
                Err(SableError::StdIoError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "writer task ended prematurely. closing connection",
                )))
            }
        }
    }

    /// Read data from the network, parse it and send it "writer" task for processing
    async fn reader_loop(
        mut rx: impl AsyncReadExt + std::marker::Unpin,
        channel_tx: tokio::sync::mpsc::Sender<Rc<RedisCommand>>,
        client_state: Rc<ClientState>,
    ) -> Result<(), SableError> {
        let mut buffer = BytesMut::new();
        loop {
            let mut request_parser = RequestParser::default();
            match request_parser.parse(&buffer) {
                Err(SableError::Parser(ParserError::NeedMoreData)) => {
                    if log_enabled!(Level::Trace) {
                        client_state.trace("(NeedMoreData)) Reading data from network");
                    }
                    // read some bytes
                    let mut read_buffer = BytesMut::with_capacity(1024);
                    rx.read_buf(&mut read_buffer).await?;
                    if read_buffer.is_empty() {
                        // connection closed
                        if log_enabled!(Level::Debug) {
                            client_state.debug("Connection closed");
                        }
                        return Ok(());
                    }

                    if log_enabled!(Level::Debug) {
                        client_state.debug(&format!(
                            "===> Len: {}, Buff: {:?}",
                            read_buffer.len(),
                            read_buffer
                        ));
                    }

                    // update the telemetry
                    Telemetry::inc_net_bytes_read(read_buffer.len() as u128);

                    // append the bytes written to the overall buffer
                    buffer.extend_from_slice(&read_buffer);
                    continue;
                }

                Err(e) => {
                    client_state.warn(&format!("Error while parsing input message. {:?}", e));
                    client_state.warn("Closing connection");
                    request_parser.reset();
                    return Ok(());
                }
                Ok(result) => {
                    // consume the parsed chunk
                    if log_enabled!(Level::Debug) {
                        client_state.debug(&format!("Parsing result: {:?}", result));
                    }
                    let _ = buffer.split_to(result.bytes_consumed);
                    if buffer.len() < 1024 {
                        // make sure we have enough room for 1K of message
                        buffer.reserve(1024 - buffer.len());
                    }
                    channel_tx.send(result.command).await?;
                }
            }
        }
    }

    /// Accepts the parsed requests, execute the command and send back the response
    async fn writer_loop(
        mut tx: impl AsyncWriteExt + std::marker::Unpin,
        mut channel_rx: TokioReceiver<Rc<RedisCommand>>,
        client_state: Rc<ClientState>,
    ) -> Result<(), SableError> {
        while let Some(command) = channel_rx.recv().await {
            // update telemetry and process the command
            Telemetry::inc_total_commands_processed();

            // Use a loop here to handle timeouts & retries
            loop {
                let response =
                    Self::handle_command(client_state.clone(), command.clone(), &mut tx).await;
                match response {
                    Ok(next_action) => match next_action {
                        ClientNextAction::NoAction => {
                            break;
                        }
                        ClientNextAction::SendResponse(response) => {
                            // command completed successfully
                            Self::send_response(&mut tx, &response, client_state.client_id).await?;
                            break;
                        }
                        ClientNextAction::Wait((rx, duration)) => {
                            // suspend the client for the specified duration or until a wakeup bit arrives
                            match Self::wait_for(rx, duration).await {
                                WaitResult::Timeout => {
                                    if log_enabled!(Level::Debug) {
                                        client_state.debug("timeout occurred");
                                    }
                                    // time-out occurred, build a proper response message and break out the inner loop
                                    let response_buffer = Self::handle_timeout(
                                        client_state.clone(),
                                        command.clone(),
                                    )?;
                                    Self::send_response(
                                        &mut tx,
                                        &response_buffer,
                                        client_state.client_id,
                                    )
                                    .await?;
                                    break;
                                }
                                WaitResult::TryAgain => {
                                    continue;
                                }
                            }
                        }
                        ClientNextAction::TerminateConnection => {
                            return Err(SableError::ConnectionClosed);
                        }
                    },
                    Err(e) => {
                        client_state.warn(&format!(
                            "failed to process command: {:?} error: {:?}",
                            command, e
                        ));
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Suspend the client until a message arrives or a time-out occurs
    pub async fn wait_for(mut cont: TokioReceiver<u8>, duration: Duration) -> WaitResult {
        tokio::select! {
            _ = cont.recv() => {
                WaitResult::TryAgain
            }
            _ = sleep(duration) => {
                WaitResult::Timeout
            }
        }
    }

    /// Handle time-out for command
    fn handle_timeout(
        _client_state: Rc<ClientState>,
        _command: Rc<RedisCommand>,
    ) -> Result<BytesMut, SableError> {
        let builder = RespBuilderV2::default();
        let mut response_buffer = BytesMut::new();

        builder.null_string(&mut response_buffer);
        Ok(response_buffer)
    }

    /// Can this client handle the command?
    /// We use this function as a sanity check for various checks, for example:
    /// A write command being called on a replica server
    fn pre_handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
    ) -> PreHandleCommandResult {
        if !client_state.active() {
            PreHandleCommandResult::ClientKilled
        } else if client_state.server_state.is_replica() && command.metadata().is_write_command() {
            PreHandleCommandResult::WriteInReadOnlyReplica
        } else if client_state.is_txn_state_multi() {
            if command.metadata().name().eq(&RedisCommandName::Exec) {
                // we are in MULTI state and received an EXEC command
                // allow it to continue
                return PreHandleCommandResult::Continue;
            } else {
                // Running within a "MULTI" block
                if command.metadata().is_notxn() {
                    PreHandleCommandResult::CmdIsNotValidForTxn
                } else {
                    PreHandleCommandResult::QueueCommand
                }
            }
        } else {
            PreHandleCommandResult::Continue
        }
    }

    /// Accepts the parsed requests, execute the command and send back the response
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<ClientNextAction, SableError> {
        {
            // In principal, can we handle this command?
            let mut resp_writer = RespWriter::new(tx, 128, client_state.clone());
            match Self::pre_handle_command(client_state.clone(), command.clone()) {
                PreHandleCommandResult::WriteInReadOnlyReplica => {
                    resp_writer
                        .error_string(Strings::WRITE_CMD_AGAINST_REPLICA)
                        .await?;
                    resp_writer.flush().await?;
                    return Ok(ClientNextAction::NoAction);
                }
                PreHandleCommandResult::ClientKilled => {
                    resp_writer
                        .error_string(Strings::SERVER_CLOSED_CONNECTION)
                        .await?;
                    resp_writer.flush().await?;
                    return Ok(ClientNextAction::TerminateConnection);
                }
                PreHandleCommandResult::CmdIsNotValidForTxn => {
                    resp_writer
                        .error_string(&format!(
                            "command {} can not be used in a MULTI / EXEC block",
                            command.main_command()
                        ))
                        .await?;
                    resp_writer.flush().await?;
                    return Ok(ClientNextAction::NoAction);
                }
                PreHandleCommandResult::QueueCommand => {
                    // queue the command and reply with "QUEUED"
                    client_state.queued_commands.push(command);
                    resp_writer.status_string(Strings::QUEUED).await?;
                    resp_writer.flush().await?;
                    return Ok(ClientNextAction::NoAction);
                }
                PreHandleCommandResult::Continue => {
                    // fall through
                }
            }
        }

        // We break the match here into 2: EXEC and all other non EXEC commands
        // we do this in order to be able to process these commands while in
        // the `TransactionCommands::handle_command`. Rust async does not allow us to
        // recursively call `Client::handle_command()`from within `TransactionCommands::handle_command()`
        // as this will cause some compilation errors
        let kind = command.metadata().name();
        match kind {
            RedisCommandName::Exec => {
                match TransactionCommands::handle_exec(client_state.clone(), command, tx).await? {
                    HandleCommandResult::Blocked(_) => Err(SableError::ClientInvalidState),
                    HandleCommandResult::ResponseSent => Ok(ClientNextAction::NoAction),
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                        Ok(ClientNextAction::NoAction)
                    }
                }
            }
            _ => Self::handle_non_exec_command(client_state.clone(), command, tx).await,
        }
    }

    /// Handle all commands, execpt for `Exec`
    pub async fn handle_non_exec_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<ClientNextAction, SableError> {
        let builder = RespBuilderV2::default();
        let kind = command.metadata().name();
        let client_action = match kind {
            RedisCommandName::Ping => {
                tx.write_all(PONG).await?;
                Telemetry::inc_net_bytes_written(PONG.len() as u128);
                ClientNextAction::NoAction
            }
            RedisCommandName::Set
            | RedisCommandName::Append
            | RedisCommandName::Get
            | RedisCommandName::Incr
            | RedisCommandName::Decr
            | RedisCommandName::DecrBy
            | RedisCommandName::IncrBy
            | RedisCommandName::IncrByFloat
            | RedisCommandName::GetDel
            | RedisCommandName::GetEx
            | RedisCommandName::GetRange
            | RedisCommandName::Lcs
            | RedisCommandName::GetSet
            | RedisCommandName::Mget
            | RedisCommandName::Mset
            | RedisCommandName::Msetnx
            | RedisCommandName::Psetex
            | RedisCommandName::Setex
            | RedisCommandName::Setnx
            | RedisCommandName::SetRange
            | RedisCommandName::Strlen
            | RedisCommandName::Substr => {
                match StringCommands::handle_command(client_state.clone(), command.clone(), tx)
                    .await?
                {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?
                    }
                    HandleCommandResult::ResponseSent => {}
                    HandleCommandResult::Blocked(_) => {
                        return Err(SableError::OtherError(
                            "Inernal error: client is in invalid state".to_string(),
                        ));
                    }
                }
                ClientNextAction::NoAction
            }
            RedisCommandName::Ttl
            | RedisCommandName::Del
            | RedisCommandName::Exists
            | RedisCommandName::Expire => {
                match GenericCommands::handle_command(client_state.clone(), command.clone(), tx)
                    .await?
                {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?
                    }
                    HandleCommandResult::ResponseSent => {}
                    HandleCommandResult::Blocked(_) => {
                        return Err(SableError::OtherError(
                            "Inernal error: client is in invalid state".to_string(),
                        ));
                    }
                }
                ClientNextAction::NoAction
            }
            RedisCommandName::Config => {
                let mut buffer = BytesMut::with_capacity(32);
                builder.ok(&mut buffer);
                Self::send_response(tx, &buffer, client_state.client_id).await?;
                ClientNextAction::NoAction
            }
            RedisCommandName::Info => {
                let mut buffer = BytesMut::with_capacity(1024);
                // build the stats
                let stats = client_state
                    .server_state
                    .shared_telemetry()
                    .lock()
                    .expect("mutex")
                    .to_string();
                builder.bulk_string(&mut buffer, &BytesMut::from(stats.as_bytes()));
                Self::send_response(tx, &buffer, client_state.client_id).await?;
                ClientNextAction::NoAction
            }
            // List commands
            RedisCommandName::Lpush
            | RedisCommandName::Lpushx
            | RedisCommandName::Rpush
            | RedisCommandName::Rpushx
            | RedisCommandName::Lpop
            | RedisCommandName::Rpop
            | RedisCommandName::Llen
            | RedisCommandName::Lindex
            | RedisCommandName::Linsert
            | RedisCommandName::Lset
            | RedisCommandName::Lpos
            | RedisCommandName::Ltrim
            | RedisCommandName::Lrange
            | RedisCommandName::Lrem
            | RedisCommandName::Lmove
            | RedisCommandName::Lmpop
            | RedisCommandName::Rpoplpush
            | RedisCommandName::Brpoplpush
            | RedisCommandName::Blmove
            | RedisCommandName::Blpop
            | RedisCommandName::Brpop
            | RedisCommandName::Blmpop => {
                match ListCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                        ClientNextAction::NoAction
                    }
                    HandleCommandResult::Blocked((rx, duration)) => {
                        ClientNextAction::Wait((rx, duration))
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                }
            }
            // Client commands
            RedisCommandName::Client | RedisCommandName::Select => {
                match ClientCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                    }
                    HandleCommandResult::Blocked((_rx, _duration)) => {}
                    HandleCommandResult::ResponseSent => {}
                }
                ClientNextAction::NoAction
            }
            RedisCommandName::ReplicaOf | RedisCommandName::SlaveOf | RedisCommandName::Command => {
                match ServerCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                    }
                    HandleCommandResult::Blocked((_rx, _duration)) => {}
                    HandleCommandResult::ResponseSent => {}
                }
                ClientNextAction::NoAction
            }
            // Hash commands
            RedisCommandName::Hset
            | RedisCommandName::Hget
            | RedisCommandName::Hdel
            | RedisCommandName::Hlen
            | RedisCommandName::Hexists
            | RedisCommandName::Hgetall
            | RedisCommandName::Hincrbyfloat
            | RedisCommandName::Hincrby
            | RedisCommandName::Hkeys
            | RedisCommandName::Hvals
            | RedisCommandName::Hmget
            | RedisCommandName::Hmset
            | RedisCommandName::Hrandfield
            | RedisCommandName::Hscan
            | RedisCommandName::Hsetnx
            | RedisCommandName::Hstrlen => {
                match HashCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::Blocked(_) => {
                        return Err(SableError::OtherError(
                            "Inernal error: client is in invalid state".to_string(),
                        ));
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                        ClientNextAction::NoAction
                    }
                }
            }
            RedisCommandName::Exec => {
                // Well, this is unexpected. We shouldn't reach this pattern matching block
                // with `Exec`... (it is handled earlier in the Client::handle_command)
                return Err(SableError::OtherError(
                    "Inernal error: client is in invalid state".to_string(),
                ));
            }
            RedisCommandName::Multi => {
                match TransactionCommands::handle_multi(client_state.clone(), command, tx).await? {
                    HandleCommandResult::Blocked(_) => {
                        return Err(SableError::OtherError(
                            "Inernal error: client is in invalid state".to_string(),
                        ));
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.client_id).await?;
                        ClientNextAction::NoAction
                    }
                }
            }
            // Misc
            RedisCommandName::NotSupported(msg) => {
                tracing::info!(msg);
                let mut buffer = BytesMut::with_capacity(128);
                builder.error_string(&mut buffer, msg.as_str());
                Self::send_response(tx, &buffer, client_state.client_id).await?;
                ClientNextAction::NoAction
            }
        };
        Ok(client_action)
    }

    /// Write buffer to `tx`, upon success, update the telemetry
    async fn send_response(
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
        buffer: &BytesMut,
        client_id: u128,
    ) -> Result<(), SableError> {
        tx.write_all(buffer).await?;
        if log_enabled!(Level::Debug) {
            ClientState::static_debug(
                client_id,
                &format!("<=== Len: {}, Buff: {:?}", buffer.len(), buffer),
            );
        }
        Telemetry::inc_net_bytes_written(buffer.len() as u128);
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        Telemetry::inc_connections_closed();
        // remove this client from this worker's list
        WORKER_CLIENTS.with(|clients| {
            let _ = clients.borrow_mut().remove(&self.state.client_id);
        });
    }
}
