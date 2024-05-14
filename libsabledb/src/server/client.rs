use crate::{
    commands::{ClientNextAction, HandleCommandResult, Strings, TimeoutResponse},
    io::RespWriter,
    server::{ClientState, Telemetry},
    utils::RequestParser,
    utils::RespBuilderV2,
    ClientCommands, GenericCommands, HashCommands, ListCommands, ParserError, RedisCommand,
    RedisCommandName, SableError, ServerCommands, ServerState, StorageAdapter, StringCommands,
    TransactionCommands, ZSetCommands,
};

use bytes::BytesMut;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;

const PONG: &[u8] = b"+PONG\r\n";

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
pub fn new_client_id() -> u128 {
    let mut value = CLIENT_ID_GENERATOR.lock().expect("poisoned mutex");
    *value += 1;
    *value
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

pub struct Client {
    state: Rc<ClientState>,
}

impl Client {
    /// Terminate a client by its ID (`client_id`). Return `true` if the client was successfully marked
    /// as terminated
    pub fn terminate_client(client_id: u128) -> bool {
        WORKER_CLIENTS.with(|clients| {
            if let Some(client_state) = clients.borrow().get(&client_id) {
                tracing::debug!("Client {} terminated", client_id);
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
            clients.borrow_mut().insert(state_clone.id(), state_clone);
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

        let (r, w) = if self.state.server_inner_state().options().use_tls() {
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
        let reader_abort_handle = r.abort_handle();
        let writer_abort_handle = w.abort_handle();
        tokio::select! {
            _ = r => {
                // terminate the writer task
                writer_abort_handle.abort();
                Err(SableError::StdIoError(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "reader task ended prematurely. closing connection",
                )))
            },
            _ = w => {
                // terminate the reader task
                reader_abort_handle.abort();
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
                            client_state.debug("Connection closed (by peer)");
                            Client::terminate_client(client_state.id());
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
                            Self::send_response(&mut tx, &response, client_state.id()).await?;
                            break;
                        }
                        ClientNextAction::Wait((rx, duration, timeout_response)) => {
                            // suspend the client for the specified duration or until a wakeup bit arrives
                            match Self::wait_for(rx, duration).await {
                                WaitResult::Timeout => {
                                    if log_enabled!(Level::Debug) {
                                        client_state.debug("timeout occurred");
                                    }
                                    if !client_state.active() {
                                        // Client is no longer active
                                        tracing::debug!("Client terminated while waiting");
                                        return Err(SableError::ConnectionClosed);
                                    }
                                    // time-out occurred, build a proper response message and break out the inner loop
                                    let response_buffer = Self::handle_timeout(
                                        client_state.clone(),
                                        command.clone(),
                                        timeout_response,
                                    )?;
                                    Self::send_response(
                                        &mut tx,
                                        &response_buffer,
                                        client_state.id(),
                                    )
                                    .await?;
                                    break;
                                }
                                WaitResult::TryAgain => {
                                    if !client_state.active() {
                                        // Client is no longer active
                                        tracing::debug!("Client terminated while waiting");
                                        return Err(SableError::ConnectionClosed);
                                    }
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
        timeout_response: TimeoutResponse,
    ) -> Result<BytesMut, SableError> {
        let builder = RespBuilderV2::default();
        let mut response_buffer = BytesMut::new();

        match timeout_response {
            TimeoutResponse::NullString => {
                builder.null_string(&mut response_buffer);
            }
            TimeoutResponse::NullArrray => {
                builder.null_array(&mut response_buffer);
            }
            TimeoutResponse::Number(num) => {
                builder.number_i64(&mut response_buffer, num);
            }
        }
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
        } else if client_state.server_inner_state().is_replica()
            && command.metadata().is_write_command()
        {
            PreHandleCommandResult::WriteInReadOnlyReplica
        } else if client_state.is_txn_state_multi() {
            // All commands by "exec" and "discard" are queued
            match command.metadata().name() {
                RedisCommandName::Exec | RedisCommandName::Discard => {
                    PreHandleCommandResult::Continue
                }
                _ => {
                    if command.metadata().is_notxn() {
                        PreHandleCommandResult::CmdIsNotValidForTxn
                    } else {
                        PreHandleCommandResult::QueueCommand
                    }
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
                            "ERR command {} can not be used in a MULTI / EXEC block",
                            command.main_command()
                        ))
                        .await?;
                    resp_writer.flush().await?;
                    return Ok(ClientNextAction::NoAction);
                }
                PreHandleCommandResult::QueueCommand => {
                    // queue the command and reply with "QUEUED"
                    client_state.add_txn_command(command);
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
                        Self::send_response(tx, &buffer, client_state.id()).await?;
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
                        Self::send_response(tx, &buffer, client_state.id()).await?
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
                        Self::send_response(tx, &buffer, client_state.id()).await?
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
                Self::send_response(tx, &buffer, client_state.id()).await?;
                ClientNextAction::NoAction
            }
            RedisCommandName::Info => {
                let mut buffer = BytesMut::with_capacity(1024);
                // build the stats
                let stats = client_state
                    .server_inner_state()
                    .shared_telemetry()
                    .lock()
                    .expect("mutex")
                    .to_string();
                builder.bulk_string(&mut buffer, &BytesMut::from(stats.as_bytes()));
                Self::send_response(tx, &buffer, client_state.id()).await?;
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
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                        ClientNextAction::NoAction
                    }
                    HandleCommandResult::Blocked((rx, duration, timeout_response)) => {
                        ClientNextAction::Wait((rx, duration, timeout_response))
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                }
            }
            // Client commands
            RedisCommandName::Client | RedisCommandName::Select => {
                match ClientCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                    }
                    HandleCommandResult::Blocked((_rx, _duration, _timeout_response)) => {}
                    HandleCommandResult::ResponseSent => {}
                }
                ClientNextAction::NoAction
            }
            RedisCommandName::ReplicaOf | RedisCommandName::SlaveOf | RedisCommandName::Command => {
                match ServerCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                    }
                    HandleCommandResult::Blocked((_rx, _duration, _timeout_response)) => {}
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
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                        ClientNextAction::NoAction
                    }
                }
            }
            RedisCommandName::Exec => {
                // Well, this is unexpected. We shouldn't reach this pattern matching block
                // with `Exec`... (it is handled earlier in the Client::handle_command)
                return Err(SableError::ClientInvalidState);
            }
            RedisCommandName::Multi
            | RedisCommandName::Discard
            | RedisCommandName::Watch
            | RedisCommandName::Unwatch => {
                match TransactionCommands::handle_command(client_state.clone(), command, tx).await?
                {
                    HandleCommandResult::Blocked(_) => {
                        return Err(SableError::OtherError(
                            "Inernal error: client is in invalid state".to_string(),
                        ));
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                        ClientNextAction::NoAction
                    }
                }
            }
            RedisCommandName::Zadd
            | RedisCommandName::Zcard
            | RedisCommandName::Zincrby
            | RedisCommandName::Zrangebyscore
            | RedisCommandName::Zcount
            | RedisCommandName::Zdiff
            | RedisCommandName::Zdiffstore
            | RedisCommandName::Zinter
            | RedisCommandName::Zintercard
            | RedisCommandName::Zinterstore
            | RedisCommandName::Zlexcount
            | RedisCommandName::Zmpop
            | RedisCommandName::Bzmpop
            | RedisCommandName::Zmscore
            | RedisCommandName::Zpopmax
            | RedisCommandName::Zpopmin
            | RedisCommandName::Bzpopmax
            | RedisCommandName::Bzpopmin
            | RedisCommandName::Zrandmember => {
                match ZSetCommands::handle_command(client_state.clone(), command, tx).await? {
                    HandleCommandResult::Blocked((rx, duration, timeout_response)) => {
                        ClientNextAction::Wait((rx, duration, timeout_response))
                    }
                    HandleCommandResult::ResponseSent => ClientNextAction::NoAction,
                    HandleCommandResult::ResponseBufferUpdated(buffer) => {
                        Self::send_response(tx, &buffer, client_state.id()).await?;
                        ClientNextAction::NoAction
                    }
                }
            }
            // Misc
            RedisCommandName::NotSupported(msg) => {
                tracing::info!(msg);
                let mut buffer = BytesMut::with_capacity(128);
                builder.error_string(&mut buffer, msg.as_str());
                Self::send_response(tx, &buffer, client_state.id()).await?;
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

    /// Perform cleanup. This method is called just before the client goes out of scope (but before
    /// the `Drop`). Put here any `async` cleanup tasks required (which can't be done in `drop`)
    pub async fn cleanup(&self) {
        self.state
            .server_inner_state()
            .remove_blocked_client(&self.state.id())
            .await;
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        Telemetry::inc_connections_closed();
        // remove this client from this worker's list
        WORKER_CLIENTS.with(|clients| {
            let _ = clients.borrow_mut().remove(&self.state.id());
        });

        // drop any transcation related info for this client
        self.state.discard_transaction();
    }
}
