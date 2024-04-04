use crate::{
    commands::{ClientNextAction, ErrorStrings, HandleCommandResult},
    ClientCommands, GenericCommands, ListCommands, ParserError, RedisCommand, RedisCommandName,
    RequestParser, RespBuilderV2, SableError, ServerCommands, ServerState, StorageAdapter,
    StringCommands, Telemetry,
};

use bytes::BytesMut;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, AtomicU16},
    Arc, Mutex, RwLock,
};

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

fn new_client_id() -> u128 {
    let mut value = CLIENT_ID_GENERATOR.lock().expect("poisoned mutex");
    *value += 1;
    *value
}

#[derive(Clone)]
pub struct ClientState {
    pub server_state: Arc<ServerState>,
    pub store: StorageAdapter,
    pub client_id: u128,
    pub tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    db_id: Rc<AtomicU16>,
    attributes: Rc<RwLock<HashMap<String, String>>>,
    is_active: Rc<AtomicBool>,
}

#[derive(PartialEq, PartialOrd)]
enum CanHandleCommandResult {
    Ok,
    WriteInReadOnlyReplica,
}

/// Used by the `block_until` return code
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitResult {
    TryAgain,
    Timeout,
}

impl ClientState {
    /// Return the client's database ID
    pub fn database_id(&self) -> u16 {
        self.db_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the active database ID for this client
    pub fn set_database_id(&self, id: u16) {
        self.db_id.store(id, std::sync::atomic::Ordering::Relaxed);
    }

    /// Return the client's database ID
    pub fn active(&self) -> bool {
        self.is_active.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Kill the current client by marking it as non active. The connection will be closed
    /// next time the client will attempt to use it or when a timeout occurs
    pub fn kill(&self) {
        self.is_active
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set a client attribute
    pub fn set_attribute(&self, name: &str, value: &str) {
        self.attributes
            .write()
            .expect("poisoned mutex")
            .insert(name.to_owned(), value.to_owned());
    }

    /// Get a client attribute
    pub fn attribute(&self, name: &String) -> Option<String> {
        self.attributes
            .read()
            .expect("poisoned mutex")
            .get(name)
            .cloned()
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
}

pub struct Client {
    state: ClientState,
}

impl Client {
    pub fn inner(&self) -> &ClientState {
        &self.state
    }

    pub fn new(
        server_state: Arc<ServerState>,
        store: StorageAdapter,
        tls_acceptor: Option<Rc<tokio_rustls::TlsAcceptor>>,
    ) -> Self {
        Telemetry::inc_connections_opened();
        Client {
            state: ClientState {
                server_state,
                store,
                client_id: new_client_id(),
                tls_acceptor,
                db_id: Rc::new(AtomicU16::new(0)),
                attributes: Rc::new(RwLock::new(HashMap::<String, String>::new())),
                is_active: Rc::new(AtomicBool::new(true)),
            },
        }
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
        channel_tx: tokio::sync::mpsc::Sender<RedisCommand>,
        client_state: ClientState,
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
        mut channel_rx: TokioReceiver<RedisCommand>,
        client_state: ClientState,
    ) -> Result<(), SableError> {
        while let Some(command) = channel_rx.recv().await {
            // update telemetry and process the command
            Telemetry::inc_total_commands_processed();

            // Use a loop here to handle timeouts & retries
            loop {
                let response = Self::handle_command(&client_state, command.clone()).await;
                match response {
                    Ok(next_action) => match next_action {
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
                                        client_state.debug("timeout occured");
                                    }
                                    // time-out occurred, build a proper response message and break out the inner loop
                                    let response_buffer =
                                        Self::handle_timeout(&client_state, command.clone())?;
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
                    },
                    Err(e) => {
                        client_state.error(&format!(
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
        _client_state: &ClientState,
        _command: RedisCommand,
    ) -> Result<BytesMut, SableError> {
        let builder = RespBuilderV2::default();
        let mut response_buffer = BytesMut::new();

        builder.null_string(&mut response_buffer);
        Ok(response_buffer)
    }

    /// Can this client handle the command?
    /// We use this function as a sanity check for various checks, for example:
    /// A write command being called on a replica server
    fn can_handle(client_state: &ClientState, command: &RedisCommand) -> CanHandleCommandResult {
        if client_state.server_state.is_replica() && command.metadata().is_write_command() {
            CanHandleCommandResult::WriteInReadOnlyReplica
        } else {
            CanHandleCommandResult::Ok
        }
    }

    /// Accepts the parsed requests, execute the command and send back the response
    pub async fn handle_command(
        client_state: &ClientState,
        command: RedisCommand,
    ) -> Result<ClientNextAction, SableError> {
        let builder = RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(1024);

        // Can we handle this command?
        if Self::can_handle(client_state, &command)
            == CanHandleCommandResult::WriteInReadOnlyReplica
        {
            builder.error_string(&mut buffer, ErrorStrings::WRITE_CMD_AGAINST_REPLICA);
            return Ok(ClientNextAction::SendResponse(buffer));
        }

        let kind = command.metadata().name();
        let client_action = match kind {
            RedisCommandName::Ping => {
                builder.pong(&mut buffer);
                ClientNextAction::SendResponse(buffer)
            }
            RedisCommandName::Cmd => {
                builder.empty_array(&mut buffer);
                ClientNextAction::SendResponse(buffer)
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
                StringCommands::handle_command(client_state, &command, &mut buffer).await?;
                ClientNextAction::SendResponse(buffer)
            }
            RedisCommandName::Ttl | RedisCommandName::Del => {
                GenericCommands::handle_command(client_state, &command, &mut buffer).await?;
                ClientNextAction::SendResponse(buffer)
            }
            RedisCommandName::Config => {
                builder.ok(&mut buffer);
                ClientNextAction::SendResponse(buffer)
            }
            RedisCommandName::Info => {
                // build the stats
                let stats = client_state
                    .server_state
                    .shared_telemetry()
                    .lock()
                    .expect("mutex")
                    .to_string();
                builder.bulk_string(&mut buffer, &BytesMut::from(stats.as_bytes()));
                ClientNextAction::SendResponse(buffer)
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
                match ListCommands::handle_command(client_state, &command, &mut buffer).await? {
                    HandleCommandResult::Completed => ClientNextAction::SendResponse(buffer),
                    HandleCommandResult::Blocked((rx, duration)) => {
                        ClientNextAction::Wait((rx, duration))
                    }
                }
            }
            // Client commands
            RedisCommandName::Client | RedisCommandName::Select => {
                ClientCommands::handle_command(client_state, &command, &mut buffer)?;
                ClientNextAction::SendResponse(buffer)
            }
            RedisCommandName::ReplicaOf | RedisCommandName::SlaveOf => {
                ServerCommands::handle_command(client_state, &command, &mut buffer).await?;
                ClientNextAction::SendResponse(buffer)
            }
            // Misc
            RedisCommandName::NotSupported(msg) => {
                tracing::info!(msg);
                builder.error_string(&mut buffer, msg.as_str());
                ClientNextAction::SendResponse(buffer)
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
    }
}
