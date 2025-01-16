use std::rc::Rc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SableError {
    /// From system IO error
    #[error("I/O error. {0}")]
    StdIoError(#[from] std::io::Error),
    #[error("Mutex poison error. {0}")]
    StorageError(#[from] sled::Error),
    /// From tokio channel error
    #[error("Tokio channel error. {0}")]
    WorkerChannel(#[from] tokio::sync::mpsc::error::SendError<crate::ValkeyCommand>),
    /// From tokio channel error
    #[error("Tokio channel error. {0}")]
    SendCommanError(#[from] tokio::sync::mpsc::error::SendError<Rc<crate::ValkeyCommand>>),
    #[error("Failed to broadcast worker message. {0}")]
    BroadcastWorkerMessage(
        #[from] tokio::sync::mpsc::error::SendError<crate::server::WorkerMessage>,
    ),
    #[error("Parse error. {0}")]
    Parser(#[from] ParserError),
    #[error("Empty command")]
    EmptyCommand,
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Broken pipe")]
    BrokenPipe,
    #[error("Not found")]
    NotFound,
    #[error("RocksDB error. {0}")]
    RocksDbError(#[from] rocksdb::Error),
    #[error("Error. {0}")]
    OtherError(String),
    #[error("ProtocolError. {0}")]
    ProtocolError(String),
    #[error("Invalid argument error. {0}")]
    InvalidArgument(String),
    #[error("Invalid state. {0}")]
    InvalidState(String),
    #[error("Already exists")]
    AlreadyExists,
    #[error("Tokio channel error. {0}")]
    ChannelSendError(#[from] tokio::sync::mpsc::error::SendError<u8>),
    #[error("Not implemented error. {0}")]
    NotImplemented(String),
    #[error("INI configuration error. {0}")]
    ConfigError(#[from] ini::Error),
    #[error("Failed parsing address. {0}")]
    AddressParseError(#[from] std::net::AddrParseError),
    #[error("Serialisation error")]
    SerialisationError,
    #[error("No active transaction")]
    NoActiveTransaction,
    /// When the client state is in "Preparing Txn", we cancel the lock
    /// and return the requested slots instead
    #[error("Lock cancelled. Client in preparing transaction state")]
    LockCancelledTxnPrep(Vec<u16>),
    #[error("Client is in invalid state")]
    ClientInvalidState,
    #[error("Corrupted database. {0}")]
    Corrupted(String),
    #[error("ValkeyError. {0}")]
    ValkeyError(#[from] redis::RedisError),
    #[error("Parsing error. {0}")]
    ParseError(String),
    #[error("Internal error. {0}")]
    InternalError(String),
    /// Failover related error
    #[error("AutoFailOverError error. {0}")]
    AutoFailOverError(String),
    #[error("ClusterDB error. {0}")]
    ClsuterDbError(String),
    #[error("Index out of rage. {0}")]
    IndexOutOfRange(String),
    #[error("Some or all the slots in the command are not owned by this node")]
    NotOwner(Vec<u16>),
}

#[allow(dead_code)]
impl SableError {
    /// Is this parser error, equals `other` ?
    pub fn eq_parser_error(&self, other: &ParserError) -> bool {
        match self {
            SableError::Parser(e) => e == other,
            _ => false,
        }
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ParserError {
    #[error("Need more to data to complete operation")]
    NeedMoreData,
    #[error("Protocol error. `{0}`")]
    ProtocolError(String),
    #[error("Input too big")]
    BufferTooBig,
    #[error("Overflow occurred")]
    Overflow,
    #[error("Invalid input. {0}")]
    InvalidInput(String),
}
