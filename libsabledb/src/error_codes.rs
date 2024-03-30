use thiserror::Error;

#[derive(Error, Debug)]
pub enum SableError {
    /// From system IO error
    #[error("I/O error. {0}")]
    StdIoError(#[from] std::io::Error),
    #[error("Sled error. {0}")]
    StorageError(#[from] sled::Error),
    /// From tokio channel error
    #[error("Tokio channel error. {0}")]
    WorkerChannel(#[from] tokio::sync::mpsc::error::SendError<crate::RedisCommand>),
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
    #[error("Invalid argument error. {0}")]
    InvalidArgument(String),
    #[error("Already exists")]
    AlreadyExists,
    #[error("Tokio channel error. {0}")]
    ChannelSendError(#[from] tokio::sync::mpsc::error::SendError<u8>),
    #[error("Feature {0} is not implemented")]
    NotImplemented(String),
    #[error("INI configuration error. {0}")]
    ConfigError(#[from] ini::Error),
    #[error("Failed parsing address. {0}")]
    AddressParseError(#[from] std::net::AddrParseError),
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
