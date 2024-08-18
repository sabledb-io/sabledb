use crate::{U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// represents a replication request sent from the secondary -> primary
#[derive(Debug, Clone)]
pub enum ReplicationMessage {
    GetUpdatesSince(u64),
    FullSync,
    GetChangesOk,
    GetChangesErr(BytesMut),
}

impl Default for ReplicationMessage {
    fn default() -> Self {
        Self::GetUpdatesSince(0)
    }
}

impl ReplicationMessage {
    const GET_UPDATES_SINCE: u8 = 0;
    const FULL_SYNC: u8 = 1;
    const GET_CHANGES_OK: u8 = 2;
    const GET_CHANGES_ERR: u8 = 3;

    /// Create a default message with type `GET_UPDATES_SINCE`
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut as_bytes);
        match *self {
            Self::GetUpdatesSince(seq) => {
                builder.write_u8(Self::GET_UPDATES_SINCE);
                builder.write_u64(seq);
            }
            Self::FullSync => {
                builder.write_u8(Self::FULL_SYNC);
            }
            Self::GetChangesOk => {
                builder.write_u8(Self::GET_CHANGES_OK);
            }
            Self::GetChangesErr(ref msg) => {
                builder.write_u8(Self::GET_CHANGES_ERR);
                builder.write_usize(msg.len());
                builder.write_bytes(msg);
            }
        }
        as_bytes
    }

    /// Construct `ReplRequest` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = reader.read_u8()?;
        match req_type {
            Self::GET_UPDATES_SINCE => {
                let seq = reader.read_u64()?;
                Some(ReplicationMessage::GetUpdatesSince(seq))
            }
            Self::GET_CHANGES_OK => Some(ReplicationMessage::GetChangesOk),
            Self::GET_CHANGES_ERR => {
                let msg = reader.read_message().unwrap_or_default();
                Some(ReplicationMessage::GetChangesErr(msg))
            }
            Self::FULL_SYNC => Some(ReplicationMessage::FullSync),
            _ => {
                tracing::error!(
                    "Replication protocol error: read unknown message {}",
                    req_type
                );
                None
            }
        }
    }
}
