use crate::{U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// represents a replication request sent from the secondary -> primary
#[derive(Debug, Clone)]
pub struct ReplicationMessage {
    /// The message type
    pub message_type: u8,
    /// Depending on the message type, this field as a different meaning
    /// - If `req_type == ReplRequest::GET_UPDATES_SINCE`, `payload` is the
    ///     starting sequence number for changes to be sent over to the replica
    pub payload: u64,
}

impl Default for ReplicationMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationMessage {
    pub const SIZE: usize = std::mem::size_of::<ReplicationMessage>();
    pub const GET_UPDATES_SINCE: u8 = 0;
    pub const FULL_SYNC: u8 = 1;
    pub const GET_CHANGES_OK: u8 = 2; // No payload
    pub const GET_CHANGES_ERR: u8 = 3; // No payload

    /// Create a default message with type `GET_UPDATES_SINCE`
    pub fn new() -> Self {
        ReplicationMessage {
            message_type: Self::GET_UPDATES_SINCE,
            payload: 0,
        }
    }

    pub fn with_sequence(mut self, seq_num: u64) -> Self {
        self.payload = seq_num;
        self
    }

    pub fn with_type(mut self, msg_type: u8) -> Self {
        self.message_type = msg_type;
        self
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::with_capacity(ReplicationMessage::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut as_bytes);
        builder.write_u8(self.message_type);
        builder.write_u64(self.payload);
        as_bytes
    }

    /// Construct `ReplRequest` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = reader.read_u8()?;
        let payload = reader.read_u64()?;

        Some(ReplicationMessage {
            message_type: req_type,
            payload,
        })
    }
}
