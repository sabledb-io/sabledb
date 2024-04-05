use crate::{U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// represents a replication request sent from the secondary -> primary
#[derive(Debug, Clone, Default)]
pub struct ReplRequest {
    /// Request type
    pub req_type: u8,
    /// Depending on the message type, this field as a different meaning
    /// - If `req_type == ReplRequest::GET_UPDATES_SINCE`, `payload` is the
    ///     starting sequence number for changes to be sent over to the replica
    pub payload: u64,
}

impl ReplRequest {
    pub const SIZE: usize = std::mem::size_of::<u64>() + std::mem::size_of::<u8>();
    pub const GET_UPDATES_SINCE: u8 = 0;
    pub const FULL_SYNC: u8 = 1;

    pub fn new_get_updates_since(seq_num: u64) -> Self {
        ReplRequest {
            req_type: ReplRequest::GET_UPDATES_SINCE,
            payload: seq_num,
        }
    }

    pub fn new_fullsync() -> Self {
        ReplRequest {
            req_type: ReplRequest::FULL_SYNC,
            payload: 0,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::with_capacity(ReplRequest::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut as_bytes);
        builder.write_u8(self.req_type);
        builder.write_u64(self.payload);
        as_bytes
    }

    /// Construct `ReplRequest` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = reader.read_u8()?;
        let payload = reader.read_u64()?;

        Some(ReplRequest { req_type, payload })
    }
}
