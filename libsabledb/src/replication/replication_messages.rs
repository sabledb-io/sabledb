use crate::{U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// Message types
const GET_UPDATES_SINCE: u8 = 0;
const FULL_SYNC: u8 = 1;
const ACK: u8 = 2; // Acknowledged
const NACK: u8 = 3; // Negative Acknowledged

/// NotOk reason
const NOTOK_REASON_UNKNOWN: u8 = 0;
const NOTOK_REASON_FULL_SYNC_NOT_DONE: u8 = 1;
const NOTOK_REASON_UPDATES_SINCE_CREATE_ERR: u8 = 2;

#[derive(Debug, Clone)]
pub struct RequestCommon {
    /// The request unique ID. The `req_id` is auto generated when constructing a new `RequestCommon` struct
    req_id: u64,
}

impl std::fmt::Display for RequestCommon {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RequestId: {}", self.req_id)
    }
}

impl RequestCommon {
    pub fn with_request_id(req_id: &mut u64) -> Self {
        let id = *req_id;
        *req_id = req_id.saturating_add(1);
        RequestCommon { req_id: id }
    }

    pub fn request_id(&self) -> u64 {
        self.req_id
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Option<Self> {
        let req_id = reader.read_u64()?;
        Some(RequestCommon { req_id })
    }

    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u64(self.req_id);
    }
}

#[derive(Debug, Clone)]
pub struct ResponseCommon {
    /// The request ID that generated this response
    req_id: u64,
    /// Contains the reason for Nack response
    reason: ResponseReason,
}

impl ResponseCommon {
    pub fn new(req_id: u64) -> Self {
        ResponseCommon {
            req_id,
            reason: ResponseReason::Invalid,
        }
    }

    pub fn with_reason(mut self, reason: ResponseReason) -> Self {
        self.reason = reason;
        self
    }

    pub fn reason(&self) -> &ResponseReason {
        &self.reason
    }

    pub fn request_id(&self) -> u64 {
        self.req_id
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Option<Self> {
        let req_id = reader.read_u64()?;
        let reason = ResponseReason::from_u8(reader.read_u8()?)?;
        Some(ResponseCommon { req_id, reason })
    }

    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u64(self.req_id);
        builder.write_u8(self.reason.to_u8());
    }
}

impl std::fmt::Display for ResponseCommon {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RequestId: {}, Reason: {}", self.req_id, self.reason)
    }
}

/// represents a replication request sent from the secondary -> primary
#[derive(Debug, Clone)]
pub enum ReplicationRequest {
    GetUpdatesSince((RequestCommon, u64)),
    FullSync(RequestCommon),
}

#[derive(Debug, Clone)]
pub enum ReplicationResponse {
    Ok(ResponseCommon),
    NotOk(ResponseCommon),
}

#[derive(Debug, Clone)]
pub enum ResponseReason {
    Invalid,
    /// Replication requested for "changes" without doing fullsync first
    NoFullSyncDone,
    /// Failed to construct an "Updates Since" changes block
    CreatingUpdatesSinceError,
}

impl Default for ResponseReason {
    fn default() -> Self {
        Self::Invalid
    }
}

impl std::fmt::Display for ResponseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Invalid => write!(f, "Invalid"),
            Self::NoFullSyncDone => write!(f, "NoFullSyncDone"),
            Self::CreatingUpdatesSinceError => write!(f, "CreatingUpdatesSinceError"),
        }
    }
}

impl ResponseReason {
    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Invalid => NOTOK_REASON_UNKNOWN,
            Self::NoFullSyncDone => NOTOK_REASON_FULL_SYNC_NOT_DONE,
            Self::CreatingUpdatesSinceError => NOTOK_REASON_UPDATES_SINCE_CREATE_ERR,
        }
    }

    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            NOTOK_REASON_UNKNOWN => Some(ResponseReason::Invalid),
            NOTOK_REASON_FULL_SYNC_NOT_DONE => Some(ResponseReason::NoFullSyncDone),
            NOTOK_REASON_UPDATES_SINCE_CREATE_ERR => {
                Some(ResponseReason::CreatingUpdatesSinceError)
            }
            _ => None,
        }
    }
}

impl ReplicationRequest {
    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut as_bytes);
        match self {
            Self::GetUpdatesSince((common, seq)) => {
                // the message type goes first
                builder.write_u8(GET_UPDATES_SINCE);
                common.to_bytes(&mut builder);
                builder.write_u64(*seq);
            }
            Self::FullSync(common) => {
                // message type
                builder.write_u8(FULL_SYNC);
                common.to_bytes(&mut builder);
            }
        }
        as_bytes
    }

    /// Construct `ReplicationRequest` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = reader.read_u8()?;
        match req_type {
            GET_UPDATES_SINCE => {
                let common = RequestCommon::from_bytes(&mut reader)?;
                let seq = reader.read_u64()?;
                Some(ReplicationRequest::GetUpdatesSince((common, seq)))
            }
            FULL_SYNC => {
                let common = RequestCommon::from_bytes(&mut reader)?;
                Some(ReplicationRequest::FullSync(common))
            }
            _ => {
                tracing::error!(
                    "Replication protocol error: read unknown replication request of type '{}'",
                    req_type
                );
                None
            }
        }
    }
}

impl std::fmt::Display for ReplicationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::FullSync(common) => write!(f, "FullSync({})", common),
            Self::GetUpdatesSince((common, seq)) => {
                write!(f, "GetUpdatesSince({}, {})", common, seq)
            }
        }
    }
}

impl ReplicationResponse {
    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut as_bytes);
        match self {
            Self::Ok(common) => {
                // the message type goes first
                builder.write_u8(ACK);
                common.to_bytes(&mut builder);
            }
            Self::NotOk(common) => {
                // message type
                builder.write_u8(NACK);
                common.to_bytes(&mut builder);
            }
        }
        as_bytes
    }

    /// Construct `ReplicationResponse` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = reader.read_u8()?;
        let common = ResponseCommon::from_bytes(&mut reader)?;
        match req_type {
            ACK => Some(ReplicationResponse::Ok(common)),
            NACK => Some(ReplicationResponse::NotOk(common)),
            _ => {
                tracing::error!(
                    "Replication protocol error: read unknown replication response of type '{}'",
                    req_type
                );
                None
            }
        }
    }
}

impl std::fmt::Display for ReplicationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Ok(common) => write!(f, "Ok({})", common),
            Self::NotOk(common) => write!(f, "NotOk({})", common),
        }
    }
}
