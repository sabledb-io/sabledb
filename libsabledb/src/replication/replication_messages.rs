use crate::{FromU8Reader, NodeId, ToU8Writer, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// Message types (requests)
const GET_UPDATES_SINCE: u8 = 0;
const FULL_SYNC: u8 = 1;
const JOIN_SHARD: u8 = 2; // Request to join an existing shard

// Message types (response)
const ACK: u8 = 100; // Acknowledged
const NACK: u8 = 101; // Negative Acknowledged

/// NotOk reason
const NOTOK_REASON_UNKNOWN: u8 = 0;
const NOTOK_REASON_FULL_SYNC_NOT_DONE: u8 = 1;
const NOTOK_REASON_UPDATES_SINCE_CREATE_ERR: u8 = 2;
const NOTOK_REASON_NO_CHANGES: u8 = 3;

#[derive(Debug, Clone)]
pub struct RequestCommon {
    /// The request unique ID. The `req_id` is auto generated when constructing a new `RequestCommon` struct
    req_id: u64,

    /// The ID of the requesting node
    node_id: String,
}

impl std::fmt::Display for RequestCommon {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RequestId: {}", self.req_id)
    }
}

impl ToU8Writer for RequestCommon {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.req_id.to_writer(builder);
        self.node_id.to_writer(builder);
    }
}

impl FromU8Reader for RequestCommon {
    type Item = RequestCommon;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        Some(RequestCommon {
            req_id: u64::from_reader(reader)?,
            node_id: String::from_reader(reader)?,
        })
    }
}

impl RequestCommon {
    pub fn new() -> Self {
        RequestCommon {
            req_id: 0,
            node_id: NodeId::current(),
        }
    }

    pub fn with_request_id(mut self, req_id: &mut u64) -> Self {
        let id = *req_id;
        *req_id = req_id.saturating_add(1);
        self.req_id = id;
        self
    }

    pub fn request_id(&self) -> u64 {
        self.req_id
    }

    pub fn node_id(&self) -> &String {
        &self.node_id
    }
}

#[derive(Debug, Clone)]
pub struct ResponseCommon {
    /// The request ID that generated this response
    req_id: u64,
    /// Contains the reason for NACK response
    reason: ResponseReason,
    /// The responding node ID
    node_id: String,
}

impl ResponseCommon {
    pub fn new(request: &RequestCommon) -> Self {
        ResponseCommon {
            req_id: request.request_id(),
            node_id: NodeId::current(),
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

    pub fn node_id(&self) -> &String {
        &self.node_id
    }
}

impl Default for RequestCommon {
    fn default() -> Self {
        Self::new()
    }
}

impl FromU8Reader for ResponseCommon {
    type Item = ResponseCommon;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        let req_id = u64::from_reader(reader)?;
        let reason = ResponseReason::from_u8(u8::from_reader(reader)?)?;
        let node_id = String::from_reader(reader)?;
        Some(ResponseCommon {
            req_id,
            reason,
            node_id,
        })
    }
}

impl ToU8Writer for ResponseCommon {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.req_id.to_writer(builder);
        self.reason.to_u8().to_writer(builder);
        self.node_id.to_writer(builder);
    }
}

impl std::fmt::Display for ResponseCommon {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RequestId: {}, Reason: {}. NodeId: {}",
            self.req_id, self.reason, self.node_id
        )
    }
}

/// represents a replication request sent from the secondary -> primary
#[derive(Debug, Clone)]
pub enum ReplicationRequest {
    GetUpdatesSince((RequestCommon, u64)),
    FullSync(RequestCommon),
    JoinShard(RequestCommon),
}

#[derive(Debug, Clone)]
pub enum ReplicationResponse {
    Ok(ResponseCommon),
    NotOk(ResponseCommon),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseReason {
    Invalid,
    /// Replication requested for "changes" without doing fullsync first
    NoFullSyncDone,
    /// Failed to construct an "Updates Since" changes block
    CreatingUpdatesSinceError,
    /// No changes available
    NoChangesAvailable,
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
            Self::NoChangesAvailable => write!(f, "NoChangesAvailable"),
        }
    }
}

impl ResponseReason {
    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Invalid => NOTOK_REASON_UNKNOWN,
            Self::NoFullSyncDone => NOTOK_REASON_FULL_SYNC_NOT_DONE,
            Self::CreatingUpdatesSinceError => NOTOK_REASON_UPDATES_SINCE_CREATE_ERR,
            Self::NoChangesAvailable => NOTOK_REASON_NO_CHANGES,
        }
    }

    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            NOTOK_REASON_UNKNOWN => Some(ResponseReason::Invalid),
            NOTOK_REASON_FULL_SYNC_NOT_DONE => Some(ResponseReason::NoFullSyncDone),
            NOTOK_REASON_UPDATES_SINCE_CREATE_ERR => {
                Some(ResponseReason::CreatingUpdatesSinceError)
            }
            NOTOK_REASON_NO_CHANGES => Some(ResponseReason::NoChangesAvailable),
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
                GET_UPDATES_SINCE.to_writer(&mut builder);
                common.to_writer(&mut builder);
                seq.to_writer(&mut builder);
            }
            Self::FullSync(common) => {
                // message type
                FULL_SYNC.to_writer(&mut builder);
                common.to_writer(&mut builder);
            }
            Self::JoinShard(common) => {
                JOIN_SHARD.to_writer(&mut builder);
                common.to_writer(&mut builder);
            }
        }
        as_bytes
    }

    /// Construct `ReplicationRequest` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let req_type = u8::from_reader(&mut reader)?;
        match req_type {
            GET_UPDATES_SINCE => {
                let common = RequestCommon::from_reader(&mut reader)?;
                let seq = u64::from_reader(&mut reader)?;
                Some(ReplicationRequest::GetUpdatesSince((common, seq)))
            }
            FULL_SYNC => {
                let common = RequestCommon::from_reader(&mut reader)?;
                Some(ReplicationRequest::FullSync(common))
            }
            JOIN_SHARD => {
                let common = RequestCommon::from_reader(&mut reader)?;
                Some(ReplicationRequest::JoinShard(common))
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
            Self::JoinShard(common) => {
                write!(f, "JoinShard({})", common)
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
                ACK.to_writer(&mut builder);
                common.to_writer(&mut builder);
            }
            Self::NotOk(common) => {
                // message type
                NACK.to_writer(&mut builder);
                common.to_writer(&mut builder);
            }
        }
        as_bytes
    }

    /// Construct `ReplicationResponse` from raw bytes
    pub fn from_bytes(buf: &BytesMut) -> Option<Self> {
        let mut reader = U8ArrayReader::with_buffer(buf);

        let response_type = u8::from_reader(&mut reader)?;
        let common = ResponseCommon::from_reader(&mut reader)?;
        match response_type {
            ACK => Some(ReplicationResponse::Ok(common)),
            NACK => Some(ReplicationResponse::NotOk(common)),
            _ => {
                tracing::error!(
                    "Replication protocol error: read unknown replication response of type '{}'",
                    response_type
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
