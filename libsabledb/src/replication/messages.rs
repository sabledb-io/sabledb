use crate::Server;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl RequestCommon {
    pub fn new() -> Self {
        RequestCommon {
            req_id: 0,
            node_id: Server::state().persistent_state().id(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponseCommon {
    /// The request ID that generated this response
    req_id: u64,
    /// Contains the reason for NACK response
    reason: ResponseReason,
    /// The responding node ID
    node_id: String,
    /// An optional context string for the response
    context: String,
}

impl ResponseCommon {
    pub fn new(request: &RequestCommon) -> Self {
        ResponseCommon {
            req_id: request.request_id(),
            node_id: Server::state().persistent_state().id(),
            reason: ResponseReason::Invalid,
            context: String::new(),
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

    pub fn with_context(mut self, context: String) -> Self {
        self.context = context;
        self
    }

    pub fn context(&self) -> &String {
        &self.context
    }
}

impl Default for RequestCommon {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ResponseCommon {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RequestId: {}, Reason: {:?}, NodeId: {}, Context: {}",
            self.req_id, self.reason, self.node_id, self.context
        )
    }
}

/// represents a message sent from NodeTalkClient -> NodeTalkServer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeTalkRequest {
    GetUpdatesSince {
        common: RequestCommon,
        from_sequence: u64,
    },
    FullSync(RequestCommon),
    JoinShard(RequestCommon),
    /// Client sends this message to notify the receiver that `slot` content is being
    /// transferred
    SendingSlotFile {
        common: RequestCommon,
        slot: u16,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeResponse {
    /// General "OK" response from the node server
    Ok(ResponseCommon),
    /// A general "Not OK" response from the node server
    NotOk(ResponseCommon),
    /// An OK response to "JoinCluster" request
    JoinShardOk {
        common: ResponseCommon,
        shard_name: String,
        cluster_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl std::fmt::Display for NodeTalkRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::FullSync(common) => write!(f, "FullSync({})", common),
            Self::GetUpdatesSince {
                common,
                from_sequence,
            } => {
                write!(f, "GetUpdatesSince({}, {})", common, from_sequence)
            }
            Self::SendingSlotFile { common, slot } => {
                write!(f, "SendingSlotContent({}, {})", common, slot)
            }
            Self::JoinShard(common) => {
                write!(f, "JoinShard({})", common)
            }
        }
    }
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Ok(common) => write!(f, "Ok({})", common),
            Self::NotOk(common) => write!(f, "NotOk({})", common),
            Self::JoinShardOk {
                common: _,
                shard_name,
                cluster_name,
            } => write!(
                f,
                "JoinShardOk(shard_nane: {}, cluster_name: {})",
                shard_name, cluster_name
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_bincode_serialization() -> Result<(), crate::SableError> {
        let req = RequestCommon::new();

        let resp = NodeResponse::NotOk(
            ResponseCommon::new(&req).with_reason(ResponseReason::NoFullSyncDone),
        );
        let as_bytes = crate::bincode_to_bytesmut!(resp);
        let de_resp = bincode::deserialize::<NodeResponse>(&as_bytes)?;
        println!("orig: {:?}", resp);
        println!("de_resp: {:?}", de_resp);
        assert_eq!(de_resp, resp);
        Ok(())
    }
}
