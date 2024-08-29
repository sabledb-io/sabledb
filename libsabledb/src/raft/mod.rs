use crate::utils::{FromU8Reader, ToU8Writer, U8ArrayBuilder, U8ArrayReader};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

mod raft_messages;
lazy_static::lazy_static! {
    static ref NODE_ID_GENERATOR: AtomicU64 = AtomicU64::new(1);
}

use crate::SableError;
pub use raft_messages::*;

#[async_trait::async_trait]
pub trait RaftTransport {
    /// Broadcast a heartbeat message to the network
    async fn broadcast_heartbeat(&self) -> Result<(), SableError>;
}

#[derive(Debug, PartialEq, Clone)]
pub enum RaftNodeFlags {
    NonVoter,
    Voter,
}

#[derive(Debug)]
pub struct RaftNode {
    ip: String,
    port: u16,
    state: AtomicU64,
    node_id: AtomicU64,
}

impl FromU8Reader for RaftNode {
    type Item = RaftNode;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        let ip = String::from_reader(reader)?;
        let port = u16::from_reader(reader)?;
        let state = AtomicU64::from_reader(reader)?;
        let node_id = AtomicU64::from_reader(reader)?;
        Some(RaftNode {
            ip,
            port,
            state,
            node_id,
        })
    }
}

impl ToU8Writer for RaftNode {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.ip.to_writer(builder);
        self.port.to_writer(builder);
        self.state.to_writer(builder);
        self.node_id.to_writer(builder);
    }
}

impl Default for RaftNode {
    fn default() -> Self {
        Self::new("", 0)
    }
}

impl RaftNode {
    pub fn new(ip: &str, port: u16) -> Self {
        let state = RaftNodeFlags::NonVoter as u64;
        RaftNode {
            ip: ip.to_string(),
            port,
            state: AtomicU64::new(state),
            node_id: AtomicU64::default(), // 0
        }
    }

    pub fn set_node_id(&self, id: u64) {
        self.node_id.store(id, Ordering::Relaxed);
    }

    pub fn node_id(&self) -> u64 {
        self.node_id.load(Ordering::Relaxed)
    }

    pub fn set_non_voter(&self) {
        self.clear_flag(RaftNodeFlags::Voter);
        self.set_flag(RaftNodeFlags::NonVoter);
    }

    pub fn set_voter(&self) {
        self.clear_flag(RaftNodeFlags::NonVoter);
        self.set_flag(RaftNodeFlags::Voter);
    }

    fn clear_flag(&self, flag: RaftNodeFlags) {
        let mut curstate = self.state.load(Ordering::Relaxed);
        curstate &= !(flag as u64);
        self.state.store(curstate, Ordering::Relaxed);
    }

    fn set_flag(&self, flag: RaftNodeFlags) {
        let mut curstate = self.state.load(Ordering::Relaxed);
        curstate |= flag as u64;
        self.state.store(curstate, Ordering::Relaxed);
    }

    fn has_flag(&self, flag: RaftNodeFlags) -> bool {
        let curstate = self.state.load(Ordering::Relaxed);
        curstate & flag as u64 != 0
    }
}

#[derive(Default)]
pub struct Raft {
    nodes: DashMap<u64, RaftNode>,
}

#[allow(dead_code)]
impl Raft {
    pub fn allocate_node_id(&self) -> u64 {
        NODE_ID_GENERATOR.fetch_add(1, Ordering::Relaxed)
    }

    /// Add a node to the network
    pub fn add_node(&self, node: RaftNode) -> Result<(), SableError> {
        self.nodes.insert(node.node_id(), node);
        Ok(())
    }
}
