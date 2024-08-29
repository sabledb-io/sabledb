use super::{RaftNode, RaftNodeFlags};
use crate::utils::{FromU8Reader, ToU8Writer, U8ArrayBuilder, U8ArrayReader};

#[derive(Debug, Clone, PartialEq)]
pub enum RaftMessageType {
    Heartbeat,
    AddNode,
}

const MT_HEARTBEAT: u8 = 0;
const MT_ADD_NODE: u8 = 1;

impl FromU8Reader for RaftMessageType {
    type Item = RaftMessageType;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        let v = reader.read_u8()?;
        match v {
            MT_HEARTBEAT => Some(Self::Heartbeat),
            MT_ADD_NODE => Some(Self::AddNode),
            _ => None,
        }
    }
}

impl ToU8Writer for RaftMessageType {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        match self {
            Self::Heartbeat => builder.write_u8(MT_HEARTBEAT),
            Self::AddNode => builder.write_u8(MT_ADD_NODE),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HeartbeatMsg {
    message_type: RaftMessageType,
}

impl Default for HeartbeatMsg {
    fn default() -> Self {
        HeartbeatMsg {
            message_type: RaftMessageType::Heartbeat,
        }
    }
}

impl ToU8Writer for HeartbeatMsg {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.message_type.to_writer(builder);
    }
}

impl FromU8Reader for HeartbeatMsg {
    type Item = HeartbeatMsg;
    fn from_reader(reader: &mut crate::utils::U8ArrayReader) -> Option<Self::Item> {
        let message_type = RaftMessageType::from_reader(reader)?;
        Some(HeartbeatMsg { message_type })
    }
}

#[derive(Debug)]
pub struct AddNodeMsg {
    message_type: RaftMessageType,
    node: RaftNode,
}

impl AddNodeMsg {
    pub fn with_node(node: RaftNode) -> Self {
        AddNodeMsg {
            message_type: RaftMessageType::AddNode,
            node,
        }
    }

    pub fn node(&self) -> &RaftNode {
        &self.node
    }

    pub fn with_flag(self, flag: RaftNodeFlags) -> Self {
        self.node.set_flag(flag);
        self
    }

    pub fn is_voter(&self) -> bool {
        self.node.has_flag(RaftNodeFlags::Voter)
    }

    pub fn is_non_voter(&self) -> bool {
        !self.is_voter()
    }

    pub fn set_voter(&self, voter: bool) {
        self.node.set_flag(if voter {
            RaftNodeFlags::Voter
        } else {
            RaftNodeFlags::NonVoter
        });
    }

    pub fn node_ip(&self) -> &String {
        &self.node.ip
    }

    pub fn node_port(&self) -> u16 {
        self.node.port
    }
}

impl ToU8Writer for AddNodeMsg {
    fn to_writer(&self, builder: &mut U8ArrayBuilder) {
        self.message_type.to_writer(builder);
        self.node.to_writer(builder);
    }
}

impl FromU8Reader for AddNodeMsg {
    type Item = AddNodeMsg;
    fn from_reader(reader: &mut crate::utils::U8ArrayReader) -> Option<Self::Item> {
        let message_type = RaftMessageType::from_reader(reader)?;
        let node = RaftNode::from_reader(reader)?;
        Some(AddNodeMsg { message_type, node })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_node_serialisation() {
        let mut buffer = bytes::BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);

        // Construct a "AddNodeMsg" and serialise it into "buffer"
        let msg1 =
            AddNodeMsg::with_node(RaftNode::new("127.0.0.1", 6999)).with_flag(RaftNodeFlags::Voter);
        msg1.to_writer(&mut builder);

        // Deserialise a "AddNodeMsg" from the buffer
        let mut reader = U8ArrayReader::with_buffer(&buffer);
        let msg2 = AddNodeMsg::from_reader(&mut reader).unwrap();

        // Compare the results
        assert_eq!(msg1.message_type, msg2.message_type);
        assert_eq!(msg1.is_voter(), msg2.is_voter());
        assert_eq!(msg1.node_port(), msg2.node_port());
        assert_eq!(msg1.node_ip(), msg2.node_ip());
    }

    #[test]
    fn test_heartbeat_serialisation() {
        let mut buffer = bytes::BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);

        // Construct a "Heartbeat" and serialise it into "buffer"
        let msg1 = HeartbeatMsg::default();
        msg1.to_writer(&mut builder);

        // Deserialise a "Heartbeat" from the buffer
        let mut reader = U8ArrayReader::with_buffer(&buffer);
        let msg2 = HeartbeatMsg::from_reader(&mut reader).unwrap();

        // Compare the results
        assert_eq!(msg1.message_type, msg2.message_type);
    }
}
