use super::UdpMessageType;
use crate::utils::{FromBytes, FromU8Reader, ToBytes, ToU8Builder, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

#[derive(Clone, Debug)]
pub struct Heartbeat {
    message_type: UdpMessageType,
}

impl ToBytes for Heartbeat {
    fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.message_type.to_builder(&mut builder);
        buffer
    }
}

impl FromBytes for Heartbeat {
    type Item = Heartbeat;
    fn from_bytes(bytes: &[u8]) -> Option<Self::Item> {
        let mut reader = U8ArrayReader::with_buffer(bytes);
        let message_type = UdpMessageType::from_reader(&mut reader)?;
        Some(Heartbeat { message_type })
    }
}
