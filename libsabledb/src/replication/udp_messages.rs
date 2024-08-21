use crate::utils::{FromU8Reader, ToU8Builder, U8ArrayBuilder, U8ArrayReader};

#[derive(Debug, Clone, PartialEq)]
pub enum UdpMessageType {
    Heartbeat,
}

impl FromU8Reader for UdpMessageType {
    type Item = UdpMessageType;
    fn from_reader(reader: &mut U8ArrayReader) -> Option<Self::Item> {
        let v = reader.read_u8()?;
        match v {
            0u8 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

impl ToU8Builder for UdpMessageType {
    fn to_builder(&self, builder: &mut U8ArrayBuilder) {
        match self {
            Self::Heartbeat => builder.write_u8(0u8),
        }
    }
}
