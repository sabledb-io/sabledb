use crate::{FromU8Reader, KeyType, SableError, ToU8Writer, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

/// RocksDB does not allow to replicate the "delete_range"
/// Instead, we place a special record in the database to indicate
/// that such an action took place
#[derive(Clone, PartialEq, Debug)]
pub struct DeleteRange {
    struct_type: KeyType,
    start_key: BytesMut,
    end_key: BytesMut,
}

impl DeleteRange {
    pub fn new(start_key: BytesMut, end_key: BytesMut) -> Self {
        DeleteRange {
            struct_type: KeyType::DeleteRange,
            start_key,
            end_key,
        }
    }

    pub fn get_start_key(&self) -> &BytesMut {
        &self.start_key
    }

    pub fn get_end_key(&self) -> &BytesMut {
        &self.end_key
    }

    pub fn is_delete_range(buffer: &[u8]) -> bool {
        if buffer.is_empty() {
            return false;
        }

        let mut reader = U8ArrayReader::with_buffer(buffer);
        let struct_type = KeyType::from_reader(&mut reader).unwrap_or_default();
        struct_type == KeyType::DeleteRange
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::default();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.struct_type.to_writer(&mut builder);
        builder.write_message(&self.start_key);
        builder.write_message(&self.end_key);
        buffer
    }

    pub fn from_bytes(buffer: &[u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buffer);
        let struct_type =
            KeyType::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let start_key = reader
            .read_message()
            .ok_or(SableError::SerialisationError)?;
        let end_key = reader
            .read_message()
            .ok_or(SableError::SerialisationError)?;
        Ok(DeleteRange {
            struct_type,
            start_key,
            end_key,
        })
    }
}
