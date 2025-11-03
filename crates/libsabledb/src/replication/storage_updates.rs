use crate::{SableError, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;
use num_format::{Locale, ToFormattedString};

const OPCODE_PUT: u8 = 0;
const OPCODE_DEL: u8 = 1;
const USIZE_SIZE: usize = std::mem::size_of::<usize>();
const U64_SIZE: usize = std::mem::size_of::<u64>();

const LEN_TYPE_8: u8 = 0u8;
const LEN_TYPE_16: u8 = 1u8;
const LEN_TYPE_32: u8 = 2u8;
const LEN_TYPE_64: u8 = 3u8;

/// Encode length + data into the builder
/// If the length of `buf` is < u8::MAX, this function uses `u8` as the length variable
/// If the length of `buf` is < u16::MAX, this function uses `u16` as the length variable
/// If the length of `buf` is < u32::MAX, this function uses `u32` as the length variable
/// else use `u64` as the length
/// The type is encoded into the first byte
fn write_buffer(builder: &mut U8ArrayBuilder, buf: &[u8]) {
    if buf.len() < u8::MAX as usize {
        builder.write_u8(LEN_TYPE_8);
        builder.write_u8(buf.len() as u8);
    } else if buf.len() < u16::MAX as usize {
        builder.write_u8(LEN_TYPE_16);
        builder.write_u16(buf.len() as u16);
    } else if buf.len() < u32::MAX as usize {
        builder.write_u8(LEN_TYPE_32);
        builder.write_u32(buf.len() as u32);
    } else {
        builder.write_u8(LEN_TYPE_64);
        builder.write_u64(buf.len() as u64);
    }
    builder.write_bytes(buf);
}

/// Simialr to `write_buffer` -> but this time, decode it
fn read_buffer(reader: &mut U8ArrayReader) -> Option<BytesMut> {
    let encoded_len = reader.read_u8()?;
    let key_len = match encoded_len {
        LEN_TYPE_8 => {
            let key_len = reader.read_u8()?;
            key_len as usize
        }
        LEN_TYPE_16 => {
            let key_len = reader.read_u16()?;
            key_len as usize
        }
        LEN_TYPE_32 => {
            let key_len = reader.read_u32()?;
            key_len as usize
        }
        LEN_TYPE_64 => {
            let key_len = reader.read_u64()?;
            key_len as usize
        }
        _ => {
            return None;
        }
    };

    reader.read_bytes(key_len)
}

#[derive(Debug, Clone, PartialEq)]
pub enum StorageUpdatesRecord {
    Put { key: BytesMut, value: BytesMut },
    Del { key: BytesMut },
}

impl StorageUpdatesRecord {
    /// Construct StorageUpdatesRecord from reader
    pub fn from(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let op_code = reader.read_u8().ok_or(SableError::SerialisationError)?;
        match op_code {
            OPCODE_PUT => {
                let key = read_buffer(reader).ok_or(SableError::SerialisationError)?;
                let value = read_buffer(reader).ok_or(SableError::SerialisationError)?;
                Ok(StorageUpdatesRecord::Put { key, value })
            }
            OPCODE_DEL => {
                let key = read_buffer(reader).ok_or(SableError::SerialisationError)?;
                Ok(StorageUpdatesRecord::Del { key })
            }
            _ => Err(SableError::SerialisationError),
        }
    }

    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        match self {
            StorageUpdatesRecord::Put { key, value } => Self::serialise_put(builder, key, value),
            StorageUpdatesRecord::Del { key } => Self::serialise_del(builder, key),
        }
    }

    pub fn serialise_put(builder: &mut U8ArrayBuilder, key: &[u8], value: &[u8]) {
        builder.write_u8(OPCODE_PUT);
        write_buffer(builder, key);
        write_buffer(builder, value);
    }

    pub fn serialise_del(builder: &mut U8ArrayBuilder, key: &[u8]) {
        builder.write_u8(OPCODE_DEL);
        write_buffer(builder, key);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// A struct that represents changes done to the database since `start_seq_number`
/// and up to `end_seq_number` (exluded)
pub struct StorageUpdates {
    /// Changes in this message are starting from `start_seq_number`
    pub start_seq_number: u64,
    /// Changes in this message up to `end_seq_number` (excluded)
    pub end_seq_number: u64,
    /// Number of changes
    pub changes_count: u64,
    /// Binary representation of the changes that can be sent over the network
    /// or stored to disk. All serialised binary data is using big endian
    pub serialised_data: BytesMut,
}

impl std::fmt::Display for StorageUpdates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = format!(
            "from: {}, to: {}, changes: {}, serialised data: {} bytes",
            self.start_seq_number.to_formatted_string(&Locale::en),
            self.end_seq_number.to_formatted_string(&Locale::en),
            self.changes_count.to_formatted_string(&Locale::en),
            self.serialised_data.len().to_formatted_string(&Locale::en)
        );
        write!(f, "{}", s)
    }
}

impl StorageUpdates {
    pub fn from_seq_number(start_seq_number: u64) -> Self {
        StorageUpdates {
            start_seq_number,
            end_seq_number: start_seq_number,
            ..Default::default()
        }
    }

    /// Serialise a `put` command
    pub fn add_put(&mut self, key: &[u8], value: &[u8]) {
        let mut writer = U8ArrayBuilder::with_buffer(&mut self.serialised_data);
        StorageUpdatesRecord::serialise_put(&mut writer, key, value);
    }

    /// Serialise a `delete` command
    pub fn add_delete(&mut self, key: &[u8]) {
        let mut writer = U8ArrayBuilder::with_buffer(&mut self.serialised_data);
        StorageUpdatesRecord::serialise_del(&mut writer, key);
    }

    /// Return the size of the changes, in bytes
    pub fn len(&self) -> u64 {
        self.serialised_data.len() as u64
    }

    /// Is empty?
    pub fn is_empty(&self) -> bool {
        self.serialised_data.is_empty()
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(
            U64_SIZE + U64_SIZE + U64_SIZE + USIZE_SIZE + self.serialised_data.len(),
        );
        let mut writer = U8ArrayBuilder::with_buffer(&mut buffer);
        writer.write_u64(self.start_seq_number);
        writer.write_u64(self.end_seq_number);
        writer.write_u64(self.changes_count);
        writer.write_usize(self.serialised_data.len());
        writer.write_bytes(&self.serialised_data);
        buffer
    }

    /// Construct `StorageUpdates` from raw bytes
    pub fn from_bytes(buffer: &BytesMut) -> Option<StorageUpdates> {
        let mut reader = U8ArrayReader::with_buffer(buffer);
        let start_seq_number = reader.read_u64()?;
        let end_seq_number = reader.read_u64()?;
        let changes_count = reader.read_u64()?;
        let data_len = reader.read_usize()?;
        let serialised_data = reader.read_bytes(data_len)?;
        Some(StorageUpdates {
            start_seq_number,
            end_seq_number,
            changes_count,
            serialised_data,
        })
    }

    pub fn next(reader: &mut U8ArrayReader) -> Option<StorageUpdatesRecord> {
        StorageUpdatesRecord::from(reader).ok()
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_serialise_storage_updates() {
        let updates = StorageUpdates {
            start_seq_number: 42,
            end_seq_number: 57,
            changes_count: 3,
            serialised_data: BytesMut::from("hello world"),
        };

        let mut buffer = updates.to_bytes();
        let deserialised_updates = StorageUpdates::from_bytes(&mut buffer).unwrap();
        assert_eq!(updates, deserialised_updates);
    }

    #[test]
    fn test_serialise_put_record() {
        let mut put_record_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut put_record_bytes);
        StorageUpdatesRecord::serialise_put(&mut builder, b"hello", b"world");

        let mut reader = U8ArrayReader::with_buffer(&put_record_bytes);
        let rec = StorageUpdatesRecord::from(&mut reader).unwrap();

        let StorageUpdatesRecord::Put { key, value } = rec else {
            panic!("Expected Put record");
        };

        assert_eq!(key, "hello");
        assert_eq!(value, "world");
    }

    #[test]
    fn test_serialise_delete_record() {
        let mut del_record_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut del_record_bytes);
        StorageUpdatesRecord::serialise_del(&mut builder, b"hello");

        let mut reader = U8ArrayReader::with_buffer(&del_record_bytes);
        let rec = StorageUpdatesRecord::from(&mut reader).unwrap();

        let StorageUpdatesRecord::Del { key } = rec else {
            panic!("Expected Del record");
        };
        assert_eq!(key, "hello");
    }

    #[test]
    fn test_change_since_message_iterator() {
        let mut message = StorageUpdates::default();
        message.add_put(b"put_key1", b"put_value1");
        message.add_put(b"put_key2", b"put_value2");
        message.add_delete(b"delete_key1");
        message.add_put(b"put_key3", b"put_value3");

        let mut reader = U8ArrayReader::with_buffer(&message.serialised_data);
        {
            let Some(StorageUpdatesRecord::Put { key, value }) = StorageUpdates::next(&mut reader)
            else {
                assert!(false);
                return;
            };
            assert_eq!(key, "put_key1");
            assert_eq!(value, "put_value1");
        }

        {
            let Some(StorageUpdatesRecord::Put { key, value }) = StorageUpdates::next(&mut reader)
            else {
                assert!(false);
                return;
            };
            assert_eq!(key, "put_key2");
            assert_eq!(value, "put_value2");
        }

        {
            let Some(StorageUpdatesRecord::Del { key }) = StorageUpdates::next(&mut reader) else {
                assert!(false);
                return;
            };
            assert_eq!(key, "delete_key1");
        }

        {
            let Some(StorageUpdatesRecord::Put { key, value }) = StorageUpdates::next(&mut reader)
            else {
                assert!(false);
                return;
            };
            assert_eq!(key, "put_key3");
            assert_eq!(value, "put_value3");
        }
    }
}
