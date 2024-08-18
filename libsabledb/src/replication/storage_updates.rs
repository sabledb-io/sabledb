use crate::{U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;
use num_format::{Locale, ToFormattedString};

const OPCODE_PUT: u8 = 0;
const OPCODE_DEL: u8 = 1;
const USIZE_SIZE: usize = std::mem::size_of::<usize>();
const U64_SIZE: usize = std::mem::size_of::<u64>();

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PutRecord {
    pub key: BytesMut,
    pub value: BytesMut,
}

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

impl PutRecord {
    pub fn to_bytes(builder: &mut U8ArrayBuilder, key: &[u8], value: &[u8]) {
        write_buffer(builder, key);
        write_buffer(builder, value);
    }

    /// Deserialise `PutRecord` from bytes.
    /// On failure return `None`. On success, return the deserialised object +
    /// remove the bytes used to construct the object from the buffer
    pub fn from_bytes(reader: &mut U8ArrayReader) -> Option<PutRecord> {
        let key = read_buffer(reader)?;
        let value = read_buffer(reader)?;
        Some(PutRecord::new(key, value))
    }

    pub fn new(key: BytesMut, value: BytesMut) -> Self {
        PutRecord { key, value }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DeleteRecord {
    pub key: BytesMut,
}

impl DeleteRecord {
    pub fn to_bytes(builder: &mut U8ArrayBuilder, key: &[u8]) {
        write_buffer(builder, key);
    }

    /// Deserialise `DeleteRecord` from bytes.
    /// On failure return `None`. On success, return the deserialised object +
    /// remove the bytes used to construct the object from the buffer
    pub fn from_bytes(reader: &mut U8ArrayReader) -> Option<DeleteRecord> {
        let key = read_buffer(reader)?;
        Some(DeleteRecord::new(key))
    }

    pub fn new(key: BytesMut) -> Self {
        DeleteRecord { key }
    }
}

#[derive(Debug, Clone)]
pub enum StorageUpdatesIterItem {
    Put(PutRecord),
    Del(DeleteRecord),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// This message is used to return a serialised updates that were
/// done to the database since txn `start_seq_number` and
/// up until `end_seq_number` (excluding).
/// The replica requests this message from the primary server
/// by sending a `ReplRequest` with type `GET_UPDATES_SINCE` and
/// providing the `start_seq_number`. In return, the primary constructs this
/// object, fills it with data and sends it over to the replica.
/// On the next call, the primary uses the value set in `end_seq_number`
/// as the `start_seq_number`. Using this mechanism, the replica is able
/// to tail the primary for updates
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
        writer.write_u8(OPCODE_PUT);
        PutRecord::to_bytes(&mut writer, key, value);
    }

    /// Serialise a `delete` command
    pub fn add_delete(&mut self, key: &[u8]) {
        let mut writer = U8ArrayBuilder::with_buffer(&mut self.serialised_data);
        writer.write_u8(OPCODE_DEL);
        DeleteRecord::to_bytes(&mut writer, key);
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

    pub fn next(&self, reader: &mut U8ArrayReader) -> Option<StorageUpdatesIterItem> {
        // The record kind is placed after the total record len
        let kind = reader.read_u8()?;
        match kind {
            OPCODE_PUT => {
                let record = PutRecord::from_bytes(reader)?;
                Some(StorageUpdatesIterItem::Put(record))
            }
            OPCODE_DEL => {
                let record = DeleteRecord::from_bytes(reader)?;
                Some(StorageUpdatesIterItem::Del(record))
            }
            _ => None,
        }
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
        PutRecord::to_bytes(&mut builder, b"hello", b"world");

        let mut reader = U8ArrayReader::with_buffer(&put_record_bytes);
        let rec = PutRecord::from_bytes(&mut reader);

        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.key, "hello");
        assert_eq!(rec.value, "world");
    }

    #[test]
    fn test_serialise_delete_record() {
        let mut del_record_bytes = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut del_record_bytes);
        DeleteRecord::to_bytes(&mut builder, b"hello");

        let mut reader = U8ArrayReader::with_buffer(&del_record_bytes);
        let rec = DeleteRecord::from_bytes(&mut reader);
        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.key, "hello");
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
            let Some(StorageUpdatesIterItem::Put(put_rec)) = message.next(&mut reader) else {
                assert!(false);
                return;
            };
            assert_eq!(put_rec.key, "put_key1");
            assert_eq!(put_rec.value, "put_value1");
        }

        {
            let Some(StorageUpdatesIterItem::Put(put_rec)) = message.next(&mut reader) else {
                assert!(false);
                return;
            };
            assert_eq!(put_rec.key, "put_key2");
            assert_eq!(put_rec.value, "put_value2");
        }

        {
            let Some(StorageUpdatesIterItem::Del(del_rec)) = message.next(&mut reader) else {
                assert!(false);
                return;
            };
            assert_eq!(del_rec.key, "delete_key1");
        }

        {
            let Some(StorageUpdatesIterItem::Put(put_rec)) = message.next(&mut reader) else {
                assert!(false);
                return;
            };
            assert_eq!(put_rec.key, "put_key3");
            assert_eq!(put_rec.value, "put_value3");
        }
    }
}
