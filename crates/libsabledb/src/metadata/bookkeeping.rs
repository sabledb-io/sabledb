use crate::{
    metadata::{KeyType, ValueType},
    FromU8Reader, SableError, ToU8Writer, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

/// A Bookkeeping record that is placed in the database whenever we create a new complex type
///
/// A complex type can be one of: Hash, List, Zset, Set
///
/// We keep the UID of the type and the type associated with it (`ValueType`)
/// we use this record to identify a complex items overwritten by `SableDB` and might have left some orphan records.
///
/// For example, a Hash with 100 items, will have in the 101 records in DB: 1 item for the metadata +
/// 100 records for the fields. A user might overwrite the main hash record (the one that holds the metadata)
/// by insert a string record into the database (e.g. `set myhash str_value`), this operation is legal but it
/// leaves the 100 hash items "orphans".
///
/// `SableDB` uses a dedicated thread (A.K.A "Evictor") that once every N minutes, goes over the Bookkeeping records
/// and identify orphan complex items and evicts their children (in the above example, 100 hash fields records)
#[derive(Clone, Default, PartialEq, Debug)]
#[allow(dead_code)]
pub struct Bookkeeping {
    /// This is always set to `KeyType::Bookkeeping`
    struct_type: KeyType,
    /// The value type expected for `uid`
    value_type: ValueType,
    /// The database ID
    db_id: u16,
    /// The item's slot number
    slot: u16,
    /// The complex type unique ID
    uid: u64,
}

impl Bookkeeping {
    pub const SIZE: usize = std::mem::size_of::<KeyType>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<ValueType>();

    pub fn new(db_id: u16, slot: u16) -> Self {
        Bookkeeping {
            struct_type: KeyType::Bookkeeping,
            uid: 0,
            db_id,
            value_type: ValueType::Str,
            slot,
        }
    }

    pub fn with_uid(mut self, uid: u64) -> Self {
        self.uid = uid;
        self
    }

    pub fn with_value_type(mut self, value_type: ValueType) -> Self {
        self.value_type = value_type;
        self
    }

    pub fn db_id(&self) -> u16 {
        self.db_id
    }

    pub fn slot(&self) -> u16 {
        self.slot
    }

    pub fn key_type(&self) -> KeyType {
        self.struct_type
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(Self::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        self.struct_type.to_writer(&mut builder);
        self.value_type.to_writer(&mut builder);
        self.db_id.to_writer(&mut builder);
        self.slot.to_writer(&mut builder);
        self.uid.to_writer(&mut builder);
        buffer
    }

    pub fn from_bytes(buffer: &[u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buffer);
        let struct_type =
            KeyType::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let value_type =
            ValueType::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let db_id = u16::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let slot = u16::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let uid = u64::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        Ok(Bookkeeping {
            struct_type,
            value_type,
            db_id,
            slot,
            uid,
        })
    }

    /// Create prefix that can be used by the database iterator to visit all
    /// bookkeeping records for `value_type`
    pub fn prefix(value_type: &ValueType) -> BytesMut {
        let mut buff = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buff);
        builder.write_u8(KeyType::Bookkeeping as u8);
        builder.write_u8(*value_type as u8);
        buff
    }

    pub fn uid(&self) -> u64 {
        self.uid
    }

    pub fn record_type(&self) -> &ValueType {
        &self.value_type
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
    pub fn test_bookkeeping_serialization() {
        let record = Bookkeeping::new(0, 14)
            .with_uid(1234)
            .with_value_type(ValueType::List);

        let buffer = record.to_bytes();
        let record_from_bytes = Bookkeeping::from_bytes(&buffer).unwrap();

        // Check that the slot serialised + de-serialised properly
        assert_eq!(record, record_from_bytes);
    }
}
