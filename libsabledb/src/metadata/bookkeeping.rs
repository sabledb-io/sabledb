use crate::{
    metadata::{FromRaw, KeyType, ValueType},
    SableError, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

/// A Bookkeeping record that is placed in the database whenever we create a new comlpex type
/// (Atm, a complex type can be: Hash, List, Zset)
/// We keep the UID of the type and the type associated with it (`uid_type`)
/// we use this record to identify a complex items overwritten by SableDb and might have left some
/// orphan records.
///
/// for example, a Hash with 100 items, will have in the 101 records in DB: 1 item for the metadata +
/// 100 records for the fields. A user might overwrite the main hash record (the one that holds the metadata)
/// by insert a string record into the database (e.g. `set myhash str_value`), this operation is legal but it
/// leaves the 100 hash items "orphans".
///
/// SableDb uses a dedicated thread "Evictor" that one every N minutes, goes over the Bookkeeping records
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
    /// The complex type unique ID
    uid: u64,
}

impl Bookkeeping {
    pub const SIZE: usize = std::mem::size_of::<KeyType>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<ValueType>();

    pub fn new(db_id: u16) -> Self {
        Bookkeeping {
            struct_type: KeyType::Bookkeeping,
            uid: 0,
            db_id,
            value_type: ValueType::Str,
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

    pub fn to_bytes(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(Self::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(self.struct_type as u8);
        builder.write_u8(self.value_type as u8);
        builder.write_u16(self.db_id);
        builder.write_u64(self.uid);
        buffer
    }

    pub fn from_bytes(buffer: &[u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buffer);
        let struct_type = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let value_type = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let db_id = reader.read_u16().ok_or(SableError::SerialisationError)?;
        let uid = reader.read_u64().ok_or(SableError::SerialisationError)?;
        Ok(Bookkeeping {
            struct_type: KeyType::from_u8(struct_type).ok_or(SableError::SerialisationError)?,
            uid,
            db_id,
            value_type: ValueType::from_u8(value_type).ok_or(SableError::SerialisationError)?,
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
        let record = Bookkeeping::new(0)
            .with_uid(1234)
            .with_value_type(ValueType::List);

        let buffer = record.to_bytes();
        let record_from_bytes = Bookkeeping::from_bytes(&buffer).unwrap();

        // Check that the slot serialised + deserialised properly
        assert_eq!(record, record_from_bytes);
    }
}
