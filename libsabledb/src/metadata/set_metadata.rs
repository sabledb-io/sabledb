use crate::{
    metadata::KeyType,
    metadata::{CommonValueMetadata, KeyPrefix},
    Expiration, FromU8Reader, SableError, ToU8Writer, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

/// Contains information about the hash item
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetValueMetadata {
    common: CommonValueMetadata,
    set_size: u64,
}

#[allow(dead_code)]
impl SetValueMetadata {
    pub const SIZE: usize = 2 * std::mem::size_of::<u64>() + CommonValueMetadata::SIZE;

    pub fn with_id(set_id: u64) -> Self {
        SetValueMetadata {
            common: CommonValueMetadata::default().set_set().with_uid(set_id),
            set_size: 0,
        }
    }

    pub fn expiration(&self) -> &Expiration {
        self.common.expiration()
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        self.common.expiration_mut()
    }

    /// Return the number of items owned by this hash
    pub fn len(&self) -> u64 {
        self.set_size
    }

    /// Equivalent to `len() == 0`
    pub fn is_empty(&self) -> bool {
        self.set_size.eq(&0u64)
    }

    /// Return the hash unique ID
    pub fn id(&self) -> u64 {
        self.common.uid()
    }

    /// Return the hash unique ID
    pub fn incr_len_by(&mut self, diff: u64) {
        self.set_size = self.set_size.saturating_add(diff);
    }

    pub fn decr_len_by(&mut self, diff: u64) {
        self.set_size = self.set_size.saturating_sub(diff);
    }

    /// Set the set ID
    pub fn set_id(&mut self, set_id: u64) {
        self.common.set_uid(set_id);
    }

    /// Serialise the set value metadata into bytes
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder);
        builder.write_u64(self.set_size);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let common = CommonValueMetadata::from_bytes(reader)?;
        let set_size = reader.read_u64().ok_or(SableError::SerialisationError)?;
        Ok(SetValueMetadata { common, set_size })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct SetMemberKey<'a> {
    prefix: KeyPrefix,
    set_id: u64,
    user_key: &'a [u8],
}

#[allow(dead_code)]
impl<'a> SetMemberKey<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = KeyPrefix::SIZE + std::mem::size_of::<u64>();

    pub fn with_user_key(set_id: u64, db_id: u16, slot: u16, user_key: &'a [u8]) -> Self {
        SetMemberKey {
            prefix: KeyPrefix::new(KeyType::SetItem, db_id, slot),
            set_id,
            user_key,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.prefix.to_writer(builder);
        builder.write_u64(self.set_id);
        if !self.user_key.is_empty() {
            builder.write_bytes(self.user_key);
        }
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let prefix = KeyPrefix::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let set_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let user_key = &buff[reader.consumed()..];
        Ok(SetMemberKey {
            prefix,
            set_id,
            user_key,
        })
    }

    pub fn set_set_id(&mut self, set_id: u64) {
        self.set_id = set_id;
    }

    pub fn set_id(&self) -> u64 {
        self.set_id
    }

    pub fn key(&self) -> &[u8] {
        self.user_key
    }

    pub fn key_type(&self) -> &KeyType {
        self.prefix.key_type()
    }

    /// Return prefix for iterating over all the set items
    pub fn prefix(set_id: u64, db_id: u16, slot: u16) -> BytesMut {
        let prefix = KeyPrefix::new(KeyType::SetItem, db_id, slot);
        let mut buffer = BytesMut::with_capacity(KeyPrefix::SIZE + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        prefix.to_writer(&mut builder);
        set_id.to_writer(&mut builder);
        buffer
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
    pub fn test_hash_key_serialization() -> Result<(), SableError> {
        let field_key = BytesMut::from("field_key");
        let slot = crate::utils::calculate_slot(&field_key);
        let set_item_key = SetMemberKey::with_user_key(42, 0, slot, &field_key);
        let kk = BytesMut::from(set_item_key.user_key);
        assert_eq!(kk, BytesMut::from("field_key"),);
        assert_eq!(set_item_key.set_id, 42);
        assert_eq!(set_item_key.key_type(), &KeyType::SetItem);

        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        set_item_key.to_bytes(&mut reader);

        let deserialised = SetMemberKey::from_bytes(&buffer).unwrap();
        assert_eq!(deserialised, set_item_key);
        Ok(())
    }
}
