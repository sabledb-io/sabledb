use crate::{
    metadata::CommonValueMetadata, metadata::Encoding, Expiration, SableError, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

/// Contains information about the hash item
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashValueMetadata {
    common: CommonValueMetadata,
    hash_id: u64,
    hash_size: u64,
}

#[allow(dead_code)]
impl HashValueMetadata {
    pub const SIZE: usize = 2 * std::mem::size_of::<u64>() + CommonValueMetadata::SIZE;

    pub fn with_id(hash_id: u64) -> Self {
        HashValueMetadata {
            common: CommonValueMetadata::default().set_hash(),
            hash_id,
            hash_size: 0,
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
        self.hash_size
    }

    /// Equivalent to `len() == 0`
    pub fn is_empty(&self) -> bool {
        self.hash_size.eq(&0u64)
    }

    /// Return the hash unique ID
    pub fn id(&self) -> u64 {
        self.hash_id
    }

    /// Return the hash unique ID
    pub fn incr_len_by(&mut self, diff: u64) {
        self.hash_size = self.hash_size.saturating_add(diff);
    }

    pub fn decr_len_by(&mut self, diff: u64) {
        self.hash_size = self.hash_size.saturating_sub(diff);
    }

    /// Set the hash ID
    pub fn set_id(&mut self, hash_id: u64) {
        self.hash_id = hash_id
    }

    /// Serialise the hash value metadata into bytes
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder);
        builder.write_u64(self.hash_id);
        builder.write_u64(self.hash_size);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let common = CommonValueMetadata::from_bytes(reader)?;

        let hash_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let hash_size = reader.read_u64().ok_or(SableError::SerialisationError)?;

        Ok(HashValueMetadata {
            common,
            hash_id,
            hash_size,
        })
    }

    /// Create a prefix for iterating all items belonged to this hash
    pub fn prefix(&self) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(Encoding::KEY_HASH_ITEM);
        builder.write_u64(self.id());
        buffer
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct HashFieldKey<'a> {
    kind: u8,
    hash_id: u64,
    user_key: &'a [u8],
}

#[allow(dead_code)]
impl<'a> HashFieldKey<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = std::mem::size_of::<u8>() + std::mem::size_of::<u64>();

    pub fn with_user_key(hash_id: u64, user_key: &'a BytesMut) -> Self {
        HashFieldKey {
            kind: Encoding::KEY_HASH_ITEM,
            hash_id,
            user_key,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.kind);
        builder.write_u64(self.hash_id);
        builder.write_bytes(self.user_key);
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let kind = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let hash_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let (_, user_key) = buff.split_at(reader.consumed());
        Ok(HashFieldKey {
            kind,
            hash_id,
            user_key,
        })
    }

    pub fn set_hash_id(&mut self, hash_id: u64) {
        self.hash_id = hash_id;
    }

    pub fn hash_id(&self) -> u64 {
        self.hash_id
    }

    pub fn key(&self) -> &[u8] {
        self.user_key
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
        let hash_item_key = HashFieldKey::with_user_key(42, &field_key);
        let kk = BytesMut::from(hash_item_key.user_key);
        assert_eq!(kk, BytesMut::from("field_key"),);
        assert_eq!(hash_item_key.hash_id, 42);
        assert_eq!(hash_item_key.kind, Encoding::KEY_HASH_ITEM);

        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        hash_item_key.to_bytes(&mut reader);

        let deserialised = HashFieldKey::from_bytes(&buffer).unwrap();
        assert_eq!(deserialised, hash_item_key);
        Ok(())
    }
}
