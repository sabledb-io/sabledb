use crate::{
    metadata::CommonValueMetadata, metadata::Encoding, SableError, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

/// Contains information about the hash item
#[allow(dead_code)]
#[derive(Clone, Debug)]
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

    /// Return the hash unique ID
    pub fn id(&self) -> u64 {
        self.hash_id
    }

    /// Return the number of items owned by this hash
    pub fn len(&self) -> u64 {
        self.hash_size
    }

    /// Equivalent to `len() == 0`
    pub fn is_empty(&self) -> bool {
        self.hash_size.eq(&0u64)
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct HashItemKey {
    kind: u8,
    hash_id: u64,
    user_key: BytesMut,
}

#[allow(dead_code)]
impl HashItemKey {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = std::mem::size_of::<u8>() + std::mem::size_of::<u64>();

    pub fn with_user_key(hash_id: u64, user_key: BytesMut) -> Self {
        HashItemKey {
            kind: Encoding::KEY_HASH_ITEM,
            hash_id,
            user_key,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.kind);
        builder.write_u64(self.hash_id);
        builder.write_bytes(&self.user_key);
    }

    pub fn from_bytes(buff: &BytesMut) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let kind = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let hash_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let (_, user_key) = buff.split_at(reader.consumed());
        Ok(HashItemKey {
            kind,
            hash_id,
            user_key: BytesMut::from(user_key),
        })
    }

    pub fn set_hash_id(&mut self, hash_id: u64) {
        self.hash_id = hash_id;
    }

    pub fn hash_id(&self) -> u64 {
        self.hash_id
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
    use crate::BytesMutUtils;

    #[test]
    pub fn test_hash_key_serialization() -> Result<(), SableError> {
        let hash_item_key = HashItemKey::with_user_key(42, BytesMut::from("my_hash_item_key"));
        assert_eq!(
            BytesMutUtils::to_string(&hash_item_key.user_key).as_str(),
            "my_hash_item_key"
        );
        assert_eq!(hash_item_key.hash_id, 42);
        assert_eq!(hash_item_key.kind, Encoding::KEY_HASH_ITEM);

        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        hash_item_key.to_bytes(&mut reader);

        let deserialised = HashItemKey::from_bytes(&buffer).unwrap();
        assert_eq!(deserialised, hash_item_key);
        Ok(())
    }
}
