use crate::{SableError, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

pub type PrimaryKeyMetadata = KeyMetadata;

///
/// Each primary key stored in the storage contains a metadata attached to it which holds information about the
/// key itself. The metadata is of a fixed size and is placed in the begining of the byte array
///
/// [ key-metadata | user key ]
///
#[derive(Clone, Default)]
pub struct KeyMetadata {
    /// The key type. For primary key, this will always be `0`
    /// This field must come first
    key_type: u8,
    /// Database ID
    db_id: u16,
    /// Keep the slot number as part of the key encoding
    /// with `<key-type><key_slot>` we can discover all keys belonged
    /// to a given slot by using a prefix iterator
    key_slot: u16,
}

impl KeyMetadata {
    pub const SIZE: usize =
        std::mem::size_of::<u8>() + std::mem::size_of::<u16>() + std::mem::size_of::<u16>();
    pub const KEY_PRIMARY: u8 = 0u8;

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.key_type);
        builder.write_u16(self.db_id);
        builder.write_u16(self.key_slot);
    }

    pub fn from_bytes(buf: &BytesMut) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buf);
        let Some(key_type) = reader.read_u8() else {
            return Err(SableError::SerialisationError);
        };
        let Some(db_id) = reader.read_u16() else {
            return Err(SableError::SerialisationError);
        };
        let Some(key_slot) = reader.read_u16() else {
            return Err(SableError::SerialisationError);
        };
        Ok(KeyMetadata {
            key_type,
            db_id,
            key_slot,
        })
    }

    /// Set the key type
    fn with_type(mut self, key_type: u8) -> Self {
        self.key_type = key_type;
        self
    }

    /// Set the database ID
    fn with_db_id(mut self, db_id: u16) -> Self {
        self.db_id = db_id;
        self
    }

    /// Create a string key that can place into the storage which includes a metadata regarding the key's encoding
    pub fn new_primary_key(user_key: &BytesMut, db_id: u16) -> BytesMut {
        let mut key_metadata = KeyMetadata::default()
            .with_type(KeyMetadata::KEY_PRIMARY)
            .with_db_id(db_id);
        key_metadata.key_slot = crate::utils::calculate_slot(user_key);

        let mut encoded_key = BytesMut::with_capacity(KeyMetadata::SIZE + user_key.len());
        let mut builder = U8ArrayBuilder::with_buffer(&mut encoded_key);
        key_metadata.to_bytes(&mut builder);
        builder.write_bytes(user_key);
        encoded_key
    }

    /// Given an encoded key, return its metadata and the user content
    pub fn from_raw(encoded_key: &BytesMut) -> Result<(KeyMetadata, BytesMut), SableError> {
        let (pk_bytes, user_bytes) = encoded_key.split_at(KeyMetadata::SIZE);
        let pk = KeyMetadata::from_bytes(&BytesMut::from(pk_bytes))?;
        Ok((pk, BytesMut::from(user_bytes)))
    }

    pub fn is_primary_key(&self) -> bool {
        self.key_type == KeyMetadata::KEY_PRIMARY
    }

    /// Return the key type: Primary or Secondary
    pub fn key_type(&self) -> u8 {
        self.key_type
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
    pub fn test_key_serialization() {
        let user_key = BytesMut::from("My Key");
        let slot = crate::utils::calculate_slot(&user_key);
        println!("user_key slot = {}", slot);

        let pk_as_bytes = PrimaryKeyMetadata::new_primary_key(&user_key, 5);
        let (pk, user_key) = PrimaryKeyMetadata::from_raw(&pk_as_bytes).unwrap();

        // Check that the slot serialised + deserialised properly
        assert_eq!(pk.key_slot, slot);
        assert!(pk.is_primary_key());
        assert_eq!(pk.db_id, 5);
        assert_eq!(BytesMutUtils::to_string(&user_key), "My Key");
    }

    #[test]
    pub fn test_diff_db_id_with_same_keys_serialization() {
        let user_key = BytesMut::from("My Key");
        let slot = crate::utils::calculate_slot(&user_key);
        println!("user_key slot = {}", slot);

        let pk1_as_bytes = PrimaryKeyMetadata::new_primary_key(&user_key, 1);
        let pk2_as_bytes = PrimaryKeyMetadata::new_primary_key(&user_key, 2);
        assert_ne!(pk1_as_bytes, pk2_as_bytes);

        let (pk1, user_key1) = PrimaryKeyMetadata::from_raw(&pk1_as_bytes).unwrap();
        let (pk2, user_key2) = PrimaryKeyMetadata::from_raw(&pk2_as_bytes).unwrap();

        // Check that the slot serialised + deserialised properly
        assert_eq!(pk1.key_slot, slot);
        assert_eq!(pk2.key_slot, slot);
        assert!(pk1.is_primary_key());
        assert!(pk2.is_primary_key());
        assert_eq!(pk1.db_id, 1);
        assert_eq!(pk2.db_id, 2);
        assert_eq!(BytesMutUtils::to_string(&user_key1), "My Key");
        assert_eq!(BytesMutUtils::to_string(&user_key2), "My Key");
    }
}
