use crate::BytesMutUtils;
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
    /// Keep the slot number as part of the key encoding
    /// with `<key-type><key_slot>` we can discover all keys belonged
    /// to a given slot by using a prefix iterator
    key_slot: u16,
}

impl KeyMetadata {
    pub const SIZE: usize = std::mem::size_of::<u8>() + std::mem::size_of::<u16>();
    pub const KEY_PRIMARY: u8 = 0u8;

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::with_capacity(KeyMetadata::SIZE);

        as_bytes.extend_from_slice(&BytesMutUtils::from_u8(&self.key_type));
        as_bytes.extend_from_slice(&BytesMutUtils::from_u16(&self.key_slot));
        as_bytes
    }

    #[allow(clippy::field_reassign_with_default)]
    pub fn from_bytes(buf: &BytesMut) -> Self {
        let mut de = Self::default();
        let mut pos = 0usize;

        de.key_type = BytesMutUtils::to_u8(&BytesMut::from(&buf[pos..]));
        pos += std::mem::size_of::<u8>();

        de.key_slot = BytesMutUtils::to_u16(&BytesMut::from(&buf[pos..]));
        de
    }

    /// Set the key type
    fn with_type(mut self, key_type: u8) -> Self {
        self.key_type = key_type;
        self
    }

    /// Create a string key that can place into the storage which includes a metadata regarding the key's encoding
    pub fn new_primary_key(user_key: &BytesMut) -> BytesMut {
        let mut key_metadata = KeyMetadata::default().with_type(KeyMetadata::KEY_PRIMARY);
        key_metadata.key_slot = crate::utils::calculate_slot(user_key);

        let mut encoded_key = BytesMut::with_capacity(KeyMetadata::SIZE + user_key.len());
        encoded_key.extend_from_slice(&key_metadata.to_bytes());
        encoded_key.extend_from_slice(user_key);
        encoded_key
    }

    /// Given an encoded key, return its metadata and the user content
    pub fn from_raw(encoded_key: &BytesMut) -> (KeyMetadata, BytesMut) {
        let (pk_bytes, user_bytes) = encoded_key.split_at(KeyMetadata::SIZE);
        let pk = KeyMetadata::from_bytes(&BytesMut::from(pk_bytes));
        (pk, BytesMut::from(user_bytes))
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

        let pk_as_bytes = PrimaryKeyMetadata::new_primary_key(&user_key);
        let (pk, user_key) = PrimaryKeyMetadata::from_raw(&pk_as_bytes);

        // Check that the slot serialised + deserialised properly
        assert_eq!(pk.key_slot, slot);
        assert!(pk.is_primary_key());
        assert_eq!(BytesMutUtils::to_string(&user_key), "My Key");
    }
}
