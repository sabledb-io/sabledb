use crate::{
    metadata::KeyType,
    metadata::{CommonValueMetadata, KeyPrefix},
    Expiration, FromU8Reader, SableError, ToU8Writer, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

const EMPTY_BUFFER: &[u8] = &[];

/// Contains information about the hash item
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZSetValueMetadata {
    common: CommonValueMetadata,
    zset_size: u64,
}

#[allow(dead_code)]
impl ZSetValueMetadata {
    pub const SIZE: usize = CommonValueMetadata::SIZE + std::mem::size_of::<u64>();

    pub fn with_id(zset_id: u64) -> Self {
        ZSetValueMetadata {
            common: CommonValueMetadata::default().set_zset().with_uid(zset_id),
            zset_size: 0,
        }
    }

    pub fn expiration(&self) -> &Expiration {
        self.common.expiration()
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        self.common.expiration_mut()
    }

    /// Return the number of items owned by this zset
    pub fn len(&self) -> u64 {
        self.zset_size
    }

    /// Equivalent to `len() == 0`
    pub fn is_empty(&self) -> bool {
        self.zset_size.eq(&0u64)
    }

    /// Return the zset unique ID
    pub fn id(&self) -> u64 {
        self.common.uid()
    }

    /// Return the zset unique ID
    pub fn incr_len_by(&mut self, diff: u64) {
        self.zset_size = self.zset_size.saturating_add(diff);
    }

    pub fn decr_len_by(&mut self, diff: u64) {
        self.zset_size = self.zset_size.saturating_sub(diff);
    }

    /// Serialise the zset value metadata into bytes
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder);
        builder.write_u64(self.zset_size);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let common = CommonValueMetadata::from_bytes(reader)?;
        let zset_size = reader.read_u64().ok_or(SableError::SerialisationError)?;
        Ok(ZSetValueMetadata { common, zset_size })
    }

    /// Create a prefix for iterating all items belonged to this zset by score
    pub fn prefix_by_score(&self, score: Option<f64>) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(KeyType::ZsetScoreItem as u8);
        builder.write_u64(self.id());
        if let Some(score) = score {
            builder.write_f64(score);
        }
        buffer
    }

    /// Create an upper bound prefix for this set (for iterating over scores)
    pub fn score_upper_bound_prefix(&self) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(KeyType::ZsetScoreItem as u8);
        builder.write_u64(self.id() + 1);
        buffer
    }

    /// Create an upper bound prefix for this set (for iterating over members)
    pub fn member_upper_bound_prefix(&self) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(KeyType::ZsetMemberItem as u8);
        builder.write_u64(self.id() + 1);
        buffer
    }

    /// Create a prefix for iterating all items belonged to this zset by member
    pub fn prefix_by_member(&self, member: Option<&[u8]>) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(KeyType::ZsetMemberItem as u8);
        builder.write_u64(self.id());
        if let Some(member) = member {
            builder.write_bytes(member);
        }
        buffer
    }
}

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub struct ZSetScoreItem<'a> {
    prefix: KeyPrefix,
    zset_id: u64,
    score: f64,
    member: &'a [u8],
}

impl<'a> ZSetScoreItem<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize =
        KeyPrefix::SIZE + std::mem::size_of::<u64>() + std::mem::size_of::<f64>();

    pub fn new(zset_id: u64, db_id: u16, slot: u16, score: f64, member: &'a [u8]) -> Self {
        ZSetScoreItem {
            prefix: KeyPrefix::new(KeyType::ZsetScoreItem, db_id, slot),
            zset_id,
            score,
            member,
        }
    }

    /// Return prefix for iterating over all sorted set items, optionally with a given score
    pub fn prefix(zset_id: u64, db_id: u16, slot: u16, score: Option<f64>) -> BytesMut {
        let prefix = KeyPrefix::new(KeyType::ZsetScoreItem, db_id, slot);
        let mut buffer = BytesMut::with_capacity(
            KeyPrefix::SIZE + std::mem::size_of::<f64>() + std::mem::size_of::<u64>(),
        );
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        prefix.to_writer(&mut builder);
        zset_id.to_writer(&mut builder);
        if let Some(score) = score {
            score.to_writer(&mut builder);
        }
        buffer
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.prefix.to_writer(builder);
        builder.write_u64(self.zset_id);
        builder.write_f64(self.score);
        builder.write_bytes(self.member);
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let prefix = KeyPrefix::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let zset_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;
        let member = &buff[reader.consumed()..];
        Ok(ZSetScoreItem {
            prefix,
            zset_id,
            score,
            member,
        })
    }

    pub fn set_zset_id(&mut self, zset_id: u64) {
        self.zset_id = zset_id;
    }

    pub fn zset_id(&self) -> u64 {
        self.zset_id
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn member(&self) -> &'a [u8] {
        self.member
    }

    pub fn slot(&self) -> u16 {
        self.prefix.key_slot()
    }

    pub fn key_type(&self) -> &KeyType {
        self.prefix.key_type()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct ZSetMemberItem<'a> {
    prefix: KeyPrefix,
    zset_id: u64,
    member: &'a [u8],
}

impl<'a> ZSetMemberItem<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize = KeyPrefix::SIZE + std::mem::size_of::<u64>();

    pub fn new(zset_id: u64, db_id: u16, slot: u16, member: &'a [u8]) -> Self {
        ZSetMemberItem {
            prefix: KeyPrefix::new(KeyType::ZsetMemberItem, db_id, slot),
            zset_id,
            member,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.prefix.to_writer(builder);
        builder.write_u64(self.zset_id);
        builder.write_bytes(self.member);
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let prefix = KeyPrefix::from_reader(&mut reader).ok_or(SableError::SerialisationError)?;
        let zset_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let member = &buff[reader.consumed()..];
        Ok(ZSetMemberItem {
            prefix,
            zset_id,
            member,
        })
    }

    pub fn set_zset_id(&mut self, zset_id: u64) {
        self.zset_id = zset_id;
    }

    pub fn zset_id(&self) -> u64 {
        self.zset_id
    }

    pub fn member(&self) -> &'a [u8] {
        self.member
    }

    pub fn slot(&self) -> u16 {
        self.prefix.key_slot()
    }

    pub fn key_type(&self) -> &KeyType {
        self.prefix.key_type()
    }

    /// Return prefix for iterating over all sorted set items
    pub fn prefix(zset_id: u64, db_id: u16, slot: u16, member: Option<&[u8]>) -> BytesMut {
        let mut buffer = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);

        let item = if let Some(member) = member {
            ZSetMemberItem::new(zset_id, db_id, slot, member)
        } else {
            ZSetMemberItem::new(zset_id, db_id, slot, EMPTY_BUFFER)
        };
        item.to_bytes(&mut builder);
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
    pub fn test_zset_key_by_member_serialization() -> Result<(), SableError> {
        let field_key = BytesMut::from("field_key");
        let slot = crate::utils::calculate_slot(&field_key);
        let zset_item = ZSetMemberItem::new(42, 0, slot, &field_key);
        let kk = BytesMut::from(zset_item.member);
        assert_eq!(kk, BytesMut::from("field_key"),);
        assert_eq!(zset_item.zset_id(), 42);
        assert_eq!(zset_item.key_type(), &KeyType::ZsetMemberItem);

        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        zset_item.to_bytes(&mut reader);

        let deserialised = ZSetMemberItem::from_bytes(&buffer).unwrap();
        assert_eq!(deserialised, zset_item);
        Ok(())
    }

    #[test]
    pub fn test_zset_key_by_score_serialization() -> Result<(), SableError> {
        let field_key = BytesMut::from("field_key");
        let slot = crate::utils::calculate_slot(&field_key);
        let zset_item = ZSetScoreItem::new(42, 0, slot, 0.75, &field_key);
        let kk = BytesMut::from(zset_item.member);
        assert_eq!(kk, BytesMut::from("field_key"),);
        assert_eq!(zset_item.zset_id(), 42);
        assert_eq!(zset_item.score(), 0.75);
        assert_eq!(zset_item.key_type(), &KeyType::ZsetScoreItem);

        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        zset_item.to_bytes(&mut reader);

        let deserialised = ZSetScoreItem::from_bytes(&buffer).unwrap();
        assert_eq!(deserialised, zset_item);
        Ok(())
    }
}
