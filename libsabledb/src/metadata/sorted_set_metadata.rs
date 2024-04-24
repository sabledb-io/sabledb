use crate::{
    metadata::CommonValueMetadata, metadata::Encoding, Expiration, SableError, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

/// Contains information about the hash item
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortedSetValueMetadata {
    common: CommonValueMetadata,
    zset_id: u64,
    hash_size: u64,
}

#[allow(dead_code)]
impl SortedSetValueMetadata {
    pub const SIZE: usize = 2 * std::mem::size_of::<u64>() + CommonValueMetadata::SIZE;

    pub fn with_id(zset_id: u64) -> Self {
        SortedSetValueMetadata {
            common: CommonValueMetadata::default().set_hash(),
            zset_id,
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
        self.zset_id
    }

    /// Return the hash unique ID
    pub fn incr_len_by(&mut self, diff: u64) {
        self.hash_size = self.hash_size.saturating_add(diff);
    }

    pub fn decr_len_by(&mut self, diff: u64) {
        self.hash_size = self.hash_size.saturating_sub(diff);
    }

    /// Set the hash ID
    pub fn set_id(&mut self, zset_id: u64) {
        self.zset_id = zset_id
    }

    /// Serialise the zset value metadata into bytes
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder);
        builder.write_u64(self.zset_id);
        builder.write_u64(self.hash_size);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let common = CommonValueMetadata::from_bytes(reader)?;

        let zset_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let hash_size = reader.read_u64().ok_or(SableError::SerialisationError)?;

        Ok(SortedSetValueMetadata {
            common,
            zset_id,
            hash_size,
        })
    }

    /// Create a prefix for iterating all items belonged to this zset by score
    pub fn prefix_by_score(&self) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(Encoding::KEY_ZSET_SCORE_ITEM);
        builder.write_u64(self.id());
        buffer
    }

    /// Create a prefix for iterating all items belonged to this zset by member
    pub fn prefix_by_member(&self) -> BytesMut {
        let mut buffer =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u64>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        builder.write_u8(Encoding::KEY_ZSET_MEMBER_ITEM);
        builder.write_u64(self.id());
        buffer
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ZSetKeyByScore<'a> {
    kind: u8,
    zset_id: u64,
    score: f64,
    member: &'a [u8],
}

#[allow(dead_code)]
impl<'a> ZSetKeyByScore<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize =
        std::mem::size_of::<u8>() + std::mem::size_of::<u64>() + std::mem::size_of::<f64>();

    pub fn new(zset_id: u64, score: f64, member: &'a [u8]) -> Self {
        ZSetKeyByScore {
            kind: Encoding::KEY_ZSET_SCORE_ITEM,
            zset_id,
            score,
            member,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.kind);
        builder.write_u64(self.zset_id);
        builder.write_f64(self.score);
        builder.write_bytes(self.member);
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let kind = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let zset_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;
        let member = &buff[reader.consumed()..];
        Ok(ZSetKeyByScore {
            kind,
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
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ZSetKeyByMember<'a> {
    kind: u8,
    zset_id: u64,
    member: &'a [u8],
}

#[allow(dead_code)]
impl<'a> ZSetKeyByMember<'a> {
    // SIZE contain only the serialisable items
    pub const SIZE: usize =
        std::mem::size_of::<u8>() + std::mem::size_of::<u64>() + std::mem::size_of::<f64>();

    pub fn new(zset_id: u64, member: &'a [u8]) -> Self {
        ZSetKeyByMember {
            kind: Encoding::KEY_ZSET_MEMBER_ITEM,
            zset_id,
            member,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.kind);
        builder.write_u64(self.zset_id);
        builder.write_bytes(self.member);
    }

    pub fn from_bytes(buff: &'a [u8]) -> Result<Self, SableError> {
        let mut reader = U8ArrayReader::with_buffer(buff);
        let kind = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let zset_id = reader.read_u64().ok_or(SableError::SerialisationError)?;
        let member = &buff[reader.consumed()..];
        Ok(ZSetKeyByMember {
            kind,
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
}
