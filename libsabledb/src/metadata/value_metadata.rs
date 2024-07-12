use crate::{
    metadata::encoding::{FromRaw, ValueType},
    Expiration, SableError, U8ArrayBuilder, U8ArrayReader,
};

/// Contains information regarding the String type metadata
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct CommonValueMetadata {
    // u8
    value_encoding: ValueType,
    /// The type UID (for strings, this is always 0)
    unique_id: u64,
    /// Value ttl information
    expiration: Expiration,
}

#[allow(dead_code)]
impl CommonValueMetadata {
    pub const SIZE: usize =
        std::mem::size_of::<ValueType>() + std::mem::size_of::<u64>() + Expiration::SIZE;

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.value_encoding as u8);
        builder.write_u64(self.unique_id);
        self.expiration.to_bytes(builder);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let value_type = reader.read_u8().ok_or(SableError::SerialisationError)?;
        let unique_id = reader.read_u64().ok_or(SableError::SerialisationError)?;

        let expiration = Expiration::from_bytes(reader)?;
        let value_encoding =
            ValueType::from_u8(value_type).ok_or(SableError::SerialisationError)?;
        Ok(CommonValueMetadata {
            value_encoding,
            unique_id,
            expiration,
        })
    }

    pub fn expiration(&self) -> &Expiration {
        &self.expiration
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        &mut self.expiration
    }

    pub fn is_string(&self) -> bool {
        self.value_encoding == ValueType::Str
    }

    pub fn is_list(&self) -> bool {
        self.value_encoding == ValueType::List
    }

    pub fn is_hash(&self) -> bool {
        self.value_encoding == ValueType::Hash
    }

    pub fn is_set(&self) -> bool {
        self.value_encoding == ValueType::Set
    }

    pub fn is_zset(&self) -> bool {
        self.value_encoding == ValueType::Zset
    }

    pub fn value_type(&self) -> ValueType {
        self.value_encoding
    }

    pub fn set_string(mut self) -> Self {
        self.value_encoding = ValueType::Str;
        self
    }

    pub fn set_list(mut self) -> Self {
        self.value_encoding = ValueType::List;
        self
    }

    pub fn set_hash(mut self) -> Self {
        self.value_encoding = ValueType::Hash;
        self
    }

    pub fn set_set(mut self) -> Self {
        self.value_encoding = ValueType::Set;
        self
    }

    pub fn set_zset(mut self) -> Self {
        self.value_encoding = ValueType::Zset;
        self
    }

    pub fn with_uid(mut self, id: u64) -> Self {
        self.unique_id = id;
        self
    }

    pub fn set_uid(&mut self, id: u64) {
        self.unique_id = id;
    }

    pub fn uid(&self) -> u64 {
        self.unique_id
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
mod test {
    use super::*;
    use crate::{SableError, TimeUtils};

    #[test]
    fn test_packing() -> Result<(), SableError> {
        let mut md = CommonValueMetadata::default();
        let mut arr = bytes::BytesMut::with_capacity(CommonValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut arr);
        md.expiration_mut().set_ttl_millis(30)?;
        md.set_uid(1234);
        md.to_bytes(&mut builder);
        assert_eq!(arr.len(), CommonValueMetadata::SIZE);

        // the buffer can be larger than `Metadata`
        arr.extend_from_slice(&[5, 5]);

        // Check that we can de-serialize it
        let mut reader = U8ArrayReader::with_buffer(&arr);
        let deserialized_md = CommonValueMetadata::from_bytes(&mut reader)?;

        // remove the deserialized part
        let _ = arr.split_to(CommonValueMetadata::SIZE);

        // change the source to 15
        let _ = md.expiration_mut().set_expire_timestamp_seconds(15);

        // confirm that the deserialized still has ttl value of 30
        assert_eq!(
            deserialized_md.expiration().ttl_ms,
            30,
            "Now: {}. deserialized_md = {:?}",
            TimeUtils::epoch_ms()?,
            deserialized_md,
        );
        assert_eq!(deserialized_md.uid(), 1234);
        assert!(deserialized_md.expiration().is_expired()? == false);
        assert_eq!(&arr[..], &[5, 5]);
        Ok(())
    }

    #[test]
    fn test_expire_api() -> Result<(), SableError> {
        let mut md = CommonValueMetadata::default();
        assert!(md.expiration().is_expired()? == false);

        md.expiration_mut().set_ttl_millis(10)?;
        assert!(md.expiration().is_expired()? == false);
        assert_eq!(md.expiration().ttl_in_seconds()?, 1);
        assert_eq!(md.expiration().ttl_in_millis()?, 10);

        let mut now = TimeUtils::epoch_ms()?;
        now -= 1; // set the expiration time in the past
        md.expiration_mut().set_expire_timestamp_millis(now)?;
        assert!(md.expiration().is_expired()? == true);

        let mut now = TimeUtils::epoch_ms()?;
        now += 42;
        md.expiration_mut().set_expire_timestamp_millis(now)?;
        assert!(md.expiration().is_expired()? == false);
        assert!(md.expiration().ttl_in_millis()? == 42);
        Ok(())
    }
}
