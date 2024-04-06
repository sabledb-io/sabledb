use crate::{metadata::Encoding, Expiration, SableError, U8ArrayBuilder, U8ArrayReader};

/// Contains information regarding the String type metadata
#[derive(Clone, Debug)]
pub struct CommonValueMetadata {
    value_encoding: u8,
    /// Value ttl information
    expiration: Expiration,
}

impl Default for CommonValueMetadata {
    fn default() -> Self {
        CommonValueMetadata {
            value_encoding: Encoding::VALUE_STRING,
            expiration: Expiration::default(),
        }
    }
}

#[allow(dead_code)]
impl CommonValueMetadata {
    pub const SIZE: usize = std::mem::size_of::<u8>() + Expiration::SIZE;

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u8(self.value_encoding);
        self.expiration.to_bytes(builder);
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        let Some(value_type) = reader.read_u8() else {
            return Err(SableError::SerialisationError);
        };

        let expiration = Expiration::from_bytes(reader)?;
        Ok(CommonValueMetadata {
            value_encoding: value_type,
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
        self.value_encoding == Encoding::VALUE_STRING
    }

    pub fn is_list(&self) -> bool {
        self.value_encoding == Encoding::VALUE_LIST
    }

    pub fn is_hash(&self) -> bool {
        self.value_encoding == Encoding::VALUE_HASH
    }

    pub fn value_type(&self) -> u8 {
        self.value_encoding
    }

    pub fn set_string(mut self) -> Self {
        self.value_encoding = Encoding::VALUE_STRING;
        self
    }

    pub fn set_list(mut self) -> Self {
        self.value_encoding = Encoding::VALUE_LIST;
        self
    }

    pub fn set_hash(mut self) -> Self {
        self.value_encoding = Encoding::VALUE_HASH;
        self
    }
}

pub trait ValueTypeIs {
    /// Return true if this instance type equals the `type_bit`
    fn is_type(&self, type_bit: u8) -> bool;
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
