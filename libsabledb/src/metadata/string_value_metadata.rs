use crate::{
    metadata::{CommonValueMetadata, Expiration, ValueTypeIs},
    SableError, U8ArrayBuilder, U8ArrayReader,
};

/// Contains information regarding the String type metadata
#[derive(Clone, Debug)]
pub struct StringValueMetadata {
    common: CommonValueMetadata,
}

#[allow(dead_code)]
impl StringValueMetadata {
    pub const SIZE: usize = CommonValueMetadata::SIZE;

    pub fn new() -> Self {
        StringValueMetadata {
            common: CommonValueMetadata::default().set_string(),
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder)
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        Ok(StringValueMetadata {
            common: CommonValueMetadata::from_bytes(reader)?,
        })
    }

    pub fn expiration(&self) -> &Expiration {
        self.common.expiration()
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        self.common.expiration_mut()
    }
}

impl ValueTypeIs for StringValueMetadata {
    fn is_type(&self, type_bit: u8) -> bool {
        self.common.value_type() == type_bit
    }
}

impl Default for StringValueMetadata {
    fn default() -> Self {
        Self::new()
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
        let mut md = StringValueMetadata::new();
        md.expiration_mut().set_ttl_millis(30)?;

        let mut arr = bytes::BytesMut::with_capacity(StringValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut arr);

        md.to_bytes(&mut builder);
        assert_eq!(arr.len(), StringValueMetadata::SIZE);

        // the buffer can be larger than `Metadata`
        arr.extend_from_slice(&[5, 5]);

        // Check that we can de-serialize it
        let mut reader = U8ArrayReader::with_buffer(&arr);
        let deserialized_md = StringValueMetadata::from_bytes(&mut reader)?;

        // remove the deserialized part
        let _ = arr.split_to(StringValueMetadata::SIZE);

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
        let mut md = StringValueMetadata::new();
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
