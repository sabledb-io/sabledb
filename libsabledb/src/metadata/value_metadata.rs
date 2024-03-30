use crate::{BytesMutUtils, Expiration};
use bytes::BytesMut;

/// Contains information regarding the String type metadata
#[derive(Clone, Debug)]
pub struct CommonValueMetadata {
    value_type: u8,
    /// Value ttl information
    expiration: Expiration,
}

impl Default for CommonValueMetadata {
    fn default() -> Self {
        CommonValueMetadata {
            value_type: CommonValueMetadata::VALUE_STR,
            expiration: Expiration::default(),
        }
    }
}

#[allow(dead_code)]
impl CommonValueMetadata {
    pub const SIZE: usize = std::mem::size_of::<u8>() + Expiration::SIZE;
    pub const VALUE_STR: u8 = 0;
    pub const VALUE_LIST: u8 = 1;

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::with_capacity(CommonValueMetadata::SIZE);
        as_bytes.extend_from_slice(&BytesMutUtils::from_u8(&self.value_type));
        as_bytes.extend_from_slice(&self.expiration.to_bytes());
        as_bytes
    }

    pub fn from_bytes(buf: &BytesMut) -> Self {
        let mut pos = 0usize;
        let mut de = Self::default();

        de.value_type = BytesMutUtils::to_u8(&BytesMut::from(
            &buf[pos..std::mem::size_of_val(&de.value_type)],
        ));
        pos += std::mem::size_of_val(&de.value_type);

        de.expiration = Expiration::from_bytes(&BytesMut::from(&buf[pos..]));
        de
    }

    pub fn expiration(&self) -> &Expiration {
        &self.expiration
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        &mut self.expiration
    }

    pub fn is_string(&self) -> bool {
        self.value_type == CommonValueMetadata::VALUE_STR
    }

    pub fn is_list(&self) -> bool {
        self.value_type == CommonValueMetadata::VALUE_LIST
    }

    pub fn value_type(&self) -> u8 {
        self.value_type
    }

    pub fn set_string(mut self) -> Self {
        self.value_type = CommonValueMetadata::VALUE_STR;
        self
    }

    pub fn set_list(mut self) -> Self {
        self.value_type = CommonValueMetadata::VALUE_LIST;
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
        md.expiration_mut().set_ttl_millis(30)?;

        let mut arr = md.to_bytes();
        assert_eq!(arr.len(), CommonValueMetadata::SIZE);

        // the buffer can be larger than `Metadata`
        arr.extend_from_slice(&[5, 5]);

        // Check that we can de-serialize it
        let deserialized_md = CommonValueMetadata::from_bytes(&arr);

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
