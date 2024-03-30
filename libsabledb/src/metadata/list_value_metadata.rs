use crate::{
    metadata::{CommonValueMetadata, Expiration, ValueTypeIs},
    BytesMutUtils,
};
use bytes::BytesMut;

/// Contains information regarding the String type metadata
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ListValueMetadata {
    common: CommonValueMetadata,
    head_id: u64,
    tail_id: u64,
    list_size: u64,
    list_id: u64,
}

#[allow(dead_code)]
impl ListValueMetadata {
    pub const SIZE: usize = 4 * std::mem::size_of::<u64>() + CommonValueMetadata::SIZE;

    pub fn new() -> Self {
        ListValueMetadata {
            common: CommonValueMetadata::default().set_list(),
            head_id: 0,
            tail_id: 0,
            list_size: 0,
            list_id: 0,
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self) -> BytesMut {
        let mut as_bytes = BytesMut::with_capacity(ListValueMetadata::SIZE);
        as_bytes.extend_from_slice(&self.common.to_bytes());
        as_bytes.extend_from_slice(&BytesMutUtils::from_u64(&self.head_id));
        as_bytes.extend_from_slice(&BytesMutUtils::from_u64(&self.tail_id));
        as_bytes.extend_from_slice(&BytesMutUtils::from_u64(&self.list_size));
        as_bytes.extend_from_slice(&BytesMutUtils::from_u64(&self.list_id));
        as_bytes
    }

    #[allow(clippy::field_reassign_with_default)]
    pub fn from_bytes(buf: &BytesMut) -> Self {
        let mut pos = 0usize;
        let mut de = Self::default();
        de.common =
            CommonValueMetadata::from_bytes(&BytesMut::from(&buf[pos..CommonValueMetadata::SIZE]));
        pos += CommonValueMetadata::SIZE;

        de.head_id = BytesMutUtils::to_u64(&BytesMut::from(&buf[pos..]));
        pos += std::mem::size_of_val(&de.head_id);

        de.tail_id = BytesMutUtils::to_u64(&BytesMut::from(&buf[pos..]));
        pos += std::mem::size_of_val(&de.tail_id);

        de.list_size = BytesMutUtils::to_u64(&BytesMut::from(&buf[pos..]));
        pos += std::mem::size_of_val(&de.list_size);

        de.list_id = BytesMutUtils::to_u64(&BytesMut::from(&buf[pos..]));
        de
    }

    pub fn expiration(&self) -> &Expiration {
        self.common.expiration()
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        self.common.expiration_mut()
    }

    pub fn head(&self) -> u64 {
        self.head_id
    }

    pub fn tail(&self) -> u64 {
        self.tail_id
    }

    pub fn id(&self) -> u64 {
        self.list_id
    }

    pub fn set_id(&mut self, list_id: u64) {
        self.list_id = list_id;
    }

    pub fn set_head(&mut self, item_id: u64) {
        self.head_id = item_id;
    }

    pub fn set_tail(&mut self, item_id: u64) {
        self.tail_id = item_id;
    }

    pub fn len(&self) -> u64 {
        self.list_size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_len(&mut self, new_len: u64) {
        self.list_size = new_len;
    }
}

impl Default for ListValueMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueTypeIs for ListValueMetadata {
    fn is_type(&self, type_bit: u8) -> bool {
        self.common.value_type() == type_bit
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
        let mut md = ListValueMetadata::new();
        md.expiration_mut().set_ttl_millis(30)?;
        md.set_id(42);
        md.set_head(121);
        md.set_tail(420);
        md.set_len(15);

        let mut arr = md.to_bytes();
        assert_eq!(arr.len(), ListValueMetadata::SIZE);

        // the buffer can be larger than `Metadata`
        arr.extend_from_slice(&[5, 5]);

        // Check that we can de-serialize it
        let deserialized_md = ListValueMetadata::from_bytes(&arr);

        // remove the deserialized part
        let _ = arr.split_to(ListValueMetadata::SIZE);

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
        assert_eq!(deserialized_md.id(), 42);
        assert_eq!(deserialized_md.head(), 121);
        assert_eq!(deserialized_md.tail(), 420);
        assert_eq!(deserialized_md.len(), 15);
        assert_eq!(&arr[..], &[5, 5]);
        Ok(())
    }

    #[test]
    fn test_expire_api() -> Result<(), SableError> {
        let mut md = ListValueMetadata::new();
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
