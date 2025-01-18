use crate::{
    metadata::{CommonValueMetadata, Expiration},
    SableError, U8ArrayBuilder, U8ArrayReader,
};

/// Contains information regarding the String type metadata
#[derive(Clone, Debug)]
pub struct LockValueMetadata {
    common: CommonValueMetadata,
}

impl LockValueMetadata {
    pub const SIZE: usize = CommonValueMetadata::SIZE;

    pub fn new() -> Self {
        LockValueMetadata {
            common: CommonValueMetadata::default().set_lock(),
        }
    }

    /// Serialise this object into `BytesMut`
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        self.common.to_bytes(builder)
    }

    pub fn from_bytes(reader: &mut U8ArrayReader) -> Result<Self, SableError> {
        Ok(LockValueMetadata {
            common: CommonValueMetadata::from_bytes(reader)?,
        })
    }

    pub fn expiration(&self) -> &Expiration {
        self.common.expiration()
    }

    pub fn expiration_mut(&mut self) -> &mut Expiration {
        self.common.expiration_mut()
    }

    pub fn common_metadata(&self) -> &CommonValueMetadata {
        &self.common
    }
}

impl Default for LockValueMetadata {
    fn default() -> Self {
        Self::new()
    }
}
