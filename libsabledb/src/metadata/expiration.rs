use crate::{BytesMutUtils, SableError, TimeUtils, U8ArrayBuilder, U8ArrayReader};
use bytes::BytesMut;

#[derive(Clone, Debug)]
pub struct Expiration {
    /// last updated, ms since UNIX_EPOCH
    pub last_updated: u64,
    /// expiration date (in milliseconds), an example value : `2000ms`
    pub ttl_ms: u64,
}

impl Default for Expiration {
    fn default() -> Self {
        Expiration {
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("duration_since failed")
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            ttl_ms: u64::MAX,
        }
    }
}

impl Expiration {
    pub const SIZE: usize = std::mem::size_of::<u64>() + std::mem::size_of::<u64>();
    pub fn to_bytes(&self, builder: &mut U8ArrayBuilder) {
        builder.write_u64(self.last_updated);
        builder.write_u64(self.ttl_ms);
    }

    pub fn from_bytes(bytearr: &BytesMut) -> Self {
        let mut de_expiration = Expiration::default();
        let u64_len = std::mem::size_of::<u64>();

        de_expiration.last_updated = BytesMutUtils::to_u64(&BytesMut::from(&bytearr[..u64_len]));
        de_expiration.ttl_ms = BytesMutUtils::to_u64(&BytesMut::from(&bytearr[u64_len..]));
        de_expiration
    }

    /// return true if this record expired
    pub fn is_expired(&self) -> Result<bool, SableError> {
        Ok(self.ttl_in_millis()? == 0)
    }

    /// Returns the remaining time to live (in millis) of a key that has a timeout
    /// If key has no timeout, return `u64::MAX`
    pub fn ttl_in_millis(&self) -> Result<u64, SableError> {
        if self.ttl_ms == u64::MAX {
            return Ok(u64::MAX);
        }

        let now_in_millis = TimeUtils::epoch_ms()?;
        Ok(self
            .last_updated
            .saturating_add(self.ttl_ms)
            .saturating_sub(now_in_millis))
    }

    /// Returns the remaining time to live (in seconds) of a key that has a timeout
    /// If key has no timeout, return `u64::MAX`
    pub fn ttl_in_seconds(&self) -> Result<u64, SableError> {
        if self.ttl_ms == u64::MAX {
            return Ok(u64::MAX);
        }

        let ttl_in_ms = self.ttl_in_millis()? as f64;
        let ttl_seconds = (ttl_in_ms / 1000_f64).ceil();
        Ok(ttl_seconds as u64)
    }

    /// Does this item has ttl set?
    pub fn has_ttl(&self) -> bool {
        self.ttl_ms != u64::MAX
    }

    /// Remove the expiration date for this key
    pub fn set_no_expiration(&mut self) -> Result<(), SableError> {
        self.ttl_ms = u64::MAX;
        self.last_updated = TimeUtils::epoch_ms()?;
        Ok(())
    }

    /// Update the key ttl in seconds.
    /// This call also updates the `last_updated` field
    pub fn set_ttl_seconds(&mut self, ttl_secs: u64) -> Result<(), SableError> {
        self.ttl_ms = ttl_secs.saturating_mul(1000);
        self.last_updated = TimeUtils::epoch_ms()?;
        Ok(())
    }

    /// Update the key ttl in milliseconds.
    /// This call also updates the `last_updated` field
    pub fn set_ttl_millis(&mut self, ttl_ms: u64) -> Result<(), SableError> {
        self.ttl_ms = ttl_ms;
        self.last_updated = TimeUtils::epoch_ms()?;
        Ok(())
    }

    /// Set key expiration timestamp (millis since UNIX_EPOCH)
    pub fn set_expire_timestamp_millis(
        &mut self,
        expire_timestamp_ms: u64,
    ) -> Result<(), SableError> {
        let now = TimeUtils::epoch_ms()?;
        self.ttl_ms = expire_timestamp_ms.saturating_sub(now);
        self.last_updated = now;
        Ok(())
    }

    /// Set key expiration timestamp (seconds since UNIX_EPOCH)
    pub fn set_expire_timestamp_seconds(
        &mut self,
        expire_timestamp_secs: u64,
    ) -> Result<(), SableError> {
        self.set_expire_timestamp_millis(expire_timestamp_secs.saturating_mul(1000))
    }
}
