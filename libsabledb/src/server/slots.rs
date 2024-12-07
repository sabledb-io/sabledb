use crate::utils::SLOT_SIZE;
use crate::SableError;

#[derive(Clone, Debug)]
pub struct SlotBitmap {
    bits: Vec<u128>,
}

impl Default for SlotBitmap {
    fn default() -> Self {
        let mut bits = Vec::<u128>::default();
        bits.resize(128, 0);
        SlotBitmap { bits }
    }
}

impl SlotBitmap {
    /// Set or clear `slot` in this slots set
    pub fn set(&mut self, slot: u16, b: bool) -> Result<(), SableError> {
        // Find the bucket that holds the bit
        let bucket = self.find_bucket_mut(&slot)?;
        let bit = Self::position_in_bucket(&slot);
        if b {
            *bucket |= bit;
        } else {
            *bucket &= !bit;
        }
        Ok(())
    }

    /// Return true if `slot` is enabled
    pub fn is_set(&self, slot: u16) -> Result<bool, SableError> {
        let bucket = self.find_bucket(&slot)?;
        let bit = Self::position_in_bucket(&slot);
        Ok((bucket & bit) != 0)
    }

    fn find_bucket_mut(&mut self, slot: &u16) -> Result<&mut u128, SableError> {
        let bucket_idx = slot.div_euclid(128) as usize;
        if bucket_idx >= self.bits.len() {
            return Err(SableError::IndexOutOfRange(format!(
                "slot {} exceeds the max slot number of {}",
                slot, SLOT_SIZE
            )));
        }

        let Some(bucket) = self.bits.get_mut(bucket_idx) else {
            return Err(SableError::InternalError(format!(
                "could not find bucket index {} for slot {}",
                bucket_idx, slot
            )));
        };
        Ok(bucket)
    }

    fn find_bucket(&self, slot: &u16) -> Result<&u128, SableError> {
        let bucket_idx = slot.div_euclid(128) as usize;
        if bucket_idx >= self.bits.len() {
            return Err(SableError::IndexOutOfRange(format!(
                "slot {} exceeds the max slot number of {}",
                slot, SLOT_SIZE
            )));
        }

        let Some(bucket) = self.bits.get(bucket_idx) else {
            return Err(SableError::InternalError(format!(
                "could not find bucket index {} for slot {}",
                bucket_idx, slot
            )));
        };
        Ok(bucket)
    }

    fn position_in_bucket(slot: &u16) -> u128 {
        let index_in_bucket = slot.rem_euclid(128) as usize;
        1u128 << index_in_bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_is_set() {
        let mut slots = SlotBitmap::default();
        assert!(slots.set(SLOT_SIZE + 1, true).is_err());
        assert!(slots.set(SLOT_SIZE, true).is_err());
        assert!(slots.is_set(SLOT_SIZE).is_err());

        assert!(slots.set(129, true).is_ok());
        assert!(slots.is_set(129).unwrap());
        assert!(!slots.is_set(13456).unwrap());
        assert!(slots.set(13456, true).is_ok());
        assert!(slots.is_set(13456).unwrap());
    }
}
