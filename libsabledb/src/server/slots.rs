use crate::utils::SLOT_SIZE;
use crate::SableError;
use std::str::FromStr;
use std::string::ToString;

#[derive(Clone, Debug, PartialEq)]
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

/// This provides a mapping of slots owned by this instance of SableDB.
/// Each slot uses a single bit to mark whether it is owned by this instance or not.
/// We use an array of 128 `u128` primitives to keep track of the slots (total of 16K memory)
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

impl FromStr for SlotBitmap {
    type Err = SableError;
    /// Parse `s` into `SlotBitmap`
    ///
    /// The expected format is a comma separated list of slots. Range of slots
    /// can be represented by `start - end` pairs.
    /// For example, in order to represent slots from `0` - `10000` and also include the slot `15000`
    /// we can use the following string format:
    /// `0-10000,15000`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut slots = SlotBitmap::default();
        let tokens: Vec<&str> = s.split(',').map(|s| s.trim()).collect();
        let into_sable_err =
            |e| SableError::OtherError(format!("Failed to parse string into u16. {}", e));
        for token in &tokens {
            if let Some(hyphen_pos) = token.find('-') {
                let first = token[0..hyphen_pos]
                    .trim()
                    .parse::<u16>()
                    .map_err(into_sable_err)?;
                let second = token[hyphen_pos + 1..] // + 1 : skip the hyphen
                    .trim()
                    .parse::<u16>()
                    .map_err(into_sable_err)?;
                if second > first {
                    for slot in first..=second {
                        slots.set(slot, true)?;
                    }
                }
            } else {
                // single token
                let slot = token.parse::<u16>().map_err(into_sable_err)?;
                slots.set(slot, true)?;
            }
        }
        Ok(slots)
    }
}

#[derive(Default)]
struct SlotRange {
    start: Option<u16>,
    end: Option<u16>,
    length: u16,
}

impl SlotRange {
    pub fn try_add(&mut self, slot: u16) -> bool {
        let (Some(_), Some(end)) = (&mut self.start, &mut self.end) else {
            self.start = Some(slot);
            self.end = Some(slot);
            self.length = 1;
            return true;
        };

        if end.saturating_add(1) == slot {
            *end = slot;
            self.length = self.length.saturating_add(1);
            true
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u16 {
        self.length
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Some(start), Some(end)) = (&self.start, &self.end) else {
            return write!(f, "");
        };

        if self.length == 1 {
            write!(f, "{}", start)
        } else {
            write!(f, "{}-{}", start, end)
        }
    }
}

impl std::fmt::Display for SlotBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ranges = Vec::<SlotRange>::default();

        let mut slot_range = SlotRange::default();
        for slot in 0..SLOT_SIZE {
            if self.is_set(slot).expect("slot not in range") {
                if slot_range.try_add(slot) {
                    continue;
                } else {
                    // close this range and start a new one
                    ranges.push(slot_range);
                    slot_range = SlotRange::default();
                    slot_range.try_add(slot); // should not fail
                }
            }
        }

        if !slot_range.is_empty() {
            ranges.push(slot_range);
        }

        // Join the vector into a string
        write!(
            f,
            "{}",
            ranges
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )
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
        assert!(slots.set(13456, false).is_ok());
        assert!(!slots.is_set(13456).unwrap());
    }

    #[test]
    fn test_slot_bitmap_to_string() {
        let mut slots = SlotBitmap::default();
        slots.set(0, true).unwrap();
        slots.set(2, true).unwrap();
        slots.set(3, true).unwrap();
        slots.set(5, true).unwrap();
        slots.set(8000, true).unwrap();
        slots.set(8001, true).unwrap();
        slots.set(8003, true).unwrap();

        assert_eq!("0,2-3,5,8000-8001,8003", slots.to_string());

        let slots2 = SlotBitmap::from_str("0,2-3,5,8000-8001,8003").unwrap();
        assert_eq!(slots, slots2);
    }
}
