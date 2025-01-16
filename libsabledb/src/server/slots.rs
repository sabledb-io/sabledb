use crate::utils::SLOT_SIZE;
use crate::SableError;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SlotBitmap {
    bits: Vec<Arc<AtomicU64>>,
}

impl Default for SlotBitmap {
    /// Construct a default empty `SlotBitmap` (i.e. the newly created instance of `SlotBitmap` owns no slots)
    fn default() -> Self {
        let mut bits = Vec::<Arc<AtomicU64>>::with_capacity(256);
        for _ in 0..256 {
            bits.push(Arc::<AtomicU64>::default());
        }
        SlotBitmap { bits }
    }
}

impl PartialEq for SlotBitmap {
    fn eq(&self, other: &SlotBitmap) -> bool {
        if self.bits.len() != other.bits.len() {
            return false;
        }

        // Compare each pair of items in the vector
        let count = self
            .bits
            .iter()
            .zip(other.bits.iter())
            .filter(|(a, b)| a.load(Ordering::Relaxed) == b.load(Ordering::Relaxed))
            .count();
        count == self.bits.len()
    }
}

/// This provides a mapping of slots owned by this instance of SableDB.
/// Each slot uses a single bit to mark whether it is owned by this instance or not.
/// We use an array of 256 items of `AtomicU64` primitives to keep track of the slots (total of ~16K memory)
impl SlotBitmap {
    /// Set or clear `slot` in this slots set
    pub fn set(&self, slot: u16, b: bool) -> Result<(), SableError> {
        // Find the bucket that holds the bit
        let bucket = self.find_bucket(&slot)?;
        let bit = Self::position_in_bucket(&slot);
        if b {
            // this equals: bucket |= bit
            bucket.fetch_or(bit, Ordering::SeqCst);
        } else {
            // this equals: bucket &= !bit
            bucket.fetch_and(!bit, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Enable all slots for this instance of `SlotBitmap`. This operation is faster than looping over all
    /// slots in the range.
    /// Note that this operation is thread-safe, but not atomic
    pub fn set_all(&self) {
        for bucket in &self.bits {
            bucket.store(u64::MAX, Ordering::SeqCst);
        }
    }

    /// Clear all slots for this instance of `SlotBitmap`. This operation is faster than looping over all
    /// slots in the range.
    /// Note that this operation is thread-safe, but not atomic
    pub fn clear_all(&self) {
        for bucket in &self.bits {
            bucket.store(0, Ordering::SeqCst);
        }
    }

    /// Return true if `slot` is enabled
    pub fn is_set(&self, slot: u16) -> Result<bool, SableError> {
        let bucket = self.find_bucket(&slot)?;
        let bit = Self::position_in_bucket(&slot);
        Ok((bucket.load(Ordering::SeqCst) & bit) != 0)
    }

    /// Iniitialise self from string
    /// For example:
    ///
    /// ```
    /// let slots = SlotBitmap::default();
    /// slots.from_string("0-9000,10000")?;
    /// assert(slots.is_set(10000));
    /// for i in 0..9000 {
    ///     assert(slots.is_set(i));
    /// }
    /// ```
    pub fn from_string(&self, s: &str) -> Result<(), SableError> {
        let other = Self::from_str(s)?;
        self.bits
            .iter()
            .zip(other.bits.iter())
            .for_each(|(a, b)| a.store(b.load(Ordering::Relaxed), Ordering::Relaxed));
        Ok(())
    }

    fn find_bucket(&self, slot: &u16) -> Result<Arc<AtomicU64>, SableError> {
        let bucket_idx = slot.div_euclid(64) as usize;
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
        Ok(bucket.clone())
    }

    fn position_in_bucket(slot: &u16) -> u64 {
        let index_in_bucket = slot.rem_euclid(64) as usize;
        1u64 << index_in_bucket
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
        let slots = SlotBitmap::default();
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

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_is_set() {
        let slots = SlotBitmap::default();
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
        // this one was never set, make sure its 0
        assert!(!slots.is_set(234).unwrap());
    }

    #[test]
    fn test_slot_bitmap_to_string() {
        let slots = SlotBitmap::default();
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

    #[test]
    fn test_all_set() {
        // All slots are disabled by default
        let slots = SlotBitmap::default();
        for slot in 0..SLOT_SIZE {
            assert!(!slots.is_set(slot).unwrap());
        }

        slots.set_all();

        for slot in 0..SLOT_SIZE {
            assert!(slots.is_set(slot).unwrap());
        }
    }

    #[test]
    fn test_clear_all() {
        // All slots are disabled by default
        let slots = SlotBitmap::default();
        slots.set_all();
        for slot in 0..SLOT_SIZE {
            assert!(slots.is_set(slot).unwrap());
        }

        slots.clear_all();

        for slot in 0..SLOT_SIZE {
            assert!(!slots.is_set(slot).unwrap());
        }
    }

    #[test]
    fn test_from_string() {
        let orig = SlotBitmap::default();
        for slot in 0..SLOT_SIZE {
            assert!(!orig.is_set(slot).unwrap());
        }
        orig.from_string("0-8000,10000").unwrap();
        for slot in 0..8000 {
            match slot {
                a if a < 8000 => {
                    // 0 -> 8000
                    assert!(orig.is_set(slot).unwrap());
                }
                10_000 => {
                    // 10,000
                    assert!(orig.is_set(slot).unwrap());
                }
                _ => {
                    // Anything else should NOT be set
                    assert!(!orig.is_set(slot).unwrap());
                }
            }
        }
    }
}
