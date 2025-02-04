use crate::utils::SLOT_SIZE;
#[allow(unused_imports)]
use crate::{
    io::TempFile,
    replication::StorageUpdatesRecord,
    storage::BatchUpdate,
    storage::GenericDb,
    utils::{U8ArrayBuilder, U8ArrayReader},
    ClientState, KeyType, SableError, Server, StorageAdapter, ToU8Writer,
};

use bytes::{Buf, BytesMut};
use enum_iterator::next;
use std::fs::File as StdFile;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;

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

    /// Return true if all `slots` are set
    pub fn is_set_multi(&self, slots: &[u16]) -> Result<bool, SableError> {
        for slot in slots {
            if !self.is_set(*slot)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Iniitialise self from string
    /// For example:
    ///
    /// ```no_compile
    /// let slots = SlotBitmap::default();
    /// slots.from_string("0-9000,10000")?;
    /// assert(slots.is_set(10000));
    /// for i in 0..9000 {
    ///     assert!(slots.is_set(i));
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

pub struct SlotFileIterator {
    buffer: BytesMut,
    fp: StdFile,
}

impl SlotFileIterator {
    pub fn new(filepath: &Path) -> Result<Self, SableError> {
        let fp = StdFile::options().read(true).open(filepath)?;
        Ok(SlotFileIterator {
            buffer: BytesMut::new(),
            fp,
        })
    }

    fn read_buffer(&mut self) -> Result<bool, SableError> {
        if !self.buffer.is_empty() {
            return Ok(false);
        }
        const BYTES_TO_READ: usize = std::mem::size_of::<usize>();
        let mut chunk_len_bytes = [0u8; BYTES_TO_READ];
        self.fp.read_exact(&mut chunk_len_bytes)?;
        let chunk_len = usize::from_be_bytes(chunk_len_bytes);
        tracing::debug!("Chunk size is: {:?} => {} ", chunk_len_bytes, chunk_len);
        self.buffer.resize(chunk_len, 0u8);
        // read exactly chunk size from the file
        Ok(self.fp.read(self.buffer.as_mut())? == chunk_len)
    }
}

impl Iterator for SlotFileIterator {
    type Item = StorageUpdatesRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            // Read next chunk from the disk
            if !self.read_buffer().ok()? {
                return None;
            }
        }

        let mut reader = U8ArrayReader::with_buffer(&self.buffer);
        let Ok(record) = StorageUpdatesRecord::from(&mut reader) else {
            return None;
        };

        self.buffer.advance(reader.consumed());
        Some(record)
    }
}

pub struct SlotFileExporter<'a> {
    prefix_arr: Vec<BytesMut>,
    db: &'a StorageAdapter,
    /// Maximum chunk size
    max_chunk_size: usize,
    slot_number: u16,
}

impl<'a> SlotFileExporter<'a> {
    pub fn new(
        db: &'a StorageAdapter,
        db_id: u16,
        slot: u16,
        max_chunk_size: usize,
    ) -> Result<Self, SableError> {
        let slot = Slot::with_slot(slot);
        let prefix_arr = slot.create_prefix_array(db_id)?;
        Ok(SlotFileExporter {
            prefix_arr,
            db,
            max_chunk_size,
            slot_number: slot.slot,
        })
    }

    /// Export slot content to a file. The format used:
    /// [ <chunk_len> | <chunk_content> |... ]
    ///
    /// Chunk content:
    /// [ PutRecord | PutRecord ... ]
    pub async fn export(&mut self) -> Result<Option<PathBuf>, SableError> {
        if self.prefix_arr.is_empty() {
            return Ok(None);
        };

        let fullpath = TempFile::create_path(format!("slot_{}", self.slot_number).as_str());

        let mut fp = TokioFile::create(&fullpath).await?;
        let mut chunk = BytesMut::with_capacity(self.max_chunk_size);
        let mut buffer_builder = U8ArrayBuilder::with_buffer(&mut chunk);
        for key_type_prefix in &self.prefix_arr {
            let mut db_iter = self.db.create_iterator(key_type_prefix)?;
            while db_iter.valid() {
                let Some((key, value)) = db_iter.key_value() else {
                    break;
                };

                if !key.starts_with(key_type_prefix) {
                    break;
                }

                StorageUpdatesRecord::serialise_put(&mut buffer_builder, key, value);
                if buffer_builder.len() >= self.max_chunk_size {
                    Self::flush_chunk(&mut fp, &mut chunk).await?;
                    buffer_builder = U8ArrayBuilder::with_buffer(&mut chunk);
                }
                db_iter.next();
            }
        }

        if !chunk.is_empty() {
            Self::flush_chunk(&mut fp, &mut chunk).await?;
        }
        fp.sync_data().await?;
        Ok(Some(PathBuf::from(fullpath)))
    }

    async fn flush_chunk(fp: &mut TokioFile, chunk: &mut BytesMut) -> Result<(), SableError> {
        let chunk_size_bytes = chunk.len().to_be_bytes();
        tracing::info!("Writing chunk size: {:?}", chunk_size_bytes);
        fp.write_all(&chunk_size_bytes).await?;
        // Write the chunk content
        fp.write_all_buf(chunk).await?;
        fp.flush().await?;
        chunk.clear();
        Ok(())
    }
}

pub struct SlotFileImporter<'a> {
    db: &'a StorageAdapter,
    filepath: PathBuf,
}

impl<'a> SlotFileImporter<'a> {
    pub fn new(db: &'a StorageAdapter, filepath: PathBuf) -> Self {
        SlotFileImporter { db, filepath }
    }

    /// Construct an iterator over the file and apply all records into the database
    pub fn import(&self) -> Result<(), SableError> {
        const MAX_BATCH_SIZE: usize = 10_000;
        tracing::info!("Importing slot to the database...");
        let mut batch_update = BatchUpdate::with_capacity(MAX_BATCH_SIZE);
        let file_iter = SlotFileIterator::new(&self.filepath)?;
        let mut del_ops = 0usize;
        let mut put_ops = 0usize;
        for record in file_iter {
            match record {
                StorageUpdatesRecord::Put { key, value } => {
                    batch_update.put(key, value);
                    put_ops = put_ops.saturating_add(1);
                }
                StorageUpdatesRecord::Del { key } => {
                    batch_update.delete(key);
                    del_ops = del_ops.saturating_add(1);
                }
            }
            if batch_update.len() % MAX_BATCH_SIZE == 0 {
                self.db.apply_batch(&batch_update)?;
                batch_update.clear();
            }
        }

        // make sure all items are applied
        if !batch_update.is_empty() {
            self.db.apply_batch(&batch_update)?;
            batch_update.clear();
        }

        tracing::info!(
            "Importing slot to the database completed successfully. {} put calls, {} delete calls",
            put_ops,
            del_ops
        );
        Ok(())
    }
}

// Slot operations
#[derive(Default)]
pub struct Slot {
    slot: u16,
}

impl Slot {
    pub fn with_slot(slot: u16) -> Self {
        Slot { slot }
    }

    /// Return a prefix bytes array that can be used a filter when iterating the database searching for items belonged to the
    /// current slot. The array contains prefix per key type (PrimaryKey, ListItem etc)
    pub fn prefix(&self, db_id: u16) -> Result<Vec<BytesMut>, SableError> {
        self.create_prefix_array(db_id)
    }

    /// Count the number of items belong to this slot exist in the database
    pub async fn count(&self, client_state: Rc<ClientState>) -> Result<u64, SableError> {
        // When counting items, we only count the primary data types (String, List, Hash, etc) but not their children
        let prefix = Self::create_prefix_for_key_type(
            KeyType::PrimaryKey,
            client_state.database_id(),
            self.slot,
        );
        let mut db_iter = client_state.database().create_iterator(&prefix)?;
        let mut items_count = 0u64;
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            items_count = items_count.saturating_add(1);
            db_iter.next();
            if items_count.rem_euclid(10_000) == 0 {
                // Let other tasks process as well
                tokio::task::yield_now().await;
            }
        }
        Ok(items_count)
    }

    /// Count the number of items belong to this slot exist in the database
    pub async fn create_chunk(&self, client_state: Rc<ClientState>) -> Result<u64, SableError> {
        // When counting items, we only count the primary data types (String, List, Hash, etc) but not their children
        let prefix = Self::create_prefix_for_key_type(
            KeyType::PrimaryKey,
            client_state.database_id(),
            self.slot,
        );
        let mut db_iter = client_state.database().create_iterator(&prefix)?;
        let mut items_count = 0u64;
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            items_count = items_count.saturating_add(1);
            db_iter.next();

            if items_count.rem_euclid(10_000) == 0 {
                // Let other tasks process as well
                tokio::task::yield_now().await;
            }
        }
        Ok(items_count)
    }

    /// Records are kept in the database in the form of:
    /// [KeyType, u16(db_id), u16(slot)] (see `KeyPrefix` struct)
    /// this method creates an array of prefixes that allow us to iterate the database
    /// over all items belong to the slot
    fn create_prefix_array(&self, db_id: u16) -> Result<Vec<BytesMut>, SableError> {
        let mut prefix_arr = Vec::<BytesMut>::default();
        let mut key_type = KeyType::Bookkeeping;
        loop {
            key_type = next(&key_type).ok_or(SableError::InternalError(
                "Expected valid enumerator".into(),
            ))?;

            if key_type.eq(&KeyType::Metadata) {
                break;
            }
            prefix_arr.push(Self::create_prefix_for_key_type(key_type, db_id, self.slot));
        }
        Ok(prefix_arr)
    }

    fn create_prefix_for_key_type(key_type: KeyType, db_id: u16, slot: u16) -> BytesMut {
        let mut prefix = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut prefix);
        crate::metadata::KeyPrefix::new(key_type, db_id, slot).to_writer(&mut builder);
        prefix
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
    fn test_slot_prefix() {
        // Check that generate prefix covers all the known key types
        let key_type_vec = vec![
            Slot::create_prefix_for_key_type(KeyType::PrimaryKey, 0, 10),
            Slot::create_prefix_for_key_type(KeyType::ListItem, 0, 10),
            Slot::create_prefix_for_key_type(KeyType::HashItem, 0, 10),
            Slot::create_prefix_for_key_type(KeyType::ZsetMemberItem, 0, 10),
            Slot::create_prefix_for_key_type(KeyType::ZsetScoreItem, 0, 10),
            Slot::create_prefix_for_key_type(KeyType::SetItem, 0, 10),
        ];

        let slot = Slot::with_slot(10);
        let prefix_arr = slot.create_prefix_array(0).unwrap();
        assert_eq!(prefix_arr, key_type_vec);
    }

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

    /// Test dumping slot content into file
    #[test]
    fn test_slot_dump_to_file() {
        use crate::{
            metadata::StringValueMetadata,
            storage::PutFlags,
            storage::{StringGetResult, StringsDb},
            utils,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let mut string_db = StringsDb::with_storage(&store, 0);
            let base_key = BytesMut::from(String::from("{1}key_0").as_bytes());

            let slot_number = utils::calculate_slot(&base_key);
            let md = StringValueMetadata::default();
            const RECORD_COUNT: usize = 10_000;
            for i in 0..RECORD_COUNT {
                let key = format!("{{1}}key_{i}");
                let value = format!("value_{i}");

                let key = BytesMut::from(key.as_bytes());
                let value = BytesMut::from(value.as_bytes());
                string_db
                    .put(&key, &value, &md, PutFlags::Override)
                    .unwrap();

                let StringGetResult::Some(_) = string_db.get(&key).unwrap() else {
                    eprintln!("error getting value");
                    std::process::exit(1);
                };
            }

            let mut slot_file = SlotFileExporter::new(&store, 0, slot_number, 1 << 20).unwrap();
            let filepath = slot_file.export().await.unwrap().unwrap();
            println!("Successfully written file: {filepath:?}");

            let file_iter = SlotFileIterator::new(&filepath).unwrap();
            let mut put_records = 0usize;
            let mut del_records = 0usize;
            for record in file_iter {
                match record {
                    StorageUpdatesRecord::Put { key: _, value: _ } => {
                        put_records = put_records.saturating_add(1);
                    }
                    StorageUpdatesRecord::Del { key: _ } => {
                        del_records = del_records.saturating_add(1);
                    }
                }
            }

            assert_eq!(put_records, RECORD_COUNT);
            assert_eq!(del_records, 0);
            let _ = std::fs::remove_file(&filepath);
        });
    }
}
