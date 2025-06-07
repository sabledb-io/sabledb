use crate::{
    storage::{BatchUpdate, GetChangesLimits, PutFlags, StorageUpdates},
    SableError,
};
use bytes::BytesMut;
use nohash_hasher::IntMap;
use std::path::Path;
use std::rc::Rc;

pub enum StorageIterator<'a> {
    RocksDb(rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DB>),
    RocksDbReverse(rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DB>),
}

pub struct IteratorAdapter<'a> {
    pub iterator: StorageIterator<'a>,
}

impl IteratorAdapter<'_> {
    /// Seek the iterator to `prefix` or the first entry that lexicographically follows it
    pub fn seek(&mut self, prefix: &BytesMut) {
        match self.iterator {
            StorageIterator::RocksDb(ref mut rocksdb_iter) => rocksdb_iter.seek(prefix),
            StorageIterator::RocksDbReverse(ref mut rocksdb_iter) => rocksdb_iter.seek(prefix),
        }
    }

    pub fn valid(&self) -> bool {
        match &self.iterator {
            StorageIterator::RocksDb(rocksdb_iter) => rocksdb_iter.valid(),
            StorageIterator::RocksDbReverse(rocksdb_iter) => rocksdb_iter.valid(),
        }
    }

    pub fn next(&mut self) {
        match &mut self.iterator {
            StorageIterator::RocksDb(rocksdb_iter) => rocksdb_iter.next(),
            StorageIterator::RocksDbReverse(rocksdb_iter) => rocksdb_iter.prev(),
        }
    }

    pub fn key_value(&self) -> Option<(&[u8], &[u8])> {
        match &self.iterator {
            StorageIterator::RocksDbReverse(rocksdb_iter)
            | StorageIterator::RocksDb(rocksdb_iter) => {
                let k = rocksdb_iter.key();
                let v = rocksdb_iter.value();

                match (k, v) {
                    (Some(k), Some(v)) => Some((k, v)),
                    _ => None,
                }
            }
        }
    }

    /// Return the key only. Use this in case you just need the key
    pub fn key(&self) -> Option<&[u8]> {
        match &self.iterator {
            StorageIterator::RocksDbReverse(rocksdb_iter)
            | StorageIterator::RocksDb(rocksdb_iter) => rocksdb_iter.key(),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct StorageMetadata {
    db_hash_map: IntMap<u16, usize>,
}

impl StorageMetadata {
    /// Incremenet the number of keys for database `db_id`
    pub fn incr_keys(&mut self, db_id: u16) {
        self.db_hash_map
            .entry(db_id)
            .and_modify(|val| {
                *val = val.saturating_add(1);
            })
            .or_insert(1);
    }

    pub fn total_key_count(&self) -> usize {
        self.db_hash_map.values().sum()
    }

    pub fn db_count(&self) -> usize {
        self.db_hash_map.len()
    }

    pub fn db_keys(&self, db_id: u16) -> usize {
        match self.db_hash_map.get(&db_id) {
            Some(v) => *v,
            None => 0,
        }
    }
}

/// Define the database interface
pub trait StorageTrait {
    /// Get a record from the store
    fn get(&self, key: &BytesMut) -> Result<Option<BytesMut>, SableError>;

    /// Put key:value in the store
    fn put(&self, key: &BytesMut, value: &BytesMut, put_flags: PutFlags) -> Result<(), SableError>;

    /// Check whether `key` exists in the store
    fn contains(&self, key: &BytesMut) -> Result<bool, SableError>;

    /// Delete a record from the store
    fn delete(&self, key: &BytesMut) -> Result<(), SableError>;

    /// Apply batch update to the database
    fn apply_batch(&self, update: &BatchUpdate) -> Result<(), SableError>;

    /// Flush all dirty buffers to the disk
    fn flush(&self) -> Result<(), SableError>;

    /// Manually flush any journal to the disk
    fn flush_wal(&self) -> Result<(), SableError>;

    /// Create a database checkpoint for backup purposes and store it at `location`. Returns the number of changes included
    /// in the checkpoint.
    /// `location` is a directory
    fn create_checkpoint(&self, location: &Path) -> Result<u64, SableError>;

    /// Restore database from a backup database located at `backup_location` (a directory)
    /// If `delete_all_before_store` is true, we will purge all current records from the
    /// db before starting the restore
    fn restore_from_checkpoint(
        &self,
        backup_location: &Path,
        delete_all_before_store: bool,
    ) -> Result<(), SableError>;

    /// Return all changes since the requested `sequence_number`
    /// If not `None`, `memory_limit` sets the limit for the
    /// memory (in bytes) that a single change since message can
    /// return
    fn storage_updates_since(
        &self,
        sequence_number: u64,
        limits: Rc<GetChangesLimits>,
    ) -> Result<StorageUpdates, SableError>;

    /// Apply storage updates into this database
    fn apply_storage_updates(&self, storage_updates: &StorageUpdates) -> Result<(), SableError>;

    /// The sequence number of the most recent transaction.
    fn latest_sequence_number(&self) -> Result<u64, SableError>;

    /// Create a database iterator starting from `prefix`
    fn create_iterator(&self, prefix: &BytesMut) -> Result<IteratorAdapter, SableError>;

    /// Create a reverse database iterator
    /// `upper_bound` should be the first prefix after the requested prefix.
    /// For example, if the caller wishes to iterate over all items starting with "1"
    /// `upper_bound` passed here should be "2"
    fn create_reverse_iterator(
        &self,
        upper_bound: &BytesMut,
    ) -> Result<IteratorAdapter, SableError>;

    /// Delete keys ranging from `[start, end)` (including `start` excluding `end`) from the database.
    /// If `start` is `None`, we start deleting from the first record
    /// If `end` is `None`, we delete until the last record
    ///
    /// This implies that calling `delete_range(None, None)` will delete all records from the database
    ///
    /// This operation is atomic
    fn delete_range(
        &self,
        start: Option<&BytesMut>,
        end: Option<&BytesMut>,
    ) -> Result<(), SableError>;

    /// Delete slot from the database
    fn delete_slot(&self, db_id: u16, slot: &crate::Slot) -> Result<(), SableError>;

    /// Trigger a database vacuum
    fn vacuum(&self) -> Result<(), SableError>;
}
