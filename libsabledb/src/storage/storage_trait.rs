use crate::{
    storage::{BatchUpdate, PutFlags, StorageUpdates},
    SableError,
};
use bytes::BytesMut;
use std::path::Path;

//pub type IterateCallback<'a> = dyn FnMut(&[u8], &[u8], &[u8]) -> bool + 'a;

pub enum StorageIterator<'a> {
    RocksDb(rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DB>),
    RocksDbReverse(rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DB>),
}

pub struct IteratorAdapter<'a> {
    pub iterator: StorageIterator<'a>,
}

impl<'a> IteratorAdapter<'a> {
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

    /// Create a database checkpoint for backup purposes and store it at `location`
    /// `location` is a directory
    fn create_checkpoint(&self, location: &Path) -> Result<(), SableError>;

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
        memory_limit: Option<u64>,
        changes_count_limit: Option<u64>,
    ) -> Result<StorageUpdates, SableError>;

    /// Create a database iterator. If `prefix` is not `None`, move the iterator
    /// to point to `prefix`
    fn create_iterator(&self, prefix: Option<&BytesMut>) -> Result<IteratorAdapter, SableError>;

    /// Create a reverse database iterator (if no prefix is provided, the iterator is placed at the end)
    /// A call to `IteratorAdapter::next()` goes backward
    fn create_reverse_iterator(
        &self,
        prefix: Option<&BytesMut>,
    ) -> Result<IteratorAdapter, SableError>;
}
