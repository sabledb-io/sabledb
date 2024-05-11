use crate::{
    replication::StorageUpdates,
    storage::{storage_trait::IteratorAdapter, StorageTrait},
    utils, StorageRocksDb,
};

use crate::{server::WatchedKeys, storage::DbCacheEntry, SableError};
use bytes::BytesMut;
use dashmap::DashMap;
#[allow(unused_imports)]
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Clone)]
pub enum PutFlags {
    PutIfNotExists,
    PutIfExists,
    Override,
}

#[derive(Clone, Debug)]
pub struct StorageOpenParams {
    pub rocksdb: RocksDbParams,
    /// Database path
    pub db_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct RocksDbParams {
    /// Number of IO threads available for RocksDb to perform flush & compaction
    pub max_background_jobs: usize,
    /// Sets the maximum number of write buffers that are built up in memory.
    /// The default 4, so that when 1 write buffer
    /// is being flushed to storage, new writes can continue to the other
    /// write buffer.
    ///
    /// Default: `4`
    pub max_write_buffer_number: usize,
    /// Amount of data to build up in memory (backed by an unsorted log
    /// on disk) before converting to a sorted on-disk file.
    ///
    /// Default: 256MB
    pub write_buffer_size: usize,
    /// If `wal_ttl_seconds` is not 0, then
    /// WAL files will be checked every `wal_ttl_seconds / 2` and those that
    /// are older than `wal_ttl_seconds` will be deleted.
    ///
    /// Default: 3600 seconds
    pub wal_ttl_seconds: usize,
    /// Enable data compression
    /// Default: true
    pub compression_enabled: bool,
    /// If true, writes will not first go to the write ahead log,
    /// and the write may get lost after a crash. The backup engine
    /// relies on write-ahead logs to back up the memtable.
    /// Default: false
    pub disable_wal: bool,
    /// Disable automatic WAL flush and do it manually after N milliseconds
    pub manual_wal_flush: bool,
    /// If `manual_wal_flush` is enabled, flush it every `manual_wal_flush_interval_ms` mislliseconds
    pub manual_wal_flush_interval_ms: usize,
    /// Sets the number of open files that can be used by the DB. You may need to
    /// increase this if your database has a large working set. Value `-1` means
    /// files opened are always kept open. You can estimate number of files based
    /// on target_file_size_base and target_file_size_multiplier for level-based
    /// compaction. For universal-style compaction, you can usually set it to `-1`.
    pub max_open_files: isize,
}

impl Default for StorageOpenParams {
    fn default() -> Self {
        StorageOpenParams {
            rocksdb: RocksDbParams {
                max_background_jobs: 8,
                max_write_buffer_number: 4,
                write_buffer_size: 256usize.saturating_mul(1024).saturating_mul(1024),
                wal_ttl_seconds: 3600,
                compression_enabled: true,
                disable_wal: false,
                manual_wal_flush: false,
                manual_wal_flush_interval_ms: 500,
                max_open_files: -1,
            },
            db_path: PathBuf::from("sabledb.db"),
        }
    }
}

lazy_static::lazy_static! {
    static ref MONOTONIC_COUNTER: AtomicU64
        = AtomicU64::new(utils::current_time(utils::CurrentTimeResolution::Nanoseconds));
    static ref LAST_WAL_FLUSH_TIMESTAMP: AtomicU64
        = AtomicU64::new(utils::current_time(utils::CurrentTimeResolution::Milliseconds));
}

impl StorageOpenParams {
    /// Enable compression
    pub fn set_compression(mut self, enable_compression: bool) -> Self {
        self.rocksdb.compression_enabled = enable_compression;
        self
    }

    /// Set the database path
    pub fn set_path(mut self, dbpath: &Path) -> Self {
        self.db_path = dbpath.to_path_buf();
        self
    }

    /// Disable the Write-Ahead-Log file
    /// This improves performance, at the cost of durability
    pub fn set_wal_disabled(mut self, disable_wal: bool) -> Self {
        self.rocksdb.disable_wal = disable_wal;
        self
    }

    /// Set the cache size
    pub fn set_cache_size(mut self, cache_size: usize) -> Self {
        self.rocksdb.write_buffer_size = cache_size;
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct BatchUpdate {
    /// List of keys to delete
    delete_keys: Option<Vec<BytesMut>>,
    /// List of keys to put
    put_keys: Option<Vec<(BytesMut, BytesMut)>>,
}

impl BatchUpdate {
    pub fn with_capacity(count: usize) -> Self {
        let delete_keys = Some(Vec::<BytesMut>::with_capacity(count));
        let put_keys = Some(Vec::<(BytesMut, BytesMut)>::with_capacity(count));
        BatchUpdate {
            delete_keys,
            put_keys,
        }
    }

    pub fn put(&mut self, key: BytesMut, value: BytesMut) {
        if self.put_keys.is_none() {
            self.put_keys = Some(Vec::<(BytesMut, BytesMut)>::new());
        }
        let Some(put_key) = &mut self.put_keys else {
            unreachable!();
        };
        put_key.push((key, value));
    }

    pub fn put_tuple(&mut self, key_value: (BytesMut, BytesMut)) {
        if self.put_keys.is_none() {
            self.put_keys = Some(Vec::<(BytesMut, BytesMut)>::new());
        }
        let Some(put_key) = &mut self.put_keys else {
            unreachable!();
        };
        put_key.push(key_value);
    }

    pub fn delete(&mut self, key: BytesMut) {
        if self.delete_keys.is_none() {
            self.delete_keys = Some(Vec::<BytesMut>::new());
        }
        let Some(delete_keys) = &mut self.delete_keys else {
            unreachable!();
        };
        delete_keys.push(key);
    }

    pub fn items_to_put(&self) -> Option<&Vec<(BytesMut, BytesMut)>> {
        self.put_keys.as_ref()
    }

    pub fn keys_to_delete(&self) -> Option<&Vec<BytesMut>> {
        self.delete_keys.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a list of keys to be modified by this batch
    pub fn modified_keys(&self) -> Vec<&BytesMut> {
        let mut keys = Vec::<&BytesMut>::with_capacity(self.len());
        if let Some(put_keys) = &self.put_keys {
            for (k, _) in put_keys {
                keys.push(k);
            }
        }

        if let Some(delete_keys) = &self.delete_keys {
            for k in delete_keys {
                keys.push(k);
            }
        }

        keys.sort();
        keys.dedup();
        keys
    }

    pub fn len(&self) -> usize {
        let mut total_items = if let Some(delete_keys) = &self.delete_keys {
            delete_keys.len()
        } else {
            0
        };

        total_items = total_items.saturating_add(if let Some(put_keys) = &self.put_keys {
            put_keys.len()
        } else {
            0
        });
        total_items
    }

    pub fn clear(&mut self) {
        self.put_keys = None;
        self.delete_keys = None;
    }
}

enum TxnWriteCacheGetResult {
    NotFound,
    Deleted,
    Found(BytesMut),
}

enum TxnWriteCacheContainsResult {
    NotFound,
    Deleted,
    Found,
}

/// Transcation write cache.
///
/// Used by the storage adapter to aggregate all writes (Put/Delete) into a single atomic batch
#[allow(dead_code)]
#[derive(Default, Clone)]
struct TxnWriteCache {
    changes: DashMap<BytesMut, Option<Rc<DbCacheEntry>>>,
}

#[allow(dead_code)]
impl TxnWriteCache {
    pub fn put(&self, key: &BytesMut, value: BytesMut) -> Result<(), SableError> {
        let entry = Rc::new(DbCacheEntry::new(value, PutFlags::Override));
        let _ = self.changes.insert(key.clone(), Some(entry));
        Ok(())
    }

    pub fn put_flags(
        &self,
        key: &BytesMut,
        value: BytesMut,
        flags: PutFlags,
    ) -> Result<(), SableError> {
        let entry = Rc::new(DbCacheEntry::new(value, flags));
        let _ = self.changes.insert(key.clone(), Some(entry));
        Ok(())
    }

    /// Note that we do not remove the entry from the `changes` hash,
    /// instead we use a `None` marker to indicate that this entry
    /// should be converted into a `delete` operation
    pub fn delete(&self, key: &BytesMut) -> Result<(), SableError> {
        let _ = self.changes.insert(key.clone(), None);
        Ok(())
    }

    /// Return true if `key` exists in the cache
    pub fn contains(&self, key: &BytesMut) -> Result<TxnWriteCacheContainsResult, SableError> {
        let Some(value) = self.changes.get(key) else {
            return Ok(TxnWriteCacheContainsResult::NotFound);
        };

        // found a match in the cache
        if value.value().is_some() {
            // an actual value
            Ok(TxnWriteCacheContainsResult::Found)
        } else {
            // the value was deleted
            Ok(TxnWriteCacheContainsResult::Deleted)
        }
    }

    /// Get a key from cache
    pub fn get(&self, key: &BytesMut) -> Result<TxnWriteCacheGetResult, SableError> {
        let Some(value) = self.changes.get(key) else {
            return Ok(TxnWriteCacheGetResult::NotFound);
        };

        // found a match in cache
        if let Some(value) = value.value() {
            // an actual value
            Ok(TxnWriteCacheGetResult::Found(value.data().clone()))
        } else {
            // the value was deleted
            Ok(TxnWriteCacheGetResult::Deleted)
        }
    }

    /// Apply `batch` into this txn
    pub fn apply_batch(&self, batch: &BatchUpdate) -> Result<(), SableError> {
        if let Some(delete_keys) = batch.keys_to_delete() {
            for key in delete_keys {
                self.delete(key)?;
            }
        }

        if let Some(put_items) = batch.items_to_put() {
            for (key, val) in put_items {
                self.put(key, val.clone())?;
            }
        }
        Ok(())
    }

    pub fn to_write_batch(&self) -> BatchUpdate {
        let mut batch_update = BatchUpdate::default();
        for entry in &self.changes {
            match entry.value() {
                None => batch_update.delete(entry.key().clone()),
                Some(value) => batch_update.put(entry.key().clone(), value.data().clone()),
            }
        }
        batch_update
    }

    pub fn clear(&self) {
        self.changes.clear()
    }
}

#[derive(Clone, Default)]
pub struct StorageAdapter {
    store: Option<Arc<dyn StorageTrait>>,
    open_params: StorageOpenParams,
    txn: Option<TxnWriteCache>,
}

/// We use an adapter to hide all `RocksDb` details and (maybe)
/// replace `RocksDb` in the future with another storage engine
impl StorageAdapter {
    /// Open the storage
    pub fn open(&mut self, open_params: StorageOpenParams) -> Result<(), SableError> {
        tracing::info!("Opening storage type: RocksDb");
        self.open_params = open_params.clone();
        self.store = Some(Arc::new(StorageRocksDb::open(open_params)?));
        Ok(())
    }

    /// Start a transcation
    pub fn transaction(&self) -> Self {
        let mut cloned = self.clone();
        cloned.txn = Some(TxnWriteCache::default());
        cloned
    }

    /// Flush all dirty buffers to the disk
    pub fn flush(&self) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        // discard any changes (txn must be explicitly committed)
        if let Some(txn) = &self.txn {
            txn.clear();
        }
        db.flush()
    }

    pub fn open_params(&self) -> &StorageOpenParams {
        &self.open_params
    }

    /// build the database path
    pub fn database_path(dbpath: &Path) -> PathBuf {
        let name = if dbpath.to_string_lossy().is_empty() {
            "sabledb.db".to_string()
        } else {
            dbpath.to_string_lossy().to_string()
        };
        PathBuf::from(name)
    }

    /// Get a value represented by `key` from the database
    pub fn get(&self, key: &BytesMut) -> Result<Option<BytesMut>, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        if let Some(txn) = &self.txn {
            match txn.get(key)? {
                TxnWriteCacheGetResult::Found(data) => Ok(Some(data)),
                TxnWriteCacheGetResult::Deleted => Ok(None),
                TxnWriteCacheGetResult::NotFound => db.get(key),
            }
        } else {
            db.get(key)
        }
    }

    /// Put a `key` : `value` pair into the database
    pub fn put(
        &self,
        key: &BytesMut,
        value: &BytesMut,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        if let Some(txn) = &self.txn {
            txn.put_flags(key, value.clone(), put_flags)?;
        } else {
            db.put(key, value, put_flags)?;
            WatchedKeys::notify(key, None);
        }
        Ok(())
    }

    /// Check whether `key` exists in the store
    pub fn contains(&self, key: &BytesMut) -> Result<bool, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        if let Some(txn) = &self.txn {
            match txn.contains(key)? {
                TxnWriteCacheContainsResult::Deleted => Ok(false),
                TxnWriteCacheContainsResult::Found => Ok(true),
                TxnWriteCacheContainsResult::NotFound => db.contains(key),
            }
        } else {
            db.contains(key)
        }
    }

    /// Similar to delete, but with no app locking
    pub fn delete(&self, key: &BytesMut) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        if let Some(txn) = &self.txn {
            txn.delete(key)
        } else {
            db.delete(key)?;
            WatchedKeys::notify(key, None);
            Ok(())
        }
    }

    /// Generated ID that is guaranteed to be unique.
    /// the returned value is always positive, greater than `0`!
    pub fn generate_id(&self) -> u64 {
        MONOTONIC_COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    /// Apply batch update to the database
    pub fn apply_batch(&self, update: &BatchUpdate) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        if let Some(txn) = &self.txn {
            txn.apply_batch(update)?;
        } else {
            db.apply_batch(update)?;
            let modified_keys = update.modified_keys();
            WatchedKeys::notify_multi(&modified_keys, None);
        }
        Ok(())
    }

    pub fn create_checkpoint(&self, location: &Path) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.create_checkpoint(location)
    }

    /// Restore database from a backup database located at `backup_location` (a directory)
    /// If `delete_all_before_store` is true, we will purge all current records from the
    /// db before starting the restore
    pub fn restore_from_checkpoint(
        &self,
        backup_location: &Path,
        delete_all_before_store: bool,
    ) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.restore_from_checkpoint(backup_location, delete_all_before_store)
    }

    /// Manually flush any journal to the disk
    pub fn flush_wal(&self) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        // sanity
        if !self.open_params.rocksdb.manual_wal_flush || self.open_params.rocksdb.disable_wal {
            return Ok(());
        }

        #[cfg(feature = "rocks_db")]
        {
            let current_ts = utils::current_time(utils::CurrentTimeResolution::Milliseconds);
            let old_ts = LAST_WAL_FLUSH_TIMESTAMP.swap(current_ts, Ordering::Relaxed);
            // Check if we need to perform flush
            if current_ts.saturating_sub(old_ts)
                >= self.open_params.rocksdb.manual_wal_flush_interval_ms as u64
            {
                // flush is needed
                db.flush_wal()?;
            } else {
                // restore the old value from the global parameter so other threads might flush it
                LAST_WAL_FLUSH_TIMESTAMP.swap(old_ts, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Return all changes since the requested `sequence_number`
    /// If not `None`, `memory_limit` sets the limit for the
    /// memory (in bytes) that a single change since message can
    /// return
    pub fn storage_updates_since(
        &self,
        sequence_number: u64,
        memory_limit: Option<u64>,
        changes_count_limit: Option<u64>,
    ) -> Result<StorageUpdates, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.storage_updates_since(sequence_number, memory_limit, changes_count_limit)
    }

    pub fn create_iterator(
        &self,
        prefix: Option<&BytesMut>,
    ) -> Result<IteratorAdapter, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.create_iterator(prefix)
    }

    /// Create a reverse database iterator
    /// `upper_bound` should be the first prefix after the requested prefix.
    /// For example, if the caller wishes to iterate over all items starting with "1"
    /// `upper_bound` passed here should be "2"
    pub fn create_reverse_iterator(
        &self,
        upper_bound: &BytesMut,
    ) -> Result<IteratorAdapter, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.create_reverse_iterator(upper_bound)
    }

    /// Commit the txn into the database as a single batch operation
    pub fn commit(&self) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };

        let Some(txn) = &self.txn else {
            return Err(SableError::NoActiveTransaction);
        };

        let updates = txn.to_write_batch();
        db.apply_batch(&updates)
    }
}

#[allow(unsafe_code)]
unsafe impl Send for StorageAdapter {}

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
    use std::fs;
    use std::path::PathBuf;
    use test_case::test_case;

    fn default_store(name: &str) -> Result<StorageAdapter, SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from(format!("tests/{}.db", name));
        let _ = fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);

        let mut store = StorageAdapter::default();
        store.open(open_params)?;
        Ok(store)
    }

    #[test_case("rocksdb", 100000 ; "put 100,000 items with rocksdb")]
    fn test_persistency(engine: &str, item_count: usize) -> Result<(), SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from(format!("tests/test_persistency_{}.db", engine));
        let _ = fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);
        {
            let store = crate::storage_rocksdb!(open_params.clone());
            for i in 0..item_count {
                let value = format!("value_string_{}", i);
                let key = format!("key_{}", i);
                store.put(
                    &BytesMut::from(&key[..]),
                    &BytesMut::from(&value[..]),
                    PutFlags::Override,
                )?;
            }
            // database is dropped here
        }

        {
            let store = crate::storage_rocksdb!(open_params);
            for i in 0..item_count {
                let expected_value = format!("value_string_{}", i);
                let key = format!("key_{}", i);
                let value = store.get(&BytesMut::from(&key[..]))?;
                assert!(value.is_some());
                assert_eq!(String::from_utf8_lossy(&value.unwrap()), expected_value);
            }
        }
        let _ = fs::remove_dir_all(db_path.clone());
        Ok(())
    }

    #[test]
    fn test_sequencer() -> Result<(), SableError> {
        let store = StorageAdapter::default();
        let seq1 = store.generate_id();
        let seq2 = store.generate_id();
        assert!(seq2 > seq1);
        Ok(())
    }

    #[test]
    fn test_contains() -> Result<(), SableError> {
        let store = default_store("test_contains").unwrap();
        let k = BytesMut::from("Test Key");
        let no_such_key = BytesMut::from("No such key");
        store.put(&k, &BytesMut::from("Some value"), PutFlags::Override)?;

        assert!(store.contains(&k).unwrap() == true);
        assert!(store.contains(&no_such_key).unwrap() == false);
        Ok(())
    }

    #[test]
    fn test_create_iterator() {
        let (_guard, store) = crate::tests::open_store();
        let seek_me = BytesMut::from("1");
        let value = BytesMut::from("string_value");

        let keys = vec![
            BytesMut::from("1_k1"),
            BytesMut::from("2_k2"),
            BytesMut::from("2_k3"),
            BytesMut::from("1_k4"),
        ];

        for key in keys.iter() {
            store.put(key, &value, PutFlags::Override).unwrap();
        }

        let mut db_iter = store.create_iterator(Some(&seek_me)).unwrap();
        let mut result = Vec::<BytesMut>::new();

        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(seek_me.as_ref()) {
                break;
            }

            result.push(BytesMut::from(key));
            db_iter.next();
        }
        assert_eq!(result.len(), 2);
        assert_eq!(result, vec![BytesMut::from("1_k1"), BytesMut::from("1_k4")]);
    }

    #[test]
    fn test_reverse_iterator_with_prefix() {
        let (_guard, store) = crate::tests::open_store();
        let value = BytesMut::from("string_value");

        let keys = vec![
            BytesMut::from("1_k1"),
            BytesMut::from("2_k2"),
            BytesMut::from("2_k3"),
            BytesMut::from("1_k4"),
            BytesMut::from("3_k5"),
        ];

        for key in keys.iter() {
            store.put(key, &value, PutFlags::Override).unwrap();
        }

        {
            let upper_bound = BytesMut::from("3"); // to place the iterator on the *last* `2` we use upper bound of `3`
            let seek_me = BytesMut::from("2");
            let mut db_iter = store.create_reverse_iterator(&upper_bound).unwrap();
            let mut result = Vec::<BytesMut>::new();

            while db_iter.valid() {
                // get the key & value
                let Some((key, _)) = db_iter.key_value() else {
                    break;
                };

                if !key.starts_with(seek_me.as_ref()) {
                    break;
                }

                let k = BytesMut::from(key);
                result.push(k);
                db_iter.next();
            }
            assert_eq!(result.len(), 2);
            assert_eq!(result, vec![BytesMut::from("2_k3"), BytesMut::from("2_k2")]);
        }
        {
            let upper_bound = BytesMut::from("4"); // to place the iterator on the *last* `3` we use upper bound of `4`
            let seek_me = BytesMut::from("3");
            let mut db_iter = store.create_reverse_iterator(&upper_bound).unwrap();
            let mut result = Vec::<BytesMut>::new();

            while db_iter.valid() {
                // get the key & value
                let Some((key, _)) = db_iter.key_value() else {
                    println!("None key/val");
                    break;
                };

                if !key.starts_with(seek_me.as_ref()) {
                    println!("!key.starts_with(seek_me.as_ref())");
                    break;
                }

                let k = BytesMut::from(key);
                println!("Adding {:?}", k);
                result.push(k);
                db_iter.next();
            }
            assert_eq!(result.len(), 1);
            assert_eq!(result, vec![BytesMut::from("3_k5")]);
        }
        {
            let upper_bound = BytesMut::from("5"); // to place the iterator on the *last* `4` we use upper bound of `5`
            let seek_me = BytesMut::from("4");
            let mut db_iter = store.create_reverse_iterator(&upper_bound).unwrap();
            let mut result = Vec::<BytesMut>::new();

            while db_iter.valid() {
                // get the key & value
                let Some((key, _)) = db_iter.key_value() else {
                    println!("None key/val");
                    break;
                };

                if !key.starts_with(seek_me.as_ref()) {
                    println!("!key.starts_with(seek_me.as_ref())");
                    break;
                }

                let k = BytesMut::from(key);
                println!("Adding {:?}", k);
                result.push(k);
                db_iter.next();
            }
            assert_eq!(result.len(), 0);
        }
        {
            let upper_bound = BytesMut::from("2"); // to get all "1"s use "2" as the upper bound
            let seek_me = BytesMut::from("1");
            let mut db_iter = store.create_reverse_iterator(&upper_bound).unwrap();
            let mut result = Vec::<BytesMut>::new();

            while db_iter.valid() {
                // get the key & value
                let Some((key, _)) = db_iter.key_value() else {
                    println!("None key/val");
                    break;
                };

                if !key.starts_with(seek_me.as_ref()) {
                    println!("!key.starts_with(seek_me.as_ref())");
                    break;
                }

                let k = BytesMut::from(key);
                println!("Adding {:?}", k);
                result.push(k);
                db_iter.next();
            }
            assert_eq!(result.len(), 2);
            assert_eq!(result, vec![BytesMut::from("1_k4"), BytesMut::from("1_k1")]);
        }
    }
}
