use crate::{
    replication::StorageUpdates,
    storage::{IterateCallback, StorageTrait},
    utils, StorageRocksDb,
};

#[allow(unused_imports)]
use std::cell::RefCell;

use crate::SableError;
use bytes::BytesMut;
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

#[derive(Clone, Default)]
pub struct StorageAdapter {
    store: Option<Arc<dyn StorageTrait>>,
    open_params: StorageOpenParams,
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

    /// Flush all dirty buffers to the disk
    pub fn flush(&self) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
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
        db.get(key)
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
        db.put(key, value, put_flags)?;
        Ok(())
    }

    /// Check whether `key` exists in the store
    pub fn contains(&self, key: &BytesMut) -> Result<bool, SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.contains(key)
    }

    /// Similar to delete, but with no app locking
    pub fn delete(&self, key: &BytesMut) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.delete(key)?;
        Ok(())
    }

    /// Generated ID that is guaranteed to be unique.
    /// the returned value is always positive, greater than `0`!
    pub fn generate_id(&self) -> u64 {
        MONOTONIC_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    /// Apply batch update to the database
    pub fn apply_batch(&self, update: &BatchUpdate) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.apply_batch(update)?;
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
                tracing::trace!("flushing WAL file...");
            } else {
                // restore the old value from the global parameter so other threads might flush it
                LAST_WAL_FLUSH_TIMESTAMP.swap(old_ts, Ordering::Relaxed);
                tracing::trace!("No need to flush WAL file...");
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

    /// Iterate on all items starting with `prefix` and apply `callback` on them
    pub fn iterate(
        &self,
        prefix: Rc<BytesMut>,
        callback: Box<IterateCallback>,
    ) -> Result<(), SableError> {
        let Some(db) = &self.store else {
            return Err(SableError::OtherError("Database is not opened".to_string()));
        };
        db.iterate(prefix, callback)
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
    #[allow(unused_imports)]
    use crate::BytesMutUtils;
    use std::cell::RefCell;
    use std::fs;
    use std::path::PathBuf;
    use std::rc::Rc;
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
    fn test_prefix_iteration() -> Result<(), SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_prefix.db");
        let _ = fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);

        let mut store = StorageAdapter::default();
        let _ = store.open(open_params);
        let value = BytesMut::from("string_value");

        let keys = vec![
            BytesMut::from("1_k1"),
            BytesMut::from("2_k2"),
            BytesMut::from("2_k3"),
            BytesMut::from("1_k4"),
        ];

        for key in keys.iter() {
            store.put(key, &value, PutFlags::Override)?;
        }

        // Collect the matching keys here
        let result = Rc::new(RefCell::new(Vec::<BytesMut>::new()));

        // this is a cheap clone
        let result_clone = result.clone();
        let seek_me = Rc::new(BytesMut::from("1"));

        let _ = store.iterate(
            seek_me.clone(),
            Box::new(move |k, _v| {
                if !k.starts_with(seek_me.as_ref()) {
                    false
                } else {
                    result_clone.borrow_mut().push(BytesMut::from(k));
                    true
                }
            }),
        );

        assert_eq!(result.borrow().len(), 2);
        assert_eq!(
            BytesMutUtils::to_string(result.borrow().get(0).unwrap()),
            BytesMutUtils::to_string(keys.get(0).unwrap())
        );
        assert_eq!(
            BytesMutUtils::to_string(result.borrow().get(1).unwrap()),
            BytesMutUtils::to_string(keys.get(3).unwrap())
        );
        Ok(())
    }
}
