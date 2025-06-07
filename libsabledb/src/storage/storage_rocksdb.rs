#[allow(unused_imports)]
use crate::{
    metadata::{DeleteRange, KeyType},
    replication::{StorageUpdates, StorageUpdatesRecord},
    storage::{
        storage_trait::{IteratorAdapter, StorageIterator, StorageMetadata},
        GetChangesLimits, PutFlags, StorageTrait, SEQUENCES_FILE,
    },
    BatchUpdate, BytesMutUtils, IoDurationStopWatch, SableError, Slot, StorageOpenParams,
    Telemetry,
};

use bytes::BytesMut;
use num_format::{Locale, ToFormattedString};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

type Database = rocksdb::DB;

pub struct StorageRocksDb {
    store: Arc<Database>,
    path: PathBuf,
    write_opts: rocksdb::WriteOptions,
}

pub struct UpdateBatchIterator {
    storage_updates: StorageUpdates,
    limits: Rc<GetChangesLimits>,
}

impl UpdateBatchIterator {
    pub fn new_with_limits(from_seq: u64, limits: Rc<GetChangesLimits>) -> Self {
        UpdateBatchIterator {
            storage_updates: StorageUpdates::from_seq_number(from_seq),
            limits,
        }
    }

    pub fn update(&mut self, seq: u64) {
        self.storage_updates.end_seq_number = seq;
        self.storage_updates.changes_count = self.storage_updates.changes_count.saturating_add(1);
    }

    /// Based on the limits provided, check if the iterator can continue
    pub fn can_continue(&self) -> bool {
        if let Some(memory_limit) = self.limits.memory_limit() {
            if self.storage_updates.len().ge(memory_limit) {
                return false;
            }
        }

        if let Some(changes_count_limit) = self.limits.changes_count_limit() {
            if self.storage_updates.changes_count.ge(changes_count_limit) {
                return false;
            }
        }
        true
    }

    /// Return whether `key` starts with `self.prefix`. If `self.prefix` is `None`, return `true`
    fn starts_with(&self, key: &[u8]) -> bool {
        let Some(prefix) = self.limits.prefix_limit() else {
            return true;
        };
        key.starts_with(prefix)
    }
}

impl rocksdb::WriteBatchIterator for UpdateBatchIterator {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        // if "prefix" limit is set, only collect keys that start with the prefix
        if !self.starts_with(&key) {
            return;
        }
        self.storage_updates.add_put(&key, &value);
    }

    fn delete(&mut self, key: Box<[u8]>) {
        // if "prefix" limit is set, only collect keys that start with the prefix
        if !self.starts_with(&key) {
            return;
        }
        self.storage_updates.add_delete(&key);
    }
}

impl StorageRocksDb {
    /// Open the storage
    pub fn open(open_params: StorageOpenParams) -> Result<Self, SableError> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_enable_pipelined_write(open_params.rocksdb.enable_pipelined_write);
        options.create_missing_column_families(true);
        options.set_max_write_buffer_number(open_params.rocksdb.max_write_buffer_number as i32);
        options.set_max_background_jobs(open_params.rocksdb.max_background_jobs as i32);
        options.set_manual_wal_flush(open_params.rocksdb.manual_wal_flush);

        if open_params.rocksdb.bloom_filter_bits_per_key > 0 {
            let mut opts = rocksdb::BlockBasedOptions::default();
            opts.set_bloom_filter(open_params.rocksdb.bloom_filter_bits_per_key as f64, true);
            options.set_block_based_table_factory(&opts);
        }

        options.set_compression_type(if open_params.rocksdb.compression_enabled {
            rocksdb::DBCompressionType::Snappy
        } else {
            rocksdb::DBCompressionType::None
        });
        options.set_write_buffer_size(open_params.rocksdb.write_buffer_size);
        options.set_log_level(rocksdb::LogLevel::Info);
        options.set_max_open_files(open_params.rocksdb.max_open_files as i32);
        options.set_wal_ttl_seconds(open_params.rocksdb.wal_ttl_seconds as u64);
        options.set_allow_mmap_writes(open_params.rocksdb.allow_mmap_writes);
        options.set_allow_mmap_reads(open_params.rocksdb.allow_mmap_reads);
        options.set_min_write_buffer_number_to_merge(
            open_params.rocksdb.min_write_buffer_number_to_merge as i32,
        );

        let store = rocksdb::DB::open(&options, open_params.db_path.clone())?;

        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(false);
        write_opts.disable_wal(open_params.rocksdb.disable_wal);

        tracing::info!(
            "Opening RocksDb storage at: {}",
            open_params.db_path.display()
        );

        Ok(StorageRocksDb {
            store: Arc::new(store),
            write_opts,
            path: open_params.db_path.clone(),
        })
    }

    fn put_internal(
        &self,
        key: &BytesMut,
        value: &BytesMut,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        let _io_stop_watch = IoDurationStopWatch::default();
        match put_flags {
            PutFlags::Override => {
                Telemetry::inc_total_io_write_calls();
                let _ = self
                    .store
                    .put_opt(key.clone(), value.clone(), &self.write_opts);
            }
            PutFlags::PutIfNotExists => {
                Telemetry::inc_total_io_read_calls();
                let old_value = self.store.get(key)?;
                if old_value.is_some() {
                    // key already exists
                    return Ok(());
                }
                Telemetry::inc_total_io_write_calls();
                let _ = self
                    .store
                    .put_opt(key.clone(), value.clone(), &self.write_opts);
            }
            PutFlags::PutIfExists => {
                Telemetry::inc_total_io_read_calls();
                let old_value = self.store.get(key)?;
                if old_value.is_none() {
                    // key not found
                    return Ok(());
                }
                Telemetry::inc_total_io_write_calls();
                let _ = self
                    .store
                    .put_opt(key.clone(), value.clone(), &self.write_opts);
            }
        }
        Ok(())
    }

    /// Write the last sequence number change
    fn write_next_sequence(&self, sequence_file: PathBuf, last_seq: u64) -> Result<(), SableError> {
        let content = format!("{}", last_seq);
        std::fs::write(sequence_file, content)?;
        Ok(())
    }

    fn create_delete_range_keys(
        &self,
        start: Option<&BytesMut>,
        end: Option<&BytesMut>,
    ) -> Option<(BytesMut, BytesMut)> {
        let snapshot = self.store.snapshot();

        // Determine the start key
        let start = if let Some(start) = start {
            start.clone()
        } else {
            let mut iter = snapshot.raw_iterator();
            iter.seek_to_first();
            if !iter.valid() {
                return None;
            }
            let start = iter.key()?;
            BytesMut::from(start)
        };

        let end = if let Some(end) = end {
            end.clone()
        } else {
            let mut iter = snapshot.raw_iterator();
            iter.seek_to_last();
            if !iter.valid() {
                return None;
            }
            let end = iter.key()?;
            let mut end = BytesMut::from(end);
            // add random trailing character to be last key (this way we ensure that
            // the last key is included in the deleted range)
            end.extend_from_slice(b"1");
            end
        };
        Some((start, end))
    }
}

impl StorageTrait for StorageRocksDb {
    /// Manually flushes the WAL files to the disk
    fn flush_wal(&self) -> Result<(), SableError> {
        self.store.flush_wal(false)?;
        Ok(())
    }

    fn apply_batch(&self, update: &BatchUpdate) -> Result<(), SableError> {
        let mut updates = rocksdb::WriteBatch::default();
        for item in update.items() {
            match item {
                StorageUpdatesRecord::Put { key, value } => {
                    updates.put(key, value.clone());
                    if DeleteRange::is_delete_range(key) {
                        if let Ok(del_range) = DeleteRange::from_bytes(key) {
                            updates
                                .delete_range(del_range.get_start_key(), del_range.get_end_key());
                        }
                    }
                }
                StorageUpdatesRecord::Del { key } => {
                    updates.delete(key);
                }
            }
        }

        Telemetry::inc_total_io_write_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        self.store.write_opt(updates, &self.write_opts)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), SableError> {
        // measure time spent doing IO
        let _io_stop_watch = IoDurationStopWatch::default();
        Telemetry::inc_total_io_write_calls();
        self.store.flush()?;
        Ok(())
    }

    fn get(&self, key: &BytesMut) -> Result<Option<BytesMut>, SableError> {
        Telemetry::inc_total_io_read_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        let raw_value = self.store.get(key)?;
        if let Some(value) = raw_value {
            Ok(Some(BytesMut::from(&value[..])))
        } else {
            Ok(None)
        }
    }

    /// Check whether `key` exists in the store. This function efficient since it does not copy the value
    fn contains(&self, key: &BytesMut) -> Result<bool, SableError> {
        Telemetry::inc_total_io_read_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        Ok((self.store.get_pinned(key)?).is_some())
    }

    fn put(&self, key: &BytesMut, value: &BytesMut, put_flags: PutFlags) -> Result<(), SableError> {
        self.put_internal(key, value, put_flags)
    }

    fn delete(&self, key: &BytesMut) -> Result<(), SableError> {
        // measure time spent doing IO
        Telemetry::inc_total_io_write_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        self.store.delete_opt(key, &self.write_opts)?;
        Ok(())
    }

    /// Create a consistent checkpoint at `location`. Return the number of changes stored in the checkpoint
    /// Note that `location` must not exist, it will be created
    fn create_checkpoint(&self, location: &Path) -> Result<u64, SableError> {
        let chk_point = rocksdb::checkpoint::Checkpoint::new(&self.store)?;
        chk_point.create_checkpoint(location)?;

        let sequence_file = location.join(SEQUENCES_FILE);
        let changes_count = self.store.latest_sequence_number();
        self.write_next_sequence(sequence_file, changes_count)?;
        Ok(changes_count)
    }

    /// The sequence number of the most recent transaction.
    fn latest_sequence_number(&self) -> Result<u64, SableError> {
        Ok(self.store.latest_sequence_number())
    }

    /// Restore the database from checkpoint database.
    /// This operation locks the entire database before it starts
    /// All write operations are stalled during this operation
    fn restore_from_checkpoint(
        &self,
        backup_location: &Path,
        delete_all_before_store: bool,
    ) -> Result<(), SableError> {
        tracing::info!(
            "Restoring database from checkpoint: {}",
            backup_location.display()
        );

        if delete_all_before_store {
            self.delete_range(None, None)?;
        }

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_log_level(rocksdb::LogLevel::Info);
        let db_backup = rocksdb::DB::open(&options, backup_location)?;

        let mut iter = db_backup.iterator(rocksdb::IteratorMode::Start);
        let last_seq = db_backup.latest_sequence_number();

        // Write in batch of 100K
        let mut updates = rocksdb::WriteBatch::default();
        let mut updates_counter = 0usize;
        while let Some(Ok((key, value))) = iter.next() {
            updates_counter = updates_counter.saturating_add(1);
            updates.put(key, value);
            if updates.len() % 100_000 == 0 {
                self.store.write_opt(updates, &self.write_opts)?;
                updates = rocksdb::WriteBatch::default();
            }
        }

        // apply the remainders
        if !updates.is_empty() {
            self.store.write_opt(updates, &self.write_opts)?;
        }

        let sequence_file = self.path.join(SEQUENCES_FILE);
        tracing::info!(
            "Restore completed. Put {} records",
            updates_counter.to_formatted_string(&Locale::en)
        );
        tracing::info!("Last sequence written to db is:{}", last_seq);
        self.write_next_sequence(sequence_file, last_seq)?;
        Ok(())
    }

    /// Return all changes since the requested `sequence_number`. Limit changes to the `GetChangesLimits`
    fn storage_updates_since(
        &self,
        sequence_number: u64,
        limits: Rc<GetChangesLimits>,
    ) -> Result<StorageUpdates, SableError> {
        let changes_iter = self.store.get_updates_since(sequence_number)?;

        let mut myiter = UpdateBatchIterator::new_with_limits(sequence_number, limits);
        for change in changes_iter {
            let (seq, write_batch) = match change {
                Err(e) => {
                    return Err(SableError::RocksDbError(e));
                }
                Ok((seq, update)) => (seq, update),
            };

            write_batch.iterate(&mut myiter);

            // update the counters
            myiter.update(seq);

            if !myiter.can_continue() {
                break;
            }
        }
        Ok(myiter.storage_updates)
    }

    fn apply_storage_updates(&self, storage_updates: &StorageUpdates) -> Result<(), SableError> {
        const MAX_BATCH_SIZE: usize = 10_000;
        let mut batch_update = BatchUpdate::with_capacity(MAX_BATCH_SIZE);
        let mut reader = crate::U8ArrayReader::with_buffer(&storage_updates.serialised_data);
        while let Some(change) = StorageUpdates::next(&mut reader) {
            batch_update.push(change);
            if batch_update.len() % MAX_BATCH_SIZE == 0 {
                self.apply_batch(&batch_update)?;
                batch_update.clear();
            }
        }

        // make sure all items are applied
        if !batch_update.is_empty() {
            self.apply_batch(&batch_update)?;
            batch_update.clear();
        }
        Ok(())
    }

    /// Create an forward iterator
    fn create_iterator<'a>(&self, prefix: &BytesMut) -> Result<IteratorAdapter, SableError> {
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.fill_cache(false);
        let mut iterator = self.store.raw_iterator_opt(read_options);
        iterator.seek(prefix);

        Ok(IteratorAdapter {
            iterator: StorageIterator::RocksDb(iterator),
        })
    }

    /// Create a reverse iterator
    fn create_reverse_iterator<'a>(
        &self,
        upper_bound: &BytesMut,
    ) -> Result<IteratorAdapter, SableError> {
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.fill_cache(false);
        let mut iterator = self.store.raw_iterator_opt(read_options);
        iterator.seek_for_prev(upper_bound);
        if iterator.valid() {
            let Some(key) = iterator.key() else {
                return Err(SableError::OtherError(
                    "failed to create reverse iterator".into(),
                ));
            };

            if key.starts_with(upper_bound) {
                iterator.prev();
            }
        }

        Ok(IteratorAdapter {
            iterator: StorageIterator::RocksDbReverse(iterator),
        })
    }

    /// Delete range of `[start, end)` (including `start` excluding `end`)
    fn delete_range(
        &self,
        start: Option<&BytesMut>,
        end: Option<&BytesMut>,
    ) -> Result<(), SableError> {
        let Some((start, end)) = self.create_delete_range_keys(start, end) else {
            return Ok(());
        };

        let mut updates = rocksdb::WriteBatch::default();
        updates.delete_range(&start, &end);
        self.store.write_opt(updates, &self.write_opts)?;

        // Place a "delete-range" marker
        let delete_range_entry = DeleteRange::new(start, end);
        // Put & Delete the "DeleteRange" record. This will ensure that we will have a WAL
        // entry but not in the database
        let mut updates = rocksdb::WriteBatch::default();
        updates.put(delete_range_entry.to_bytes(), b"");
        updates.delete(delete_range_entry.to_bytes());
        self.store.write_opt(updates, &self.write_opts)?;
        Ok(())
    }

    /// Slot from the database
    fn delete_slot(&self, db_id: u16, slot: &Slot) -> Result<(), SableError> {
        let prefix_arr = slot.prefix(db_id)?;
        if prefix_arr.is_empty() {
            return Ok(());
        }

        // Create the start and end keys
        for prefix in &prefix_arr {
            let mut read_options = rocksdb::ReadOptions::default();
            read_options.fill_cache(false);
            let mut iterator = self.store.raw_iterator_opt(read_options);
            iterator.seek(prefix);

            if !iterator.valid() {
                continue;
            }

            // determine that start and end key for this prefix
            let Some(start_key) = iterator.key() else {
                continue;
            };

            let start_key = BytesMut::from(start_key);
            iterator.next();
            let end_key = loop {
                if !iterator.valid() {
                    // eof?
                    break None;
                }
                let Some(k) = iterator.key() else {
                    break None;
                };

                if !k.starts_with(prefix) {
                    // the first key that does not start with the prefix, is the end key
                    break Some(BytesMut::from(k));
                }
                iterator.next();
            };
            self.delete_range(Some(&start_key), end_key.as_ref())?;
        }
        Ok(())
    }

    /// Trigger a database vacuum
    fn vacuum(&self) -> Result<(), SableError> {
        self.store.compact_range::<&BytesMut, &BytesMut>(None, None);
        Ok(())
    }
}

#[allow(unsafe_code)]
unsafe impl Send for StorageRocksDb {}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
#[cfg(feature = "rocks_db")]
mod tests {
    use super::*;
    use crate::tests::DirDeleter;

    const KEY_EXISTED_BEFORE_TXN: &str = "key_exists";
    const KEY_DOES_NOT_EXIST: &str = "no_such_key";
    const DB_PATH: &str = "rocks_db_test.db";

    fn create_database(name: &str) -> (StorageRocksDb, DirDeleter) {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from(format!("tests/{}.db", name));
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        (
            crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open"),
            DirDeleter::with_path(db_path.to_string_lossy().to_string()),
        )
    }

    /// Generate a fixed length key
    fn generate_key(counter: &mut usize) -> BytesMut {
        let key_len = 10usize;
        let right_string = BytesMutUtils::from::<usize>(counter);
        let mut left_string =
            BytesMutUtils::from(&"0".repeat(key_len.saturating_sub(right_string.len())));
        left_string.extend_from_slice(&right_string);
        *counter = counter.saturating_add(1);
        left_string
    }

    #[test]
    #[serial_test::serial]
    fn test_should_fail_if_key_updated_while_in_txn() -> Result<(), SableError> {
        let mut options = rocksdb::Options::default();
        {
            let _ = std::fs::remove_dir_all(DB_PATH);
            options.create_if_missing(true);

            let store: rocksdb::OptimisticTransactionDB =
                rocksdb::OptimisticTransactionDB::open(&options, DB_PATH)?;
            store.put(KEY_EXISTED_BEFORE_TXN, "old value")?;

            let tx = store.transaction();
            // mark KEY1 and KEY2 for updates.
            // any change to these keys before commiting the txn
            // will fail the txn
            {
                let old_value = tx.get_for_update(KEY_EXISTED_BEFORE_TXN, true);
                assert!(old_value.is_ok());
                assert!(old_value.unwrap().is_some());

                let old_value = tx.get_for_update(KEY_DOES_NOT_EXIST, true);
                assert!(old_value.is_ok());
                assert!(old_value.unwrap().is_none());
            }

            // modify TEST_KEY_1 outside the txn
            let res = store.put(KEY_EXISTED_BEFORE_TXN, "new value");
            assert!(res.is_ok());

            // should still be Ok
            let res = tx.put(KEY_EXISTED_BEFORE_TXN, "txn value 1");
            assert!(res.is_ok());

            let res = tx.put(KEY_DOES_NOT_EXIST, "txn value 2");
            assert!(res.is_ok());

            // should fail during commit (keys were updated outside of txn)
            let commit_res = tx.commit();
            assert!(commit_res.is_err());
        }

        let _ = rocksdb::DB::destroy(&options, DB_PATH)?;
        let _ = std::fs::remove_dir_all(DB_PATH);
        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn test_get_updates_since() -> Result<(), SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_get_updates_since.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let rocks = crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open");
        // put some items
        println!("Populating db...");
        let mut all_keys = std::collections::HashSet::<String>::new();
        for i in 0..20 {
            let mut batch = BatchUpdate::default();
            let key = format!("key_{}", i);
            let value = format!("value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            all_keys.insert(key);

            let key = format!("2nd_key_{}", i);
            let value = format!("2nd_value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            all_keys.insert(key);
            rocks.apply_batch(&batch)?;
        }

        // read 10 changes, starting 0
        let limits = Rc::new(
            GetChangesLimits::builder()
                .with_max_changes_count(10)
                .build(),
        );
        let changes = rocks.storage_updates_since(0, limits.clone())?;
        assert_eq!(changes.changes_count, 10);

        let next_batch_seq = changes.end_seq_number;
        let mut counter = 0;
        let mut reader = crate::U8ArrayReader::with_buffer(&changes.serialised_data);
        while let Some(item) = StorageUpdates::next(&mut reader) {
            let StorageUpdatesRecord::Put { key, value: _ } = item else {
                return Err(SableError::OtherError("Expected put record".to_string()));
            };
            let key_to_remove = String::from_utf8_lossy(&key).to_string();
            assert!(all_keys.remove(&key_to_remove));
            counter += 1;
        }
        assert_eq!(counter, 20);

        let changes = rocks.storage_updates_since(next_batch_seq, limits.clone())?;
        assert_eq!(changes.changes_count, 10);
        let mut counter = 0;
        let mut reader = crate::U8ArrayReader::with_buffer(&changes.serialised_data);
        while let Some(item) = StorageUpdates::next(&mut reader) {
            let StorageUpdatesRecord::Put { key, value: _ } = item else {
                return Err(SableError::OtherError("Expected put record".to_string()));
            };
            let key_to_remove = String::from_utf8_lossy(&key).to_string();
            assert!(all_keys.remove(&key_to_remove));
            counter += 1;
        }
        assert_eq!(counter, 20);

        // verify that all keys have been visited and removed
        assert!(all_keys.is_empty());
        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn test_get_updates_since_with_delete_range() -> Result<(), SableError> {
        let (rocks1, _rocks1_path) = create_database("test_get_updates_since_with_delete_range_1");
        // put some items
        println!("Populating db...");
        let mut all_keys = std::collections::HashSet::<String>::new();
        for i in 0..20 {
            let mut batch = BatchUpdate::default();
            let key = format!("key_{}", i);
            let value = format!("value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            all_keys.insert(key);

            let key = format!("2nd_key_{}", i);
            let value = format!("2nd_value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            all_keys.insert(key);
            rocks1.apply_batch(&batch)?;
        }

        rocks1.delete_range(None, None).unwrap();
        let limits = Rc::new(GetChangesLimits::builder().build());

        let changes = rocks1.storage_updates_since(0, limits.clone())?;
        assert_eq!(changes.changes_count, 22); // 20 put + delete_range which puts 2 items: Put + Del

        rocks1.apply_storage_updates(&changes).unwrap();

        // Confirm that non of the keys exist in the database
        for key in &all_keys {
            assert!(rocks1
                .get(&BytesMut::from(key.as_bytes()))
                .unwrap()
                .is_none());
        }
        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn test_get_updates_since_with_delete_range_2() -> Result<(), SableError> {
        let (rocks1, _rocks1_path) = create_database("test_get_updates_since_with_delete_range_2");
        // put some items
        println!("Populating db...");
        let mut not_exist_keys = std::collections::HashSet::<String>::new();
        let mut exist_keys = std::collections::HashSet::<String>::new();

        // Add 20 items
        for i in 0..20 {
            let mut batch = BatchUpdate::default();
            let key = format!("key_{}", i);
            let value = format!("value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            not_exist_keys.insert(key);
            rocks1.apply_batch(&batch)?;
        }

        // Delete all of them...
        rocks1.delete_range(None, None).unwrap();

        // Add 20 more items
        for i in 20..40 {
            let mut batch = BatchUpdate::default();
            let key = format!("key_{}", i);
            let value = format!("value_string_{}", i);
            batch.put(BytesMut::from(&key[..]), BytesMut::from(&value[..]));
            exist_keys.insert(key);
            rocks1.apply_batch(&batch)?;
        }

        let limits = Rc::new(GetChangesLimits::builder().build());

        let changes = rocks1.storage_updates_since(0, limits.clone())?;
        assert_eq!(changes.changes_count, 42); // 40 put + delete_range which puts 2 items: Put + Del

        rocks1.apply_storage_updates(&changes).unwrap();

        // Confirm that only keys 20-40 exist in the database
        for key in &exist_keys {
            assert!(rocks1
                .get(&BytesMut::from(key.as_bytes()))
                .unwrap()
                .is_some());
        }
        for key in &not_exist_keys {
            assert!(rocks1
                .get(&BytesMut::from(key.as_bytes()))
                .unwrap()
                .is_none());
        }
        Ok(())
    }

    #[cfg(feature = "rocks_db")]
    #[test]
    fn test_checkpoint() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let _ = std::fs::create_dir_all("tests");
            let db_path = PathBuf::from(format!("tests/test_checkpoint.db"));
            let backup_db_path = PathBuf::from(format!("tests/test_checkpoint.db.checkpoint"));
            let _ = std::fs::remove_dir_all(db_path.clone());
            // checkpoint path must not exist
            let _ = std::fs::remove_dir_all(backup_db_path.clone());
            let open_params = StorageOpenParams::default()
                .set_compression(true)
                .set_cache_size(64)
                .set_path(&db_path);
            let db = StorageRocksDb::open(open_params.clone()).unwrap();

            // put some items
            println!("Populating db...");
            for i in 0..100_000 {
                let value = format!("value_string_{}", i);
                let key = format!("key_{}", i);
                db.put(
                    &BytesMut::from(&key[..]),
                    &BytesMut::from(&value[..]),
                    PutFlags::Override,
                )
                .unwrap();
            }

            println!(
                "Creating backup...{}->{}",
                db_path.display(),
                backup_db_path.display()
            );

            // create a snapshot and drop the database
            db.create_checkpoint(&backup_db_path).unwrap();
            drop(db);
            println!("Success");

            // Delete the db content
            let _ = std::fs::remove_dir_all(db_path.clone());
            // Reopen it
            let db = StorageRocksDb::open(open_params.clone()).unwrap();

            // Confirm that all the keys are missing
            for i in 0..100_000 {
                let key = format!("key_{}", i);
                assert!(db.get(&BytesMut::from(&key[..])).unwrap().is_none());
            }

            println!("Restoring database...");
            let _unused = crate::LockManager::lock_all_keys_shared().await.unwrap();
            tracing::info!("Database is now locked (read-only mode)");

            db.restore_from_checkpoint(&backup_db_path, false).unwrap();
            println!("Success");

            // Confirm that all the keys are present
            for i in 0..100_000 {
                let key = format!("key_{}", i);
                let expected_value = format!("value_string_{}", i);
                let value = db.get(&BytesMut::from(&key[..])).unwrap().unwrap();
                assert_eq!(BytesMutUtils::to_string(&value), expected_value);
            }
            println!("All records restored successfully");
        });
    }

    #[cfg(feature = "rocks_db")]
    #[test]
    fn test_delete_all_keys() {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_delete_all_keys.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let db = crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open");

        // fill the database
        println!("Populating db...");
        for i in 0..10_000 {
            let value = format!("value_string_{}", i);
            let key = format!("key_{}", i);
            db.put(
                &BytesMut::from(&key[..]),
                &BytesMut::from(&value[..]),
                PutFlags::Override,
            )
            .unwrap();
        }

        println!("Deleting range...");
        db.delete_range(None, None).unwrap();
        println!("Deleting range...success");

        println!("Checking db...");
        for i in 0..10_000 {
            let key = format!("key_{}", i);
            assert_eq!(
                db.contains(&BytesMut::from(&key[..])).unwrap(),
                false,
                "{}",
                format!("{key} was found!"),
            );
        }
        println!("Checking db...done");
    }

    #[cfg(feature = "rocks_db")]
    #[test]
    fn test_keys_by_range_with_end() {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_keys_by_range_with_end.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let db = crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open");

        // fill the database
        println!("Populating db...");
        let mut counter = 0usize;
        let value = format!("value");
        for _ in 0..10_000 {
            let key = generate_key(&mut counter);
            db.put(&key, &BytesMut::from(&value[..]), PutFlags::Override)
                .unwrap();
        }

        println!("Deleting range [None..5,000)");
        counter = 5000usize;
        let end_key = generate_key(&mut counter);
        db.delete_range(None, Some(&end_key)).unwrap();
        println!("Deleting range [None..5,000)...success");

        println!("Checking db...");
        counter = 0;
        for _ in 0..5_000 {
            let key = generate_key(&mut counter);
            assert_eq!(
                db.contains(&key).unwrap(),
                false,
                "{}",
                format!("{:?} was found!", key),
            );
        }
        println!("OK: Keys 0-4999 do not exist");
        counter = 5000;
        for _ in 5_000..10_000 {
            let key = generate_key(&mut counter);
            assert_eq!(
                db.contains(&key).unwrap(),
                true,
                "{}",
                format!("{:?} was not found!", key),
            );
        }
        println!("OK: Keys 5000-9999 are present");
        println!("Checking db...done");
    }

    #[test]
    fn test_keys_by_range_with_start() {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_keys_by_range_with_start.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let db = crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open");

        // fill the database
        println!("Populating db...");
        let mut counter = 0usize;
        let value = format!("value");
        for _ in 0..10_000 {
            let key = generate_key(&mut counter);
            db.put(&key, &BytesMut::from(&value[..]), PutFlags::Override)
                .unwrap();
        }

        println!("Deleting range [None..5,000)");
        counter = 5000usize;
        let start_key = generate_key(&mut counter);
        db.delete_range(Some(&start_key), None).unwrap();
        println!("Deleting range [None..5,000)...success");

        println!("Checking db...");
        counter = 0;
        for _ in 0..5_000 {
            let key = generate_key(&mut counter);
            assert_eq!(
                db.contains(&key).unwrap(),
                true,
                "{}",
                format!("{:?} was NOT found!", key),
            );
        }
        println!("OK: Keys 0-4999 are present");
        counter = 5000;
        for _ in 5_000..10_000 {
            let key = generate_key(&mut counter);
            assert_eq!(
                db.contains(&key).unwrap(),
                false,
                "{}",
                format!("{:?} was found!", key),
            );
        }
        println!("OK: Keys 5000-9999 do not exist");
        println!("Checking db...done");
    }

    fn populate_slot(db: &mut StorageRocksDb, slot: &Slot) -> Vec<BytesMut> {
        // Fill entries of all types
        let arr = slot.prefix(0).unwrap();

        // Insert 10 items per prefix
        let mut keys_inserted = Vec::<BytesMut>::new();
        for prefix in &arr {
            for i in 0..10 {
                let mut key = prefix.clone();
                key.extend_from_slice(format!(":{i}").as_bytes());
                db.put(&key, &key, PutFlags::Override).unwrap();
                keys_inserted.push(key);
            }
        }
        keys_inserted
    }

    #[test]
    fn test_delete_slot() {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_delete_slot.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let mut db = crate::StorageRocksDb::open(open_params.clone()).expect("rockdb open");

        let slot_41 = Slot::with_slot(41);
        let slot_42 = Slot::with_slot(42);
        let slot_43 = Slot::with_slot(43);

        let keys_inserted_slot_41 = populate_slot(&mut db, &slot_41);
        let keys_inserted_slot_42 = populate_slot(&mut db, &slot_42);
        let keys_inserted_slot_43 = populate_slot(&mut db, &slot_43);

        for k in &keys_inserted_slot_42 {
            assert!(db.contains(k).unwrap());
        }

        for k in &keys_inserted_slot_41 {
            assert!(db.contains(k).unwrap());
        }

        for k in &keys_inserted_slot_43 {
            assert!(db.contains(k).unwrap());
        }

        // Delete the slot
        db.delete_slot(0, &slot_42).unwrap();

        // All keys for slot 42 should not exist in the database
        for k in &keys_inserted_slot_42 {
            assert!(!db.contains(k).unwrap());
        }

        // Slots 41 & 43 should remain in the database
        for k in &keys_inserted_slot_41 {
            assert!(db.contains(k).unwrap());
        }

        for k in &keys_inserted_slot_43 {
            assert!(db.contains(k).unwrap());
        }
    }
}
