#[allow(unused_imports)]
use crate::{
    replication::{StorageUpdates, StorageUpdatesIterItem},
    storage::{IterateCallback, PutFlags, StorageTrait},
    BatchUpdate, BytesMutUtils, IoDurationStopWatch, SableError, StorageOpenParams, Telemetry,
};

use bytes::BytesMut;
use num_format::{Locale, ToFormattedString};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use sled::{pin, IVec, LogKind, PageId};

const PID: PageId = 4;
const REPLACE: LogKind = LogKind::Replace;

type Database = sled::Db;

struct UpdateBatchIterator {
    storage_updates: StorageUpdates,
}

impl UpdateBatchIterator {
    pub fn new(from_seq: u64) -> Self {
        UpdateBatchIterator {
            storage_updates: StorageUpdates::from_seq_number(from_seq),
        }
    }

    pub fn update(&mut self, seq: u64) {
        self.storage_updates.end_seq_number = seq;
        self.storage_updates.changes_count = self.storage_updates.changes_count.saturating_add(1);
    }
}

pub struct StorageSledDb {
    store: Arc<Database>,
    path: PathBuf,
}

impl StorageSledDb {
    /// Open the storage
    pub fn open(open_params: StorageOpenParams) -> Result<Self, SableError> {
        let store = sled::open(open_params.db_path.clone())?;
        Ok(StorageSledDb {
            store: Arc::new(store),
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
                let _ = self.store.insert(key.clone(), value.clone().to_vec());
            }
            PutFlags::PutIfNotExists => {
                Telemetry::inc_total_io_read_calls();
                let old_value = self.store.get(key)?;
                if old_value.is_some() {
                    // key already exists
                    return Ok(());
                }
                Telemetry::inc_total_io_write_calls();
                let _ = self.store.insert(key.clone(), value.clone().to_vec());
            }
            PutFlags::PutIfExists => {
                Telemetry::inc_total_io_read_calls();
                let old_value = self.store.get(key)?;
                if old_value.is_none() {
                    // key not found
                    return Ok(());
                }
                Telemetry::inc_total_io_write_calls();
                let _ = self.store.insert(key.clone(), value.clone().to_vec());
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
}

impl StorageTrait for StorageSledDb {
    /// Manually flushes the WAL files to the disk
    fn flush_wal(&self) -> Result<(), SableError> {
        self.store.flush()?;
        Ok(())
    }

    fn apply_batch(&self, update: &BatchUpdate) -> Result<(), SableError> {
        let mut batch = sled::Batch::default();
        if let Some(keys) = update.keys_to_delete() {
            for k in keys.iter() {
                batch.remove(k.clone().to_vec());
            }
        }

        if let Some(put_keys) = update.items_to_put() {
            for (k, v) in put_keys.iter() {
                batch.insert(k.clone().to_vec(), v.clone().to_vec());
            }
        }

        Telemetry::inc_total_io_write_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        self.store.apply_batch(batch)?;
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
        Ok((self.store.get(key)?).is_some())
    }

    fn put(&self, key: &BytesMut, value: &BytesMut, put_flags: PutFlags) -> Result<(), SableError> {
        self.put_internal(key, value, put_flags)
    }

    fn delete(&self, key: &BytesMut) -> Result<(), SableError> {
        // measure time spent doing IO
        Telemetry::inc_total_io_write_calls();
        let _io_stop_watch = IoDurationStopWatch::default();
        self.store.remove(key)?;
        Ok(())
    }

    /// Create a consistent checkpoint at `location`
    /// Note that `location` must not exist, it will be created
    fn create_checkpoint(&self, location: &Path) -> Result<(), SableError> {
        let log = &self.store.context.pagecache.log;
        let guard = pin();

        let Some(path) = location.to_str() else {
            return Ok(());
        };

        let last_res = log.reserve(REPLACE, PID, &IVec::from(path.as_bytes()), &guard)?;
        let last_res_lsn = last_res.lsn();

        let sequence_file = location.join("changes.seq");
        self.write_next_sequence(sequence_file, last_res_lsn as u64)?;

        Ok(())
    }

    /// Restore the database from checkpoint database.
    /// This operation locks the entire database before it starts
    /// All write operations are stalled during this operation
    fn restore_from_checkpoint(
        &self,
        backup_location: &Path,
        _delete_all_before_store: bool,
    ) -> Result<(), SableError> {
        tracing::info!(
            "Restoring database from checkpoint: {}",
            backup_location.display()
        );

        let _unused = crate::LockManager::lock_all_keys_shared();
        tracing::info!("Database is now locked (read-only mode)");

        let db_backup = sled::open(backup_location)?;
        let mut iter = db_backup.into_iter();
        let log = &db_backup.context.pagecache.log;
        let lsn = log.stable_offset();

        // Write in batch of 100K
        let mut updates = sled::Batch::default();
        let mut updates_counter = 0usize;
        while let Some(Ok((key, value))) = iter.next() {
            updates_counter = updates_counter.saturating_add(1);
            updates.insert(key.to_vec(), value.to_vec());
            if updates_counter % 100_000 == 0 {
                self.store.apply_batch(updates)?;
                updates = sled::Batch::default();
            }
        }

        // apply the remainders
        if updates_counter % 100_000 != 0 {
            self.store.apply_batch(updates)?;
        }

        let sequence_file = self.path.join("changes.seq");
        tracing::info!(
            "Restore completed. Put {} records",
            updates_counter.to_formatted_string(&Locale::en)
        );
        tracing::info!("Last sequence written to db is:{}", lsn);
        self.write_next_sequence(sequence_file, lsn as u64)?;
        Ok(())
    }

    /// Return all changes since the requested `sequence_number`
    /// If not `None`, `memory_limit` sets the limit for the
    /// memory (in bytes) that a single change since message can
    /// return
    fn storage_updates_since(
        &self,
        sequence_number: u64,
        memory_limit: Option<u64>,
        changes_count_limit: Option<u64>,
    ) -> Result<StorageUpdates, SableError> {
        let log = &self.store.context.pagecache.log;
        let mut changes_iter = log.iter_from(sequence_number as i64);

        let mut myiter = UpdateBatchIterator::new(sequence_number);

        while let Some(t) = changes_iter.next() {
            // update the counters
            myiter.update(t.2 as u64);

            if let Some(memory_limit) = memory_limit {
                if myiter.storage_updates.len() >= memory_limit {
                    break;
                }
            }

            if let Some(changes_count_limit) = changes_count_limit {
                if myiter.storage_updates.changes_count >= changes_count_limit {
                    break;
                }
            }
        }
        Ok(myiter.storage_updates)
    }

    fn iterate(
        &self,
        prefix: Rc<BytesMut>,
        callback: Box<IterateCallback>,
    ) -> Result<(), SableError> {
        // // search our prefix
        let mut iter = self.store.scan_prefix(prefix.as_ref());

        loop {
            let Some(result) = iter.next() else {
                break;
            };

            match result {
                Ok((k, v)) => {
                    if !callback(&k.to_vec(), &v.to_vec()) {
                        break;
                    }
                }
                Err(_e) => break,
            }
        }
        Ok(())
    }
}

#[allow(unsafe_code)]
unsafe impl Send for StorageSledDb {}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
// #[cfg(feature = "sled_db")]
mod tests {
    use super::*;
    #[test]
    fn test_get_updates_since() -> Result<(), SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_get_updates_since.db");
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        let sled = crate::StorageSledDb::open(open_params.clone()).expect("sleddb open");
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
            sled.apply_batch(&batch)?;
        }

        // read 10 changes, starting 0
        let changes = sled.storage_updates_since(0, None, Some(10))?;
        assert_eq!(changes.changes_count, 10);

        let next_batch_seq = changes.end_seq_number;
        let mut counter = 0;
        let mut reader = crate::U8ArrayReader::with_buffer(&changes.serialised_data);
        while let Some(item) = changes.next(&mut reader) {
            let StorageUpdatesIterItem::Put(put_record) = item else {
                return Err(SableError::OtherError("Expected put record".to_string()));
            };
            let key_to_remove = String::from_utf8_lossy(&put_record.key).to_string();
            assert!(all_keys.remove(&key_to_remove));
            counter += 1;
        }
        assert_eq!(counter, 20);

        let changes = sled.storage_updates_since(next_batch_seq, None, Some(10))?;
        assert_eq!(changes.changes_count, 10);
        let mut counter = 0;
        let mut reader = crate::U8ArrayReader::with_buffer(&changes.serialised_data);
        while let Some(item) = changes.next(&mut reader) {
            let StorageUpdatesIterItem::Put(put_record) = item else {
                return Err(SableError::OtherError("Expected put record".to_string()));
            };
            let key_to_remove = String::from_utf8_lossy(&put_record.key).to_string();
            assert!(all_keys.remove(&key_to_remove));
            counter += 1;
        }
        assert_eq!(counter, 20);

        // verify that all keys have been visited and removed
        assert!(all_keys.is_empty());
        Ok(())
    }
}
