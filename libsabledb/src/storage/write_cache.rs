use crate::{storage::BatchUpdate, storage::PutFlags, SableError, StorageAdapter};
use bytes::BytesMut;
use std::collections::HashMap;
use std::rc::Rc;

pub struct DbCacheEntry {
    data: BytesMut,
    #[allow(dead_code)]
    flags: PutFlags,
}

impl DbCacheEntry {
    pub fn new(data: BytesMut, flags: PutFlags) -> Self {
        DbCacheEntry { data, flags }
    }

    pub fn data(&self) -> &BytesMut {
        &self.data
    }

    pub fn flags(&self) -> &PutFlags {
        &self.flags
    }
}

/// A write cache for reducing I/O calls.
///
/// This class provides a thin layer that
/// allows the higher level databases (`HashDb`, `GenericDb`, `StringsDb` etc)
/// to perform `put` / `delete` in-memory without interacting with the disk
///
/// `get` operations are directed to the disk
///
/// The changes accumulated in the cache can be flushed to disk by
/// calling to `DbWriteCache::to_write_batch()` followed by `StroageAdapter::apply_batch` call
pub struct DbWriteCache<'a> {
    store: &'a StorageAdapter,
    changes: HashMap<BytesMut, Option<Rc<DbCacheEntry>>>,
}

impl<'a> DbWriteCache<'a> {
    pub fn with_storage(store: &'a StorageAdapter) -> Self {
        DbWriteCache {
            store,
            changes: HashMap::<BytesMut, Option<Rc<DbCacheEntry>>>::default(),
        }
    }

    pub fn put(&mut self, key: &BytesMut, value: BytesMut) -> Result<(), SableError> {
        let entry = Rc::new(DbCacheEntry::new(value, PutFlags::Override));
        let _ = self.changes.insert(key.clone(), Some(entry));
        Ok(())
    }

    pub fn put_flags(
        &mut self,
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
    pub fn delete(&mut self, key: &BytesMut) -> Result<(), SableError> {
        let _ = self.changes.insert(key.clone(), None);
        Ok(())
    }

    /// Return true if `key` exists in the cache or in the underlying storage
    pub fn contains(&self, key: &BytesMut) -> Result<bool, SableError> {
        if let Some(value) = self.changes.get(key) {
            Ok(value.is_some())
        } else {
            self.store.contains(key)
        }
    }

    /// Get a key from cache. If the item was deleted, return `None`.
    /// This function does not update the cache
    pub fn get(&self, key: &BytesMut) -> Result<Option<BytesMut>, SableError> {
        let Some(value) = self.changes.get(key) else {
            return self.store.get(key);
        };

        // found a match in cache
        if let Some(value) = value {
            // an actual value
            Ok(Some(value.data.clone()))
        } else {
            // the value was deleted
            Ok(None)
        }
    }

    pub fn to_write_batch(&self) -> BatchUpdate {
        let mut batch_update = BatchUpdate::default();
        for (k, v) in &self.changes {
            match v {
                None => batch_update.delete(k.clone()),
                Some(value) => batch_update.put(k.clone(), value.data.clone()),
            }
        }
        batch_update
    }

    pub fn clear(&mut self) {
        self.changes.clear()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Apply the cache to the disk as a single atomic operation
    pub fn flush(&mut self) -> Result<(), SableError> {
        if self.is_empty() {
            return Ok(());
        }

        if self.changes.len() == 1 {
            // do not create a batch
            for (k, v) in &self.changes {
                match v {
                    None => {
                        self.store.delete(k)?;
                    }
                    Some(value) => {
                        self.store.put(k, &value.data, PutFlags::Override)?;
                    }
                }
            }
        } else {
            let updates = self.to_write_batch();
            self.store.apply_batch(&updates)?;
        }

        self.changes.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::{PutFlags, StorageUpdatesRecord},
        StorageOpenParams,
    };
    use std::fs;
    use std::path::PathBuf;
    fn open_store(name: &str) -> Result<StorageAdapter, SableError> {
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

    #[test]
    fn test_get_from_cache() {
        let store = open_store("test_write_cache").unwrap();

        // put some records into the database
        for i in 0..100 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            let value = BytesMut::from(format!("v{}", i).as_str());
            store.put(&key, &value, PutFlags::Override).unwrap();
        }

        // Test that `get` accesses the storage
        let db_cache = DbWriteCache::with_storage(&store);
        for i in 0..100 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            let value = db_cache.get(&key).unwrap().unwrap();
            let expected_value = BytesMut::from(format!("v{}", i).as_str());
            assert_eq!(value, expected_value);
        }

        // get a write batch from the cache
        // it should be empty, since we only did "get" operations on the cache
        let updates = db_cache.to_write_batch();
        assert_eq!(updates.len(), 0);
    }

    #[test]
    fn test_put_cache() {
        let store = open_store("test_put_cache").unwrap();

        // put some records into the database
        for i in 0..100 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            let value = BytesMut::from(format!("v{}", i).as_str());
            store.put(&key, &value, PutFlags::Override).unwrap();
        }

        // Test that `get` accesses the storage
        let mut db_cache = DbWriteCache::with_storage(&store);
        for i in 0..10 {
            let key = BytesMut::from(format!("new_k{}", i).as_str());
            let value = BytesMut::from(format!("v{}", i).as_str());
            db_cache.put(&key, value).unwrap();
        }

        // get a write batch from the cache
        // the batch update should be of size 10 (this is the number of records we
        // put in the cache)
        let updates = db_cache.to_write_batch();
        assert_eq!(updates.len(), 10);

        for item in updates.items() {
            match item {
                StorageUpdatesRecord::Put { key: _, value: _ } => {}
                other => {
                    assert!(false, "Expected item of type Put. Got {:?}", other);
                }
            }
        }
    }

    #[test]
    fn test_contains_cache() {
        let store = open_store("test_contains_cache").unwrap();
        let key = BytesMut::from("key");
        let value = BytesMut::from("value");
        store.put(&key, &value, PutFlags::Override).unwrap();

        let db_cache = DbWriteCache::with_storage(&store);
        assert!(db_cache.contains(&key).unwrap());

        let no_such_key = BytesMut::from("no_such_key");
        assert!(!db_cache.contains(&no_such_key).unwrap());
    }

    #[test]
    fn test_delete_cache() {
        let store = open_store("test_delete_cache").unwrap();

        // put some records into the database
        for i in 0..100 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            let value = BytesMut::from(format!("v{}", i).as_str());
            store.put(&key, &value, PutFlags::Override).unwrap();
        }

        // Test that `get` accesses the storage
        let mut db_cache = DbWriteCache::with_storage(&store);
        let mut expected_keys = std::collections::HashSet::<BytesMut>::with_capacity(50);
        for i in 0..50 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            expected_keys.insert(key.clone());
            db_cache.delete(&key).unwrap();
        }

        // get a write batch from the cache
        // the batch update should be of size 10 (this is the number of records we
        // put in the cache)
        let updates = db_cache.to_write_batch();
        assert_eq!(updates.len(), 50);

        // Make sure that all the keys that should be deleted are included in the batch update
        for item in updates.items() {
            match item {
                StorageUpdatesRecord::Put { key: _, value: _ } => {}
                StorageUpdatesRecord::Del { key } => {
                    expected_keys.remove(key);
                }
            }
        }
        assert!(expected_keys.is_empty());
    }
}
