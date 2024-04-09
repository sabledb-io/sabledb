use crate::{storage::BatchUpdate, SableError, StorageAdapter};
use bytes::BytesMut;
use dashmap::DashMap;
use std::rc::Rc;

#[allow(dead_code)]
#[derive(Default)]
struct CacheEntry {
    data: BytesMut,
    is_dirty: bool,
}

#[allow(dead_code)]
impl CacheEntry {
    pub fn new(data: BytesMut, is_dirty: bool) -> Self {
        CacheEntry { data, is_dirty }
    }
}

/// A write cache for reducing I/O calls.
///
/// This class provides a thin layer that
/// allows the higher level databases (`HashDb`, `GenericDb`, `StringsDb` etc)
/// to perform `gut` / `delete` in-memory without interacting with the disk
///
/// `get` operations are directed to the disk
///
/// The changes accumlated in the cacne can be flushed to disk by
/// calling to `DbWriteCache::to_write_batch()` followed by `StroageAdapter::apply_batch` call
#[allow(dead_code)]
pub struct DbWriteCache<'a> {
    store: &'a StorageAdapter,
    changes: DashMap<BytesMut, Option<Rc<CacheEntry>>>,
}

#[allow(dead_code)]
impl<'a> DbWriteCache<'a> {
    pub fn with_storage(store: &'a StorageAdapter) -> Self {
        DbWriteCache {
            store,
            changes: DashMap::<BytesMut, Option<Rc<CacheEntry>>>::default(),
        }
    }

    pub fn put(&self, key: BytesMut, value: BytesMut) -> Result<(), SableError> {
        let entry = Rc::new(CacheEntry::new(value, true));
        let _ = self.changes.insert(key, Some(entry));
        Ok(())
    }

    /// Note that we do not remove the entry from the `changes` hash,
    /// instead we use a `None` marker to indicate that this entry
    /// should be converted into a `delete` operation
    pub fn delete(&self, key: BytesMut) -> Result<(), SableError> {
        let _ = self.changes.insert(key, None);
        Ok(())
    }

    /// Return true if `key` exists in the cache or in the underlying storage
    pub fn contains(&self, key: BytesMut) -> Result<bool, SableError> {
        if let Some(value) = self.changes.get(&key) {
            Ok(value.is_some())
        } else {
            self.store.contains(&key)
        }
    }

    /// Get a key from cache. If the key does not exist in the cache, fetch it from the store
    /// and keep a copy in the cache. If the key exists in the cache, but with a `None` value
    /// this means that it was deleted, so return a `None` as well
    pub fn get(&self, key: BytesMut) -> Result<Option<BytesMut>, SableError> {
        let Some(value) = self.changes.get(&key) else {
            // No such entry
            if let Some(value) = self.store.get(&key)? {
                // update the cache
                let entry = Rc::new(CacheEntry::new(value, false));
                self.changes.insert(key, Some(entry.clone()));
                return Ok(Some(entry.data.clone()));
            }
            return Ok(None);
        };

        // found an match in cache
        if let Some(value) = value.value() {
            // an actual value
            Ok(Some(value.data.clone()))
        } else {
            // the value was deleted
            Ok(None)
        }
    }

    pub fn to_write_batch(&self) -> BatchUpdate {
        let mut batch_update = BatchUpdate::default();
        for entry in &self.changes {
            match entry.value() {
                None => batch_update.delete(entry.key().clone()),
                Some(value) => {
                    if value.is_dirty {
                        batch_update.put(entry.key().clone(), value.data.clone());
                    }
                }
            }
        }
        batch_update
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::PutFlags, StorageOpenParams};
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
            let value = db_cache.get(key).unwrap().unwrap();
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
        let db_cache = DbWriteCache::with_storage(&store);
        for i in 0..10 {
            let key = BytesMut::from(format!("new_k{}", i).as_str());
            let value = BytesMut::from(format!("v{}", i).as_str());
            db_cache.put(key, value).unwrap();
        }

        // get a write batch from the cache
        // the batch update should be of size 10 (this is the number of records we
        // put in the cache)
        let updates = db_cache.to_write_batch();
        assert_eq!(updates.len(), 10);

        assert_eq!(updates.items_to_put().unwrap().len(), 10);
        assert!(updates.keys_to_delete().is_none());
    }

    #[test]
    fn test_contains_cache() {
        let store = open_store("test_contains_cache").unwrap();
        let key = BytesMut::from("key");
        let value = BytesMut::from("value");
        store.put(&key, &value, PutFlags::Override).unwrap();

        let db_cache = DbWriteCache::with_storage(&store);
        assert!(db_cache.contains(key).unwrap());

        let no_such_key = BytesMut::from("no_such_key");
        assert!(!db_cache.contains(no_such_key).unwrap());
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
        let db_cache = DbWriteCache::with_storage(&store);
        let mut expected_keys = std::collections::HashSet::<BytesMut>::with_capacity(50);
        for i in 0..50 {
            let key = BytesMut::from(format!("k{}", i).as_str());
            expected_keys.insert(key.clone());
            db_cache.delete(key).unwrap();
        }

        // get a write batch from the cache
        // the batch update should be of size 10 (this is the number of records we
        // put in the cache)
        let updates = db_cache.to_write_batch();
        assert_eq!(updates.len(), 50);

        assert!(updates.items_to_put().is_none());

        let keys = updates.keys_to_delete().unwrap();
        assert_eq!(keys.len(), 50);

        // Make sure that all the keys that should be deleted are included in the batch update
        for key in keys {
            assert!(expected_keys.remove(key));
        }
        assert!(expected_keys.is_empty());
    }
}
