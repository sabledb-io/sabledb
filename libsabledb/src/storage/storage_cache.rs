use crate::{BatchUpdate, SableError, StorageAdapter};
use bytes::BytesMut;
use std::collections::HashMap;

/// An item that can be stored into the storage
pub trait Storable {
    /// Return the item's key as a byte array
    fn serialised_key(&self) -> BytesMut;

    /// Return the item's value as a byte array
    fn serialised_value(&self) -> BytesMut;

    /// Deserialise the value from bytes (key and value)
    /// this function consumes the `key` and `value` in the process
    fn construct_from(
        &mut self,
        key: &mut BytesMut,
        value: &mut BytesMut,
    ) -> Result<(), SableError>;
}

/// Transaction cache. Useful for manipulating
/// items in memory and then applying them in
/// a single batch update into the database
pub struct StorageCache<T: Storable + Clone + Default> {
    cache: HashMap<BytesMut, Option<T>>,
}

impl<T: Storable + Clone + Default> StorageCache<T> {
    pub fn new() -> Self {
        StorageCache {
            cache: HashMap::<BytesMut, Option<T>>::new(),
        }
    }

    /// Get an item from the cache. If the item represented by `key` can not be found in the cache
    /// search for the item in the `store` and update the cache.
    /// If an item is found in the cache, it can be of 2 states:
    /// - Deleted item
    /// - Concerete value
    /// A deleted item is kept as `key:None` pair
    pub fn get(&mut self, key: &BytesMut, store: &StorageAdapter) -> Result<Option<T>, SableError> {
        if let Some(cached_item) = self.cache.get(key) {
            // we distinguish between a deleted record in the cache and
            // an actual record
            match cached_item {
                Some(cached_item_value) => Ok(Some(cached_item_value.clone())),
                None => Ok(None),
            }
        } else if let Some(mut value) = store.get(key)? {
            // construct the value from the record found in the storage
            // update the cache and reutrn the match to the caller
            let mut new_item = T::default();
            let mut from_key = key.clone();
            new_item.construct_from(&mut from_key, &mut value)?;
            self.cache.insert(key.clone(), Some(new_item.clone()));
            Ok(Some(new_item))
        } else {
            Ok(None)
        }
    }

    /// Put an item in the cache. If an item with this key already exists
    /// in the cache - override it
    pub fn put(&mut self, item: T) {
        let key = item.serialised_key();
        self.cache.insert(key, Some(item));
    }

    /// This function perform `put` with `None` value for the `key`
    pub fn delete(&mut self, item: &T) {
        let key = item.serialised_key();
        self.cache.insert(key, None);
    }

    /// Convert the cache content into a `BatchUpdate` structure
    /// and clear the cache
    pub fn as_batch_update(&mut self, updates: &mut BatchUpdate) {
        for (key, val) in &self.cache {
            if let Some(val) = val {
                updates.put(key.clone(), val.serialised_value());
            } else {
                updates.delete(key.clone());
            }
        }
        self.cache.clear();
    }
}

impl<T: Storable + Clone + Default> Default for StorageCache<T> {
    fn default() -> Self {
        Self::new()
    }
}
