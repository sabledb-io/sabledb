use crate::{
    storage::{DbWriteCache, PutFlags, StorageAdapter},
    PrimaryKeyMetadata, SableError, StringValueMetadata, U8ArrayBuilder, U8ArrayReader,
};
use bytes::{Buf, BytesMut};

#[allow(dead_code)]

/// This class handles String command database access
pub struct StringsDb<'a> {
    store: &'a StorageAdapter,
    db_id: u16,
    cache: Box<DbWriteCache<'a>>,
}

impl<'a> StringsDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        StringsDb {
            store,
            db_id,
            cache: Box::new(DbWriteCache::with_storage(store)),
        }
    }

    /// Put or Replace key
    /// No locks involved here
    pub fn put(
        &self,
        user_key: &BytesMut,
        value: &BytesMut,
        metadata: &StringValueMetadata,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        self.put_internal(user_key, value, metadata, &put_flags)?;
        self.flush_cache()
    }

    /// Get a string key from the underlying storage
    pub fn get(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, StringValueMetadata)>, SableError> {
        let result = self.get_internal(user_key)?;
        self.flush_cache()?;
        Ok(result)
    }

    /// Delete key. The key is assumed to be a user key (i.e. not encoded)
    pub fn delete(&self, user_key: &BytesMut) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&internal_key)?;
        self.flush_cache()
    }

    /// Put multiple items in the store. On success, return `true`
    pub fn multi_put(
        &self,
        user_keys_and_values: &[(&BytesMut, &BytesMut)],
        put_flags: PutFlags,
    ) -> Result<bool, SableError> {
        let metadata = StringValueMetadata::new();

        // Check for the flags first
        for (key, value) in user_keys_and_values.iter() {
            if !self.put_internal(key, value, &metadata, &put_flags)? {
                return Ok(false);
            }
        }
        self.flush_cache()?;
        Ok(true)
    }

    // =========-------------------------------------------
    // Internal helpers
    // =========-------------------------------------------
    fn put_internal(
        &self,
        user_key: &BytesMut,
        value: &BytesMut,
        metadata: &StringValueMetadata,
        put_flags: &PutFlags,
    ) -> Result<bool, SableError> {
        // Check for the flags first

        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let can_continue = match put_flags {
            PutFlags::Override => true,
            PutFlags::PutIfNotExists => self.cache.get(&internal_key)?.is_none(),
            PutFlags::PutIfExists => self.cache.get(&internal_key)?.is_some(),
        };

        // Can not continue
        if !can_continue {
            return Ok(false);
        }

        let mut joined_value = BytesMut::with_capacity(StringValueMetadata::SIZE + value.len());
        let mut builder = U8ArrayBuilder::with_buffer(&mut joined_value);
        metadata.to_bytes(&mut builder);
        builder.write_bytes(value);
        self.cache.put(&internal_key, joined_value)?;
        Ok(true)
    }

    /// Get a string key from the underlying storage
    fn get_internal(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, StringValueMetadata)>, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);

        let raw_value = self.cache.get(&internal_key)?;
        if let Some(mut value) = raw_value {
            let mut reader = U8ArrayReader::with_buffer(&value);
            let md = StringValueMetadata::from_bytes(&mut reader)?;

            if md.expiration().is_expired()? {
                self.cache.delete(&internal_key)?;
                Ok(None)
            } else {
                value.advance(StringValueMetadata::SIZE);
                Ok(Some((value, md)))
            }
        } else {
            Ok(None)
        }
    }

    /// Apply the changes to the store and clear the cache
    fn flush_cache(&self) -> Result<(), SableError> {
        if self.cache.is_empty() {
            return Ok(());
        }
        let batch = self.cache.to_write_batch();
        self.cache.clear();
        self.store.apply_batch(&batch)
    }
}
