use crate::{
    storage::{DbWriteCache, PutFlags, StorageAdapter},
    PrimaryKeyMetadata, SableError, StringValueMetadata, U8ArrayBuilder, U8ArrayReader,
};
use bytes::{Buf, BytesMut};

pub enum StringGetResult {
    None,
    Some((BytesMut, StringValueMetadata)),
    WrongType,
}

/// This class handles String command database access
#[allow(dead_code)]
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
        &mut self,
        user_key: &BytesMut,
        value: &BytesMut,
        metadata: &StringValueMetadata,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        self.put_internal(user_key, value, metadata, &put_flags)?;
        self.cache.flush()
    }

    /// Get a string key from the underlying storage
    pub fn get(&mut self, user_key: &BytesMut) -> Result<StringGetResult, SableError> {
        let result = self.get_internal(user_key)?;
        self.cache.flush()?;
        Ok(result)
    }

    /// Delete key. The key is assumed to be a user key (i.e. not encoded)
    pub fn delete(&mut self, user_key: &BytesMut) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&internal_key)?;
        self.cache.flush()
    }

    /// Put multiple items in the store. On success, return `true`
    pub fn multi_put(
        &mut self,
        user_keys_and_values: &[(&BytesMut, &BytesMut)],
        put_flags: PutFlags,
    ) -> Result<bool, SableError> {
        let metadata = StringValueMetadata::new();

        // Check for the flags first
        for (key, value) in user_keys_and_values {
            if !self.put_internal(key, value, &metadata, &put_flags)? {
                return Ok(false);
            }
        }
        self.cache.flush()?;
        Ok(true)
    }

    // =========-------------------------------------------
    // Internal helpers
    // =========-------------------------------------------
    fn put_internal(
        &mut self,
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
    fn get_internal(&mut self, user_key: &BytesMut) -> Result<StringGetResult, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);

        let raw_value = self.cache.get(&internal_key)?;
        if let Some(mut value) = raw_value {
            let mut reader = U8ArrayReader::with_buffer(&value);
            let md = StringValueMetadata::from_bytes(&mut reader)?;

            // Not a string
            if !md.common_metadata().is_string() {
                return Ok(StringGetResult::WrongType);
            }

            if md.expiration().is_expired()? {
                self.cache.delete(&internal_key)?;
                Ok(StringGetResult::None)
            } else {
                value.advance(StringValueMetadata::SIZE);
                Ok(StringGetResult::Some((value, md)))
            }
        } else {
            Ok(StringGetResult::None)
        }
    }
}
