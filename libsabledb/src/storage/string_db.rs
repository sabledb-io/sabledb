use crate::{
    storage::{BatchUpdate, PutFlags, StorageAdapter},
    PrimaryKeyMetadata, SableError, StringValueMetadata, U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

#[allow(dead_code)]

/// This class handles String command database access
pub struct StringsDb<'a> {
    store: &'a StorageAdapter,
}

#[allow(dead_code)]
impl<'a> StringsDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter) -> Self {
        StringsDb { store }
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
        self.put_internal(user_key, value, metadata, put_flags)
    }

    /// Get a string key from the underlying storage
    pub fn get(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, StringValueMetadata)>, SableError> {
        self.get_internal(user_key)
    }

    /// Delete key. The key is assumed to be a user key (i.e. not encoded)
    pub fn delete(&self, user_key: &BytesMut) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key);
        self.store.delete(&internal_key)
    }

    /// Put multiple items in the store. On success, return `true`
    pub fn multi_put(
        &self,
        user_keys_and_values: &[(&BytesMut, &BytesMut)],
        put_flags: PutFlags,
    ) -> Result<bool, SableError> {
        let metadata = StringValueMetadata::new();
        let mut updates = BatchUpdate::default();

        // Check for the flags first
        for (key, value) in user_keys_and_values.iter() {
            let internal_key = PrimaryKeyMetadata::new_primary_key(key);
            let can_continue = match put_flags {
                PutFlags::Override => true,
                PutFlags::PutIfNotExists => self.store.get(&internal_key)?.is_none(),
                PutFlags::PutIfExists => self.store.get(&internal_key)?.is_some(),
            };

            // Can not continue
            if !can_continue {
                return Ok(false);
            }

            let mut joined_value = BytesMut::with_capacity(StringValueMetadata::SIZE + value.len());
            let mut builder = U8ArrayBuilder::with_buffer(&mut joined_value);
            metadata.to_bytes(&mut builder);
            builder.write_bytes(value);
            updates.put(internal_key, joined_value);
        }

        self.store.apply_batch(&updates)?;
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
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        let mut joined_value = BytesMut::with_capacity(value.len() + StringValueMetadata::SIZE);
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key);
        let mut builder = U8ArrayBuilder::with_buffer(&mut joined_value);
        metadata.to_bytes(&mut builder);
        builder.write_bytes(value);
        self.store.put(&internal_key, &joined_value, put_flags)
    }

    /// Get a string key from the underlying storage
    fn get_internal(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, StringValueMetadata)>, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key);

        let raw_value = self.store.get(&internal_key)?;
        if let Some(value) = raw_value {
            let mut arr = BytesMut::from(&value[..]);
            let md = StringValueMetadata::from_bytes(&arr);

            if md.expiration().is_expired()? {
                self.store.delete(&internal_key)?;
                Ok(None)
            } else {
                let _ = arr.split_to(StringValueMetadata::SIZE);
                Ok(Some((arr, md)))
            }
        } else {
            Ok(None)
        }
    }
}
