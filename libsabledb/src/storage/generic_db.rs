/// A database accessor that does not really care about the value
use crate::{
    storage::PutFlags, CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter,
    U8ArrayBuilder, U8ArrayReader,
};
use bytes::BytesMut;

#[allow(dead_code)]
/// General purpose database wrapper. This class uses the fact that
/// all primary types (strings, lists, hashs etc) are using the same
/// key `PrimaryKeyMetadata` and the value has similar prefix:
/// `[primary_key]` -> `[common_metadata | <per type metadata> | < user data >]`
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
pub struct GenericDb<'a> {
    /// This class handles String command database access
    store: &'a StorageAdapter,
    db_id: u16,
}

#[allow(dead_code)]
impl<'a> GenericDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        GenericDb { store, db_id }
    }

    /// Get a key value + its common metadata.
    /// We do not care about key type (i.e whether it is a string, list, hash etc)
    pub fn get(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, CommonValueMetadata)>, SableError> {
        self.get_internal(user_key)
    }

    /// Delete key. The key is assumed to be a user key (i.e. not encoded)
    pub fn delete(&self, user_key: &BytesMut) -> Result<(), SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.store.delete(&internal_key)
    }

    /// Put or Replace key
    /// No locks involved here
    pub fn put(
        &self,
        user_key: &BytesMut,
        value: &BytesMut,
        metadata: &CommonValueMetadata,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        self.put_internal(user_key, value, metadata, put_flags)
    }

    /// Return true if user key exists in the db
    /// Note that same keys might exists for different db IDs
    pub fn contains(&self, user_key: &BytesMut) -> Result<bool, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.store.contains(&internal_key)
    }

    // =========-------------------------------------------
    // Internal helpers
    // =========-------------------------------------------
    fn put_internal(
        &self,
        user_key: &BytesMut,
        value: &BytesMut,
        metadata: &CommonValueMetadata,
        put_flags: PutFlags,
    ) -> Result<(), SableError> {
        let mut joined_value = BytesMut::with_capacity(value.len() + CommonValueMetadata::SIZE);
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let mut builder = U8ArrayBuilder::with_buffer(&mut joined_value);
        metadata.to_bytes(&mut builder);
        builder.write_bytes(value);
        self.store.put(&internal_key, &joined_value, put_flags)
    }

    /// Get a string key from the underlying storage
    fn get_internal(
        &self,
        user_key: &BytesMut,
    ) -> Result<Option<(BytesMut, CommonValueMetadata)>, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);

        let raw_value = self.store.get(&internal_key)?;
        if let Some(mut value) = raw_value {
            let mut reader = U8ArrayReader::with_buffer(&value);
            let md = CommonValueMetadata::from_bytes(&mut reader)?;

            if md.expiration().is_expired()? {
                self.store.delete(&internal_key)?;
                Ok(None)
            } else {
                let _ = value.split_to(CommonValueMetadata::SIZE);
                Ok(Some((value, md)))
            }
        } else {
            Ok(None)
        }
    }
}

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
    use crate::{storage::StringsDb, StorageOpenParams, StringValueMetadata};
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_generic_db() -> Result<(), SableError> {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from("tests/test_generic_db.db");
        let _ = fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);
        let store = crate::storage_rocksdb!(open_params.clone());

        let strings_db = StringsDb::with_storage(&store, 0);
        let generic_db = GenericDb::with_storage(&store, 0);

        // Write 10 entries using `StringDb`
        for i in 1..10 {
            let mut md = StringValueMetadata::new();
            md.expiration_mut().set_ttl_seconds(i * 10)?;

            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            strings_db.put(
                &BytesMut::from(key.as_bytes()),
                &BytesMut::from(value.as_bytes()),
                &md,
                PutFlags::Override,
            )?;
        }

        // Now read these 10 entries using `GenericDb`
        for i in 1..10 {
            let key = format!("key_{}", i);
            let key = BytesMut::from(key.as_bytes());
            let (value, md) = generic_db.get(&key)?.unwrap();
            let max_bound: u64 = i * 10;
            let lower_bound: u64 = max_bound - 1;
            // check that we were able to read the common metadata properly
            assert_eq!(md.value_type(), CommonValueMetadata::VALUE_STR);
            assert!(md.expiration().ttl_in_seconds()? <= max_bound);
            assert!(md.expiration().ttl_in_seconds()? >= lower_bound);

            // write the value back
            generic_db.put(&key, &value, &md, PutFlags::Override)?;
        }

        // Last check: read these 10 entries using `StringDb` and confirm nothing was corrupted
        for i in 1..10 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);

            let key = BytesMut::from(key.as_bytes());
            let expected_value = BytesMut::from(expected_value.as_bytes());

            let (value, md) = strings_db.get(&key)?.unwrap();
            let max_bound: u64 = i * 10;
            let lower_bound: u64 = max_bound - 1;

            assert!(md.expiration().ttl_in_seconds()? <= max_bound);
            assert!(md.expiration().ttl_in_seconds()? >= lower_bound);
            assert_eq!(value, expected_value);
        }
        Ok(())
    }
}
