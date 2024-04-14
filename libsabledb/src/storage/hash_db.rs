/// A database accessor that does not really care about the value
#[allow(unused_imports)]
use crate::{
    metadata::{HashFieldKey, HashValueMetadata},
    storage::{DbWriteCache, PutFlags},
    CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

// Internal enum
#[derive(Debug, PartialEq, Eq)]
pub enum GetHashMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(HashValueMetadata),
    /// No entry exist
    NotFound,
}

/// `HashDb::put` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashPutResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Put `usize` elements (usize is > 0)
    Some(usize),
}

/// `HashDb::get_multi` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashGetMultiResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The results
    Some(Vec<Option<BytesMut>>),
    /// No fields were found
    None,
}

/// `HashDb::get` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashGetResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The results
    Some(BytesMut),
    /// No fields were found
    NotFound,
    /// Field does not exist in hash
    FieldNotFound,
}

/// `HashDb::delete` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashDeleteResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Number of items deleted
    Some(usize),
}

/// `HashDb::delete` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashLenResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Number of items deleted
    Some(usize),
}

/// `HashDb::contains_hash_field` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashExistsResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Field exists in hash
    Exists,
    /// Field does not exist in hash
    NotExists,
}

enum PutFieldResult {
    /// Ok...
    Inserted,
    /// Already exists in hash
    Updated,
}

/// Hash DB wrapper. This class is specialized in reading/writing hash
/// (commands from the `HSET`, `HLEN` etc family)
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
pub struct HashDb<'a> {
    /// This class handles String command database access
    store: &'a StorageAdapter,
    db_id: u16,
    cache: Box<DbWriteCache<'a>>,
}

#[allow(dead_code)]
impl<'a> HashDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        let cache = Box::new(DbWriteCache::with_storage(store));
        HashDb {
            store,
            db_id,
            cache,
        }
    }

    /// Sets the specified fields to their respective values in the hash stored at `user_key`
    pub fn put_multi(
        &self,
        user_key: &BytesMut,
        field_vals: &[(&BytesMut, &BytesMut)],
    ) -> Result<HashPutResult, SableError> {
        if field_vals.is_empty() {
            return Ok(HashPutResult::Some(0));
        }

        // locate the hash
        let mut hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => return Ok(HashPutResult::WrongType),
            GetHashMetadataResult::NotFound => {
                // Create a entry
                self.create_hash_metadata(user_key)?
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let mut items_added = 0usize;
        for (key, value) in field_vals {
            // we overide the field's value
            match self.put_hash_field_value(hash.id(), key, value)? {
                PutFieldResult::Updated => {}
                PutFieldResult::Inserted => items_added = items_added.saturating_add(1),
            }
        }

        hash.incr_len_by(items_added as u64);
        self.put_hash_metadata(user_key, &hash)?;

        // flush the changes
        self.flush_cache()?;

        Ok(HashPutResult::Some(items_added))
    }

    /// Return the values associated with the provided fields.
    pub fn get_multi(
        &self,
        user_key: &BytesMut,
        fields: &[&BytesMut],
    ) -> Result<HashGetMultiResult, SableError> {
        if fields.is_empty() {
            return Ok(HashGetMultiResult::None);
        }

        // locate the hash
        let hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashGetMultiResult::WrongType);
            }
            GetHashMetadataResult::NotFound => {
                return Ok(HashGetMultiResult::None);
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let mut values = Vec::<Option<BytesMut>>::with_capacity(fields.len());
        for field in fields {
            values.push(self.get_hash_field_value(hash.id(), field)?);
        }

        Ok(HashGetMultiResult::Some(values))
    }

    /// Return the value of a hash field
    pub fn get(&self, user_key: &BytesMut, field: &BytesMut) -> Result<HashGetResult, SableError> {
        // locate the hash
        let hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashGetResult::WrongType);
            }
            GetHashMetadataResult::NotFound => {
                return Ok(HashGetResult::NotFound);
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let Some(value) = self.get_hash_field_value(hash.id(), field)? else {
            return Ok(HashGetResult::FieldNotFound);
        };

        Ok(HashGetResult::Some(value))
    }

    /// Removes the specified fields from the hash stored at `user_key`
    pub fn delete(
        &self,
        user_key: &BytesMut,
        fields: &[&BytesMut],
    ) -> Result<HashDeleteResult, SableError> {
        // locate the hash
        let mut hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashDeleteResult::WrongType);
            }
            GetHashMetadataResult::NotFound => {
                return Ok(HashDeleteResult::Some(0));
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let mut items_deleted = 0usize;
        for field in fields {
            if self.contains_hash_field(hash.id(), field)? {
                self.delete_hash_field_key(hash.id(), field)?;
                items_deleted = items_deleted.saturating_add(1);
            }
        }

        // update the hash metadata
        hash.decr_len_by(items_deleted as u64);
        if hash.is_empty() {
            self.delete_hash_metadata(user_key)?;
        } else {
            self.put_hash_metadata(user_key, &hash)?;
        }

        // flush the changes
        self.flush_cache()?;

        Ok(HashDeleteResult::Some(items_deleted))
    }

    /// Return the size of the hash
    pub fn len(&self, user_key: &BytesMut) -> Result<HashLenResult, SableError> {
        let hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashLenResult::WrongType);
            }
            GetHashMetadataResult::NotFound => {
                return Ok(HashLenResult::Some(0));
            }
            GetHashMetadataResult::Some(hash) => hash,
        };
        Ok(HashLenResult::Some(hash.len() as usize))
    }

    /// Check whether `user_field` exists in the hash `user_key`
    pub fn field_exists(
        &self,
        user_key: &BytesMut,
        user_field: &BytesMut,
    ) -> Result<HashExistsResult, SableError> {
        // locate the hash
        let hash = match self.hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashExistsResult::WrongType);
            }
            GetHashMetadataResult::NotFound => {
                return Ok(HashExistsResult::NotExists);
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        if self.contains_hash_field(hash.id(), user_field)? {
            Ok(HashExistsResult::Exists)
        } else {
            Ok(HashExistsResult::NotExists)
        }
    }

    /// Load hash value metadata from the store
    pub fn hash_metadata(&self, user_key: &BytesMut) -> Result<GetHashMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.cache.get(&encoded_key)? else {
            return Ok(GetHashMetadataResult::NotFound);
        };

        match self.try_decode_hash_value_metadata(&value)? {
            None => Ok(GetHashMetadataResult::WrongType),
            Some(hash_md) => Ok(GetHashMetadataResult::Some(hash_md)),
        }
    }

    ///=======================================================
    /// Internal API for this class
    ///=======================================================

    /// Apply the changes to the store and clear the cache
    fn flush_cache(&self) -> Result<(), SableError> {
        let batch = self.cache.to_write_batch();
        if batch.is_empty() {
            return Ok(());
        }
        self.cache.clear();
        self.store.apply_batch(&batch)
    }

    /// Put a hash entry in the database
    fn put_hash_metadata(
        &self,
        user_key: &BytesMut,
        hash_md: &HashValueMetadata,
    ) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);

        // serialise the hash value into bytes
        let mut buffer = BytesMut::with_capacity(HashValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        hash_md.to_bytes(&mut builder);

        self.cache.put(&encoded_key, buffer)?;
        Ok(())
    }

    /// Delete the hash metadata
    fn delete_hash_metadata(&self, user_key: &BytesMut) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&encoded_key)?;
        Ok(())
    }

    /// Create or replace a hash entry in the database
    /// If `hash_id_opt` is `None`, create a new id and put it
    /// else, override the existing entry
    fn create_hash_metadata(&self, user_key: &BytesMut) -> Result<HashValueMetadata, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let hash_md = HashValueMetadata::with_id(self.store.generate_id());

        // serialise the hash value into bytes
        let mut buffer = BytesMut::with_capacity(HashValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        hash_md.to_bytes(&mut builder);

        self.cache.put(&encoded_key, buffer)?;
        Ok(hash_md)
    }

    /// Encode an hash field key from user field
    fn encode_hash_field_key(
        &self,
        hash_id: u64,
        user_field: &BytesMut,
    ) -> Result<BytesMut, SableError> {
        let mut buffer = BytesMut::with_capacity(256);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        let field_key = HashFieldKey::with_user_key(hash_id, user_field);
        field_key.to_bytes(&mut builder);
        Ok(buffer)
    }

    /// Delete hash field from the database
    fn delete_hash_field_key(&self, hash_id: u64, user_field: &BytesMut) -> Result<(), SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        self.cache.delete(&key)?;
        Ok(())
    }

    /// Return the value of hash field
    fn get_hash_field_value(
        &self,
        hash_id: u64,
        user_field: &BytesMut,
    ) -> Result<Option<BytesMut>, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        self.cache.get(&key)
    }

    /// Return the value of hash field
    fn contains_hash_field(&self, hash_id: u64, user_field: &BytesMut) -> Result<bool, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        self.cache.contains(&key)
    }

    /// Put the value for a hash field
    fn put_hash_field_value(
        &self,
        hash_id: u64,
        user_field: &BytesMut,
        user_value: &BytesMut,
    ) -> Result<PutFieldResult, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        let updating = self.cache.contains(&key)?;
        self.cache.put(&key, user_value.clone())?;
        Ok(if updating {
            PutFieldResult::Updated
        } else {
            PutFieldResult::Inserted
        })
    }

    /// Given raw bytes (read from the db) return whether it represents a `HashValueMetadata`
    fn try_decode_hash_value_metadata(
        &self,
        value: &BytesMut,
    ) -> Result<Option<HashValueMetadata>, SableError> {
        let mut reader = U8ArrayReader::with_buffer(value);
        let common_md = CommonValueMetadata::from_bytes(&mut reader)?;
        if !common_md.is_hash() {
            return Ok(None);
        }

        reader.rewind();
        let hash_md = HashValueMetadata::from_bytes(&mut reader)?;
        Ok(Some(hash_md))
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
    use crate::StorageOpenParams;
    use std::path::PathBuf;

    fn create_database(db_name: &str) -> StorageAdapter {
        let _ = std::fs::create_dir_all("tests");
        let db_path = PathBuf::from(format!("tests/{}.db", db_name));
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(true)
            .set_cache_size(64)
            .set_path(&db_path);
        crate::storage_rocksdb!(open_params.clone())
    }

    #[test]
    fn test_hash_wrong_type() -> Result<(), SableError> {
        let db = create_database("test_hash_wrong_type");
        let hash_db = HashDb::with_storage(&db, 0);
        let strings_db = crate::storage::StringsDb::with_storage(&db, 0);

        let string_md = crate::StringValueMetadata::default();

        let key = BytesMut::from("key");
        let value = BytesMut::from("value");
        strings_db.put(&key, &value, &string_md, PutFlags::Override)?;

        // run a hash operation on a string key
        assert_eq!(hash_db.len(&key).unwrap(), HashLenResult::WrongType);
        assert_eq!(
            hash_db.get_multi(&key, &[&key]).unwrap(),
            HashGetMultiResult::WrongType
        );
        assert_eq!(
            hash_db.delete(&key, &[&key]).unwrap(),
            HashDeleteResult::WrongType
        );
        assert_eq!(
            hash_db.put_multi(&key, &[(&key, &key)]).unwrap(),
            HashPutResult::WrongType
        );
        Ok(())
    }

    #[test]
    fn test_hash_db() -> Result<(), SableError> {
        let db = create_database("test_hash_db");
        let hash_db = HashDb::with_storage(&db, 0);

        let hash_name = BytesMut::from("myhash");
        let hash_name_2 = BytesMut::from("myhash_2");

        let field1 = BytesMut::from("field1");
        let field2 = BytesMut::from("field2");
        let field3 = BytesMut::from("field3");
        let no_such_field = BytesMut::from("nosuchfield");

        assert_eq!(
            hash_db.put_multi(
                &hash_name,
                &[(&field1, &field1), (&field2, &field2), (&field3, &field3)]
            )?,
            HashPutResult::Some(3)
        );

        assert_eq!(
            hash_db.put_multi(
                &hash_name_2,
                &[(&field1, &field1), (&field2, &field2), (&field3, &field3)]
            )?,
            HashPutResult::Some(3)
        );

        assert_eq!(hash_db.len(&hash_name).unwrap(), HashLenResult::Some(3));
        assert_eq!(
            hash_db
                .delete(&hash_name, &[&field1, &no_such_field])
                .unwrap(),
            HashDeleteResult::Some(1)
        );

        assert_eq!(hash_db.len(&hash_name).unwrap(), HashLenResult::Some(2));

        {
            // Check the first hash

            let HashGetMultiResult::Some(results_vec) = hash_db
                .get_multi(&hash_name, &[&field1, &no_such_field, &field2, &field3])
                .unwrap()
            else {
                panic!("get failed");
            };

            // non existing fields, will create a `None` value in the result array
            assert_eq!(results_vec.len(), 4);

            // Since we deleted `field1` earlier, we expect a None value
            let expected_values = vec![None, None, Some(field2.clone()), Some(field3.clone())];
            for i in 0..4 {
                let result = results_vec.get(i).unwrap();
                assert_eq!(result, &expected_values[i]);
            }
        }
        {
            // Confirm that the manipulations on the first hash did not impact the second one
            let HashGetMultiResult::Some(results_vec) = hash_db
                .get_multi(&hash_name_2, &[&field1, &no_such_field, &field2, &field3])
                .unwrap()
            else {
                panic!("get failed");
            };

            // non existing fields, will create a `None` value in the result array
            assert_eq!(results_vec.len(), 4);

            // This time, `field1` should still be in the hash
            let expected_values = vec![Some(field1), None, Some(field2), Some(field3)];
            for i in 0..4 {
                let result = results_vec.get(i).unwrap();
                assert_eq!(result, &expected_values[i]);
            }
            assert_eq!(hash_db.len(&hash_name_2).unwrap(), HashLenResult::Some(3));
        }
        Ok(())
    }
}
