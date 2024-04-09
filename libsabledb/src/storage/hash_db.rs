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
enum GetHashMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(HashValueMetadata),
    /// No entry exist
    None,
}

/// `HashDb::put` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashPutResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Put `usize` elements (usize is > 0)
    Some(usize),
}

/// `HashDb::get` result
#[derive(PartialEq, Eq, Debug)]
pub enum HashGetResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The results
    Some(Vec<Option<BytesMut>>),
    /// No fields were found
    None,
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

// internal enums
enum DeleteHashMetadataResult {
    /// Delete succeeded
    Ok,
    /// Hash not found
    NotFound,
    /// Exists, but with wrong type
    WrongType,
}

enum PutFieldResult {
    /// Ok...
    Ok,
    /// Already exists in hash
    AlreadyExists,
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
    #[allow(dead_code)]
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
    pub fn put(
        &self,
        user_key: &BytesMut,
        field_vals: &[(&BytesMut, &BytesMut)],
    ) -> Result<HashPutResult, SableError> {
        if field_vals.is_empty() {
            return Ok(HashPutResult::Some(0));
        }

        // locate the hash
        let mut hash = match self.get_hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => return Ok(HashPutResult::WrongType),
            GetHashMetadataResult::None => {
                // Create a entry
                self.create_hash_metadata(user_key)?
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let mut items_added = 0usize;
        for (key, value) in field_vals {
            // we overide the field's value
            match self.put_hash_field_value(hash.id(), key, value)? {
                PutFieldResult::AlreadyExists => {}
                PutFieldResult::Ok => items_added = items_added.saturating_add(1),
            }
        }

        hash.incr_len_by(items_added as u64);
        self.put_hash_metadata(user_key, &hash)?;
        Ok(HashPutResult::Some(items_added))
    }

    /// Return the values associated with the provided fields.
    pub fn get(
        &self,
        user_key: &BytesMut,
        fields: &[&BytesMut],
    ) -> Result<HashGetResult, SableError> {
        if fields.is_empty() {
            return Ok(HashGetResult::None);
        }

        // locate the hash
        let hash = match self.get_hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashGetResult::WrongType);
            }
            GetHashMetadataResult::None => {
                return Ok(HashGetResult::None);
            }
            GetHashMetadataResult::Some(hash) => hash,
        };

        let mut values = Vec::<Option<BytesMut>>::with_capacity(fields.len());
        for field in fields {
            values.push(self.get_hash_field_value(hash.id(), field)?);
        }

        Ok(HashGetResult::Some(values))
    }

    /// Removes the specified fields from the hash stored at `user_key`
    pub fn delete(
        &self,
        user_key: &BytesMut,
        fields: &[&BytesMut],
    ) -> Result<HashDeleteResult, SableError> {
        // locate the hash
        let mut hash = match self.get_hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashDeleteResult::WrongType);
            }
            GetHashMetadataResult::None => {
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
        self.put_hash_metadata(user_key, &hash)?;
        Ok(HashDeleteResult::Some(items_deleted))
    }

    /// Return the size of the hash
    pub fn len(&self, user_key: &BytesMut) -> Result<HashLenResult, SableError> {
        let hash = match self.get_hash_metadata(user_key)? {
            GetHashMetadataResult::WrongType => {
                return Ok(HashLenResult::WrongType);
            }
            GetHashMetadataResult::None => {
                return Ok(HashLenResult::Some(0));
            }
            GetHashMetadataResult::Some(hash) => hash,
        };
        Ok(HashLenResult::Some(hash.len() as usize))
    }

    ///=======================================================
    /// Internal API for this class
    ///=======================================================

    /// Load hash value metadata from the store
    fn get_hash_metadata(&self, user_key: &BytesMut) -> Result<GetHashMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.store.get(&encoded_key)? else {
            return Ok(GetHashMetadataResult::None);
        };

        match self.try_decode_hash_value_metadata(&value)? {
            None => Ok(GetHashMetadataResult::WrongType),
            Some(hash_md) => Ok(GetHashMetadataResult::Some(hash_md)),
        }
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

        self.store.put(&encoded_key, &buffer, PutFlags::Override)?;
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

        self.store.put(&encoded_key, &buffer, PutFlags::Override)?;
        Ok(hash_md)
    }

    /// Delete an hash from the database
    fn delete_hash_metadata(
        &self,
        user_key: &BytesMut,
    ) -> Result<DeleteHashMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.store.get(&encoded_key)? else {
            return Ok(DeleteHashMetadataResult::NotFound);
        };

        match self.try_decode_hash_value_metadata(&value)? {
            None => Ok(DeleteHashMetadataResult::WrongType),
            Some(_) => {
                self.store.delete(&encoded_key)?;
                Ok(DeleteHashMetadataResult::Ok)
            }
        }
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
        self.store.delete(&key)?;
        Ok(())
    }

    /// Return the value of hash field
    fn get_hash_field_value(
        &self,
        hash_id: u64,
        user_field: &BytesMut,
    ) -> Result<Option<BytesMut>, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        self.store.get(&key)
    }

    /// Return the value of hash field
    fn contains_hash_field(&self, hash_id: u64, user_field: &BytesMut) -> Result<bool, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        self.store.contains(&key)
    }

    /// Put the value for a hash field
    fn put_hash_field_value(
        &self,
        hash_id: u64,
        user_field: &BytesMut,
        user_value: &BytesMut,
    ) -> Result<PutFieldResult, SableError> {
        let key = self.encode_hash_field_key(hash_id, user_field)?;
        if self.store.contains(&key)? {
            return Ok(PutFieldResult::AlreadyExists);
        }
        self.store.put(&key, user_value, PutFlags::Override)?;
        Ok(PutFieldResult::Ok)
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
            hash_db.get(&key, &[&key]).unwrap(),
            HashGetResult::WrongType
        );
        assert_eq!(
            hash_db.delete(&key, &[&key]).unwrap(),
            HashDeleteResult::WrongType
        );
        assert_eq!(
            hash_db.put(&key, &[(&key, &key)]).unwrap(),
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
            hash_db.put(
                &hash_name,
                &[(&field1, &field1), (&field2, &field2), (&field3, &field3)]
            )?,
            HashPutResult::Some(3)
        );

        assert_eq!(
            hash_db.put(
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

            let HashGetResult::Some(results_vec) = hash_db
                .get(&hash_name, &[&field1, &no_such_field, &field2, &field3])
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
            let HashGetResult::Some(results_vec) = hash_db
                .get(&hash_name_2, &[&field1, &no_such_field, &field2, &field3])
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
