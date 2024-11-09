use crate::{
    metadata::{Bookkeeping, SetMemberKey, SetValueMetadata, ValueType},
    storage::DbWriteCache,
    CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

// Internal enum
#[derive(Debug, PartialEq, Eq)]
pub enum GetSetMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(SetValueMetadata),
    /// No entry exist
    NotFound,
}

/// `SetDb::put` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetPutResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Returns the number of members added to the SET
    Some(usize),
}

/// `SetDb::get_multi` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetGetMultiResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The results
    Some(Vec<Option<BytesMut>>),
    /// No members were found
    None,
}

/// `SetDb::get` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetGetResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// The results
    Some(BytesMut),
    /// No members were found
    NotFound,
    /// member does not exist in hash
    MemberNotFound,
}

/// `SetDb::delete` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetDeleteResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Number of items deleted
    Some(usize),
}

/// `SetDb::delete` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetLenResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Number of items deleted
    Some(usize),
}

/// `SetDb::contains_hash_member` result
#[derive(PartialEq, Eq, Debug)]
pub enum SetExistsResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// member exists in hash
    Exists,
    /// member does not exist in hash
    NotExists,
}

enum PutMemberResult {
    /// Ok...
    Inserted,
    /// Already exists in set
    AlreadyExists,
}

/// Result for the `find_smallest`
#[derive(PartialEq, Debug)]
pub enum FindSmallestResult<'a> {
    /// None of the provided keys exist in the database
    NotFound,
    /// One of the provided input keys is not a SET
    WrongType,
    /// Contains the smallest SET name
    Some(&'a BytesMut),
}

/// Set DB wrapper. This class is specialized in reading/writing SET
/// (commands from the `SADD`, `SMEMBERS` etc family)
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
pub struct SetDb<'a> {
    store: &'a StorageAdapter,
    db_id: u16,
    cache: Box<DbWriteCache<'a>>,
}

#[allow(dead_code)]
impl<'a> SetDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        let cache = Box::new(DbWriteCache::with_storage(store));
        SetDb {
            store,
            db_id,
            cache,
        }
    }

    /// Add `members` to the set identified by `user_key`
    /// If the set `user_key` does not exist, a new set is created
    /// If `overwrite_key` is `true` and a key with `user_key` already exists in the database
    /// it is overwritten
    pub fn put_multi(
        &mut self,
        user_key: &BytesMut,
        members: &[&BytesMut],
    ) -> Result<SetPutResult, SableError> {
        self.put_multi_internal(user_key, members, false)
    }

    /// Add `members` to the set identified by `user_key`, if the set `user_key` does not exist, a new set is created.
    /// If a key with `user_key` already exists in the database, it is overwritten
    /// Returns the number of items added to the set
    pub fn put_multi_overwrite(
        &mut self,
        user_key: &BytesMut,
        members: &[&BytesMut],
    ) -> Result<usize, SableError> {
        match self.put_multi_internal(user_key, members, true)? {
            SetPutResult::WrongType => {
                tracing::error!("Set::put_multi_overwrite returned 'WrongType'");
                Err(SableError::ClientInvalidState)
            }
            SetPutResult::Some(count) => Ok(count),
        }
    }

    /// Return the values associated with the provided members.
    pub fn get_multi(
        &self,
        user_key: &BytesMut,
        members: &[&BytesMut],
    ) -> Result<SetGetMultiResult, SableError> {
        if members.is_empty() {
            return Ok(SetGetMultiResult::None);
        }

        // locate the hash
        let hash = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => {
                return Ok(SetGetMultiResult::WrongType);
            }
            GetSetMetadataResult::NotFound => {
                return Ok(SetGetMultiResult::None);
            }
            GetSetMetadataResult::Some(hash) => hash,
        };

        let mut values = Vec::<Option<BytesMut>>::with_capacity(members.len());
        for member in members {
            values.push(self.get_hash_member_value(hash.id(), member)?);
        }

        Ok(SetGetMultiResult::Some(values))
    }

    /// Return the value of a hash member
    pub fn get(&self, user_key: &BytesMut, member: &BytesMut) -> Result<SetGetResult, SableError> {
        // locate the hash
        let hash = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => {
                return Ok(SetGetResult::WrongType);
            }
            GetSetMetadataResult::NotFound => {
                return Ok(SetGetResult::NotFound);
            }
            GetSetMetadataResult::Some(hash) => hash,
        };

        let Some(value) = self.get_hash_member_value(hash.id(), member)? else {
            return Ok(SetGetResult::MemberNotFound);
        };

        Ok(SetGetResult::Some(value))
    }

    /// Removes the specified members from the hash stored at `user_key`
    pub fn delete(
        &mut self,
        user_key: &BytesMut,
        members: &[&BytesMut],
    ) -> Result<SetDeleteResult, SableError> {
        // locate the hash
        let mut hash = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => {
                return Ok(SetDeleteResult::WrongType);
            }
            GetSetMetadataResult::NotFound => {
                return Ok(SetDeleteResult::Some(0));
            }
            GetSetMetadataResult::Some(hash) => hash,
        };

        let mut items_deleted = 0usize;
        for member in members {
            if self.contains_set_member(hash.id(), member)? {
                self.delete_set_member_key(hash.id(), member)?;
                items_deleted = items_deleted.saturating_add(1);
            }
        }

        // update the hash metadata
        hash.decr_len_by(items_deleted as u64);
        if hash.is_empty() {
            self.delete_set_metadata(user_key, &hash)?;
        } else {
            self.put_set_metadata(user_key, &hash)?;
        }
        Ok(SetDeleteResult::Some(items_deleted))
    }

    /// Return the size of the hash
    pub fn len(&self, user_key: &BytesMut) -> Result<SetLenResult, SableError> {
        let md = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => {
                return Ok(SetLenResult::WrongType);
            }
            GetSetMetadataResult::NotFound => {
                return Ok(SetLenResult::Some(0));
            }
            GetSetMetadataResult::Some(md) => md,
        };
        Ok(SetLenResult::Some(md.len() as usize))
    }

    /// Check whether `user_member` exists in the hash `user_key`
    pub fn member_exists(
        &self,
        user_key: &BytesMut,
        user_member: &BytesMut,
    ) -> Result<SetExistsResult, SableError> {
        // locate the hash
        let hash = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => {
                return Ok(SetExistsResult::WrongType);
            }
            GetSetMetadataResult::NotFound => {
                return Ok(SetExistsResult::NotExists);
            }
            GetSetMetadataResult::Some(hash) => hash,
        };

        if self.contains_set_member(hash.id(), user_member)? {
            Ok(SetExistsResult::Exists)
        } else {
            Ok(SetExistsResult::NotExists)
        }
    }

    /// Load hash value metadata from the store
    pub fn set_metadata(&self, user_key: &BytesMut) -> Result<GetSetMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.cache.get(&encoded_key)? else {
            return Ok(GetSetMetadataResult::NotFound);
        };

        match self.try_decode_set_value_metadata(&value)? {
            None => Ok(GetSetMetadataResult::WrongType),
            Some(hash_md) => Ok(GetSetMetadataResult::Some(hash_md)),
        }
    }

    /// Given list of sets, return the one with the least items
    pub fn find_smallest<'b>(
        &self,
        keys: &'b [&BytesMut],
    ) -> Result<FindSmallestResult<'b>, SableError> {
        if keys.is_empty() {
            // fast path
            return Ok(FindSmallestResult::NotFound);
        }

        let mut smallest_len = usize::MAX;
        let mut smallest_set_index = 0usize;
        let mut curidx = 0usize;
        for key in keys {
            match self.len(key)? {
                SetLenResult::WrongType => return Ok(FindSmallestResult::WrongType),
                SetLenResult::Some(curlen) => {
                    if curlen < smallest_len {
                        smallest_len = curlen;
                        smallest_set_index = curidx;
                    }
                }
            }
            curidx = curidx.saturating_add(1);
        }
        Ok(FindSmallestResult::Some(keys[smallest_set_index]))
    }

    /// Apply cache changes to the disk
    pub fn commit(&mut self) -> Result<(), SableError> {
        self.flush_cache()
    }

    ///=======================================================
    /// Internal API for this class
    ///=======================================================

    /// Add `members` to the set identified by `user_key`
    /// If the set `user_key` does not exist, a new set is created
    /// If `overwrite_key` is `true`, delete any entry that holds `user_key` before we proceed
    fn put_multi_internal(
        &mut self,
        user_key: &BytesMut,
        members: &[&BytesMut],
        overwrite_key: bool,
    ) -> Result<SetPutResult, SableError> {
        if overwrite_key {
            let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
            self.cache.delete(&encoded_key)?;
        }

        // locate the set
        let mut set = match self.set_metadata(user_key)? {
            GetSetMetadataResult::WrongType => return Ok(SetPutResult::WrongType),
            GetSetMetadataResult::NotFound => {
                // Create a entry
                self.create_set_metadata(user_key)?
            }
            GetSetMetadataResult::Some(set) => set,
        };

        let mut items_added = 0usize;
        for member in members {
            match self.put_set_member(set.id(), member)? {
                PutMemberResult::AlreadyExists => {}
                PutMemberResult::Inserted => items_added = items_added.saturating_add(1),
            }
        }

        set.incr_len_by(items_added as u64);
        self.put_set_metadata(user_key, &set)?;
        Ok(SetPutResult::Some(items_added))
    }
    /// Apply the changes to the store and clear the cache
    fn flush_cache(&mut self) -> Result<(), SableError> {
        self.cache.flush()
    }

    /// Put a hash entry in the database
    fn put_set_metadata(
        &mut self,
        user_key: &BytesMut,
        set_md: &SetValueMetadata,
    ) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);

        // serialise the hash value into bytes
        let mut buffer = BytesMut::with_capacity(SetValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        set_md.to_bytes(&mut builder);

        self.cache.put(&encoded_key, buffer)?;
        Ok(())
    }

    /// Delete the SET metadata
    fn delete_set_metadata(
        &mut self,
        user_key: &BytesMut,
        set_md: &SetValueMetadata,
    ) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&encoded_key)?;

        // Delete the bookkeeping record
        let bookkeeping_record =
            Bookkeeping::new(self.db_id, crate::utils::calculate_slot(user_key))
                .with_uid(set_md.id())
                .with_value_type(ValueType::Set)
                .to_bytes();
        self.cache.delete(&bookkeeping_record)?;
        Ok(())
    }

    /// Create or replace a SET entry in the database
    fn create_set_metadata(&mut self, user_key: &BytesMut) -> Result<SetValueMetadata, SableError> {
        let set_md = SetValueMetadata::with_id(self.store.generate_id());
        self.put_set_metadata(user_key, &set_md)?;

        // Add a bookkeeping record
        let bookkeeping_record =
            Bookkeeping::new(self.db_id, crate::utils::calculate_slot(user_key))
                .with_uid(set_md.id())
                .with_value_type(ValueType::Set)
                .to_bytes();
        self.cache.put(&bookkeeping_record, user_key.clone())?;
        Ok(set_md)
    }

    /// Encode an hash member key from user member
    fn encode_set_member_key(
        &self,
        set_id: u64,
        user_member: &BytesMut,
    ) -> Result<BytesMut, SableError> {
        let mut buffer = BytesMut::with_capacity(256);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        let member_key = SetMemberKey::with_user_key(set_id, user_member);
        member_key.to_bytes(&mut builder);
        Ok(buffer)
    }

    /// Delete hash member from the database
    fn delete_set_member_key(
        &mut self,
        set_id: u64,
        user_member: &BytesMut,
    ) -> Result<(), SableError> {
        let key = self.encode_set_member_key(set_id, user_member)?;
        self.cache.delete(&key)?;
        Ok(())
    }

    /// Return the value of hash member
    fn get_hash_member_value(
        &self,
        set_id: u64,
        user_member: &BytesMut,
    ) -> Result<Option<BytesMut>, SableError> {
        let key = self.encode_set_member_key(set_id, user_member)?;
        self.cache.get(&key)
    }

    /// Return the value of hash member
    fn contains_set_member(&self, set_id: u64, user_member: &BytesMut) -> Result<bool, SableError> {
        let key = self.encode_set_member_key(set_id, user_member)?;
        self.cache.contains(&key)
    }

    /// Put the value for a hash member
    fn put_set_member(
        &mut self,
        set_id: u64,
        user_member: &BytesMut,
    ) -> Result<PutMemberResult, SableError> {
        let key = self.encode_set_member_key(set_id, user_member)?;
        let updating = self.cache.contains(&key)?;
        self.cache.put(&key, BytesMut::default())?;
        Ok(if updating {
            PutMemberResult::AlreadyExists
        } else {
            PutMemberResult::Inserted
        })
    }

    /// Given raw bytes (read from the db) return whether it represents a `SetValueMetadata`
    fn try_decode_set_value_metadata(
        &self,
        value: &BytesMut,
    ) -> Result<Option<SetValueMetadata>, SableError> {
        let mut reader = U8ArrayReader::with_buffer(value);
        let common_md = CommonValueMetadata::from_bytes(&mut reader)?;
        if !common_md.is_set() {
            return Ok(None);
        }

        reader.rewind();
        let set_md = SetValueMetadata::from_bytes(&mut reader)?;
        Ok(Some(set_md))
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
    use crate::storage::PutFlags;

    #[test]
    fn test_set_wrong_type() -> Result<(), SableError> {
        let (_deleter, db) = crate::tests::open_store();
        let mut set_db = SetDb::with_storage(&db, 0);
        let mut strings_db = crate::storage::StringsDb::with_storage(&db, 0);

        let string_md = crate::StringValueMetadata::default();

        let key = BytesMut::from("key");
        let value = BytesMut::from("value");
        strings_db.put(&key, &value, &string_md, PutFlags::Override)?;

        // run a hash operation on a string key
        assert_eq!(set_db.len(&key).unwrap(), SetLenResult::WrongType);
        assert_eq!(
            set_db.get_multi(&key, &[&key]).unwrap(),
            SetGetMultiResult::WrongType
        );
        assert_eq!(
            set_db.delete(&key, &[&key]).unwrap(),
            SetDeleteResult::WrongType
        );
        assert_eq!(
            set_db.put_multi(&key, &[(&key)]).unwrap(),
            SetPutResult::WrongType
        );
        Ok(())
    }

    #[test]
    fn test_bookkeeping_record() {
        let (_deleter, db) = crate::tests::open_store();
        let mut set_db = SetDb::with_storage(&db, 0);
        let set_name = BytesMut::from("myset");

        // put (which creates) a new hash item in the database
        let member1 = BytesMut::from("member1");
        assert_eq!(
            set_db.put_multi(&set_name, &[(&member1)]).unwrap(),
            SetPutResult::Some(1)
        );

        set_db.commit().unwrap();

        // confirm that we have a bookkeeping record
        let set_id = match set_db.set_metadata(&set_name).unwrap() {
            GetSetMetadataResult::Some(md) => md.id(),
            _ => {
                panic!("Expected to find the set MD in the database");
            }
        };

        let bookkeeping_record_key = Bookkeeping::new(0, crate::utils::calculate_slot(&set_name))
            .with_uid(set_id)
            .with_value_type(ValueType::Set)
            .to_bytes();

        let db_hash_name = db.get(&bookkeeping_record_key).unwrap().unwrap();
        assert_eq!(db_hash_name, set_name);

        // delete the only entry from the hash -> this should remove the hash completely from the database
        set_db.delete(&set_name, &[&member1]).unwrap();
        set_db.commit().unwrap();

        // confirm that the bookkeeping record was also removed from the database
        assert!(db.get(&bookkeeping_record_key).unwrap().is_none());
    }

    #[test]
    fn test_set_db() -> Result<(), SableError> {
        let (_deleter, db) = crate::tests::open_store();
        let mut set_db = SetDb::with_storage(&db, 0);

        let hash_name = BytesMut::from("myset");
        let hash_name_2 = BytesMut::from("myset_2");

        let member1 = BytesMut::from("member1");
        let member2 = BytesMut::from("member2");
        let member3 = BytesMut::from("member3");
        let no_such_member = BytesMut::from("nosuchmember");

        assert_eq!(
            set_db.put_multi(&hash_name, &[&member1, &member2, &member3])?,
            SetPutResult::Some(3)
        );

        assert_eq!(
            set_db.put_multi(&hash_name_2, &[&member1, &member2, &member3])?,
            SetPutResult::Some(3)
        );

        assert_eq!(set_db.len(&hash_name).unwrap(), SetLenResult::Some(3));
        assert_eq!(
            set_db
                .delete(&hash_name, &[&member1, &no_such_member])
                .unwrap(),
            SetDeleteResult::Some(1)
        );

        assert_eq!(set_db.len(&hash_name).unwrap(), SetLenResult::Some(2));

        {
            // Check the first hash

            let SetGetMultiResult::Some(results_vec) = set_db
                .get_multi(&hash_name, &[&member1, &no_such_member, &member2, &member3])
                .unwrap()
            else {
                panic!("get failed");
            };

            // non existing members, will create a `None` value in the result array
            assert_eq!(results_vec.len(), 4);

            // Since we deleted `member1` earlier, we expect a None value
            let expected_values = vec![
                None,
                None,
                Some(BytesMut::default()),
                Some(BytesMut::default()),
            ];
            for i in 0..4 {
                let result = results_vec.get(i).unwrap();
                assert_eq!(result, &expected_values[i]);
            }
        }
        {
            // Confirm that the manipulations on the first hash did not impact the second one
            let SetGetMultiResult::Some(results_vec) = set_db
                .get_multi(
                    &hash_name_2,
                    &[&member1, &no_such_member, &member2, &member3],
                )
                .unwrap()
            else {
                panic!("get failed");
            };

            // non existing members, will create a `None` value in the result array
            assert_eq!(results_vec.len(), 4);

            // This time, `member1` should still be in the hash
            let expected_values = vec![
                Some(BytesMut::default()),
                None,
                Some(BytesMut::default()),
                Some(BytesMut::default()),
            ];
            for i in 0..4 {
                let result = results_vec.get(i).unwrap();
                assert_eq!(result, &expected_values[i]);
            }
            assert_eq!(set_db.len(&hash_name_2).unwrap(), SetLenResult::Some(3));
        }
        Ok(())
    }

    #[test]
    fn test_find_smallest_set() {
        let (_deleter, db) = crate::tests::open_store();

        let mut set_db = SetDb::with_storage(&db, 0);
        let val1 = BytesMut::from("value1");
        let val2 = BytesMut::from("value2");
        let val3 = BytesMut::from("value3");
        let val4 = BytesMut::from("value4");

        let set1 = BytesMut::from("set1");
        let set2 = BytesMut::from("set2");
        let set3 = BytesMut::from("set3");
        let set4 = BytesMut::from("set4");

        set_db.put_multi(&set1, &vec![&val1]).unwrap();
        set_db.put_multi(&set2, &vec![&val1, &val2]).unwrap();
        set_db.put_multi(&set3, &vec![&val1, &val2, &val3]).unwrap();
        set_db
            .put_multi(&set4, &vec![&val1, &val2, &val3, &val4])
            .unwrap();
        set_db.commit().unwrap();

        let sets = vec![&set2, &set1, &set3, &set4];
        let result = set_db.find_smallest(&sets).unwrap();
        assert_eq!(result, FindSmallestResult::Some(&set1));
    }
}
