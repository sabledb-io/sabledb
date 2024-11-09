/// A database accessor that does not really care about the value
use crate::{
    metadata::{Bookkeeping, ValueType, ZSetMemberItem, ZSetScoreItem, ZSetValueMetadata},
    storage::DbWriteCache,
    CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSet {
    pub key: PrimaryKeyMetadata,
    pub metadata: ZSetValueMetadata,
}

impl SortedSet {
    pub fn slot(&self) -> u16 {
        self.key.slot()
    }

    pub fn database_id(&self) -> u16 {
        self.key.database_id()
    }

    pub fn id(&self) -> u64 {
        self.metadata.id()
    }

    pub fn len(&self) -> u64 {
        self.metadata.len()
    }

    pub fn is_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    pub fn incr_len_by(&mut self, n: u64) {
        self.metadata.incr_len_by(n);
    }

    pub fn decr_len_by(&mut self, n: u64) {
        self.metadata.decr_len_by(n);
    }

    /// Return a prefix for iterating over all items in the set with optional score
    pub fn prefix_by_score(&self, score: Option<f64>) -> BytesMut {
        ZSetScoreItem::prefix(self.id(), self.database_id(), self.slot(), score)
    }

    /// Create an upper bound prefix for this set (for iterating over scores)
    pub fn score_upper_bound_prefix(&self) -> BytesMut {
        ZSetScoreItem::prefix(
            self.id().saturating_add(1),
            self.database_id(),
            self.slot(),
            None,
        )
    }

    /// Create a prefix for iterating all items belonged to this zset by member
    pub fn prefix_by_member(&self, member: Option<&[u8]>) -> BytesMut {
        ZSetMemberItem::prefix(self.id(), self.database_id(), self.slot(), member)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FindZSetResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(SortedSet),
    /// No entry exist
    NotFound,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ZSetGetSmallestResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Return the index of the set with the smallest length
    Some(usize),
    /// No entry exist
    None,
}

/// `ZSetDb::delete` result
#[derive(PartialEq, Eq, Debug)]
pub enum ZSetLenResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Number of items deleted
    Some(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ZSetAddMemberResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Put `usize` elements (usize is > 0)
    Some(usize),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ZSetDeleteMemberResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Delete was successful
    Ok,
    /// Member was not found
    MemberNotFound,
    /// The set does not exist
    SetNotFound,
}

#[derive(PartialEq, Debug)]
enum PutMemberResult {
    /// Ok...
    Inserted(f64),
    /// Already exists in hash
    Updated(f64),
    /// No modifications were made to the member
    NotModified,
}

#[derive(PartialEq, Debug)]
pub enum ZSetGetScoreResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// Found the score
    Score(f64),
    /// Not found
    NotFound,
}

bitflags::bitflags! {
pub struct ZWriteFlags: u32  {
    const None = 0;
    /// Only update elements that already exist. Don't add new elements.
    const Xx = 1 << 0;
    /// Only add new elements. Don't update already existing elements.
    const Nx = 1 << 1;
    /// Only update existing elements if the new score is less than the current score.
    /// This flag doesn't prevent adding new elements.
    const Lt = 1 << 2;
    /// Only update existing elements if the new score is greater than the current score.
    /// This flag doesn't prevent adding new elements.
    const Gt = 1 << 3;
    /// Modify the return value from the number of new elements added, to the total number of elements changed
    const Ch = 1 << 4;
    /// When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode
    const Incr = 1 << 5;
}
}

/// ZSet DB wrapper. This class is specialized in reading/writing sorted set data structure (ZSet)
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
pub struct ZSetDb<'a> {
    store: &'a StorageAdapter,
    db_id: u16,
    cache: Box<DbWriteCache<'a>>,
}

#[allow(dead_code)]
impl<'a> ZSetDb<'a> {
    pub fn with_storage(store: &'a StorageAdapter, db_id: u16) -> Self {
        let cache = Box::new(DbWriteCache::with_storage(store));
        ZSetDb {
            store,
            db_id,
            cache,
        }
    }

    /// Return the size of the set
    pub fn len(&self, user_key: &BytesMut) -> Result<ZSetLenResult, SableError> {
        let zset = match self.find_set(user_key)? {
            FindZSetResult::WrongType => {
                return Ok(ZSetLenResult::WrongType);
            }
            FindZSetResult::NotFound => {
                return Ok(ZSetLenResult::Some(0));
            }
            FindZSetResult::Some(zset) => zset,
        };
        Ok(ZSetLenResult::Some(zset.len() as usize))
    }

    /// Add. Return the number of items modified / inserted (depends on the `flags`) input
    /// argument
    pub fn add(
        &mut self,
        user_key: &BytesMut,
        member: &BytesMut,
        score: f64,
        flags: &ZWriteFlags,
        flush_cache: bool,
    ) -> Result<ZSetAddMemberResult, SableError> {
        // locate the hash
        let mut set = match self.find_set(user_key)? {
            FindZSetResult::WrongType => return Ok(ZSetAddMemberResult::WrongType),
            FindZSetResult::NotFound => {
                // Create a new set
                self.create_set(user_key)?
            }
            FindZSetResult::Some(set) => set,
        };

        let mut items_added = 0usize;
        let mut return_value = 0usize;

        let new_score = if flags.intersects(ZWriteFlags::Incr) {
            match self.get_member_score(&set, member)? {
                Some(mut old_value) => {
                    old_value += score;
                    old_value
                }
                None => score,
            }
        } else {
            score
        };

        match self.put_member(&set, member, new_score, flags)? {
            PutMemberResult::Updated(_) => {
                if flags.intersects(ZWriteFlags::Ch) {
                    return_value = return_value.saturating_add(1);
                }
            }
            PutMemberResult::NotModified => {}
            PutMemberResult::Inserted(_) => {
                return_value = return_value.saturating_add(1);
                items_added = items_added.saturating_add(1);
            }
        }

        if items_added > 0 {
            set.incr_len_by(items_added as u64);
            self.put_metadata(user_key, &set.metadata)?;
        }

        // flush the changes
        if flush_cache {
            self.flush_cache()?;
        }

        Ok(ZSetAddMemberResult::Some(return_value))
    }

    /// Delete key (we don't care about the children or the type of the key)
    pub fn delete(&mut self, user_key: &BytesMut, flush_cache: bool) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&encoded_key)?;
        if flush_cache {
            self.flush_cache()?;
        }
        Ok(())
    }

    /// Returns the score of `member` in the sorted set at `user_key`
    pub fn get_score(
        &self,
        user_key: &BytesMut,
        member: &[u8],
    ) -> Result<ZSetGetScoreResult, SableError> {
        let set = match self.find_set(user_key)? {
            FindZSetResult::WrongType => {
                return Ok(ZSetGetScoreResult::WrongType);
            }
            FindZSetResult::NotFound => {
                return Ok(ZSetGetScoreResult::NotFound);
            }
            FindZSetResult::Some(md) => md,
        };

        if let Some(score) = self.get_member_score(&set, member)? {
            Ok(ZSetGetScoreResult::Score(score))
        } else {
            Ok(ZSetGetScoreResult::NotFound)
        }
    }

    /// Commit the changes to the disk
    pub fn commit(&mut self) -> Result<(), SableError> {
        self.flush_cache()
    }

    /// Load SortedSet from the database
    pub fn find_set(&self, user_key: &BytesMut) -> Result<FindZSetResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.cache.get(&encoded_key)? else {
            return Ok(FindZSetResult::NotFound);
        };

        match self.try_decode_zset_value_metadata(&value)? {
            None => Ok(FindZSetResult::WrongType),
            Some(zset_md) => Ok(FindZSetResult::Some(SortedSet {
                key: PrimaryKeyMetadata::new(user_key, self.db_id),
                metadata: zset_md,
            })),
        }
    }

    /// Decode the score from `score_as_bytes`
    pub fn score_from_bytes(&self, score_as_bytes: &[u8]) -> Result<f64, SableError> {
        let mut reader = U8ArrayReader::with_buffer(score_as_bytes);
        reader.read_f64().ok_or(SableError::SerialisationError)
    }

    /// Encode `f64` (AKA score) into `BytesMut`
    pub fn encode_score(&self, score: f64) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(std::mem::size_of::<f64>());
        let mut writer = U8ArrayBuilder::with_buffer(&mut buffer);
        writer.write_f64(score);
        buffer
    }

    /// Given multiple sets represented by `user_keys` return the one with the smallest length
    pub fn find_smallest(
        &self,
        user_keys: &[&BytesMut],
    ) -> Result<ZSetGetSmallestResult, SableError> {
        if user_keys.is_empty() {
            return Ok(ZSetGetSmallestResult::None);
        }

        let mut smallest_len = usize::MAX;
        let mut smallest_set_index = 0usize;
        let mut curidx = 0usize;
        for k in user_keys {
            let zset_length = match self.len(k)? {
                ZSetLenResult::WrongType => return Ok(ZSetGetSmallestResult::WrongType),
                ZSetLenResult::Some(zset_length) => zset_length,
            };

            if zset_length < smallest_len {
                smallest_len = zset_length;
                smallest_set_index = curidx;
            }
            curidx = curidx.saturating_add(1);
        }

        Ok(ZSetGetSmallestResult::Some(smallest_set_index))
    }

    pub fn delete_member(
        &mut self,
        user_key: &BytesMut,
        member: &[u8],
        flush_cache: bool,
    ) -> Result<ZSetDeleteMemberResult, SableError> {
        let mut set = match self.find_set(user_key)? {
            FindZSetResult::WrongType => return Ok(ZSetDeleteMemberResult::WrongType),
            FindZSetResult::NotFound => {
                return Ok(ZSetDeleteMemberResult::SetNotFound);
            }
            FindZSetResult::Some(set) => set,
        };

        if self.delete_member_internal(&set, member)? {
            set.decr_len_by(1);
            if set.is_empty() {
                self.delete_metadata(user_key, &set.metadata)?;
            } else {
                self.put_metadata(user_key, &set.metadata)?;
            }
        } else {
            return Ok(ZSetDeleteMemberResult::MemberNotFound);
        }

        if flush_cache {
            self.commit()?;
        }
        Ok(ZSetDeleteMemberResult::Ok)
    }

    //=== ----------------------------
    // Private methods
    //=== ----------------------------

    /// Apply the changes to the store and clear the cache
    fn flush_cache(&mut self) -> Result<(), SableError> {
        self.cache.flush()
    }

    /// Given raw bytes (read from the db) return whether it represents a `ZSetValueMetadata`
    fn try_decode_zset_value_metadata(
        &self,
        value: &BytesMut,
    ) -> Result<Option<ZSetValueMetadata>, SableError> {
        let mut reader = U8ArrayReader::with_buffer(value);
        let common_md = CommonValueMetadata::from_bytes(&mut reader)?;
        if !common_md.is_zset() {
            return Ok(None);
        }

        reader.rewind();
        let zset_md = ZSetValueMetadata::from_bytes(&mut reader)?;
        Ok(Some(zset_md))
    }

    /// Insert or replace a set entry in the database
    fn create_set(&mut self, user_key: &BytesMut) -> Result<SortedSet, SableError> {
        let md = ZSetValueMetadata::with_id(self.store.generate_id());
        self.put_metadata(user_key, &md)?;

        // Add a bookkeeping record
        let bookkeeping_record =
            Bookkeeping::new(self.db_id, crate::utils::calculate_slot(user_key))
                .with_uid(md.id())
                .with_value_type(ValueType::Zset)
                .to_bytes();
        self.cache.put(&bookkeeping_record, user_key.clone())?;
        Ok(SortedSet {
            key: PrimaryKeyMetadata::new(user_key, self.db_id),
            metadata: md,
        })
    }

    /// Insert or replace a set entry in the database
    fn put_metadata(
        &mut self,
        user_key: &BytesMut,
        md: &ZSetValueMetadata,
    ) -> Result<(), SableError> {
        // serialise the hash value into bytes
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let mut buffer = BytesMut::with_capacity(ZSetValueMetadata::SIZE);
        let mut builder = U8ArrayBuilder::with_buffer(&mut buffer);
        md.to_bytes(&mut builder);

        self.cache.put(&encoded_key, buffer)?;
        Ok(())
    }

    /// Delete the set
    fn delete_metadata(
        &mut self,
        user_key: &BytesMut,
        md: &ZSetValueMetadata,
    ) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&encoded_key)?;

        // Delete the bookkeeping record
        let bookkeeping_record =
            Bookkeeping::new(self.db_id, crate::utils::calculate_slot(user_key))
                .with_uid(md.id())
                .with_value_type(ValueType::Zset)
                .to_bytes();
        self.cache.delete(&bookkeeping_record)?;
        Ok(())
    }

    /// Put member
    fn put_member(
        &mut self,
        set: &SortedSet,
        member: &[u8],
        score: f64,
        flags: &ZWriteFlags,
    ) -> Result<PutMemberResult, SableError> {
        let key_by_member = self.encode_key_by_memebr(set, member);
        let key_by_score = self.encode_key_by_score(set, score, member);

        let result = match self.get_member_score(set, member)? {
            Some(old_score) => {
                if flags.intersects(ZWriteFlags::Nx)
                    || (flags.intersects(ZWriteFlags::Gt) && !score.gt(&old_score))
                    || (flags.intersects(ZWriteFlags::Lt) && !score.lt(&old_score))
                {
                    return Ok(PutMemberResult::NotModified);
                }

                // Ch: update when value is changed
                if old_score.eq(&score) {
                    // No change is needed
                    return Ok(PutMemberResult::NotModified);
                } else {
                    PutMemberResult::Updated(score)
                }
            }
            None => {
                if flags.intersects(ZWriteFlags::Xx) {
                    return Ok(PutMemberResult::NotModified);
                }
                PutMemberResult::Inserted(score)
            }
        };

        let score_as_bytes = self.encode_score(score);
        self.cache.put(&key_by_member, score_as_bytes)?;
        self.cache.put(&key_by_score, BytesMut::new())?;
        Ok(result)
    }

    /// Get member score
    fn get_member_score(&self, set: &SortedSet, member: &[u8]) -> Result<Option<f64>, SableError> {
        let key_by_member = self.encode_key_by_memebr(set, member);

        let Some(value) = self.cache.get(&key_by_member)? else {
            return Ok(None);
        };

        let mut reader = U8ArrayReader::with_buffer(&value);
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;
        Ok(Some(score))
    }

    /// Check if `member` is already part of this set
    fn contains_member(&self, set: &SortedSet, member: &[u8]) -> Result<bool, SableError> {
        let key_by_member = self.encode_key_by_memebr(set, member);
        self.cache.contains(&key_by_member)
    }

    /// Delete member from the set
    fn delete_member_internal(
        &mut self,
        set: &SortedSet,
        member: &[u8],
    ) -> Result<bool, SableError> {
        let key_by_member = self.encode_key_by_memebr(set, member);
        let Some(value) = self.cache.get(&key_by_member)? else {
            return Ok(false);
        };
        let mut reader = U8ArrayReader::with_buffer(&value);
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;

        let key_by_score = self.encode_key_by_score(set, score, member);
        self.cache.delete(&key_by_member)?;
        self.cache.delete(&key_by_score)?;
        Ok(true)
    }

    fn encode_key_by_score(&self, set: &SortedSet, score: f64, member: &[u8]) -> BytesMut {
        let key_by_score =
            ZSetScoreItem::new(set.id(), set.database_id(), set.slot(), score, member);
        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        key_by_score.to_bytes(&mut reader);
        buffer
    }

    fn encode_key_by_memebr(&self, set: &SortedSet, member: &[u8]) -> BytesMut {
        let key_by_member = ZSetMemberItem::new(set.id(), set.database_id(), set.slot(), member);
        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        key_by_member.to_bytes(&mut reader);
        buffer
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
    fn test_zset_wrong_type() -> Result<(), SableError> {
        let (_deleter, db) = crate::tests::open_store();
        let zset_db = ZSetDb::with_storage(&db, 0);
        let mut strings_db = crate::storage::StringsDb::with_storage(&db, 0);
        let string_md = crate::StringValueMetadata::default();

        let key = BytesMut::from("key");
        let value = BytesMut::from("value");
        strings_db.put(&key, &value, &string_md, PutFlags::Override)?;

        // run a hash operation on a string key
        assert_eq!(zset_db.len(&key).unwrap(), ZSetLenResult::WrongType);
        assert_eq!(zset_db.find_set(&key).unwrap(), FindZSetResult::WrongType);
        Ok(())
    }

    #[test]
    fn test_bookkeeping_record() {
        let (_deleter, db) = crate::tests::open_store();
        let mut zset_db = ZSetDb::with_storage(&db, 0);
        let set_name = BytesMut::from("myset");

        // put (which creates) a new hash item in the database
        let rein = BytesMut::from("Reinhardt");

        zset_db
            .add(&set_name, &rein, 10.0, &ZWriteFlags::None, true)
            .unwrap();

        // confirm that we have a bookkeeping record
        let set = match zset_db.find_set(&set_name).unwrap() {
            FindZSetResult::Some(md) => md,
            _ => {
                panic!("Expected to find the set MD in the database");
            }
        };

        let bookkeeping_record_key = Bookkeeping::new(0, crate::utils::calculate_slot(&set_name))
            .with_uid(set.id())
            .with_value_type(ValueType::Zset)
            .to_bytes();

        let db_set_name = db.get(&bookkeeping_record_key).unwrap().unwrap();
        assert_eq!(db_set_name, set_name);

        // delete the only entry from the hash -> this should remove the hash completely from the database
        zset_db.delete_metadata(&set_name, &set.metadata).unwrap();
        zset_db.commit().unwrap();

        // confirm that the bookkeeping record was also removed from the database
        assert!(db.get(&bookkeeping_record_key).unwrap().is_none());
    }

    #[test]
    fn test_delete_member() {
        let (_deleter, db) = crate::tests::open_store();
        let mut zset_db = ZSetDb::with_storage(&db, 0);
        let set_name = BytesMut::from("myset");

        // put (which creates) a new hash item in the database
        let rein = BytesMut::from("Reinhardt");

        zset_db
            .add(&set_name, &rein, 20.0, &ZWriteFlags::None, true)
            .unwrap();

        let orisa = BytesMut::from("Orisa");
        zset_db
            .add(&set_name, &orisa, 15.0, &ZWriteFlags::None, true)
            .unwrap();

        // delete the only entry from the hash -> this should remove the hash completely from the database
        zset_db.delete_member(&set_name, &rein, false).unwrap();
        zset_db.commit().unwrap();

        // confirm that the bookkeeping record was also removed from the database
        assert_eq!(zset_db.len(&set_name).unwrap(), ZSetLenResult::Some(1));
        assert_eq!(
            zset_db.get_score(&set_name, &orisa).unwrap(),
            ZSetGetScoreResult::Score(15.0)
        );
        assert_eq!(
            zset_db.get_score(&set_name, &rein).unwrap(),
            ZSetGetScoreResult::NotFound
        );
    }

    #[test]
    fn test_zset_get_score() -> Result<(), SableError> {
        let (_deleter, db) = crate::tests::open_store();
        let mut zset_db = ZSetDb::with_storage(&db, 0);

        let overwatch_tanks = BytesMut::from("overwatch_tanks");

        // rank our tanks
        let orisa = BytesMut::from("Orisa");
        let rein = BytesMut::from("Rein");
        let dva = BytesMut::from("Dva");
        let roadhog = BytesMut::from("Roadhog");

        let overwatch_tanks_scores =
            vec![(&orisa, 1.0), (&rein, 2.0), (&dva, 3.0), (&roadhog, 4.0)];

        for (tank, score) in overwatch_tanks_scores {
            assert_eq!(
                zset_db
                    .add(&overwatch_tanks, tank, score, &ZWriteFlags::None, false)
                    .unwrap(),
                ZSetAddMemberResult::Some(1)
            );
        }
        zset_db.commit().unwrap();

        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &orisa).unwrap(),
            ZSetGetScoreResult::Score(1.0)
        );

        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(2.0)
        );

        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &dva).unwrap(),
            ZSetGetScoreResult::Score(3.0)
        );

        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &roadhog).unwrap(),
            ZSetGetScoreResult::Score(4.0)
        );
        Ok(())
    }

    #[test]
    fn test_zset_add() -> Result<(), SableError> {
        let (_deleter, db) = crate::tests::open_store();
        let mut zset_db = ZSetDb::with_storage(&db, 0);

        // rank our tanks
        let overwatch_tanks = BytesMut::from("overwatch_tanks");
        let orisa = BytesMut::from("Orisa");
        let rein = BytesMut::from("Reinhardt");
        let score = 1.0f64;

        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &orisa, score, &(ZWriteFlags::Nx), true)
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &orisa, score, &(ZWriteFlags::Nx), true)
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &rein, 42.0, &(ZWriteFlags::Xx), true,)
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &rein, 42.0, &ZWriteFlags::None, true)
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        // Try to set the same score -> should return `0`
        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &rein,
                    42.0,
                    &(ZWriteFlags::Xx | ZWriteFlags::Ch),
                    true
                )
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );
        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(42.0)
        );

        // Expected: 0 updates. 41 < old score(42)
        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &rein, 41.0, &(ZWriteFlags::Gt), true,)
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        // Score should remain 42
        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(42.0)
        );

        // Expected: 0 updates. 47 > old score(42)
        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &rein, 47.0, &(ZWriteFlags::Lt), true,)
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        // Score should remain 42
        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(42.0)
        );

        // Expected: 1 updates. 43 < old score(42)
        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &rein,
                    43.0,
                    &(ZWriteFlags::Gt | ZWriteFlags::Ch),
                    true
                )
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        // Score should now be 43
        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(43.0)
        );

        // Expected: 1 updates. 47 > old score(41)
        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &rein,
                    40.0,
                    &(ZWriteFlags::Lt | ZWriteFlags::Ch),
                    true
                )
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        // Score should now be 40
        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(40.0)
        );

        // Test the `Incr`
        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &rein,
                    40.0,
                    &(ZWriteFlags::Incr | ZWriteFlags::Ch),
                    true
                )
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        assert_eq!(
            zset_db.get_score(&overwatch_tanks, &rein).unwrap(),
            ZSetGetScoreResult::Score(80.0)
        );
        Ok(())
    }
}
