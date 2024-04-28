/// A database accessor that does not really care about the value
use crate::{
    metadata::{ZSetKeyByMember, ZSetKeyByScore, ZSetValueMetadata},
    storage::DbWriteCache,
    CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};
use bytes::BytesMut;

// Internal enum
#[derive(Debug, PartialEq, Eq)]
pub enum GetZSetMetadataResult {
    /// An entry exists in the db for the given key, but for a different type
    WrongType,
    /// A match was found
    Some(ZSetValueMetadata),
    /// No entry exist
    NotFound,
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
pub struct ZAddFlags: u32  {
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
    /// When set, commit the changes to the disk
    const Commit = 1 << 6;
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
        let zset = match self.get_metadata(user_key)? {
            GetZSetMetadataResult::WrongType => {
                return Ok(ZSetLenResult::WrongType);
            }
            GetZSetMetadataResult::NotFound => {
                return Ok(ZSetLenResult::Some(0));
            }
            GetZSetMetadataResult::Some(zset) => zset,
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
        flags: &ZAddFlags,
    ) -> Result<ZSetAddMemberResult, SableError> {
        // locate the hash
        let mut md = match self.get_metadata(user_key)? {
            GetZSetMetadataResult::WrongType => return Ok(ZSetAddMemberResult::WrongType),
            GetZSetMetadataResult::NotFound => {
                // Create a new set
                self.create_metadata(user_key)?
            }
            GetZSetMetadataResult::Some(hash) => hash,
        };

        let mut items_added = 0usize;
        let mut return_value = 0usize;

        let new_score = if flags.intersects(ZAddFlags::Incr) {
            match self.get_member_score(md.id(), member)? {
                Some(mut old_value) => {
                    old_value += score;
                    old_value
                }
                None => score,
            }
        } else {
            score
        };

        match self.put_member(md.id(), member, new_score, flags)? {
            PutMemberResult::Updated(_) => {
                if flags.intersects(ZAddFlags::Ch) {
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
            md.incr_len_by(items_added as u64);
            self.put_metadata(user_key, &md)?;
        }

        // flush the changes
        if flags.intersects(ZAddFlags::Commit) {
            self.flush_cache()?;
        }

        Ok(ZSetAddMemberResult::Some(return_value))
    }

    /// Returns the score of `member` in the sorted set at `user_key`
    pub fn get_score(
        &self,
        user_key: &BytesMut,
        member: &[u8],
    ) -> Result<ZSetGetScoreResult, SableError> {
        let md = match self.get_metadata(user_key)? {
            GetZSetMetadataResult::WrongType => {
                return Ok(ZSetGetScoreResult::WrongType);
            }
            GetZSetMetadataResult::NotFound => {
                return Ok(ZSetGetScoreResult::NotFound);
            }
            GetZSetMetadataResult::Some(md) => md,
        };

        if let Some(score) = self.get_member_score(md.id(), member)? {
            Ok(ZSetGetScoreResult::Score(score))
        } else {
            Ok(ZSetGetScoreResult::NotFound)
        }
    }

    /// Commit the changes to the disk
    pub fn commit(&mut self) -> Result<(), SableError> {
        self.flush_cache()
    }

    //=== ----------------------------
    // Private methods
    //=== ----------------------------

    /// Load zset value metadata from the store
    fn get_metadata(&self, user_key: &BytesMut) -> Result<GetZSetMetadataResult, SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        let Some(value) = self.cache.get(&encoded_key)? else {
            return Ok(GetZSetMetadataResult::NotFound);
        };

        match self.try_decode_zset_value_metadata(&value)? {
            None => Ok(GetZSetMetadataResult::WrongType),
            Some(zset_md) => Ok(GetZSetMetadataResult::Some(zset_md)),
        }
    }

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
    fn create_metadata(&mut self, user_key: &BytesMut) -> Result<ZSetValueMetadata, SableError> {
        let md = ZSetValueMetadata::with_id(self.store.generate_id());
        self.put_metadata(user_key, &md)?;
        Ok(md)
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
    fn delete_metadata(&mut self, user_key: &BytesMut) -> Result<(), SableError> {
        let encoded_key = PrimaryKeyMetadata::new_primary_key(user_key, self.db_id);
        self.cache.delete(&encoded_key)?;
        Ok(())
    }

    /// Put member
    fn put_member(
        &mut self,
        set_id: u64,
        member: &[u8],
        score: f64,
        flags: &ZAddFlags,
    ) -> Result<PutMemberResult, SableError> {
        let key_by_member = self.encode_key_by_memebr(set_id, member);
        let key_by_score = self.encode_key_by_score(set_id, score, member);

        let result = match self.get_member_score(set_id, member)? {
            Some(old_score) => {
                if flags.intersects(ZAddFlags::Nx)
                    || (flags.intersects(ZAddFlags::Gt) && !score.gt(&old_score))
                    || (flags.intersects(ZAddFlags::Lt) && !score.lt(&old_score))
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
                if flags.intersects(ZAddFlags::Xx) {
                    return Ok(PutMemberResult::NotModified);
                }
                PutMemberResult::Inserted(score)
            }
        };

        let mut score_as_bytes = BytesMut::new();
        let mut writer = U8ArrayBuilder::with_buffer(&mut score_as_bytes);
        writer.write_f64(score);

        self.cache.put(&key_by_member, score_as_bytes)?;
        self.cache.put(&key_by_score, BytesMut::new())?;
        Ok(result)
    }

    /// Get member score
    fn get_member_score(&self, set_id: u64, member: &[u8]) -> Result<Option<f64>, SableError> {
        let key_by_member = self.encode_key_by_memebr(set_id, member);

        let Some(value) = self.cache.get(&key_by_member)? else {
            return Ok(None);
        };

        let mut reader = U8ArrayReader::with_buffer(&value);
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;
        Ok(Some(score))
    }

    /// Check if `member` is already part of this set
    fn contains_member(&self, set_id: u64, member: &[u8]) -> Result<bool, SableError> {
        let key_by_member = self.encode_key_by_memebr(set_id, member);
        self.cache.contains(&key_by_member)
    }

    /// Delete member from the set
    fn delete_member(&mut self, set_id: u64, member: &[u8]) -> Result<bool, SableError> {
        let key_by_member = self.encode_key_by_memebr(set_id, member);
        let Some(value) = self.cache.get(&key_by_member)? else {
            return Ok(false);
        };
        let mut reader = U8ArrayReader::with_buffer(&value);
        let score = reader.read_f64().ok_or(SableError::SerialisationError)?;

        let key_by_score = self.encode_key_by_score(set_id, score, member);
        self.cache.delete(&key_by_member)?;
        self.cache.delete(&key_by_score)?;
        Ok(true)
    }

    fn encode_key_by_score(&self, set_id: u64, score: f64, member: &[u8]) -> BytesMut {
        let key_by_score = ZSetKeyByScore::new(set_id, score, member);
        let mut buffer = BytesMut::with_capacity(256);
        let mut reader = U8ArrayBuilder::with_buffer(&mut buffer);
        key_by_score.to_bytes(&mut reader);
        buffer
    }

    fn encode_key_by_memebr(&self, set_id: u64, member: &[u8]) -> BytesMut {
        let key_by_member = ZSetKeyByMember::new(set_id, member);
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
        assert_eq!(
            zset_db.get_metadata(&key).unwrap(),
            GetZSetMetadataResult::WrongType
        );
        Ok(())
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
                    .add(&overwatch_tanks, tank, score, &ZAddFlags::None)
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
                .add(
                    &overwatch_tanks,
                    &orisa,
                    score,
                    &(ZAddFlags::Commit | ZAddFlags::Nx)
                )
                .unwrap(),
            ZSetAddMemberResult::Some(1)
        );

        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &orisa,
                    score,
                    &(ZAddFlags::Commit | ZAddFlags::Nx)
                )
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        assert_eq!(
            zset_db
                .add(
                    &overwatch_tanks,
                    &rein,
                    42.0,
                    &(ZAddFlags::Commit | ZAddFlags::Xx)
                )
                .unwrap(),
            ZSetAddMemberResult::Some(0)
        );

        assert_eq!(
            zset_db
                .add(&overwatch_tanks, &rein, 42.0, &ZAddFlags::Commit)
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
                    &(ZAddFlags::Commit | ZAddFlags::Xx | ZAddFlags::Ch)
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
                .add(
                    &overwatch_tanks,
                    &rein,
                    41.0,
                    &(ZAddFlags::Commit | ZAddFlags::Gt)
                )
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
                .add(
                    &overwatch_tanks,
                    &rein,
                    47.0,
                    &(ZAddFlags::Commit | ZAddFlags::Lt)
                )
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
                    &(ZAddFlags::Commit | ZAddFlags::Gt | ZAddFlags::Ch)
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
                    &(ZAddFlags::Commit | ZAddFlags::Lt | ZAddFlags::Ch)
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
                    &(ZAddFlags::Commit | ZAddFlags::Incr | ZAddFlags::Ch)
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
