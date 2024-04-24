/// A database accessor that does not really care about the value
#[allow(unused_imports)]
use crate::{
    metadata::{HashFieldKey, HashValueMetadata},
    storage::DbWriteCache,
    CommonValueMetadata, PrimaryKeyMetadata, SableError, StorageAdapter, U8ArrayBuilder,
    U8ArrayReader,
};
#[allow(unused_imports)]
use bytes::BytesMut;

/// Sorted Set DB wrapper. This class is specialized in reading/writing sorted set items
/// (`Z*` commands)
///
/// Locking strategy: this class does not lock anything and relies on the caller
/// to obtain the locks if needed
#[allow(dead_code)]
pub struct ZSetDb<'a> {
    /// This class handles String command database access
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
}
