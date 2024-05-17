mod generic_db;
mod hash_db;
mod scan_cursor;
mod storage_adapter;
mod storage_rocksdb;
mod storage_trait;
mod string_db;
mod write_cache;
mod zset_db;

pub use crate::replication::{StorageUpdates, StorageUpdatesIterItem};
pub use crate::storage::storage_adapter::{
    BatchUpdate, PutFlags, StorageAdapter, StorageOpenParams,
};
pub use generic_db::GenericDb;
pub use hash_db::{
    GetHashMetadataResult, HashDb, HashDeleteResult, HashExistsResult, HashGetMultiResult,
    HashGetResult, HashLenResult, HashPutResult,
};
pub use scan_cursor::ScanCursor;
pub use storage_rocksdb::StorageRocksDb;
pub use storage_trait::{StorageIterator, StorageTrait};
pub use string_db::*;
pub use write_cache::{DbCacheEntry, DbWriteCache};
pub use zset_db::*;

#[macro_export]
macro_rules! storage_rocksdb {
    ($open_params:expr) => {{
        let mut db = $crate::storage::StorageAdapter::default();
        db.open($open_params).expect("rockdb open");
        db
    }};
}
