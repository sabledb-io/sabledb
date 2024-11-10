mod generic_db;
mod hash_db;
mod limits;
mod list_db;
mod scan_cursor;
mod set_db;
mod storage_adapter;
mod storage_rocksdb;
mod storage_trait;
mod string_db;
mod write_cache;
mod zset_db;

pub use crate::replication::{StorageUpdates, StorageUpdatesIterItem};
pub use crate::storage::storage_adapter::*;
pub use generic_db::GenericDb;
pub use hash_db::{
    FindHashResult, HashDb, HashDeleteResult, HashExistsResult, HashGetMultiResult, HashGetResult,
    HashLenResult, HashPutResult,
};
pub use limits::*;
pub use list_db::*;
pub use scan_cursor::ScanCursor;
pub use set_db::*;
pub use storage_rocksdb::StorageRocksDb;
pub use storage_trait::{IteratorAdapter, StorageIterator, StorageMetadata, StorageTrait};
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

pub const SEQUENCES_FILE: &str = "changes.seq";
