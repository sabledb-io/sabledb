mod storage_adapter;
mod storage_cache;
mod storage_rocksdb;
mod string_db;
mod generic_db;

pub use crate::replication::{StorageUpdates, StorageUpdatesIterItem};
pub use crate::storage::storage_adapter::{
    BatchUpdate, PutFlags, StorageAdapter, StorageOpenParams,
};
pub use storage_cache::{Storable, StorageCache};
pub use storage_rocksdb::StorageRocksDb;
pub use string_db::StringsDb;

#[macro_export]
macro_rules! storage_rocksdb {
    ($open_params:expr) => {{
        let mut db = $crate::storage::StorageAdapter::default();
        db.open($open_params).expect("rockdb open");
        db
    }};
}
