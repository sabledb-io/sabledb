mod generic_db;
mod hash_db;
mod storage_adapter;
mod storage_cache;
mod storage_rocksdb;
mod storage_trait;
mod string_db;

pub use crate::replication::{StorageUpdates, StorageUpdatesIterItem};
pub use crate::storage::storage_adapter::{
    BatchUpdate, PutFlags, StorageAdapter, StorageOpenParams,
};
pub use generic_db::GenericDb;
pub use hash_db::{HashDb, HashDeleteResult, HashGetResult, HashPutResult};
pub use storage_cache::{Storable, StorageCache};
pub use storage_rocksdb::StorageRocksDb;
pub use storage_trait::{IterateCallback, StorageTrait};
pub use string_db::StringsDb;

#[macro_export]
macro_rules! storage_rocksdb {
    ($open_params:expr) => {{
        let mut db = $crate::storage::StorageAdapter::default();
        db.open($open_params).expect("rockdb open");
        db
    }};
}
