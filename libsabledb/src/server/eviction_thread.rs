use crate::{
    metadata::{Bookkeeping, KeyType, ValueType},
    storage::{DbWriteCache, GenericDb},
    LockManager, SableError, ServerOptions, StorageAdapter, WorkerHandle,
};
use bytes::BytesMut;
use std::collections::HashSet;

pub type EvictorReceiver = tokio::sync::mpsc::Receiver<EvictorMessage>;
pub type EvictorSender = tokio::sync::mpsc::Sender<EvictorMessage>;

#[derive(Default, Debug)]
#[allow(dead_code)]
pub enum EvictorMessage {
    /// Shutdown the replicator thread
    Shutdown,
    /// Perform eviction now
    #[default]
    Evict,
}

enum RecordExistsResult {
    /// The record exists, but with the wrong type
    WrongType,
    /// Not found
    NotFound,
    /// Found and has the correct type
    Found,
}

#[allow(dead_code)]
pub struct Evictor {
    /// Shared server state
    server_options: ServerOptions,
    /// The store
    store: StorageAdapter,
    /// The channel on which this worker accepts commands
    rx_channel: EvictorReceiver,
}

#[derive(Clone, Debug)]
/// The `EvictorContext` allows other threads to communicate with the replicator
/// thread using a dedicated channel
pub struct EvictorContext {
    runtime_handle: WorkerHandle,
    worker_send_channel: EvictorSender,
}

#[allow(unsafe_code)]
unsafe impl Send for EvictorContext {}

#[allow(dead_code)]
impl EvictorContext {
    /// Send message to the worker
    pub async fn send(&self, message: EvictorMessage) -> Result<(), SableError> {
        // before using the message, enter the worker's context
        let _guard = self.runtime_handle.enter();
        let _ = self.worker_send_channel.send(message).await;
        Ok(())
    }

    /// Send message to the worker (non async)
    pub fn send_sync(&self, message: EvictorMessage) -> Result<(), SableError> {
        if let Err(e) = self.worker_send_channel.try_send(message) {
            return Err(SableError::OtherError(format!("{:?}", e)));
        }
        Ok(())
    }
}

impl Evictor {
    /// Private method: create new eviction thread
    pub async fn new(
        rx_channel: EvictorReceiver,
        server_options: ServerOptions,
        store: StorageAdapter,
    ) -> Self {
        Evictor {
            server_options,
            store,
            rx_channel,
        }
    }

    /// Spawn the replication thread returning a a context for the caller
    /// The context can be used to communicate with the replicator
    pub fn run(
        server_options: ServerOptions,
        store: StorageAdapter,
    ) -> Result<EvictorContext, SableError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<EvictorMessage>(100);
        let (handle_sender, handle_receiver) = std::sync::mpsc::channel();
        let _ = std::thread::Builder::new()
            .name("Evictor".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("Evictor")
                    .build()
                    .unwrap_or_else(|e| {
                        panic!("failed to create tokio runtime. {:?}", e);
                    });

                // send the current runtime handle to the calling thread
                // this error is non-recoverable, so call `panic!` here
                handle_sender.send(rt.handle().clone()).unwrap_or_else(|e| {
                    panic!(
                        "failed to send tokio runtime handle to caller thread!. {:?}",
                        e
                    );
                });

                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    let mut evictor = Evictor::new(rx, server_options.clone(), store.clone()).await;
                    if let Err(e) = evictor.main_loop().await {
                        tracing::error!("Evictor error. {:?}", e);
                    }
                });
            });

        let thread_runtime_handle = handle_receiver.recv().unwrap_or_else(|e| {
            panic!(
                "failed to recv tokio runtime handle from replicator thread. {:?}",
                e
            );
        });

        Ok(EvictorContext {
            runtime_handle: thread_runtime_handle.clone(),
            worker_send_channel: tx,
        })
    }

    /// The evictor thread main loop
    async fn main_loop(&mut self) -> Result<(), SableError> {
        tracing::info!("Started");

        let sleep_internal = self.server_options.maintenance.purge_zombie_records_secs as u64;
        loop {
            tokio::select! {
                msg = self.rx_channel.recv() => {
                    // Check the message type
                    match msg {
                        Some(EvictorMessage::Evict) => {
                            // Do evict now
                            tracing::info!("Evicting records from the database");
                            Self::evict(&self.store, &self.server_options).await?;
                        }
                        Some(EvictorMessage::Shutdown) => {
                            tracing::info!("Exiting");
                            break;
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(sleep_internal)) => {
                    // Evict
                    Self::evict(&self.store, &self.server_options).await?;
                }
            }
        }
        Ok(())
    }

    /// Eviction strategy:
    /// It is possible to have orphan entries in the database due to several reasons.
    /// For example: user creates a HASH using the following code:
    ///
    /// ```no_compile
    /// HSET myhash a b c d
    /// ```
    /// After the command is executed, we get 3 records in the database:
    ///
    /// ```no_compile
    /// [1 | myhash] -> [2| Expiration | HASH_ID | size ]
    /// [3 | HASH_ID | a] -> [b]
    /// [3 | HASH_ID | c] -> [d]
    /// ```
    ///
    /// The only way to get to get to records `a` and `b` is by visiting the metadata first
    /// and obtaining the `HASH_ID`. Now, lets assume that the user runs this command:
    ///
    /// ```no_compile
    /// set myhash some_value
    /// ```
    ///
    /// We now have new entry the database:
    ///
    /// ```no_compile
    /// [0 | myhash] -> [0| some_value ]
    /// ```
    ///
    /// By doing this, we overrode the previous value which kept the `HASH_ID` so there is no way
    /// access `a` and `b`
    ///
    /// SableDb keeps a special records called `Bookkeeping` which is created whenever a new complex
    /// item is created. The `Bookkeeping` stores the following data:
    ///
    /// ```no_compile
    /// [0 | UID | Type ] -> [ user key ]
    /// ```
    /// So in the above example, even if a user overode the value by calling `set` command
    /// we can still access the orphan values and remove them from the database
    async fn evict(
        store: &StorageAdapter,
        _server_options: &ServerOptions,
    ) -> Result<usize, SableError> {
        let prefix_arr = vec![
            (ValueType::Hash, vec![KeyType::HashItem]),
            (ValueType::List, vec![KeyType::ListItem]),
            (
                ValueType::Zset,
                vec![KeyType::ZsetMemberItem, KeyType::ZsetScoreItem],
            ),
        ];

        let mut items_evicted = 0usize;
        let mut records_to_delete = HashSet::<BytesMut>::new();
        for (primary_type, sub_items) in &prefix_arr {
            let prefix = Bookkeeping::prefix(primary_type);
            let mut db_iter = store.create_iterator(&prefix)?;
            while db_iter.valid() {
                let Some((key, user_key)) = db_iter.key_value() else {
                    break;
                };

                if !key.starts_with(&prefix) {
                    break;
                }

                let record = Bookkeeping::from_bytes(key)?;

                {
                    let user_key = BytesMut::from(user_key);
                    let _unused = LockManager::lock_user_key_shared_unconditionally(
                        &user_key,
                        record.db_id(),
                    )
                    .await?;

                    match Self::record_exists(store, &record, &user_key, primary_type)? {
                        RecordExistsResult::WrongType | RecordExistsResult::NotFound => {
                            for key_type in sub_items {
                                // purge sub-items
                                let count = Self::purge_subitems(store, &record, key_type)?;
                                tracing::debug!(
                                    "Deleted {} zombie items of type {:?} belonged to: {:?}",
                                    count,
                                    key_type,
                                    user_key
                                );
                                items_evicted = items_evicted.saturating_add(count);
                            }
                            records_to_delete.insert(record.to_bytes());
                        }
                        RecordExistsResult::Found => {}
                    }
                }
                db_iter.next();
            }
        }

        // Delete the defunct bookkeeping records
        if !records_to_delete.is_empty() {
            let mut write_cache = DbWriteCache::with_storage(store);
            for rec in &records_to_delete {
                write_cache.delete(rec)?;
            }
            write_cache.flush()?;
        }
        Ok(items_evicted)
    }

    /// Purge sub items from the database belonged to a zombie
    /// key (hash that was overwritten, but its sub items are still there)
    fn purge_subitems(
        store: &StorageAdapter,
        record: &Bookkeeping,
        key_type: &KeyType,
    ) -> Result<usize, SableError> {
        let mut write_cache = DbWriteCache::with_storage(store);
        let mut prefix = BytesMut::new();
        let mut builder = crate::U8ArrayBuilder::with_buffer(&mut prefix);
        builder.write_u8(*key_type as u8);
        builder.write_u64(record.uid());

        let mut db_iter = store.create_iterator(&prefix)?;
        while db_iter.valid() {
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            write_cache.delete(&BytesMut::from(key))?;
            db_iter.next();
        }

        let count = write_cache.len();
        write_cache.flush()?;
        Ok(count)
    }

    /// Check if the record exists in the database and has the correct type
    fn record_exists(
        store: &StorageAdapter,
        record: &Bookkeeping,
        user_key: &BytesMut,
        expected_value_type: &ValueType,
    ) -> Result<RecordExistsResult, SableError> {
        let mut generic_db = GenericDb::with_storage(store, record.db_id());
        let md = generic_db.value_common_metadata(user_key)?;
        match md {
            Some(md) => Ok(if md.value_type().eq(expected_value_type) {
                RecordExistsResult::Found
            } else {
                tracing::debug!(
                    "'{:?}' found but with wrong type. Expected {:?}, Found: {:?}",
                    user_key,
                    expected_value_type,
                    md.value_type()
                );
                RecordExistsResult::WrongType
            }),
            None => Ok(RecordExistsResult::NotFound),
        }
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
    use crate::storage::{
        PutFlags, StringsDb, ZSetAddMemberResult, ZSetDb, ZSetLenResult, ZWriteFlags,
    };

    #[test]
    fn test_eviction_of_zset_records() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_deleter, db) = crate::tests::open_store();
            let mut zset_db = ZSetDb::with_storage(&db, 0);

            let overwatch_tanks = BytesMut::from("overwatch_tanks");
            let overwatch_tanks_2 = BytesMut::from("overwatch_tanks2");

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
                assert_eq!(
                    zset_db
                        .add(&overwatch_tanks_2, tank, score, &ZWriteFlags::None, false)
                        .unwrap(),
                    ZSetAddMemberResult::Some(1)
                );
            }
            zset_db.commit().unwrap();
            assert_eq!(
                zset_db.len(&overwatch_tanks).unwrap(),
                ZSetLenResult::Some(4)
            );

            assert_eq!(
                zset_db.len(&overwatch_tanks_2).unwrap(),
                ZSetLenResult::Some(4)
            );

            // overide the set creating zombie entries

            let mut strings_db = StringsDb::with_storage(&db, 0);
            let string_md = crate::StringValueMetadata::default();

            let key = BytesMut::from("overwatch_tanks");
            let value = BytesMut::from("a string value");
            strings_db
                .put(&key, &value, &string_md, PutFlags::Override)
                .unwrap();

            // value now has string type
            assert_eq!(
                zset_db.len(&overwatch_tanks).unwrap(),
                ZSetLenResult::WrongType
            );

            // but this ZSet is still valid
            assert_eq!(
                zset_db.len(&overwatch_tanks_2).unwrap(),
                ZSetLenResult::Some(4)
            );

            let server_options = ServerOptions::default();
            let items_evicted = Evictor::evict(&db, &server_options).await.unwrap();
            assert_eq!(items_evicted, 8); // we expected 4 items for the "score" + 4 items for the "member"
        });
    }
}
