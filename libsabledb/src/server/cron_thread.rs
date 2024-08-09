use crate::{
    metadata::{Bookkeeping, KeyType, ValueType},
    server::telemetry::Telemetry,
    storage::{DbWriteCache, GenericDb, StorageMetadata},
    utils::ticker::{TickInterval, Ticker},
    LockManager, SableError, ServerOptions, StorageAdapter, U8ArrayBuilder, U8ArrayReader,
    WorkerHandle,
};
use bytes::BytesMut;

pub type CronReceiver = tokio::sync::mpsc::Receiver<CronMessage>;
pub type CronSender = tokio::sync::mpsc::Sender<CronMessage>;

#[derive(Default, Debug)]
#[allow(dead_code)]
pub enum CronMessage {
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
pub struct Cron {
    /// Shared server state
    server_options: ServerOptions,
    /// The store
    store: StorageAdapter,
    /// The channel on which this worker accepts commands
    rx_channel: CronReceiver,
}

#[derive(Clone, Debug)]
/// The `EvictorContext` allows other threads to communicate with the replicator
/// thread using a dedicated channel
pub struct CronContext {
    runtime_handle: WorkerHandle,
    worker_send_channel: CronSender,
}

#[allow(unsafe_code)]
unsafe impl Send for CronContext {}

#[allow(dead_code)]
impl CronContext {
    /// Send message to the worker
    pub async fn send(&self, message: CronMessage) -> Result<(), SableError> {
        // before using the message, enter the worker's context
        let _guard = self.runtime_handle.enter();
        let _ = self.worker_send_channel.send(message).await;
        Ok(())
    }

    /// Send message to the worker (non async)
    pub fn send_sync(&self, message: CronMessage) -> Result<(), SableError> {
        if let Err(e) = self.worker_send_channel.try_send(message) {
            return Err(SableError::OtherError(format!("{:?}", e)));
        }
        Ok(())
    }
}

impl Cron {
    /// Private method: create new eviction thread
    pub async fn new(
        rx_channel: CronReceiver,
        server_options: ServerOptions,
        store: StorageAdapter,
    ) -> Self {
        Cron {
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
    ) -> Result<CronContext, SableError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<CronMessage>(100);
        let (handle_sender, handle_receiver) = std::sync::mpsc::channel();
        let _ = std::thread::Builder::new()
            .name("Cron".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("Cron")
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
                    let mut cron = Cron::new(rx, server_options.clone(), store.clone()).await;
                    if let Err(e) = cron.main_loop().await {
                        tracing::error!("Cron error. {:?}", e);
                    }
                });
            });

        let thread_runtime_handle = handle_receiver.recv().unwrap_or_else(|e| {
            panic!(
                "failed to recv tokio runtime handle from replicator thread. {:?}",
                e
            );
        });

        Ok(CronContext {
            runtime_handle: thread_runtime_handle.clone(),
            worker_send_channel: tx,
        })
    }

    /// The evictor thread main loop
    async fn main_loop(&mut self) -> Result<(), SableError> {
        tracing::info!("Started");

        let mut evict_ticker = Ticker::new(TickInterval::Seconds(
            self.server_options.cron.evict_orphan_records_secs as u64,
        ));

        let mut scan_ticker = Ticker::new(TickInterval::Seconds(
            self.server_options.cron.scan_keys_secs as u64,
        ));

        loop {
            tokio::select! {
                msg = self.rx_channel.recv() => {
                    // Check the message type
                    match msg {
                        Some(CronMessage::Evict) => {
                            // Do evict now
                            tracing::info!("Evicting records from the database");
                            Self::evict(&self.store, &self.server_options).await?;
                        }
                        Some(CronMessage::Shutdown) => {
                            tracing::info!("Exiting");
                            break;
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    if evict_ticker.try_tick()? {
                        Self::evict(&self.store, &self.server_options).await?;
                    }

                    if scan_ticker.try_tick()? {
                        Self::scan(&self.store, &self.server_options).await?;
                    }
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
    /// So in the above example, even if a user overrode the value by calling `set` command
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
            (ValueType::Set, vec![KeyType::SetItem]),
        ];

        let mut items_evicted = 0usize;
        let mut write_cache = DbWriteCache::with_storage(store);
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
                                let count = Self::purge_subitems(
                                    store,
                                    &mut write_cache,
                                    &record,
                                    key_type,
                                )?;
                                tracing::info!(
                                    "Deleted {} zombie items of type {:?} belonged to: {:?}",
                                    count,
                                    key_type,
                                    user_key
                                );
                                items_evicted = items_evicted.saturating_add(count);
                            }
                            write_cache.delete(&record.to_bytes())?;

                            // Avoid building too many records in memory
                            // TODO: move this to the configuration
                            if write_cache.len() > 1000 {
                                write_cache.flush()?;
                            }
                        }
                        RecordExistsResult::Found => {}
                    }
                }
                db_iter.next();
            }
        }

        // Apply any remaining deletions
        if !write_cache.is_empty() {
            write_cache.flush()?;
        }
        Ok(items_evicted)
    }

    /// Purge sub items from the database belonged to a zombie
    /// key (hash that was overwritten, but its sub items are still there)
    fn purge_subitems(
        store: &StorageAdapter,
        write_cache: &mut DbWriteCache,
        record: &Bookkeeping,
        key_type: &KeyType,
    ) -> Result<usize, SableError> {
        let mut prefix = BytesMut::new();

        // Sub-items are always encoded with: the type (u8) followed by the instance UID (u64)
        let mut builder = crate::U8ArrayBuilder::with_buffer(&mut prefix);
        builder.write_key_type(*key_type);
        builder.write_u64(record.uid());

        let mut db_iter = store.create_iterator(&prefix)?;
        let mut count = 0usize;
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            write_cache.delete(&BytesMut::from(key))?;
            count = count.saturating_add(1);
            db_iter.next();
        }
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
        // Load from the database CommonValueMetadata and verify that it has
        // the same type (e.g. List, Hash etc) AND has the same UID (i.e. it is the same instance)
        let md = generic_db.value_common_metadata(user_key)?;
        match md {
            Some(ref md) => Ok(
                if md.value_type().eq(expected_value_type) && md.uid().eq(&record.uid()) {
                    RecordExistsResult::Found
                } else {
                    tracing::info!(
                        "'{:?}' found but with wrong type. Expected {:?}, Found: {:?}",
                        user_key,
                        expected_value_type,
                        md.value_type()
                    );
                    RecordExistsResult::WrongType
                },
            ),
            None => Ok(RecordExistsResult::NotFound),
        }
    }

    /// Scan the database count keys / databases
    async fn scan(
        store: &StorageAdapter,
        _server_options: &ServerOptions,
    ) -> Result<(), SableError> {
        // Scan of all keys, regardless of their database association
        let mut prefix = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut prefix);
        builder.write_key_type(KeyType::PrimaryKey);
        let mut db_iter = store.create_iterator(&prefix)?;
        let mut storage_metadata = StorageMetadata::default();
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            // Extract the database ID from the key
            let mut reader = U8ArrayReader::with_buffer(key);

            // Skip the primary key encoding
            reader.advance(std::mem::size_of::<KeyType>())?;

            // Read the database ID
            let db_id = reader.read_u16().ok_or(SableError::SerialisationError)?;

            storage_metadata.incr_keys(db_id);
            db_iter.next();
        }

        tracing::debug!("Scan output: {:?}", storage_metadata);
        Telemetry::set_database_info(storage_metadata);
        Ok(())
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
        PutFlags, SetDb, SetExistsResult, SetLenResult, StringsDb, ZSetAddMemberResult, ZSetDb,
        ZSetLenResult, ZWriteFlags,
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

            // override the set creating zombie entries

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
            let items_evicted = Cron::evict(&db, &server_options).await.unwrap();
            assert_eq!(items_evicted, 8); // we expected 4 items for the "score" + 4 items for the "member"
        });
    }

    #[test]
    fn test_composite_item_overriden_with_the_same_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_deleter, db) = crate::tests::open_store();
            let mut set_db = SetDb::with_storage(&db, 0);

            let set1 = BytesMut::from("set1");

            // set1 items
            let one1 = BytesMut::from("one1");
            let two1 = BytesMut::from("two1");
            let three1 = BytesMut::from("three1");

            // new set1 items
            let one2 = BytesMut::from("one2");
            let two2 = BytesMut::from("two2");
            let three2 = BytesMut::from("three2");

            set_db
                .put_multi(&set1, &vec![&one1, &two1, &three1])
                .unwrap();
            set_db.commit().unwrap();
            assert_eq!(set_db.len(&set1).unwrap(), SetLenResult::Some(3));

            let server_options = ServerOptions::default();
            let items_evicted = Cron::evict(&db, &server_options).await.unwrap();
            assert_eq!(items_evicted, 0); // 0 items should be evicted

            // Create another set, using the same *name* but with a different UID
            set_db
                .put_multi_overwrite(&set1, &vec![&one2, &two2, &three2])
                .unwrap();
            set_db.commit().unwrap();
            assert_eq!(set_db.len(&set1).unwrap(), SetLenResult::Some(3));

            // Try evicting again, this time we expect 3 items to be evicted
            let items_evicted = Cron::evict(&db, &server_options).await.unwrap();
            assert_eq!(items_evicted, 3); // 3 items should be evicted

            // The length should be still 3
            assert_eq!(set_db.len(&set1).unwrap(), SetLenResult::Some(3));

            // Make sure that the correct items were purged (one1, two1 and three1) and the new items (one2, two2 and three2)
            // exist in the database
            let items = vec![(&one1, &one2), (&two1, &two2), (&three1, &three2)];
            for (item_does_not_exist, item_exists) in items {
                assert_eq!(
                    set_db.member_exists(&set1, item_does_not_exist).unwrap(),
                    SetExistsResult::NotExists
                );
                assert_eq!(
                    set_db.member_exists(&set1, item_exists).unwrap(),
                    SetExistsResult::Exists
                );
            }
        });
    }
}
