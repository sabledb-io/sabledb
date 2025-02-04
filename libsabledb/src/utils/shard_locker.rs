use crate::{utils::calculate_slot, ClientState, PrimaryKeyMetadata, SableError, ValkeyCommand};
use bytes::BytesMut;
use std::rc::Rc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

lazy_static::lazy_static! {
    static ref MULTI_LOCK: ShardLocker = ShardLocker::default();
}

macro_rules! check_state {
    ($client_state:expr, $slots:expr) => {
        if !$client_state
            .server_inner_state()
            .slots()
            .is_set_multi(&$slots)?
        {
            return Err(SableError::NotOwner($slots));
        } else if $client_state.is_txn_state_calc_slots() {
            return Err(SableError::LockCancelledTxnPrep($slots));
        } else if $client_state.is_txn_state_exec() {
            // The client is running an active transaction, lock was already obtained
            return Self::noop_lock();
        }
    };
}

macro_rules! double_check_slot_ownership_inner {
    ($client_state:expr, $slots:expr) => {
        if !$client_state
            .server_inner_state()
            .slots()
            .is_set_multi(&$slots)?
        {
            return Err(SableError::NotOwner($slots));
        }
    };
}

/// Try to lock slots using `$statement`. In case of a success, double check that the locked
/// slot is still owned by this process. This is used to make sure that after a successful call
/// for the command: `slot sendto ...` a slot might not be owned by this instance any longer
macro_rules! double_check_slot_ownership {
    ($statement:expr, $client_state:expr, $slots:expr) => {
        match $statement {
            Ok(lk) => {
                double_check_slot_ownership_inner!($client_state, $slots);
                Ok(lk)
            }
            Err(e) => Err(e),
        }
    };
}

#[allow(dead_code)]
pub struct ShardLockGuard<'a> {
    read_locks: Option<Vec<RwLockReadGuard<'a, u16>>>,
    read_count: usize,
    write_locks: Option<Vec<RwLockWriteGuard<'a, u16>>>,
    write_count: usize,
}

impl ShardLockGuard<'_> {
    pub fn len(&self) -> usize {
        self.read_count.saturating_add(self.write_count)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_locks_len(&self) -> usize {
        self.read_count
    }

    pub fn write_locks_len(&self) -> usize {
        self.write_count
    }
}

struct ShardLocker {
    locks: Vec<RwLock<u16>>,
}

impl ShardLocker {
    const SLOT_SIZE: u16 = 16384;

    /// Create lock per slot
    pub fn default() -> Self {
        let mut locks = Vec::<RwLock<u16>>::with_capacity(Self::SLOT_SIZE.into());
        for _ in 0..Self::SLOT_SIZE {
            let lk = RwLock::new(0u16);
            locks.push(lk);
        }
        ShardLocker { locks }
    }
}

pub struct LockManager {}

impl LockManager {
    /// Obtain lock based on the command. If the command is marked as "read-only"
    /// we obtain a read lock, otherwise, an exclusive lock
    pub async fn lock<'a>(
        user_key: &BytesMut,
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let db_id = client_state.database_id();
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
        if command.metadata().is_write_command() {
            Self::lock_internal_key_exclusive(&internal_key, client_state).await
        } else {
            Self::lock_internal_key_shared(&internal_key, client_state).await
        }
    }

    /// Obtain a multi lock based on the command. If the command is marked as "read-only"
    /// we obtain a read lock, otherwise, an exclusive lock
    pub async fn lock_multi<'a>(
        user_keys: &[&BytesMut],
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let db_id = client_state.database_id();
        let keys: Vec<Rc<BytesMut>> = user_keys
            .iter()
            .map(|user_key| Rc::new(PrimaryKeyMetadata::new_primary_key(user_key, db_id)))
            .collect();
        if command.metadata().is_write_command() {
            Self::lock_multi_internal_keys_exclusive(&keys, client_state).await
        } else {
            Self::lock_multi_internal_keys_shared(&keys, client_state).await
        }
    }

    /// Obtain exclusive lock on a user key, without conditions.
    /// other methods in this class will check for various variables
    /// like whether or not we have an open transaction and in which state.
    /// This function skip these checks
    pub async fn lock_user_key_shared_unconditionally<'a>(
        user_key: &BytesMut,
        db_id: u16,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
        Self::lock_internal_key_shared_unconditionally(&internal_key).await
    }

    /// Lock the entire storage
    pub async fn lock_all_keys_shared<'a>() -> Result<ShardLockGuard<'a>, SableError> {
        let mut read_locks =
            Vec::<RwLockReadGuard<'a, u16>>::with_capacity(crate::utils::SLOT_SIZE.into());

        let mut slots = Vec::<u16>::with_capacity(crate::utils::SLOT_SIZE.into());
        for slot in 0..crate::utils::SLOT_SIZE {
            slots.push(slot);
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in &slots {
            let Some(lk) = MULTI_LOCK.locks.get(*idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            let lk = lk.read().await;
            read_locks.push(lk);
        }

        Ok(ShardLockGuard {
            read_locks: Some(read_locks),
            read_count: slots.len(),
            write_locks: None,
            write_count: 0,
        })
    }

    /// Lock `slots`, exclusively
    pub async fn lock_multi_slots_exclusive<'a>(
        slots: Vec<u16>,
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        check_state!(client_state, slots);
        double_check_slot_ownership!(
            Self::lock_multi_slots_exclusive_unconditionally(slots.clone()).await,
            client_state,
            slots
        )
    }

    /// Lock `slots`, exclusively
    pub async fn lock_multi_slots_exclusive_unconditionally<'a>(
        mut slots: Vec<u16>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut write_locks = Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(slots.len());

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in &slots {
            let Some(lk) = MULTI_LOCK.locks.get(*idx as usize) else {
                return Err(SableError::InternalError(format!(
                    "No lock in index {}",
                    idx
                )));
            };

            let lk = lk.write().await;
            write_locks.push(lk);
        }

        Ok(ShardLockGuard {
            read_locks: None,
            read_count: 0,
            write_locks: Some(write_locks),
            write_count: slots.len(),
        })
    }

    /// Lock `slots`, shared
    pub async fn lock_multi_slots_shared<'a>(
        slots: Vec<u16>,
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        check_state!(client_state, slots);
        double_check_slot_ownership!(
            Self::lock_multi_slots_exclusive_unconditionally(slots.clone()).await,
            client_state,
            slots
        )
    }

    /// Lock `slots`, shared
    pub async fn lock_multi_slots_shared_unconditionally<'a>(
        mut slots: Vec<u16>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(slots.len());

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in &slots {
            let Some(lk) = MULTI_LOCK.locks.get(*idx as usize) else {
                return Err(SableError::InternalError(format!(
                    "No lock in index {}",
                    idx
                )));
            };

            let lk = lk.read().await;
            read_locks.push(lk);
        }

        Ok(ShardLockGuard {
            read_locks: Some(read_locks),
            read_count: slots.len(),
            write_locks: None,
            write_count: 0,
        })
    }

    // ===-------------------------------------------
    // Internal API
    // ===-------------------------------------------

    async fn lock_multi_internal_keys_exclusive<'a>(
        keys: &[Rc<BytesMut>],
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut write_locks = Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(keys.len());
        // Calculate the slots and sort them
        let mut slots = Vec::<u16>::with_capacity(keys.len());
        for key in keys.iter() {
            slots.push(calculate_slot(key));
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        check_state!(client_state, slots);

        for idx in &slots {
            let Some(lk) = MULTI_LOCK.locks.get(*idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            let lk = lk.write().await;
            write_locks.push(lk);
        }

        Ok(ShardLockGuard {
            read_locks: None,
            read_count: 0,
            write_locks: Some(write_locks),
            write_count: slots.len(),
        })
    }

    async fn lock_multi_internal_keys_shared<'a>(
        keys: &[Rc<BytesMut>],
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(keys.len());
        // Calculate the slots and sort them
        let mut slots = Vec::<u16>::with_capacity(keys.len());
        for key in keys.iter() {
            slots.push(calculate_slot(key));
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        check_state!(client_state, slots);

        for idx in &slots {
            let Some(lk) = MULTI_LOCK.locks.get(*idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            let lk = lk.read().await;
            read_locks.push(lk);
        }

        Ok(ShardLockGuard {
            read_locks: Some(read_locks),
            read_count: slots.len(),
            write_locks: None,
            write_count: 0,
        })
    }

    async fn lock_internal_key_shared<'a>(
        key: &BytesMut,
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(1);
        // Calculate the slots and sort them
        let slot = calculate_slot(key);

        let slots = vec![slot];
        check_state!(client_state, slots);
        read_locks.push(
            MULTI_LOCK
                .locks
                .get(slot as usize)
                .expect("lock")
                .read()
                .await,
        );

        Ok(ShardLockGuard {
            read_locks: Some(read_locks),
            read_count: 1,
            write_locks: None,
            write_count: 0,
        })
    }

    fn noop_lock<'a>() -> Result<ShardLockGuard<'a>, SableError> {
        Ok(ShardLockGuard {
            write_locks: None,
            write_count: 0,
            read_locks: None,
            read_count: 0,
        })
    }

    async fn lock_internal_key_exclusive<'a>(
        key: &BytesMut,
        client_state: Rc<ClientState>,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut write_locks = Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(1);
        // Calculate the slots and sort them
        let slot = calculate_slot(key);

        let slots = vec![slot];
        check_state!(client_state, slots);

        write_locks.push(
            MULTI_LOCK
                .locks
                .get(slot as usize)
                .expect("lock")
                .write()
                .await,
        );

        Ok(ShardLockGuard {
            write_locks: Some(write_locks),
            write_count: 1,
            read_locks: None,
            read_count: 0,
        })
    }

    async fn lock_internal_key_shared_unconditionally<'a>(
        user_key: &BytesMut,
    ) -> Result<ShardLockGuard<'a>, SableError> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(1);
        let slot = calculate_slot(user_key);
        read_locks.push(
            MULTI_LOCK
                .locks
                .get(slot as usize)
                .expect("lock")
                .read()
                .await,
        );

        Ok(ShardLockGuard {
            write_locks: None,
            write_count: 0,
            read_locks: Some(read_locks),
            read_count: 1,
        })
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
    use crate::{Client, ServerState};
    use std::sync::Arc;

    #[test]
    fn test_write_locks() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));
            let k3 = Rc::new(BytesMut::from("key1"));

            let mut keys = Vec::<Rc<BytesMut>>::with_capacity(3);
            keys.push(k1);
            keys.push(k2);
            keys.push(k3);

            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            for _ in 0..100 {
                let locker = LockManager::lock_multi_internal_keys_exclusive(&keys, client.inner())
                    .await
                    .unwrap();
                assert!(locker.write_locks.is_some());
                assert!(locker.read_locks.is_none());
                // we expect two locks (key1 is duplicated)
                assert_eq!(locker.write_locks.unwrap().len(), 2);
            }
        });
    }

    #[test]
    fn test_read_locks() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));

            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            let mut keys = Vec::<Rc<BytesMut>>::with_capacity(2);
            keys.push(k1);
            keys.push(k2);

            for _ in 0..100 {
                let locker = LockManager::lock_multi_internal_keys_shared(&keys, client.inner())
                    .await
                    .unwrap();
                assert!(locker.write_locks.is_none());
                assert!(locker.read_locks.is_some());
                assert_eq!(locker.read_locks.unwrap().len(), 2);
            }
        });
    }

    #[test]
    fn test_lock_in_txn_prep_state() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));

            let mut expected_slots = vec![calculate_slot(&k1), calculate_slot(&k2)];
            expected_slots.dedup();
            expected_slots.sort();

            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            client.inner().set_txn_state_calc_slots(true);

            let mut keys = Vec::<Rc<BytesMut>>::with_capacity(2);
            keys.push(k1);
            keys.push(k2);

            let result = LockManager::lock_multi_internal_keys_shared(&keys, client.inner()).await;
            match result {
                Err(SableError::LockCancelledTxnPrep(slots)) => {
                    assert_eq!(expected_slots, slots);
                }
                _ => {
                    panic!("expected SableError::LockCancelledTxnPrep");
                }
            }
        });
    }

    #[test]
    fn test_lock_in_active_txn_state() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));

            let mut expected_slots = vec![calculate_slot(&k1), calculate_slot(&k2)];
            expected_slots.dedup();
            expected_slots.sort();

            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            client.inner().set_txn_state_exec(true);

            let mut keys = Vec::<Rc<BytesMut>>::with_capacity(2);
            keys.push(k1);
            keys.push(k2);

            let guard = LockManager::lock_multi_internal_keys_shared(&keys, client.inner())
                .await
                .unwrap();
            // No-op locks
            assert!(guard.write_locks.is_none());
            assert!(guard.read_locks.is_none());
        });
    }

    #[test]
    fn test_multithreaded_locks() {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

        let h1 = std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("Worker")
                .build()
                .unwrap_or_else(|e| {
                    panic!("failed to create tokio runtime. {:?}", e);
                });
            rt.block_on(async move {
                let (_guard, store) = crate::tests::open_store();
                let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

                let k4 = BytesMut::from("key4");
                let k1 = BytesMut::from("key1");
                let k2 = BytesMut::from("key2");

                let mut keys = Vec::<&BytesMut>::new();
                keys.push(&k1);
                keys.push(&k4);
                keys.push(&k2);

                // Create a "write" command to force `lock_multi` to use exclusive lock
                let args = "set a b".split(' ').collect();
                let cmd = Rc::new(ValkeyCommand::for_test(args));
                for _ in 0..100 {
                    let locker = LockManager::lock_multi(&keys, client.inner(), cmd.clone())
                        .await
                        .unwrap();
                    let curvalue_before = COUNTER.load(std::sync::atomic::Ordering::Relaxed);
                    crate::test_assert!(locker.len(), keys.len());
                    crate::test_assert!(locker.write_locks_len(), keys.len());
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    // verify that the value is still the same
                    let curvalue_after = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::test_assert!(curvalue_before, curvalue_after);
                }
            });
        });

        let h2 = std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("Worker")
                .build()
                .unwrap_or_else(|e| {
                    panic!("failed to create tokio runtime. {:?}", e);
                });
            rt.block_on(async move {
                let (_guard, store) = crate::tests::open_store();
                let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

                let k1 = BytesMut::from("key1");
                let k2 = BytesMut::from("key2");
                let k3 = BytesMut::from("key3");
                let k4 = BytesMut::from("key4");

                let mut keys = Vec::<&BytesMut>::new();
                keys.push(&k1);
                keys.push(&k3);
                keys.push(&k4);
                keys.push(&k2);

                // Create a "write" command to force `lock_multi` to use exclusive lock
                let args = "set a b".split(' ').collect();
                let cmd = Rc::new(ValkeyCommand::for_test(args));

                for _ in 0..100 {
                    let locker = LockManager::lock_multi(&keys, client.inner(), cmd.clone())
                        .await
                        .unwrap();
                    let curvalue_before = COUNTER.load(std::sync::atomic::Ordering::Relaxed);
                    crate::test_assert!(locker.len(), keys.len());
                    crate::test_assert!(locker.write_locks_len(), keys.len());
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    // verify that the value is still the same
                    let curvalue_after = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::test_assert!(curvalue_before, curvalue_after);
                }
            });
        });
        let _ = h1.join();
        let _ = h2.join();
    }
}
