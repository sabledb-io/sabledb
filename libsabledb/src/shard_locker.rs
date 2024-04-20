use crate::{utils::calculate_slot, PrimaryKeyMetadata};
use bytes::BytesMut;
use std::rc::Rc;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

lazy_static::lazy_static! {
    static ref MULTI_LOCK: ShardLocker = ShardLocker::default();
}

#[allow(dead_code)]
pub struct ShardLockGuard<'a> {
    read_locks: Option<Vec<RwLockReadGuard<'a, u16>>>,
    write_locks: Option<Vec<RwLockWriteGuard<'a, u16>>>,
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
            let lock = RwLock::new(0u16);
            locks.push(lock);
        }
        ShardLocker { locks }
    }
}

pub struct LockManager {}

impl LockManager {
    fn lock_multi_internal_keys_exclusive<'a>(keys: &[Rc<BytesMut>]) -> ShardLockGuard<'a> {
        let mut write_locks = Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(keys.len());
        // Calculate the slots and sort them
        let mut slots = Vec::<u16>::with_capacity(keys.len());
        for key in keys.iter() {
            slots.push(calculate_slot(key));
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in slots.into_iter() {
            let Some(lock) = MULTI_LOCK.locks.get(idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            if let Ok(lock) = lock.write() {
                write_locks.push(lock);
            } else {
                panic!("Can't obtain lock for slot: {}", idx);
            }
        }

        ShardLockGuard {
            read_locks: None,
            write_locks: Some(write_locks),
        }
    }

    // obtain execlusive lock on a user key
    pub fn lock_user_key_exclusive<'a>(user_key: &BytesMut, db_id: u16) -> ShardLockGuard<'a> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
        Self::lock_internal_key_exclusive(&internal_key)
    }

    // obtain a shared lock on a user key
    pub fn lock_user_key_shared<'a>(user_key: &BytesMut, db_id: u16) -> ShardLockGuard<'a> {
        let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
        Self::lock_internal_key_shared(&internal_key)
    }

    // obtain a shared lock on a user key
    pub fn lock_user_keys_shared<'a>(user_keys: &[&BytesMut], db_id: u16) -> ShardLockGuard<'a> {
        let mut primary_keys = Vec::<Rc<BytesMut>>::with_capacity(user_keys.len());
        let mut primary_keys_refs = Vec::<Rc<BytesMut>>::with_capacity(user_keys.len());
        for user_key in user_keys.iter() {
            let internal_key = Rc::new(PrimaryKeyMetadata::new_primary_key(user_key, db_id));
            primary_keys.push(internal_key.clone());
            primary_keys_refs.push(internal_key);
        }
        Self::lock_multi_internal_keys_shared(&primary_keys_refs)
    }

    // obtain a shared lock on a user key
    pub fn lock_user_keys_exclusive<'a>(user_keys: &[&BytesMut], db_id: u16) -> ShardLockGuard<'a> {
        let mut primary_keys = Vec::<Rc<BytesMut>>::with_capacity(user_keys.len());
        let mut primary_keys_refs = Vec::<Rc<BytesMut>>::with_capacity(user_keys.len());
        for user_key in user_keys.iter() {
            let internal_key = Rc::new(PrimaryKeyMetadata::new_primary_key(user_key, db_id));
            primary_keys.push(internal_key.clone());
            primary_keys_refs.push(internal_key);
        }
        Self::lock_multi_internal_keys_exclusive(&primary_keys_refs)
    }

    /// Lock the entire storage
    pub fn lock_all_keys_exclusive<'a>() -> ShardLockGuard<'a> {
        let mut write_locks =
            Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(crate::utils::SLOT_SIZE.into());

        let mut slots = Vec::<u16>::with_capacity(crate::utils::SLOT_SIZE.into());
        for slot in 0..crate::utils::SLOT_SIZE {
            slots.push(slot);
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in slots.into_iter() {
            let Some(lock) = MULTI_LOCK.locks.get(idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            if let Ok(lock) = lock.write() {
                write_locks.push(lock);
            } else {
                panic!("Can't obtain lock for slot: {}", idx);
            }
        }

        ShardLockGuard {
            read_locks: None,
            write_locks: Some(write_locks),
        }
    }

    /// Lock the entire storage
    pub fn lock_all_keys_shared<'a>() -> ShardLockGuard<'a> {
        let mut read_locks =
            Vec::<RwLockReadGuard<'a, u16>>::with_capacity(crate::utils::SLOT_SIZE.into());

        let mut slots = Vec::<u16>::with_capacity(crate::utils::SLOT_SIZE.into());
        for slot in 0..crate::utils::SLOT_SIZE {
            slots.push(slot);
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in slots.into_iter() {
            let Some(lock) = MULTI_LOCK.locks.get(idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            if let Ok(lock) = lock.read() {
                read_locks.push(lock);
            } else {
                panic!("Can't obtain lock for slot: {}", idx);
            }
        }

        ShardLockGuard {
            read_locks: Some(read_locks),
            write_locks: None,
        }
    }

    // Internal API
    fn lock_multi_internal_keys_shared<'a>(keys: &[Rc<BytesMut>]) -> ShardLockGuard<'a> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(keys.len());
        // Calculate the slots and sort them
        let mut slots = Vec::<u16>::with_capacity(keys.len());
        for key in keys.iter() {
            slots.push(calculate_slot(key));
        }

        // the sorting is required to avoid deadlocks
        slots.sort();
        slots.dedup();

        for idx in slots.into_iter() {
            let Some(lock) = MULTI_LOCK.locks.get(idx as usize) else {
                unreachable!("No lock in index {}", idx);
            };

            if let Ok(lock) = lock.read() {
                read_locks.push(lock);
            } else {
                panic!("Can't obtain lock for slot: {}", idx);
            }
        }

        ShardLockGuard {
            read_locks: Some(read_locks),
            write_locks: None,
        }
    }

    fn lock_internal_key_shared<'a>(key: &BytesMut) -> ShardLockGuard<'a> {
        let mut read_locks = Vec::<RwLockReadGuard<'a, u16>>::with_capacity(1);
        // Calculate the slots and sort them
        let slot = calculate_slot(key);

        read_locks.push(
            MULTI_LOCK
                .locks
                .get(slot as usize)
                .expect("lock")
                .read()
                .expect("poisoned mutex"),
        );

        ShardLockGuard {
            read_locks: Some(read_locks),
            write_locks: None,
        }
    }

    #[allow(dead_code)]
    fn noop_lock<'a>() -> ShardLockGuard<'a> {
        ShardLockGuard {
            write_locks: None,
            read_locks: None,
        }
    }

    fn lock_internal_key_exclusive<'a>(key: &BytesMut) -> ShardLockGuard<'a> {
        let mut write_locks = Vec::<RwLockWriteGuard<'a, u16>>::with_capacity(1);
        // Calculate the slots and sort them
        let slot = calculate_slot(key);

        write_locks.push(
            MULTI_LOCK
                .locks
                .get(slot as usize)
                .expect("lock")
                .write()
                .expect("poisoned mutex"),
        );

        ShardLockGuard {
            write_locks: Some(write_locks),
            read_locks: None,
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
    #[test]
    fn test_write_locks() {
        let k1 = Rc::new(BytesMut::from("key1"));
        let k2 = Rc::new(BytesMut::from("key2"));
        let k3 = Rc::new(BytesMut::from("key1"));

        let mut keys = Vec::<Rc<BytesMut>>::with_capacity(3);
        keys.push(k1);
        keys.push(k2);
        keys.push(k3);
        for _ in 0..100 {
            let locker = LockManager::lock_multi_internal_keys_exclusive(&keys);
            assert!(locker.write_locks.is_some());
            assert!(locker.read_locks.is_none());
            // we expect two locks (key1 is duplicated)
            assert_eq!(locker.write_locks.unwrap().len(), 2);
        }
    }

    #[test]
    fn test_read_locks() {
        let k1 = Rc::new(BytesMut::from("key1"));
        let k2 = Rc::new(BytesMut::from("key2"));

        let mut keys = Vec::<Rc<BytesMut>>::with_capacity(2);
        keys.push(k1);
        keys.push(k2);

        for _ in 0..100 {
            let locker = LockManager::lock_multi_internal_keys_shared(&keys);
            assert!(locker.write_locks.is_none());
            assert!(locker.read_locks.is_some());
            assert_eq!(locker.read_locks.unwrap().len(), 2);
        }
    }

    #[test]
    fn test_multithreaded_locks() {
        let h1 = std::thread::spawn(|| {
            let k4 = Rc::new(BytesMut::from("key4"));
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));

            let mut keys = Vec::<Rc<BytesMut>>::new();
            keys.push(k1);
            keys.push(k4);
            keys.push(k2);
            for _ in 0..100 {
                let locker = LockManager::lock_multi_internal_keys_exclusive(&keys);
                assert!(locker.write_locks.is_some());
                assert!(locker.read_locks.is_none());
                assert_eq!(locker.write_locks.unwrap().len(), keys.len());
            }
        });

        let h2 = std::thread::spawn(|| {
            let k1 = Rc::new(BytesMut::from("key1"));
            let k2 = Rc::new(BytesMut::from("key2"));
            let k3 = Rc::new(BytesMut::from("key3"));
            let k4 = Rc::new(BytesMut::from("key4"));

            let mut keys = Vec::<Rc<BytesMut>>::new();
            keys.push(k2);
            keys.push(k3);
            keys.push(k4);
            keys.push(k1);
            for _ in 0..100 {
                let locker = LockManager::lock_multi_internal_keys_exclusive(&keys);
                assert!(locker.write_locks.is_some());
                assert!(locker.read_locks.is_none());
                assert_eq!(locker.write_locks.unwrap().len(), keys.len());
            }
        });
        let _ = h1.join();
        let _ = h2.join();
    }
}
