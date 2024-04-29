use crate::metadata::PrimaryKeyMetadata;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::RwLock;

macro_rules! watching_table {
    ($watched_keys_table:expr) => {{
        let table = if let Some(watched_keys_table) = $watched_keys_table {
            watched_keys_table
        } else {
            &WATCHED_KEYS_TABLE
        };
        table
    }};
}

// Keys being watched during a transaction
pub type KeysTable = RwLock<HashMap<BytesMut, Key>>;

#[derive(Default)]
/// A watched key value
pub struct Key {
    /// Was this key modified?
    pub modified: AtomicBool,
    /// Number of clients watching this key
    pub watchers: AtomicU32,
}

impl Key {
    /// Increase the number of clients watching this key by 1
    pub fn incr_ref(&self) {
        let _ = self.watchers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrease the number of clients watching this key by 1
    pub fn decr_ref(&self) {
        let _ = self.watchers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Is this key is modified?
    pub fn is_modified(&self) -> bool {
        self.modified.load(Ordering::Relaxed)
    }

    /// Change this key state to "modified"
    pub fn set_modified(&self) {
        self.modified.store(true, Ordering::Relaxed)
    }

    /// Return the number of clients watching this key
    pub fn ref_count(&self) -> u32 {
        self.watchers.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct WatchedKeysTable {
    pub watched_keys: KeysTable,
    pub watching_clients_count: AtomicU32,
}

lazy_static::lazy_static! {
    /// Global watched keys table
    static ref WATCHED_KEYS_TABLE: WatchedKeysTable = WatchedKeysTable::default();
}

#[derive(Default)]
pub struct WatchedKeys {}

#[allow(dead_code)]
impl WatchedKeys {
    /// Fast path: do we have watchers?
    pub fn has_watchers(watched_keys_table: Option<&WatchedKeysTable>) -> bool {
        let table = watching_table!(watched_keys_table);
        table.watching_clients_count.load(Ordering::Relaxed) > 0
    }

    /// Return the number of watchers ("clients")
    pub fn watchers_count(watched_keys_table: Option<&WatchedKeysTable>) -> usize {
        let table = watching_table!(watched_keys_table);
        table.watching_clients_count.load(Ordering::Relaxed) as usize
    }

    /// Add `user_keys` to the watched table
    pub fn add_watcher(
        user_keys: &[&BytesMut],
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) {
        let table = watching_table!(watched_keys_table);
        let mut lock = table.watched_keys.write().expect("Failed to write lock");
        for user_key in user_keys {
            let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
            if let Some(watched_key) = lock.get_mut(&internal_key) {
                // increase the watchers count for this key
                watched_key.incr_ref();
            } else {
                let watched_key = Key::default();
                watched_key.incr_ref();
                let _ = lock.insert(internal_key, watched_key);
            }
        }

        if table.watching_clients_count.load(Ordering::Relaxed) < u32::MAX {
            table.watching_clients_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Notify that a key was changed
    /// If there are no watchers, this function returns immediately
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn notify_multi(
        internal_keys: &[&BytesMut],
        watched_keys_table: Option<&WatchedKeysTable>,
    ) {
        // fast path
        if !Self::has_watchers(watched_keys_table) {
            return;
        }

        let table = watching_table!(watched_keys_table);

        let lock = table.watched_keys.read().expect("Failed to read lock");
        for internal_key in internal_keys {
            if let Some(watched_key) = lock.get(*internal_key) {
                watched_key.set_modified();
            }
        }
    }

    /// Notify that a key was changed
    /// If there are no watchers, this function returns immediately
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn notify(internal_key: &BytesMut, watched_keys_table: Option<&WatchedKeysTable>) {
        Self::notify_multi(&[internal_key], watched_keys_table)
    }

    /// Notify that a key was changed
    /// If there are no watchers, this function returns immediately
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn notify_user_key_multi(
        user_keys: &[&BytesMut],
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) {
        // fast path
        if !Self::has_watchers(watched_keys_table) {
            return;
        }

        let table = watching_table!(watched_keys_table);

        let lock = table.watched_keys.read().expect("Failed to read lock");
        for user_key in user_keys {
            let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
            if let Some(watched_key) = lock.get(&internal_key) {
                watched_key.set_modified();
            }
        }
    }

    /// Notify that a key was changed
    /// If there are no watchers, this function returns immediately
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn notify_user_key(
        user_key: &BytesMut,
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) {
        Self::notify_user_key_multi(&[user_key], db_id, watched_keys_table)
    }

    /// Remove `1` watcher from the list and all its `keys`. Keys are kept with reference counting
    /// a key is removed when its reference count reaches `0`
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn remove_watcher(
        user_keys: &[&BytesMut],
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) {
        if user_keys.is_empty() {
            return;
        }

        let table = watching_table!(watched_keys_table);
        let mut lock = table.watched_keys.write().expect("Failed to write lock");

        // Reduce the reference for all the keys
        for user_key in user_keys {
            let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
            if let Some(watched_key) = lock.get(&internal_key) {
                // increase the watchers count for this key
                watched_key.decr_ref();
                if watched_key.ref_count() == 0 {
                    lock.remove(&internal_key);
                }
            }
        }

        // and finally reduce the watchers count
        if table.watching_clients_count.load(Ordering::Relaxed) > 0 {
            table.watching_clients_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Return true if *any* of `keys` is marked as modified.
    /// Note that `keys` are user keys, so they need to be transformed into `PrimaryKeyMetadata`
    /// before we check the table.
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn is_user_key_modified_multi(
        user_keys: &[&BytesMut],
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) -> bool {
        if user_keys.is_empty() {
            return false;
        }

        let table = watching_table!(watched_keys_table);
        let lock = table.watched_keys.read().expect("Failed to read lock");
        user_keys.iter().any(|&user_key| {
            let internal_key = PrimaryKeyMetadata::new_primary_key(user_key, db_id);
            if let Some(watched_key) = lock.get(&internal_key) {
                watched_key.is_modified()
            } else {
                false
            }
        })
    }

    /// Return true if `key` is marked as modified.
    ///
    /// If `watched_keys_table` is `None`, the global `WATCHED_KEYS_TABLE` is used
    pub fn is_user_key_modified(
        user_key: &BytesMut,
        db_id: u16,
        watched_keys_table: Option<&WatchedKeysTable>,
    ) -> bool {
        Self::is_user_key_modified_multi(&[user_key], db_id, watched_keys_table)
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
mod test {
    use super::*;

    #[test]
    fn test_watching() {
        let watching_table = WatchedKeysTable::default();

        let k1 = BytesMut::from("k1");
        let k2 = BytesMut::from("k2");
        let k3 = BytesMut::from("k3");
        let k4 = BytesMut::from("k4");

        // internally, the keys are stored encoded
        let k1_encoded = PrimaryKeyMetadata::new_primary_key(&k1, 0);
        let k2_encoded = PrimaryKeyMetadata::new_primary_key(&k2, 0);
        let k3_encoded = PrimaryKeyMetadata::new_primary_key(&k3, 0);

        let keys_1 = vec![&k1, &k2];
        let keys_2 = vec![&k2, &k3];
        let keys_3 = vec![&k1, &k2, &k3];

        WatchedKeys::add_watcher(&keys_1, 0, Some(&watching_table));
        WatchedKeys::add_watcher(&keys_2, 0, Some(&watching_table));
        WatchedKeys::add_watcher(&keys_3, 0, Some(&watching_table));

        {
            let table = watching_table.watched_keys.read().unwrap();
            // we expect 3 entries in the watched keys table
            assert_eq!(table.len(), 3);
            assert_eq!(table.get(&k1_encoded).unwrap().ref_count(), 2);
            assert_eq!(table.get(&k2_encoded).unwrap().ref_count(), 3);
            assert_eq!(table.get(&k3_encoded).unwrap().ref_count(), 2);
        }

        WatchedKeys::notify_user_key_multi(&keys_1, 0, Some(&watching_table));
        WatchedKeys::notify_user_key_multi(&keys_2, 0, Some(&watching_table));
        WatchedKeys::notify_user_key_multi(&keys_3, 0, Some(&watching_table));

        assert!(WatchedKeys::is_user_key_modified(
            &k1,
            0,
            Some(&watching_table)
        ));
        assert!(WatchedKeys::is_user_key_modified(
            &k2,
            0,
            Some(&watching_table)
        ));
        assert!(WatchedKeys::is_user_key_modified(
            &k3,
            0,
            Some(&watching_table)
        ));
        assert!(!WatchedKeys::is_user_key_modified(
            &k4,
            0,
            Some(&watching_table)
        ));

        WatchedKeys::remove_watcher(&keys_1, 0, Some(&watching_table));
        WatchedKeys::remove_watcher(&keys_2, 0, Some(&watching_table));
        WatchedKeys::remove_watcher(&keys_3, 0, Some(&watching_table));

        {
            let table = watching_table.watched_keys.read().unwrap();
            // we expect 3 entries in the watched keys table
            assert!(table.is_empty());
        }
    }
}
