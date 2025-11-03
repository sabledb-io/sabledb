use crate::SableError;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const POISONED_MUTEX: &str = "poisoned mutex";
const NO_OWNER_ERR: &str = "A lock with no owner";

#[derive(Debug)]
pub enum LockResult {
    /// Lock was acquired successfully
    Ok,
    /// The lock is already owned by another client
    Pending(Receiver<u8>),
    /// Potential deadlock detected, this can happen if the client that holds the lock
    /// attempts to lock it again
    Deadlock,
    /// The lock is not available, try again later
    WouldBlock,
}

#[derive(Debug, PartialEq)]
pub enum UnlockResult {
    /// Lock was acquired successfully
    Ok,
    /// The lock is not owned by the requester
    NotOwner,
    /// The requested lock could not be found
    NotFound,
}

impl PartialEq for LockResult {
    fn eq(&self, other: &LockResult) -> bool {
        matches!(
            (self, other),
            (Self::Ok, Self::Ok)
                | (Self::Pending(_), Self::Pending(_))
                | (Self::WouldBlock, Self::WouldBlock)
                | (Self::Deadlock, Self::Deadlock)
        )
    }
}

impl std::fmt::Display for LockResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Ok => write!(f, "Ok"),
            Self::Pending(_) => write!(f, "Pending(..)"),
            Self::Deadlock => write!(f, "Deadlock"),
            Self::WouldBlock => write!(f, "TryAgain"),
        }
    }
}

#[derive(Default, Clone)]
struct LockEntry {
    /// The current owner of the key
    owner: Option<u128>,
    /// List of clients pending to lock this key
    pending_clients: Vec<(u128, Sender<u8>)>,
}

#[derive(Default, Clone)]
struct LockDbInner {
    locks: HashMap<BytesMut, LockEntry>,
}

#[derive(Default, Clone)]
pub struct LockDb {
    inner: Arc<RwLock<LockDbInner>>,
}

impl LockDb {
    /// Lock `key`. If the operation is successful, return `LockResult::Ok`, otherwise, return a channel
    /// on which the caller can wait
    pub fn lock(&self, key: &[u8], client_id: u128) -> Result<LockResult, SableError> {
        let mut inner = self.inner.write().expect(POISONED_MUTEX);
        if let Some(entry) = inner.locks.get_mut(key) {
            if entry
                .owner
                .ok_or(SableError::InvalidState(NO_OWNER_ERR.into()))?
                == client_id
            {
                return Ok(LockResult::Deadlock);
            }

            // Someone already owns this lock, create a channel and return it to the caller
            // so it could wait on it
            let (tx, rx) = channel::<u8>(100);
            // We keep the sender channel and the client that requested the lock
            entry.pending_clients.push((client_id, tx));
            // And return the receiver to the caller (so we can notify it when the lock is available)
            Ok(LockResult::Pending(rx))
        } else {
            // No one owns the lock
            let entry = LockEntry {
                owner: Some(client_id),
                pending_clients: Vec::default(),
            };
            inner.locks.insert(key.into(), entry);
            Ok(LockResult::Ok)
        }
    }

    /// Try to lock `key`. Return `true` on success, `false`, otherwise. If you wish to be notified
    /// when the lock is available, use `lock` to get a waiting channel
    pub fn try_lock(&self, key: &[u8], client_id: u128) -> Result<LockResult, SableError> {
        let mut inner = self.inner.write().expect(POISONED_MUTEX);
        if let Some(entry) = inner.locks.get_mut(key) {
            if entry
                .owner
                .ok_or(SableError::InvalidState(NO_OWNER_ERR.into()))?
                == client_id
            {
                return Ok(LockResult::Deadlock);
            }
            Ok(LockResult::WouldBlock)
        } else {
            // No one owns the lock
            let entry = LockEntry {
                owner: Some(client_id),
                pending_clients: Vec::default(),
            };
            inner.locks.insert(key.into(), entry);
            Ok(LockResult::Ok)
        }
    }

    /// Unlock `key`. If the lock is not owned by `client`, return `SableError`
    pub fn unlock(&self, key: &[u8], client_id: u128) -> Result<UnlockResult, SableError> {
        let mut inner = self.inner.write().expect(POISONED_MUTEX);
        let Some(entry) = inner.locks.get_mut(key) else {
            return Ok(UnlockResult::NotFound);
        };
        let res = self.unlock_internal(entry, client_id)?;
        if entry.owner.is_none() {
            inner.locks.remove(key);
        }
        Ok(res)
    }

    /// Unlock mul
    pub fn unlock_multi(
        &self,
        keys: &[&BytesMut],
        client_id: u128,
    ) -> Result<UnlockResult, SableError> {
        let mut inner = self.inner.write().expect(POISONED_MUTEX);
        for key in keys {
            if let Some(entry) = inner.locks.get_mut(*key) {
                self.unlock_internal(entry, client_id)?;
                if entry.owner.is_none() {
                    inner.locks.remove(*key);
                }
            } else {
                tracing::warn!("key '{:?}' does not exist in locking table", key);
            }
        }
        Ok(UnlockResult::Ok)
    }

    // Help API (internal)

    /// Unlock key and wakeup and pending client for the key's lock.
    ///
    /// This method does not use locks and should be used after a lock was obtained.
    fn unlock_internal(
        &self,
        entry: &mut LockEntry,
        client_id: u128,
    ) -> Result<UnlockResult, SableError> {
        if entry
            .owner
            .ok_or(SableError::InvalidState(NO_OWNER_ERR.into()))?
            != client_id
        {
            return Ok(UnlockResult::NotOwner);
        }

        // Clear the current owner
        entry.owner = None;
        while let Some((client_id, tx)) = entry.pending_clients.pop() {
            let res = tx.try_send(1u8);
            if res.is_ok() {
                // Ownership transfer was ok, update the new owner for the lock and break
                entry.owner = Some(client_id);
                break;
            }
            // else: probably the waiting client was dropped, try the next one
        }
        Ok(UnlockResult::Ok)
    }
}

unsafe impl Sync for LockDb {}

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
    use tokio::sync::mpsc::error::TryRecvError;

    #[test]
    fn test_lock() {
        let lock_db = LockDb::default();
        assert_eq!(lock_db.lock(b"mykey", 1).unwrap(), LockResult::Ok);
        let Ok(LockResult::Pending(mut chan)) = lock_db.lock(b"mykey", 2) else {
            panic!("Expected mykey to be locked");
        };

        assert_eq!(Err(TryRecvError::Empty), chan.try_recv());
        assert_eq!(UnlockResult::NotOwner, lock_db.unlock(b"mykey", 2).unwrap()); // Not Ok, not the owner

        assert_eq!(UnlockResult::Ok, lock_db.unlock(b"mykey", 1).unwrap()); // should be OK
        assert_eq!(Ok(1u8), chan.try_recv()); // we should now have a wakeup message on the channel

        let key = BytesMut::from("mykey");
        {
            let map = &lock_db.inner.read().unwrap().locks;
            assert!(map.contains_key(&key));
            assert_eq!(map.get(&key).unwrap().owner.unwrap(), 2); // the new owner is 2
        }

        assert_eq!(UnlockResult::Ok, lock_db.unlock(&key, 2).unwrap()); // Ok
        {
            let map = &lock_db.inner.read().unwrap().locks;
            // No more locks
            assert!(map.is_empty());
        }
    }

    #[test]
    fn test_multi_lock() {
        let key1 = BytesMut::from("mykey1");
        let key2 = BytesMut::from("mykey2");
        let lock_db = LockDb::default();
        assert_eq!(lock_db.lock(&key1, 1).unwrap(), LockResult::Ok);
        assert_eq!(lock_db.lock(&key2, 1).unwrap(), LockResult::Ok);
        {
            let map = &lock_db.inner.read().unwrap().locks;
            // 2 locks
            assert_eq!(map.len(), 2);
        }

        lock_db.unlock_multi(&[&key1, &key2], 1).unwrap();

        {
            let map = &lock_db.inner.read().unwrap().locks;
            // No more locks
            assert!(map.is_empty());
        }
    }
}
