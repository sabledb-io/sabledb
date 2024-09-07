use crate::{
    replication::{ClusterDB, LockResult, UnLockResult},
    SableError, Server, ServerOptions,
};
use std::sync::{Arc, RwLock as StdRwLock};

pub trait Lock {
    fn is_locked(&self) -> bool;
    fn lock(&mut self) -> Result<(), SableError>;
    fn unlock(&mut self) -> Result<(), SableError>;
}

pub struct BlockingLock {
    options: Arc<StdRwLock<ServerOptions>>,
    name: String,
    locked: bool,
}

impl BlockingLock {
    pub fn new(options: Arc<StdRwLock<ServerOptions>>, name: String) -> Self {
        BlockingLock {
            options,
            name,
            locked: false,
        }
    }
}

impl Lock for BlockingLock {
    fn is_locked(&self) -> bool {
        self.locked
    }

    fn lock(&mut self) -> Result<(), SableError> {
        let db = ClusterDB::with_options(self.options.clone());
        loop {
            let res = db.lock(&self.name)?;
            match res {
                LockResult::Ok => {
                    break;
                }
                LockResult::AlreadyExist(owner) => {
                    tracing::debug!(
                        "Unable to lock '{}'. Already locked by '{}'",
                        self.name,
                        owner
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        self.locked = true;
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), SableError> {
        if self.is_locked() {
            self.locked = false;
            let db = ClusterDB::with_options(self.options.clone());
            let res = db.unlock(&self.name)?;
            match res {
                UnLockResult::Ok => {}
                UnLockResult::NotOwner(owner) => {
                    tracing::warn!("Failed to unlock '{}'. New owner {}", self.name, owner);
                }
                UnLockResult::NoSuchLock => {
                    tracing::warn!("Failed to unlock '{}'. No such lock", self.name);
                }
            }
        }
        Ok(())
    }
}

impl Drop for BlockingLock {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}

pub struct PrimaryLock {
    lock: BlockingLock,
}

impl PrimaryLock {
    pub fn new(options: Arc<StdRwLock<ServerOptions>>) -> Result<Self, SableError> {
        let primary_node_id = Server::state().persistent_state().primary_node_id();
        if primary_node_id.is_empty() {
            return Err(SableError::InvalidArgument(
                "Failed to create PrimaryLock. No primary".into(),
            ));
        }

        Ok(PrimaryLock {
            lock: BlockingLock::new(options, format!("{}_PRIMARY_LOCK", primary_node_id)),
        })
    }
}

impl Lock for PrimaryLock {
    fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }

    fn lock(&mut self) -> Result<(), SableError> {
        self.lock.lock()
    }

    fn unlock(&mut self) -> Result<(), SableError> {
        self.lock.unlock()
    }
}
