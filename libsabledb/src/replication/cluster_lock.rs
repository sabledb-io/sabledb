use crate::{replication::Persistence, SableError, ServerOptions};
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
        let db = Persistence::with_options(self.options.clone());
        if let Err(e) = db.lock(&self.name, 60_000) {
            tracing::warn!("{e}");
        } else {
            self.locked = true;
        }
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), SableError> {
        if self.is_locked() {
            let db = Persistence::with_options(self.options.clone());
            if let Err(e) = db.unlock(&self.name) {
                tracing::warn!("{e}");
            } else {
                self.locked = false;
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

pub struct ClusterShardLock {
    lock: BlockingLock,
}

impl ClusterShardLock {
    pub fn with_name(
        shard_name: &String,
        options: Arc<StdRwLock<ServerOptions>>,
    ) -> Result<Self, SableError> {
        if shard_name.is_empty() {
            return Err(SableError::InvalidArgument(
                "Failed to create ClusterShardLock. Empty shard name provided".into(),
            ));
        }

        Ok(ClusterShardLock {
            lock: BlockingLock::new(options, format!("{}", shard_name)),
        })
    }
}

impl Lock for ClusterShardLock {
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
