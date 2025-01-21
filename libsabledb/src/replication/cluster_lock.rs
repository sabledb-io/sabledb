use crate::{replication::Persistence, SableError};

pub trait Lock {
    fn is_locked(&self) -> bool;
    fn lock(&mut self) -> Result<(), SableError>;
    fn unlock(&mut self) -> Result<(), SableError>;
}

pub struct BlockingLock<'a> {
    db: &'a Persistence,
    name: String,
    locked: bool,
}

impl<'a> BlockingLock<'a> {
    pub fn with_db(db: &'a Persistence, name: String) -> Self {
        BlockingLock {
            db,
            name,
            locked: false,
        }
    }
}

impl Lock for BlockingLock<'_> {
    fn is_locked(&self) -> bool {
        self.locked
    }

    fn lock(&mut self) -> Result<(), SableError> {
        if let Err(e) = self.db.lock(&self.name, 5_000) {
            tracing::warn!("{e}");
        } else {
            self.locked = true;
        }
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), SableError> {
        if self.is_locked() {
            if let Err(e) = self.db.unlock(&self.name) {
                tracing::warn!("{e}");
            } else {
                self.locked = false;
            }
        }
        Ok(())
    }
}

impl Drop for BlockingLock<'_> {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}
