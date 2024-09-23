use crate::{storage::IteratorAdapter, SableError};
use bytes::BytesMut;
use std::rc::Rc;
use std::sync::atomic;

lazy_static::lazy_static! {
    static ref COUNTER: atomic::AtomicU64
        = atomic::AtomicU64::new(crate::TimeUtils::epoch_micros().unwrap_or(1));
}

/// A generic cursor that remembers the state of the iteration
/// Used by the *SCAN* commands (`SCAN`, `HSCAN`, `ZSCAN`)
#[derive(Debug)]
pub struct ScanCursor {
    cursor_id: u64,
    /// If not `None`, seek the storage iterator to this prefix
    search_prefix: Option<BytesMut>,
}

impl Default for ScanCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanCursor {
    /// Create a new cursor for a given data type with a unique ID
    pub fn new() -> Self {
        ScanCursor {
            cursor_id: COUNTER.fetch_add(1, atomic::Ordering::Relaxed),
            search_prefix: None,
        }
    }

    /// Create a new cursor with the same ID as this one, but with a different
    /// prefix
    pub fn progress(&self, search_prefix: BytesMut) -> Self {
        ScanCursor {
            cursor_id: self.cursor_id,
            search_prefix: Some(search_prefix),
        }
    }

    /// Return the current prefix
    pub fn prefix(&self) -> Option<&[u8]> {
        self.search_prefix.as_deref()
    }

    /// Return the cursor ID (we send this back to the client)
    pub fn id(&self) -> u64 {
        self.cursor_id
    }

    /// Given database iterator, return a new cursor to be used by the client for the next "*SCAN" command
    pub fn create_next_cursor_for_prefix(
        &self,
        db_iter: &mut IteratorAdapter,
        encoded_prefix: &[u8],
    ) -> Result<Option<Rc<ScanCursor>>, SableError> {
        if !db_iter.valid() {
            return Ok(None);
        }

        // read the next key to be used as the next starting point for next iteration
        let encoded_key = db_iter.key().ok_or(SableError::InternalError(
            "failed to read key from the database".into(),
        ))?;

        if !encoded_key.starts_with(encoded_prefix) {
            return Ok(None);
        }

        // Create new cursor
        Ok(Some(Rc::new(self.progress(BytesMut::from(encoded_key)))))
    }
}
