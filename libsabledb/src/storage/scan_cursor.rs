use crate::types::DataType;
use bytes::BytesMut;
use std::rc::Rc;
use std::sync::atomic;

lazy_static::lazy_static! {
    static ref COUNTER: atomic::AtomicU64
        = atomic::AtomicU64::new(crate::TimeUtils::epoch_micros().unwrap_or(1));
}

/// A generic cursor that remembers the state of the iteration
/// Used by the *SCAN* commands (`SCAN`, `HSCAN`, `ZSCAN`)
pub struct ScanCursor {
    cursor_id: u64,
    data_type: DataType,
    search_prefix: Rc<BytesMut>,
}

impl ScanCursor {
    /// Create a new cursor for a given data type with a unique ID
    pub fn new(data_type: DataType, search_prefix: Rc<BytesMut>) -> Self {
        ScanCursor {
            cursor_id: COUNTER.fetch_add(1, atomic::Ordering::Relaxed),
            data_type,
            search_prefix,
        }
    }

    /// Create a new cursor with the same ID as this one, but with a different
    /// prefix
    pub fn progress(&self, search_prefix: Rc<BytesMut>) -> Self {
        ScanCursor {
            cursor_id: self.cursor_id,
            data_type: self.data_type,
            search_prefix,
        }
    }

    /// Return the current prefix
    pub fn prefix(&self) -> Rc<BytesMut> {
        self.search_prefix.clone()
    }

    /// Return the cursor ID (we send this back to the client)
    pub fn id(&self) -> u64 {
        self.cursor_id
    }
}
