use std::path::PathBuf;
use std::sync::{atomic, atomic::Ordering};

lazy_static::lazy_static! {
    static ref COUNTER: atomic::AtomicU64
        = atomic::AtomicU64::new(crate::TimeUtils::epoch_ms().unwrap_or(0));
}

/// Create a unique file for the process
pub struct TempFile {
    full_path: PathBuf,
}

#[allow(dead_code)]
impl TempFile {
    pub fn with_name(name: &str) -> Self {
        let full_path = format!(
            "{}{}.{}.txt",
            std::env::temp_dir().to_path_buf().display(),
            name,
            COUNTER.load(Ordering::Relaxed)
        );
        TempFile {
            full_path: PathBuf::from(full_path),
        }
    }

    pub fn fullpath(&self) -> String {
        self.full_path.to_string_lossy().to_string()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.full_path);
    }
}
