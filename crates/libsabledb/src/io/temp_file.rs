use std::sync::{atomic, atomic::Ordering};

lazy_static::lazy_static! {
    static ref COUNTER: atomic::AtomicU64
        = atomic::AtomicU64::new(crate::TimeUtils::epoch_micros().unwrap_or(0));
}

/// Create a unique file for the process
pub struct TempFile {
    full_path: String,
}

impl TempFile {
    pub fn with_name(name: &str) -> Self {
        TempFile {
            full_path: Self::create_path(name),
        }
    }

    pub fn fullpath(&self) -> &String {
        &self.full_path
    }

    pub fn create_path(name: &str) -> String {
        let full_path = format!(
            "{}/{}.{}.{}",
            std::env::temp_dir().to_path_buf().display(),
            name,
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        let full_path = full_path.replace('\\', "/");
        full_path.replace("//", "/")
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.full_path);
    }
}
