mod shard;

pub use shard::*;

use crate::SableError;
use std::path::PathBuf;

/// Attempt to find a file from the current working directory
/// and going up. On success, return the file's path (directory only)
pub fn findup(name: &str) -> Result<Option<PathBuf>, SableError> {
    let mut curdir = std::env::current_dir()?;

    loop {
        let Some(curdir_str) = curdir.to_str() else {
            break;
        };
        let fullpath = PathBuf::from(format!("{}/{}", curdir_str, name));
        if fullpath.exists() {
            return Ok(Some(PathBuf::from(curdir_str)));
        }
        if !curdir.pop() {
            break;
        }
    }
    Ok(None)
}
