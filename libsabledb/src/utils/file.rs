use std::io::{Read, Write};
use std::path::Path;

/// Replace `filepath` content with `content`
pub fn write_file_content(filepath: &Path, content: &String) {
    if let Some(parent) = filepath.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let Ok(mut file) = std::fs::File::create(filepath) else {
        tracing::warn!("Could not open file {} for write", filepath.display());
        return;
    };
    if let Err(e) = file.write_all(content.as_bytes()) {
        tracing::warn!(
            "Could not write file {} content. {:?}",
            filepath.display(),
            e
        );
    }
}

/// Read entire file content into a string
pub fn read_file_content(filepath: &Path) -> Option<String> {
    let mut file = match std::fs::File::open(filepath) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return None;
        }
        Err(error) => {
            tracing::warn!("Error reading file {}. {:?}", filepath.display(), error);
            return None;
        }
    };
    let mut content = String::new();
    if let Err(error) = file.read_to_string(&mut content) {
        tracing::warn!("Unable to read file {}: {}", filepath.display(), error);
        return None;
    }
    Some(content.trim().to_string())
}
