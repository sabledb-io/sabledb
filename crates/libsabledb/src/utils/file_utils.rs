use std::io::{Read, Write};
use std::path::Path;

/// Writes content to a file, replacing any existing content.
///
/// This function creates or overwrites a file at the specified path with the provided content.
/// If the parent directory doesn't exist, it will be created automatically along with any
/// necessary intermediate directories.
///
/// # Arguments
///
/// * `filepath` - A reference to a `Path` representing the target file location
/// * `content` - A reference to a `String` containing the content to write to the file
///
/// # Behavior
///
/// * Creates parent directories if they don't exist (using `create_dir_all`)
/// * Overwrites the file if it already exists
/// * If the file cannot be created or written to, logs a warning and returns without panicking
///
/// # Error Handling
///
/// This function handles errors gracefully without returning a `Result`:
/// * Directory creation failures - Ignored silently
/// * File creation failures - Logs a warning and returns early
/// * Write operation failures - Logs a warning with error details
///
/// # Examples
///
/// ```no_compile
/// use std::path::Path;
///
/// let path = Path::new("output/example.txt");
/// let content = String::from("Hello, World!");
/// write_file_content(path, &content);
/// ```
///
/// ```no_compile
/// use std::path::Path;
///
/// // Parent directories will be created automatically
/// let path = Path::new("deeply/nested/path/file.txt");
/// let content = String::from("Content");
/// write_file_content(path, &content);
/// ```
///
/// # Note
///
/// This function does not return an error or status indicator. Callers should check
/// the logs or verify the file existence independently if confirmation is needed.
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

/// Reads the entire content of a file from the filesystem and returns it as a trimmed string.
///
/// This function attempts to open and read a file at the specified path. If successful,
/// it returns the file's content with leading and trailing whitespace removed.
///
/// # Arguments
///
/// * `filepath` - A reference to a `Path` representing the file to read
///
/// # Returns
///
/// * `Some(String)` - The trimmed content of the file if successfully read
/// * `None` - If the file doesn't exist, cannot be opened, or cannot be read
///
/// # Error Handling
///
/// This function handles errors gracefully by returning `None` in the following cases:
/// * File not found - Returns `None` silently
/// * Other I/O errors during file opening - Logs a warning and returns `None`
/// * Errors reading file content - Logs a warning and returns `None`
///
/// # Examples
///
/// ```no_compile
/// use std::path::Path;
///
/// let path = Path::new("example.txt");
/// if let Some(content) = read_file_content(path) {
///     println!("File content: {}", content);
/// } else {
///     println!("Failed to read file");
/// }
/// ```
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
