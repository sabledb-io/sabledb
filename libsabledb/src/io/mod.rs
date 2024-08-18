mod file_output_sink;
mod resp_writer;
mod temp_file;

pub use file_output_sink::FileResponseSink;
pub use resp_writer::RespWriter;
pub use temp_file::TempFile;

const MAX_BUFFER_SIZE: usize = 10 << 20; // 10MB
use crate::SableError;
use bytes::BytesMut;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Read some bytes from the `reader` and return them
pub fn read_bytes<R>(reader: &mut R, count: usize) -> Result<Option<BytesMut>, SableError>
where
    R: ?Sized + Read,
{
    let buflen = if count > MAX_BUFFER_SIZE {
        MAX_BUFFER_SIZE
    } else {
        count
    };

    let mut buffer = vec![0u8; buflen];
    let result = reader.read(&mut buffer);
    match result {
        Ok(0usize) => Err(SableError::ConnectionClosed),
        Ok(count) => {
            Ok(Some(BytesMut::from(&buffer[..count])))
        }
        Err(e)
            // On Windows, TcpStream returns `TimedOut`
            // while on Linux, it would typically return
            // `WouldBlock`
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock =>
        {
            // Timeout occurred, not an error
            tracing::trace!("Read timedout");
            Ok(None)
        }
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
            // Interrupted, not an error
            tracing::warn!("Connection interrupted");
            Ok(None)
        }
        Err(e) => Err(SableError::StdIoError(e)),
    }
}

/// Read exactly `count` bytes from `reader` and write them into `writer`
pub fn read_exact<R, W>(reader: &mut R, writer: &mut W, count: usize) -> Result<(), SableError>
where
    R: ?Sized + Read,
    W: ?Sized + Write,
{
    // split the buffer into chunks and read
    const MAX_BUFFER_SIZE: usize = 10 << 20; // 10MB
    let mut bytes_left = count;
    tracing::debug!("Reading {} bytes", bytes_left);

    while bytes_left > 0 {
        let bytes_to_read = if bytes_left > MAX_BUFFER_SIZE {
            MAX_BUFFER_SIZE
        } else {
            bytes_left
        };

        match read_bytes(reader, bytes_to_read)? {
            Some(mut content) => {
                writer.write_all(content.as_mut())?;
                bytes_left = bytes_left.saturating_sub(content.len());
            }
            None => {
                continue;
            }
        }
    }
    tracing::debug!("Successfully reading {} bytes", count);
    Ok(())
}

/// Write `buff` into `writer`
pub fn write_bytes<W>(writer: &mut W, buff: &mut BytesMut) -> Result<(), SableError>
where
    W: ?Sized + Write,
{
    match writer.write_all(buff) {
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
            // translate io::ErrorKind::BrokenPipe -> SableError::BrokenPipe
            return Err(SableError::BrokenPipe);
        }
        Err(e) => return Err(SableError::StdIoError(e)),
        Ok(_) => {}
    }
    tracing::trace!("Successfully wrote message of {} bytes", buff.len());
    Ok(())
}

#[derive(Default)]
pub struct Archive {}

impl Archive {
    /// Archive the directory `dir_path` returning the path to the
    /// newly created archive
    pub fn create(&self, dir_path: &Path) -> Result<PathBuf, SableError> {
        let tar_gz_path = format!("{}.tar", dir_path.display());
        let tar_gz = std::fs::File::create(&tar_gz_path)?;
        //let enc = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
        let mut tar = tar::Builder::new(tar_gz);
        tar.append_dir_all(".", dir_path)?;
        tar.finish()?;
        Ok(PathBuf::from(tar_gz_path))
    }

    /// Extract `src` (which is usually a "tar" file) int `target` location (a directory)
    pub fn extract(&self, src: &Path, target: &Path) -> Result<(), SableError> {
        let tar_gz = std::fs::File::open(src)?;
        let mut archive = tar::Archive::new(tar_gz);
        // let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(tar_gz));
        archive.unpack(target)?;
        Ok(())
    }
}
