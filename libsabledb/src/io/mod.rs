mod file_output_sink;
mod resp_writer;
mod temp_file;

pub use file_output_sink::FileResponseSink;
pub use resp_writer::RespWriter;
pub use temp_file::TempFile;

const MAX_BUFFER_SIZE: usize = 10 << 20; // 10MB

use crate::{utils::BytesMutUtils, SableError};
use bytes::BytesMut;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[cfg(not(test))]
use tracing::{debug, info};

#[cfg(test)]
use std::{println as info, println as debug};

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

    while bytes_left > 0 {
        let bytes_to_read = if bytes_left > MAX_BUFFER_SIZE {
            MAX_BUFFER_SIZE
        } else {
            bytes_left
        };

        match read_bytes(reader, bytes_to_read)? {
            Some(content) => {
                writer.write_all(&content[..])?;
                bytes_left = bytes_left.saturating_sub(content.len());
            }
            None => {
                continue;
            }
        }
    }
    writer.flush()?;
    debug!("Successfully read {} bytes", count);
    Ok(())
}

/// See if the socket buffer has enough data to read at least `count` bytes without blocking. This call does not
/// remove the data from the socket buffer
pub fn can_read_at_least(stream: &std::net::TcpStream, count: usize) -> Result<bool, SableError> {
    let mut buffer = vec![0u8; count];
    let result = stream.peek(&mut buffer);
    match result {
        Ok(0usize) => Err(SableError::ConnectionClosed),
        Ok(bytes_read) => Ok(bytes_read >= count),
        Err(e)
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::Interrupted =>
        {
            Ok(false)
        }
        Err(e) => Err(SableError::StdIoError(e)),
    }
}

/// Write `buff` into `writer`
pub fn write_bytes<W>(writer: &mut W, buff: &mut BytesMut) -> Result<(), SableError>
where
    W: ?Sized + Write,
{
    match writer.write_all(&buff[..]) {
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
            // translate io::ErrorKind::BrokenPipe -> SableError::BrokenPipe
            return Err(SableError::BrokenPipe);
        }
        Err(e) => return Err(SableError::StdIoError(e)),
        Ok(()) => {}
    }
    writer.flush()?;
    Ok(())
}

/// Write `fp` content into `writer`
pub fn send_file<W>(writer: &mut W, filepath: &std::path::Path) -> Result<u64, SableError>
where
    W: ?Sized + Write,
{
    // Send the file size first
    let mut fp = std::fs::File::options().read(true).open(filepath)?;
    let file_len = fp.metadata()?.len() as usize;
    let mut bytes_left: usize = file_len;

    // Send the file length first
    let mut file_len_bytes = BytesMutUtils::from_usize(&file_len);
    let file_len_bytes = &mut file_len_bytes[..];
    info!("Sending file size: {}. {:?}", file_len, file_len_bytes);
    writer.write_all(file_len_bytes)?;
    info!("Success");

    info!("Sending file content...");
    while bytes_left > 0 {
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];

        // pull some bytes from the file
        let count = fp.read(&mut buffer)?;

        // and write them to writer
        writer.write_all(&buffer[..count])?;

        bytes_left = bytes_left.saturating_sub(count);
    }

    writer.flush()?;
    info!("Success");
    Ok(file_len as u64)
}

/// Write `fp` content into `writer`
pub fn recv_file<R>(filepath: &std::path::Path, stream: &mut R) -> Result<u64, SableError>
where
    R: ?Sized + Read,
{
    // Prepare the local file
    let mut fp = std::fs::File::create(filepath)?;

    // Read the file's length
    const BYTES_TO_READ: usize = std::mem::size_of::<usize>();
    let mut file_len = Vec::<u8>::with_capacity(BYTES_TO_READ);
    read_exact(stream, &mut file_len, BYTES_TO_READ)?;

    info!("Read file size of: {:?}", &file_len);

    let file_len = BytesMut::from(&file_len[..]);
    let file_len = BytesMutUtils::to_usize(&file_len);

    info!("Expecting file with size of: {} bytes", file_len);

    read_exact(stream, &mut fp, file_len)?;
    info!(
        "File {} received successfully. {} bytes",
        filepath.display(),
        file_len
    );
    fp.flush()?;
    fp.sync_all()?;
    Ok(fp.metadata()?.len())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::NodeTalkClient;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_send_recv_file() {
        for _ in 0..5 {
            // Create some file
            const FILE_CONTENT: &str = "hello world";
            let tempfile = crate::io::TempFile::with_name("test_send_recv_file");
            std::fs::write(&tempfile.fullpath(), FILE_CONTENT).unwrap();

            let server_address = format!("127.0.0.1:{}", portpicker::pick_unused_port().unwrap());
            let server = std::net::TcpListener::bind(server_address.clone()).unwrap();

            let tempfile_clone = tempfile.fullpath().clone();
            let event = Arc::new(Mutex::<()>::default());
            let _guard = event.lock().unwrap();

            let event_clone = event.clone();
            std::thread::spawn(move || {
                let (mut stream, addr) = server.accept().unwrap();
                println!("Successfully got connection from: {:?}", addr);
                crate::replication::socket_set_timeout(&stream).unwrap();
                send_file(
                    &mut stream,
                    &std::path::PathBuf::from(tempfile_clone.as_str()),
                )
                .unwrap();
                // this will wait until the client has ended
                let _g = event_clone.lock().unwrap();
            });

            // let the server start enough time to start
            std::thread::sleep(std::time::Duration::from_millis(50));

            let mut client = NodeTalkClient::default();
            client.connect_timeout(&server_address).unwrap();

            let tempfile = crate::io::TempFile::with_name("test_send_recv_file_out");
            let file_size = NodeTalkClient::recv_file(
                &std::path::PathBuf::from(tempfile.fullpath().as_str()),
                client.stream_mut().unwrap(),
            )
            .unwrap();
            assert_eq!(file_size, FILE_CONTENT.len() as u64);

            // Release the lock, let the server thread to terminate
            drop(_guard);
        }
    }
}
