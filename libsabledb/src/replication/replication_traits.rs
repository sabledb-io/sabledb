use crate::{BytesMutUtils, SableError, U8ArrayReader};
use bytes::BytesMut;
use std::io::{Read, Write};
use std::net::TcpStream;

pub trait BytesWriter {
    /// Write a message. A "message" format is always: the length of the message
    /// followed by the message content (the length is in big-endia notation)
    fn write_message(&mut self, message: &mut BytesMut) -> Result<(), SableError>;
}

pub trait BytesReader {
    /// Read a message. A "message" format is always: the length of the message
    /// followed by the message content (the length is in big-endia notation)
    fn read_message(&mut self) -> Result<Option<BytesMut>, SableError>;
}

/// TCP based writer
pub struct TcpStreamBytesWriter<'a> {
    tcp_stream: &'a TcpStream,
}

impl<'a> TcpStreamBytesWriter<'a> {
    pub fn new(tcp_stream: &'a TcpStream) -> Self {
        TcpStreamBytesWriter { tcp_stream }
    }
}

impl<'a> TcpStreamBytesWriter<'a> {
    fn write_usize(&mut self, num: usize) -> Result<(), SableError> {
        let num = BytesMutUtils::from_usize(&num);
        self.tcp_stream.write_all(&num)?;
        Ok(())
    }
}

impl<'a> BytesWriter for TcpStreamBytesWriter<'a> {
    fn write_message(&mut self, message: &mut BytesMut) -> Result<(), SableError> {
        self.write_usize(message.len())?;
        match self.tcp_stream.write_all(message) {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                // translate io::ErrorKind::BrokenPipe -> SableError::BrokenPipe
                return Err(SableError::BrokenPipe);
            }
            Err(e) => return Err(SableError::StdIoError(e)),
            Ok(_) => {}
        }
        tracing::trace!("Successfully wrote message of {} bytes", message.len());
        Ok(())
    }
}

/// TCP based reader
pub struct TcpStreamBytesReader<'a> {
    tcp_stream: &'a TcpStream,
    bytes_read: BytesMut,
}

impl<'a> TcpStreamBytesReader<'a> {
    pub fn new(tcp_stream: &'a TcpStream) -> TcpStreamBytesReader {
        TcpStreamBytesReader {
            tcp_stream,
            bytes_read: BytesMut::new(),
        }
    }
}

impl<'a> TcpStreamBytesReader<'a> {
    // TODO: MAX_BUFFER_SIZE should be configurable
    const MAX_BUFFER_SIZE: usize = 10 << 20; // 10MB
    const LEN_SIZE: usize = std::mem::size_of::<usize>();

    /// Pull some bytes from the socket
    fn read_some(&mut self) -> Result<Option<usize>, SableError> {
        let mut buffer = vec![0u8; Self::MAX_BUFFER_SIZE];
        let result = self.tcp_stream.read(&mut buffer);
        match result {
            Ok(0usize) => Err(SableError::ConnectionClosed),
            Ok(count) => {
                self.bytes_read.extend_from_slice(&buffer[..count]);
                Ok(Some(count))
            }
            Err(e)
                // On Windows, TcpStream returns `TimedOut`
                // while on Linux, it would typically return
                // `WouldBlock`
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                // Timeout occured, not an error
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
}

macro_rules! try_socket_read {
    ($reader:expr) => {
        // try read some bytes
        tracing::trace!("Reading some bytes from the socket...");
        let bytes_read = match $reader.read_some()? {
            None => return Ok(None),
            Some(count) => count,
        };
        tracing::trace!(
            "Read: {} bytes from the socket. Internal buffer length: {}",
            bytes_read,
            $reader.bytes_read.len()
        );
    };
}

impl<'a> BytesReader for TcpStreamBytesReader<'a> {
    fn read_message(&mut self) -> Result<Option<BytesMut>, SableError> {
        // read the length
        if self.bytes_read.len() < Self::LEN_SIZE {
            // try read some bytes
            try_socket_read!(self);
        }

        let mut bytes_reader = U8ArrayReader::with_buffer(&self.bytes_read);
        let Some(count) = bytes_reader.read_usize() else {
            // None can happen if the buffer does not have enough bytes to parse usize
            tracing::trace!("Don't enough bytes to determine the message length");
            return Ok(None);
        };

        tracing::trace!("Expecting message of size: {} bytes", count);

        // Do we have the complete message in the buffer already?
        if self.bytes_read.len() >= (count + Self::LEN_SIZE) {
            tracing::trace!(
                "Internal buffer contains the complete message - no need to read from the socket"
            );
            // remove the length
            let _ = self.bytes_read.split_to(Self::LEN_SIZE);
            // and return the message bytes
            let message = self.bytes_read.split_to(count);
            return Ok(Some(message));
        }

        try_socket_read!(self);

        if self.bytes_read.len() >= (count + Self::LEN_SIZE) {
            tracing::trace!(
                "After reading some more bytes from the socket, internal buffer contains the complete message"
            );
            // remove the length
            let _ = self.bytes_read.split_to(Self::LEN_SIZE);
            // and return the message bytes
            let message = self.bytes_read.split_to(count);
            return Ok(Some(message));
        }
        Ok(None)
    }
}
