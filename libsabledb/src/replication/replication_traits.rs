use crate::io;
use crate::{BytesMutUtils, SableError, U8ArrayReader};
use bytes::BytesMut;
use std::net::TcpStream;

pub trait BytesWriter {
    /// Write a message. A "message" format is always: the length of the message
    /// followed by the message content (the length is in big-endian notation)
    fn write_message(&mut self, message: &mut BytesMut) -> Result<(), SableError>;
}

pub trait BytesReader {
    /// Read a message. A "message" format is always: the length of the message
    /// followed by the message content (the length is in big-endian notation)
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
        let mut num = BytesMutUtils::from_usize(&num);
        io::write_bytes(&mut self.tcp_stream, &mut num)
    }
}

impl<'a> BytesWriter for TcpStreamBytesWriter<'a> {
    fn write_message(&mut self, message: &mut BytesMut) -> Result<(), SableError> {
        self.write_usize(message.len())?;
        io::write_bytes(&mut self.tcp_stream, message)?;
        Ok(())
    }
}

/// TCP based reader
pub struct TcpStreamBytesReader<'a> {
    tcp_stream: &'a TcpStream,
    bytes_read: BytesMut,
}

impl<'a> TcpStreamBytesReader<'a> {
    pub fn new(tcp_stream: &TcpStream) -> TcpStreamBytesReader {
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
        match io::read_bytes(&mut self.tcp_stream, Self::MAX_BUFFER_SIZE)? {
            Some(data) => {
                self.bytes_read.extend_from_slice(&data);
                Ok(Some(data.len()))
            }
            None => Ok(None),
        }
    }
}

macro_rules! try_socket_read {
    ($reader:expr) => {
        // try read some bytes
        match $reader.read_some()? {
            None => return Ok(None),
            Some(count) => count,
        }
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
            // This can happen if the buffer does not have enough bytes to parse usize
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
