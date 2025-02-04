use crate::io;
use crate::{BytesMutUtils, SableError};
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

impl TcpStreamBytesWriter<'_> {
    fn write_usize(&mut self, num: usize) -> Result<(), SableError> {
        let mut num = BytesMutUtils::from_usize(&num);
        io::write_bytes(&mut self.tcp_stream, &mut num)
    }
}

impl BytesWriter for TcpStreamBytesWriter<'_> {
    fn write_message(&mut self, message: &mut BytesMut) -> Result<(), SableError> {
        self.write_usize(message.len())?;
        io::write_bytes(&mut self.tcp_stream, message)?;
        Ok(())
    }
}

/// TCP based reader
pub struct TcpStreamBytesReader<'a> {
    tcp_stream: &'a TcpStream,
    bytes_left: usize,
    message_data: BytesMut,
}

impl TcpStreamBytesReader<'_> {
    const LEN_SIZE: usize = std::mem::size_of::<usize>();
    pub fn new(tcp_stream: &TcpStream) -> TcpStreamBytesReader {
        TcpStreamBytesReader {
            tcp_stream,
            bytes_left: 0,
            message_data: BytesMut::default(),
        }
    }
}

impl BytesReader for TcpStreamBytesReader<'_> {
    fn read_message(&mut self) -> Result<Option<BytesMut>, SableError> {
        if self.bytes_left == 0 {
            // new message, read the length
            if io::can_read_at_least(self.tcp_stream, Self::LEN_SIZE)? {
                // pull them out of the socket buffer
                let mut message_len_buf = Vec::<u8>::with_capacity(Self::LEN_SIZE);
                io::read_exact(&mut self.tcp_stream, &mut message_len_buf, Self::LEN_SIZE)?;

                let message_len = BytesMut::from(&message_len_buf[..]);
                self.bytes_left = BytesMutUtils::to_usize(&message_len);
            } else {
                return Ok(None);
            }
        }

        let Some(data) = io::read_bytes(&mut self.tcp_stream, self.bytes_left)? else {
            return Ok(None);
        };

        self.message_data.extend_from_slice(&data);
        self.bytes_left = self.bytes_left.saturating_sub(data.len());

        if self.bytes_left == 0 {
            // a complete message
            let mut msg = BytesMut::default();
            std::mem::swap(&mut self.message_data, &mut msg);
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }
}
