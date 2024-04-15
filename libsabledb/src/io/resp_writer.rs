use crate::{RespBuilderV2, SableError};
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

pub struct RespWriter<'a, W> {
    tx: &'a mut W,
    buffer: BytesMut,
    resp_builder: RespBuilderV2,
    flush_threshold: usize,
}

#[allow(dead_code)]
impl<'a, W> RespWriter<'a, W>
where
    W: AsyncWriteExt + std::marker::Unpin,
{
    pub fn new(tx: &'a mut W, capacity: usize, flush_threshold: usize) -> Self {
        Self {
            tx,
            buffer: BytesMut::with_capacity(capacity),
            resp_builder: RespBuilderV2::default(),
            flush_threshold,
        }
    }

    /// RESP API
    pub async fn ok(&mut self) -> Result<(), SableError> {
        self.resp_builder.ok(&mut self.buffer);
        self.flush_if_needed().await
    }

    pub async fn error_string(&mut self, msg: &str) -> Result<(), SableError> {
        self.resp_builder.error_string(&mut self.buffer, msg);
        self.flush_if_needed().await
    }

    pub async fn empty_array(&mut self) -> Result<(), SableError> {
        self.resp_builder.empty_array(&mut self.buffer);
        self.flush_if_needed().await
    }

    pub async fn add_empty_array(&mut self) -> Result<(), SableError> {
        self.resp_builder.add_empty_array(&mut self.buffer);
        self.flush_if_needed().await
    }

    pub async fn add_null_string(&mut self) -> Result<(), SableError> {
        self.resp_builder.add_null_string(&mut self.buffer);
        self.flush_if_needed().await
    }

    pub async fn add_array_len(&mut self, len: usize) -> Result<(), SableError> {
        self.resp_builder.add_array_len(&mut self.buffer, len);
        self.flush_if_needed().await
    }

    pub async fn add_bulk_string(&mut self, s: &[u8]) -> Result<(), SableError> {
        self.resp_builder
            .add_bulk_string_u8_arr(&mut self.buffer, s);
        self.flush_if_needed().await
    }

    pub async fn add_number<NumberT: std::fmt::Display>(
        &mut self,
        num: NumberT,
    ) -> Result<(), SableError> {
        self.resp_builder
            .add_number::<NumberT>(&mut self.buffer, num, false);
        self.flush_if_needed().await
    }

    pub async fn add_float<NumberT: std::fmt::Display>(
        &mut self,
        num: NumberT,
    ) -> Result<(), SableError> {
        self.resp_builder
            .add_number::<NumberT>(&mut self.buffer, num, true);
        self.flush_if_needed().await
    }

    /// Unconditionally flush the buffer
    pub async fn flush(&mut self) -> Result<(), SableError> {
        if !self.buffer.is_empty() {
            self.tx.write_all(&self.buffer).await?;
            self.buffer.clear();
        }
        Ok(())
    }

    //---------------------------------
    // Private methods
    //---------------------------------

    /// Write the content to the stream
    async fn flush_if_needed(&mut self) -> Result<(), SableError> {
        if self.buffer.len() > self.flush_threshold {
            self.tx.write_all(&self.buffer).await?;
            self.buffer.clear();
        }
        Ok(())
    }
}
