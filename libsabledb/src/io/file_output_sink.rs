use crate::SableError;
use tokio::io::AsyncReadExt;

pub struct FileResponseSink {
    temp_file: crate::io::TempFile,
    pub fp: tokio::fs::File,
}

impl FileResponseSink {
    pub const BUFFER_SIZE: usize = 4096;
    pub async fn new() -> Result<Self, SableError> {
        let temp_file = crate::io::TempFile::with_name("tmp_sink");
        let fp = tokio::fs::File::create(&temp_file.fullpath()).await?;
        Ok(FileResponseSink { temp_file, fp })
    }

    pub async fn read_all(&mut self) -> Result<bytes::BytesMut, SableError> {
        self.read_all_with_size(Self::BUFFER_SIZE).await
    }

    pub async fn read_all_with_size(&mut self, size: usize) -> Result<bytes::BytesMut, SableError> {
        self.fp.sync_data().await?;
        let mut fp = tokio::fs::File::open(&self.temp_file.fullpath()).await?;

        let mut buffer = bytes::BytesMut::with_capacity(size);
        fp.read_buf(&mut buffer).await?;
        Ok(buffer)
    }
}
