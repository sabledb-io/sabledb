use crate::{
    bincode_to_bytesmut_or,
    replication::messages::*,
    replication::{BytesReader, BytesWriter, TcpStreamBytesReader, TcpStreamBytesWriter},
    SableError, Server,
};
use num_format::{Locale, ToFormattedString};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

#[cfg(not(test))]
use tracing::{error, info};

#[cfg(test)]
use std::{println as info, println as error};

#[derive(Debug, PartialEq)]
pub enum JoinShardResult {
    /// Successfully joined the shard, returns the node-id
    Ok { shard_name: String, node_id: String },
    /// Failed to join the shard
    Err,
}

/// A client used for communicating with the NodeTalkServer
#[derive(Default)]
#[allow(dead_code)]
pub struct NodeTalkClient {
    stream: Option<TcpStream>,
    request_id: u64,
    remote_addr: String,
}

#[allow(dead_code)]
impl NodeTalkClient {
    /// Connect to NodeServer at a given address - on success, make the socket non blocking
    pub fn connect_with_timeout(&mut self, remote_addr: &str) -> Result<(), SableError> {
        let addr = remote_addr.parse::<SocketAddr>()?;
        // TODO: make the timeout configurable
        let stream = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5))?;
        crate::replication::socket_set_timeout(&stream)?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        self.remote_addr = remote_addr.to_string();
        Ok(())
    }

    /// Send `slot` to connected remote server. After this call, the client is NOT usable
    pub fn send_slot_file(
        &mut self,
        slot: u16,
        filepath: &std::path::Path,
    ) -> Result<(), SableError> {
        let request = NodeTalkRequest::SendingSlotFile {
            common: RequestCommon::new().with_request_id(&mut self.request_id),
            slot,
        };

        // Send the message followed by the content
        let mut buffer = bincode_to_bytesmut_or!(request, Err(SableError::SerialisationError));
        {
            let (mut writer, _) = self.split_stream()?;
            writer.write_message(&mut buffer)?;
        }

        // Send over the file
        let file_len = filepath.metadata()?.len();
        tracing::info!(
            "Sending slot {slot} content to remote: {}. Content size: {} bytes",
            self.remote_addr,
            file_len.to_formatted_string(&Locale::en)
        );
        {
            let Some(stream) = &mut self.stream else {
                return Err(SableError::ConnectionNotOpened);
            };

            if Self::send_file(filepath, stream)? != file_len {
                return Err(SableError::OtherError(
                    "failed to send file to remote".into(),
                ));
            }
        }

        tracing::info!("Waiting for ACK from remote");
        // Wait for confirmation
        let (_, mut reader) = self.split_stream()?;
        match self.read_response(&mut reader)? {
            NodeResponse::Ok(_) => {
                tracing::info!("Received ACK");
                Server::state()
                    .persistent_state()
                    .slots()
                    .set(slot, false)?;
                Server::state().persistent_state().save();
                tracing::info!("Removed slot: {} from this node configuration", slot);
                Ok(())
            }
            NodeResponse::NotOk(resp) => Err(SableError::OtherError(format!(
                "Remote {} did not acknowledged slot content acceptance. {resp}",
                self.remote_addr
            ))),
        }
    }

    pub fn stream(&self) -> Option<&TcpStream> {
        self.stream.as_ref()
    }

    pub fn stream_mut(&mut self) -> Option<&mut TcpStream> {
        self.stream.as_mut()
    }

    /// Request to join the shard
    pub fn join_shard(&self) -> Result<JoinShardResult, SableError> {
        let (mut writer, mut reader) = self.split_stream()?;
        Ok(self.join_shard_internal(&mut reader, &mut writer))
    }

    /// Request to join the shard, on success, return the node-id
    fn join_shard_internal(
        &self,
        reader: &mut impl BytesReader,
        writer: &mut impl BytesWriter,
    ) -> JoinShardResult {
        let join_request = NodeTalkRequest::JoinShard(RequestCommon::new());
        match self.send_receive(reader, writer, join_request) {
            Err(e) => {
                error!("join_shard: error reading replication message. {:?}", e);
                JoinShardResult::Err
            }
            Ok(msg) => {
                match msg {
                    NodeResponse::Ok(common) => {
                        // fall through
                        let shard_name = common.context();
                        info!(
                            "Successfully joined shard: {}. Primary Node ID: {}",
                            shard_name,
                            common.node_id()
                        );
                        JoinShardResult::Ok {
                            shard_name: shard_name.clone(),
                            node_id: common.node_id().to_string(),
                        }
                    }
                    NodeResponse::NotOk(common) => {
                        // the requested sequence was is not acceptable by the server
                        // do a full sync
                        info!("Failed to join the shard! {}", common);
                        JoinShardResult::Err
                    }
                }
            }
        }
    }

    fn read_response(&self, reader: &mut dyn BytesReader) -> Result<NodeResponse, SableError> {
        // Read the request (this is a fixed size request)
        loop {
            let result = match reader.read_message()? {
                None => None,
                Some(bytes) => Some(bincode::deserialize::<NodeResponse>(&bytes)?),
            };
            match result {
                Some(req) => break Ok(req),
                None => {
                    continue;
                }
            };
        }
    }

    /// Efficiently send a file over the network
    pub fn send_file<W>(filepath: &std::path::Path, stream: &mut W) -> Result<u64, SableError>
    where
        W: ?Sized + Write,
    {
        crate::io::send_file(stream, filepath)
    }

    /// Read file content from `stream` and write it into `filepath`
    pub fn recv_file<R>(filepath: &std::path::Path, stream: &mut R) -> Result<u64, SableError>
    where
        R: ?Sized + Read,
    {
        crate::io::recv_file(filepath, stream)
    }

    /// Send `request` and read `response` from the server
    fn send_receive(
        &self,
        reader: &mut impl BytesReader,
        writer: &mut impl BytesWriter,
        requst: NodeTalkRequest,
    ) -> Result<NodeResponse, SableError> {
        let mut buffer = bincode_to_bytesmut_or!(requst, Err(SableError::SerialisationError));
        writer.write_message(&mut buffer)?;
        self.read_response(reader)
    }

    fn split_stream(&self) -> Result<(TcpStreamBytesWriter, TcpStreamBytesReader), SableError> {
        let Some(stream) = &self.stream else {
            return Err(SableError::ConnectionNotOpened);
        };
        Ok((
            TcpStreamBytesWriter::new(stream),
            TcpStreamBytesReader::new(stream),
        ))
    }
}

impl Drop for NodeTalkClient {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}
