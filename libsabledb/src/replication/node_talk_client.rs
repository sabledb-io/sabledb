use crate::{
    bincode_to_bytesmut_or,
    replication::messages::*,
    replication::{BytesReader, BytesWriter, TcpStreamBytesReader, TcpStreamBytesWriter},
    SableError,
};
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
}

#[allow(dead_code)]
impl NodeTalkClient {
    /// Connect to NodeServer at a given address
    pub fn connect(&mut self, primary_address: &str) -> Result<(), SableError> {
        let addr = primary_address.parse::<SocketAddr>()?;
        let stream = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(1))?;
        crate::replication::prepare_std_socket(&stream)?;
        self.stream = Some(stream);
        Ok(())
    }

    pub fn stream(&self) -> Option<&TcpStream> {
        self.stream.as_ref()
    }

    pub fn stream_mut(&mut self) -> Option<&mut TcpStream> {
        self.stream.as_mut()
    }

    /// Request to join the shard
    pub fn join_shard(&self) -> Result<JoinShardResult, SableError> {
        let Some(stream) = &self.stream else {
            return Err(SableError::ConnectionNotOpened);
        };

        let mut reader = TcpStreamBytesReader::new(stream);
        let mut writer = TcpStreamBytesWriter::new(stream);
        Ok(self.join_shard_internal(&mut reader, &mut writer))
    }

    /// Request to join the shard, on success, return the node-id
    fn join_shard_internal(
        &self,
        reader: &mut impl BytesReader,
        writer: &mut impl BytesWriter,
    ) -> JoinShardResult {
        let join_request = ReplicationRequest::JoinShard(RequestCommon::new());
        let mut buffer = bincode_to_bytesmut_or!(join_request, JoinShardResult::Err);

        if let Err(e) = writer.write_message(&mut buffer) {
            error!("Failed to send replication request. {:?}", e);
            return JoinShardResult::Err;
        };

        // We expect now an "Ok" or "NotOk" response
        match self.read_response(reader) {
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
}

impl Drop for NodeTalkClient {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}
