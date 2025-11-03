#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::Strings,
    commands::{HandleCommandResult, StringCommands},
    metadata::{CommonValueMetadata, KeyType},
    parse_string_to_number,
    replication::{ClusterManager, NodeBuilder, NodeTalkClient},
    server::ClientState,
    server::SlotFileExporter,
    storage::StringsDb,
    utils::SLOT_SIZE,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RespBuilderV2, SableError, Server,
    Slot, StorageAdapter, StringUtils, Telemetry, TimeUtils, U8ArrayBuilder, ValkeyCommand,
    ValkeyCommandName,
};
use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct ClusterCommands {}

impl ClusterCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::Cluster => {
                Self::cluster(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non cluster command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Handle "CLUSTER <SUBCOMMAND>" here
    async fn cluster(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let sub_command = String::from_utf8_lossy(command_arg_at!(command, 1)).to_ascii_uppercase();

        match sub_command.as_str() {
            "NODES" => Self::cluster_nodes(client_state, command, response_buffer).await,
            "MYID" => Self::cluster_myid(client_state, command, response_buffer).await,
            _ => {
                let builder = RespBuilderV2::default();
                builder_return_syntax_error!(builder, response_buffer);
            }
        }
    }

    /// Return detailed information about the cluster nodes. The output of the command is just a space-separated CSV
    /// string, where each line represents a node in the cluster
    ///
    /// ```text
    /// <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    /// ```
    async fn cluster_nodes(
        client_state: Rc<ClientState>,
        _command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let lines = client_state
            .server_inner_state()
            .persistent_state()
            .cluster_nodes_lines();

        let output = lines
            .iter()
            .map(|node_line| {
                node_line
                    .iter()
                    .map(|(_, value)| value.to_string())
                    .collect::<Vec<String>>()
                    .join(" ")
            })
            .collect::<Vec<String>>();

        let builder = RespBuilderV2::default();
        builder.add_array_len(response_buffer, output.len());
        for line in output {
            builder.add_bulk_string(response_buffer, line.as_bytes());
        }
        Ok(())
    }

    /// The CLUSTER MYID command returns the unique, auto-generated identifier that is associated with the connected
    /// cluster node.
    async fn cluster_myid(
        client_state: Rc<ClientState>,
        _command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let myid = client_state.server_inner_state().persistent_state().id();
        builder.add_bulk_string(response_buffer, myid.as_bytes());
        Ok(())
    }
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod test {}
