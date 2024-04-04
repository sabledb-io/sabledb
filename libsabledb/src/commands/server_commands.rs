#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type,
    client::ClientState,
    command_arg_at,
    commands::ErrorStrings,
    commands::{HandleCommandResult, StringCommands},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    storage::StringsDb,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use bytes::BytesMut;
use std::rc::Rc;

pub struct ServerCommands {}

impl ServerCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::ReplicaOf | RedisCommandName::SlaveOf => {
                Self::replica_of(client_state, command, response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non server command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::Completed)
    }

    /// REPLICAOF <IP PORT | NO ONE>
    /// SLAVEOF <IP PORT | NO ONE>
    async fn replica_of(
        client_state: Rc<ClientState>,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        // Actions to perform here:
        // If args are: "NO ONE":
        // - If we are already replica:
        //      - notify the replication thread to disconnect from the master
        //      - Change the server state to primary from replica
        //
        // - If we are primary:
        //      - Do nothing
        //
        // If args are: <IP> <PORT>:
        // - Notify the replication thread to:
        //      - Disconnect from the current primary (if any)
        //      - Connect to the new primary
        //  - Change the server state to "replica"
        let first_arg = command_arg_at_as_str!(command, 1);
        let second_arg = command_arg_at_as_str!(command, 2);
        let builder = RespBuilderV2::default();
        match (first_arg.as_str(), second_arg.as_str()) {
            ("no", "one") => {
                client_state
                    .server_inner_state()
                    .switch_role_to_primary()
                    .await?;
            }
            (_, _) => {
                let Ok(port) = second_arg.parse::<u16>() else {
                    builder.error_string(response_buffer, ErrorStrings::INVALID_PRIMARY_PORT);
                    return Ok(());
                };
                tracing::info!("Connecting to primary at: {}:{}", first_arg, port);
                client_state
                    .server_inner_state()
                    .connect_to_primary(first_arg, port)
                    .await?;
            }
        }

        builder.ok(response_buffer);
        Ok(())
    }
}
