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

pub struct ClientCommands {}

impl ClientCommands {
    pub fn handle_command(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::Client => {
                Self::client(client_state, command, response_buffer)?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non client command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::Completed)
    }

    /// Execute the `client` command
    fn client(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let sub_command = command_arg_at_as_str!(command, 1);
        let builder = RespBuilderV2::default();
        match sub_command.as_str() {
            "setinfo" => {
                builder.ok(response_buffer);
            }
            "id" => {
                builder.number::<u128>(response_buffer, client_state.client_id, false);
            }
            _ => {
                let msg = format!("command `client {}` is not supported", sub_command.as_str());
                builder.error_string(response_buffer, msg.as_str());
            }
        }
        Ok(())
    }
}
