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
        _client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let sub_command = command_arg_at_as_str!(command, 1);
        if sub_command.as_str() == "setinfo" {
            let builder = RespBuilderV2::default();
            builder.ok(response_buffer);
        }
        Ok(())
    }
}
