#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::Strings,
    commands::{HandleCommandResult, StringCommands},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    server::ClientState,
    storage::StringsDb,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct ServerCommands {}

impl ServerCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            RedisCommandName::ReplicaOf | RedisCommandName::SlaveOf => {
                Self::replica_of(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Command => {
                Self::command(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non server command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Generate output for the `command` command
    async fn command(
        _client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(4096);
        if !command.expect_args_count(1) {
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                command.main_command()
            );
            builder.add_bulk_string(&mut buffer, errmsg.as_bytes());
            tx.write_all(buffer.as_ref()).await?;
            return Ok(());
        }

        let manager = crate::commands::commands_manager();
        if let Some(sub_command) = command.arg(1) {
            let sub_command = BytesMutUtils::to_string(sub_command).to_lowercase();
            if sub_command == "docs" {
                tx.write_all(&manager.cmmand_docs_output()).await?;
            } else {
                // send an supported response
                builder.error_string(
                    &mut buffer,
                    format!("ERR unknown subcommand '{}'", sub_command).as_str(),
                );
                tx.write_all(&buffer).await?;
            }
        } else {
            tx.write_all(&manager.cmmand_output()).await?;
        }
        Ok(())
    }

    async fn replica_of(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
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
                    builder.error_string(response_buffer, Strings::INVALID_PRIMARY_PORT);
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

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod test {
    use super::*;
    use crate::{Client, ServerState};

    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn test_command() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            // Send a `command` command
            let mut sink = crate::tests::ResponseSink::with_name("test_commands").await;
            let cmd = Rc::new(RedisCommand::for_test(["command"].to_vec()));
            Client::handle_command(client.inner(), cmd, &mut sink.fp)
                .await
                .unwrap();

            // the output might be large, use a large buffer
            let raw_response = sink.read_all_with_size(128 << 10).await;
            let manager = crate::commands::commands_manager();

            // the response should starts with "*<commands-count>\r\n"
            let prefix = format!("*{}\r\n", manager.all_commands().len());
            assert!(raw_response.starts_with(&prefix));

            // Check that all the commands exist in the output
            for cmd_name in manager.all_commands().keys() {
                let cmd_bulk_string = format!("${}\r\n{}\r\n", cmd_name.len(), cmd_name);
                assert!(raw_response.contains(&cmd_bulk_string));
            }
        });
    }

    #[test]
    fn test_command_docs() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            // Send a `command` command
            let mut sink = crate::tests::ResponseSink::with_name("test_commands").await;
            let cmd = Rc::new(RedisCommand::for_test(["command", "docs"].to_vec()));
            Client::handle_command(client.inner(), cmd, &mut sink.fp)
                .await
                .unwrap();

            // the output might be large, use a large buffer
            let raw_response = sink.read_all_with_size(128 << 10).await;
            let manager = crate::commands::commands_manager();

            // the response should starts with "*<2 x commands-count>\r\n"
            let prefix = format!("*{}\r\n", manager.all_commands().len() * 2);
            assert!(raw_response.starts_with(&prefix));

            // Check that all the commands exist in the output
            for cmd_name in manager.all_commands().keys() {
                let cmd_bulk_string = format!("${}\r\n{}\r\n", cmd_name.len(), cmd_name);
                assert!(raw_response.contains(&cmd_bulk_string));
            }
        });
    }
}
