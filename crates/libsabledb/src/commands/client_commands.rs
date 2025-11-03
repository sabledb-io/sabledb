#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::Strings,
    commands::{HandleCommandResult, StringCommands},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    server::BroadcastMessageType,
    server::ClientState,
    server::SableError,
    storage::StringsDb,
    utils::RespBuilderV2,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, StorageAdapter, StringUtils,
    Telemetry, TimeUtils, ValkeyCommand, ValkeyCommandName,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct ClientCommands {}

impl ClientCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::Client => {
                Self::client(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Select => {
                Self::select(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non client command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Execute the `client` command
    async fn client(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let sub_command = command_arg_at_as_str!(command, 1);
        let builder = RespBuilderV2::default();
        match sub_command.as_str() {
            "setinfo" => {
                // we now expect 4 arguments command
                check_args_count!(command, 4, response_buffer);
                let attribute_name = command_arg_at_as_str!(command, 2);
                let attribute_val = command_arg_at_as_str!(command, 3);
                match attribute_name.as_str() {
                    "lib-name" | "lib-ver" => {}
                    other => {
                        builder.error_string(
                            response_buffer,
                            &format!("ERR Unrecognized option '{}'", other),
                        );
                        return Ok(());
                    }
                }

                client_state.set_attribute(&attribute_name, &attribute_val);
                builder.ok(response_buffer);
            }
            "id" => {
                builder.number::<u128>(response_buffer, client_state.id(), false);
            }
            "kill" => {
                check_args_count!(command, 4, response_buffer);
                let filter = command_arg_at_as_str!(command, 2);
                match filter.as_str() {
                    "id" => {
                        // CLIENT KILL ID client-id
                        let Ok(client_id) = command_arg_at_as_str!(command, 3).parse::<u128>()
                        else {
                            builder.error_string(
                                response_buffer,
                                "ERR client-id should be greater than 0",
                            );
                            return Ok(());
                        };
                        client_state
                            .server_inner_state()
                            .terminate_client(client_id)
                            .await?;
                        builder.ok(response_buffer);
                    }
                    other => {
                        let msg = format!("command `client kill {}` is not supported", other);
                        builder.error_string(response_buffer, msg.as_str());
                    }
                }
            }
            _ => {
                let msg = format!("command `client {}` is not supported", sub_command.as_str());
                builder.error_string(response_buffer, msg.as_str());
            }
        }
        Ok(())
    }

    /// Select the Valkey logical database having the specified zero-based numeric index.
    /// New connections always use the database 0.
    async fn select(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let db_index = command_arg_at_as_str!(command, 1);
        let builder = RespBuilderV2::default();
        let Ok(db_index) = db_index.parse::<u16>() else {
            // parsing failed
            builder.error_string(
                response_buffer,
                "ERR value is not an integer or out of range",
            );
            return Ok(());
        };
        client_state.set_database_id(db_index);
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
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::{commands::ClientNextAction, test_assert, Client, ServerState, Telemetry};
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(vec![
        (vec!["select", "abc"], "-ERR value is not an integer or out of range\r\n"),
        (vec!["select", "-1"], "-ERR value is not an integer or out of range\r\n"),
        (vec!["select", "67000"], "-ERR value is not an integer or out of range\r\n"),
        (vec!["select", "1"], "+OK\r\n"),
        (vec!["set", "key", "value_1"], "+OK\r\n"),
        (vec!["select", "2"], "+OK\r\n"),
        (vec!["set", "key", "value_2"], "+OK\r\n"),
        (vec!["select", "0"], "+OK\r\n"),
        (vec!["get", "key"], "$-1\r\n"),
        (vec!["select", "3"], "+OK\r\n"),
        (vec!["get", "key"], "$-1\r\n"),
        (vec!["select", "1"], "+OK\r\n"),
        (vec!["get", "key"], "$7\r\nvalue_1\r\n"),
        (vec!["select", "2"], "+OK\r\n"),
        (vec!["get", "key"], "$7\r\nvalue_2\r\n"),
        ], "select"; "select")]
    #[test_case(vec![
        (vec!["client", "setinfo", "key", "value"], "-ERR Unrecognized option 'key'\r\n"),
        (vec!["client", "setinfo", "lib-ver", "v0.0.1"], "+OK\r\n"),
        (vec!["client", "setinfo", "lib-name", "sabledb-lib"], "+OK\r\n"),
        ], "client_setinfo"; "client_setinfo")]
    fn test_client_commands(
        args_vec: Vec<(Vec<&'static str>, &'static str)>,
        test_name: &str,
    ) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args_vec {
                let mut sink = crate::tests::ResponseSink::with_name(test_name).await;
                let cmd = Rc::new(ValkeyCommand::for_test(args));
                match Client::handle_command(client.inner(), cmd, &mut sink.fp)
                    .await
                    .unwrap()
                {
                    ClientNextAction::NoAction => {
                        assert_eq!(sink.read_all().await.as_str(), expected_value);
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }

    #[test]
    fn test_client_kill() -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();

            let client1 = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client2 = Client::new(Arc::<ServerState>::default(), store, None);

            let client1_id = format!("{}", client1.inner().id());
            let kill_command = Rc::new(
                ValkeyCommand::new(vec![
                    BytesMut::from("client"),
                    BytesMut::from("kill"),
                    BytesMut::from("id"),
                    BytesMutUtils::from_string(client1_id.as_str()),
                ])
                .unwrap(),
            );

            // Kill client 1
            let mut sink = crate::tests::ResponseSink::with_name("test_client_kill").await;
            match Client::handle_command(client2.inner(), kill_command, &mut sink.fp)
                .await
                .unwrap()
            {
                ClientNextAction::NoAction => {
                    assert_eq!(sink.read_all().await.as_str(), "+OK\r\n");
                }
                other => {
                    panic!("Did not expect this result! {:?}", other)
                }
            }

            // Try to use client 1
            let some_command = Rc::new(ValkeyCommand::for_test(vec!["set", "some", "value"]));
            let mut sink = crate::tests::ResponseSink::with_name("test_client_kill").await;
            match Client::handle_command(client1.inner(), some_command, &mut sink.fp)
                .await
                .unwrap()
            {
                ClientNextAction::TerminateConnection => {
                    assert_eq!(
                        sink.read_all().await.as_str(),
                        format!("-{}\r\n", Strings::SERVER_CLOSED_CONNECTION).as_str()
                    );
                }
                other => {
                    panic!("Did not expect this result! {:?}", other)
                }
            }
        });
        Ok(())
    }
}
