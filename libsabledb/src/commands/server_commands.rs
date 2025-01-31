#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::Strings,
    commands::{HandleCommandResult, StringCommands},
    metadata::{CommonValueMetadata, KeyType},
    parse_string_to_number,
    server::ClientState,
    storage::StringsDb,
    utils::SLOT_SIZE,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RespBuilderV2, SableError, Slot,
    StorageAdapter, StringUtils, Telemetry, TimeUtils, U8ArrayBuilder, ValkeyCommand,
    ValkeyCommandName,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct ServerCommands {}

impl ServerCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::ReplicaOf | ValkeyCommandName::SlaveOf => {
                Self::replica_of(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Command => {
                Self::command(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            ValkeyCommandName::FlushDb => {
                Self::flushdb(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::FlushAll => {
                Self::flushall(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::DbSize => {
                Self::dbsize(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Slot => {
                Self::slot(client_state, command, &mut response_buffer).await?;
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
        command: Rc<ValkeyCommand>,
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
        command: Rc<ValkeyCommand>,
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

    /// `FLUSHALL [ASYNC | SYNC]`
    /// Delete all the keys of all the existing databases, not just the currently selected one. This command never fails
    /// `SableDB` always uses the `SYNC` method
    async fn flushall(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        client_state.database().delete_range(None, None)?;
        let builder = RespBuilderV2::default();
        builder.ok(response_buffer);
        Ok(())
    }

    /// `DBSIZE`
    /// Return the number of keys in the currently-selected database
    async fn dbsize(
        client_state: Rc<ClientState>,
        _command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        builder.number_usize(
            response_buffer,
            Telemetry::db_key_count(client_state.database_id()),
        );
        Ok(())
    }

    async fn flushdb(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let db_id = client_state.database_id();

        // In order to delete all the items owned by a given database, we only need to build the prefix:
        // [ 1 | db_id ]
        //   ^    ^
        //   |    |__ The database ID
        //   |
        //   |____ Primary key type: always "1"
        // Complex items will be deleted by the evictor thread
        let mut start_key =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u16>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut start_key);
        builder.write_key_type(KeyType::PrimaryKey);
        builder.write_u16(db_id);

        // Now build the end key
        let mut end_key =
            BytesMut::with_capacity(std::mem::size_of::<u8>() + std::mem::size_of::<u16>());
        let mut builder = U8ArrayBuilder::with_buffer(&mut end_key);
        builder.write_key_type(KeyType::PrimaryKey);
        builder.write_u16(db_id.saturating_add(1));

        // Delete the database records
        client_state
            .database()
            .delete_range(Some(&start_key), Some(&end_key))?;
        let builder = RespBuilderV2::default();
        builder.ok(response_buffer);
        Ok(())
    }

    /// `SLOT`
    /// Slot management command.
    /// - `SLOT COUNT <NUMBER>` return the number of items are stored for this slot
    /// - `SLOT SENDTO <NUMBER> <IP> <PORT>` - move slot ownership to node at a given address
    async fn slot(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // Min of 3 arguments
        check_args_count!(command, 2, response_buffer);
        let sub_command = String::from_utf8_lossy(command_arg_at!(command, 1)).to_ascii_uppercase();

        match sub_command.as_str() {
            "COUNT" => Self::slot_count(client_state, command, response_buffer).await,
            "SENDTO" => Self::slot_sendto(client_state, command, response_buffer).await,
            _ => {
                let builder = RespBuilderV2::default();
                builder_return_syntax_error!(builder, response_buffer);
            }
        }
    }

    // `SLOT COUNT <NUMBER>`
    async fn slot_count(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let slot_number = command_arg_at!(command, 2);

        // Make sure that slot passed is a u16
        let builder = RespBuilderV2::default();
        let Some(slot_number) = BytesMutUtils::parse::<u16>(slot_number) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        // And it is in the valid range [0..SLOT_SIZE)
        if slot_number >= SLOT_SIZE {
            builder_return_value_not_int!(builder, response_buffer);
        }
        let slot = Slot::with_slot(slot_number);
        let items_count = slot.count(client_state).await?;
        builder.number_u64(response_buffer, items_count);
        Ok(())
    }

    // `SLOT SENDTO <NUMBER> <IP> <PORT>`
    async fn slot_sendto(
        _client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 5, response_buffer);
        let builder = RespBuilderV2::default();
        builder.error_string(response_buffer, Strings::SYNTAX_ERROR);
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
    use crate::{commands::ClientNextAction, Client, ServerState};
    use test_case::test_case;

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
            let cmd = Rc::new(ValkeyCommand::for_test(["command"].to_vec()));
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
            let cmd = Rc::new(ValkeyCommand::for_test(["command", "docs"].to_vec()));
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

    #[test_case(vec![
        ("select 0", "+OK\r\n"),
        ("hset myhash_0 a 1 b 2 c 3", ":3\r\n"),
        ("hset heroes_0 orisa tank rein tank tracer dps", ":3\r\n"),
        ("select 1", "+OK\r\n"),
        ("hset myhash_1 a 1 b 2 c 3", ":3\r\n"),
        ("hset heroes_1 orisa tank rein tank tracer dps", ":3\r\n"),
        ("flushall async", "+OK\r\n"),
        ("select 0", "+OK\r\n"),
        ("hgetall myhash_0", "*0\r\n"),
        ("hgetall heroes_0", "*0\r\n"),
        ("select 1", "+OK\r\n"),
        ("hgetall myhash_1", "*0\r\n"),
        ("hgetall heroes_1", "*0\r\n"),
    ]; "test_flushall")]
    #[test_case(vec![
        ("select 0", "+OK\r\n"),
        ("hset myhash_0 a 1 b 2 c 3", ":3\r\n"),
        ("hset heroes_0 orisa tank rein tank tracer dps", ":3\r\n"),
        ("select 1", "+OK\r\n"),
        ("hset myhash_1 a 1 b 2 c 3", ":3\r\n"),
        ("hset heroes_1 rein tank orisa tank tracer dps", ":3\r\n"),
        ("select 0", "+OK\r\n"),
        ("flushdb async", "+OK\r\n"),
        ("hgetall myhash_0", "*0\r\n"),
        ("hgetall heroes_0", "*0\r\n"),
        ("select 1", "+OK\r\n"),
        ("hgetall myhash_1", "*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"),
        ("hgetall heroes_1", "*6\r\n$5\r\norisa\r\n$4\r\ntank\r\n$4\r\nrein\r\n$4\r\ntank\r\n$6\r\ntracer\r\n$3\r\ndps\r\n"),
    ]; "test_flushdb")]
    // key1=9189
    // key2=4998
    // key3=935
    // key4=13120
    #[test_case(vec![
        ("set key1 v", "+OK\r\n"),
        ("set key2 v", "+OK\r\n"),
        ("set key3 v", "+OK\r\n"),
        ("set key4 v", "+OK\r\n"),
        ("slot", "-ERR wrong number of arguments for 'slot' command\r\n"),
        ("slot COUNT abc", "-ERR value is not an integer or out of range\r\n"),
        ("slot 100 abc", "-ERR syntax error\r\n"),
        ("slot COUNT 9189", ":1\r\n"),
        ("slot COUNT 4998", ":1\r\n"),
        ("slot COUNT 935", ":1\r\n"),
        ("slot COUNT 13120", ":1\r\n"),
        ("slot COUNT 0", ":0\r\n"),
        ("slot COUNT 16383", ":0\r\n"),
        ("slot COUNT 16384", "-ERR value is not an integer or out of range\r\n"),
    ]; "test_slot")]
    fn test_server_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args {
                let mut sink = crate::io::FileResponseSink::new().await.unwrap();
                let args = args.split(' ').collect();
                let cmd = Rc::new(ValkeyCommand::for_test(args));
                match Client::handle_command(client.inner(), cmd, &mut sink.fp)
                    .await
                    .unwrap()
                {
                    ClientNextAction::NoAction => {
                        assert_eq!(
                            sink.read_all_as_string().await.unwrap().as_str(),
                            expected_value
                        );
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }
}
