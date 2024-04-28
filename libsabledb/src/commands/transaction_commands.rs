#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::Strings,
    commands::{ClientNextAction, HandleCommandResult, StringCommands},
    io::{FileResponseSink, RespWriter},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    server::SableError,
    server::{Client, ClientState},
    storage::StringsDb,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct TransactionCommands {}

impl TransactionCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::Multi => Self::multi(client_state, command, tx).await,
            RedisCommandName::Discard => Self::discard(client_state, command, tx).await,
            _ => Err(SableError::InvalidArgument(format!(
                "Unexpected command {}",
                command.main_command()
            ))),
        }
    }

    /// Handle `EXEC` command
    pub async fn handle_exec(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 1024, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        // Confirm that we are in the MULTI state
        if !client_state.is_txn_state_multi() {
            resp_writer
                .error_string(Strings::EXEC_WITHOUT_MULTI)
                .await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        // Clear the multi state
        client_state.set_txn_state_multi(false);

        // Step 1:
        // Get a list of slots that we should lock

        // Change the client state into "calculating slots"
        client_state.set_txn_state_calc_slots(true);

        let queued_commands = client_state.take_queued_commands();
        let mut slots = Vec::<u16>::with_capacity(queued_commands.len());

        for cmd in &queued_commands {
            let mut file_output = FileResponseSink::new().await?;
            match Client::handle_non_exec_command(
                client_state.clone(),
                cmd.clone(),
                &mut file_output.fp,
            )
            .await
            {
                Err(SableError::LockCancelledTxnPrep(command_slots)) => {
                    slots.extend_from_slice(&command_slots);
                }
                Err(e) => {
                    // other error occured, propogate it
                    client_state.discard_transaction();
                    return Err(e);
                }
                Ok(ClientNextAction::TerminateConnection) => {
                    client_state.discard_transaction();
                    return Err(SableError::ConnectionClosed);
                }
                _ => {
                    client_state.discard_transaction();
                    let inner_message = file_output.read_all().await?;
                    let mut resp_writer = RespWriter::new(tx, 1024, client_state.clone());
                    resp_writer
                        .error_string(BytesMutUtils::to_string(&inner_message).as_str())
                        .await?;
                    resp_writer.flush().await?;
                    return Ok(HandleCommandResult::ResponseSent);
                }
            }
        }

        // move out of the `calc slots` state
        client_state.set_txn_state_calc_slots(false);

        // Step 2:
        // Lock all the slots that are about to be affected
        let _unused = LockManager::lock_multi_slots_exclusive(slots, client_state.clone())?;

        // Step 3:
        // Execute the commands, buffering the responses
        client_state.set_txn_state_exec(true);

        // Create an output stream
        let mut file_output = FileResponseSink::new().await?;
        for cmd in &queued_commands {
            Client::handle_non_exec_command(client_state.clone(), cmd.clone(), &mut file_output.fp)
                .await?;
        }

        // if we got here, everything was OK -> commit the transaction
        client_state.database().commit()?;

        // Clear the txn state & send back the response
        client_state.set_txn_state_exec(false);

        // write the array length
        let mut resp_writer = RespWriter::new(tx, 4096, client_state.clone());
        resp_writer.add_array_len(queued_commands.len()).await?;

        // now write the responses
        let output = file_output.read_all().await?;
        resp_writer.add_resp_string(&output).await?;
        resp_writer.flush().await?;

        Ok(HandleCommandResult::ResponseSent)
    }

    /// Handle `MULTI` command
    async fn multi(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        // set the client into the "MULTI" state
        client_state.set_txn_state_multi(true);
        resp_writer.ok().await?;
        resp_writer.flush().await?;
        Ok(HandleCommandResult::ResponseSent)
    }

    /// Handle `MULTI` command
    async fn discard(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        // Confirm that we are in the MULTI state
        if !client_state.is_txn_state_multi() {
            resp_writer
                .error_string(Strings::DISCARD_WITHOUT_MULTI)
                .await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        // clear all the transaction details (state + command queue)
        client_state.discard_transaction();

        resp_writer.ok().await?;
        resp_writer.flush().await?;
        Ok(HandleCommandResult::ResponseSent)
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
    use crate::{
        commands::Strings, tests::run_and_return_output, Client, ClientState, ServerState,
    };
    use std::rc::Rc;
    use std::sync::Arc;

    async fn check_command(
        client_state: Rc<ClientState>,
        vec_args: Vec<&'static str>,
        expected_output: RedisObject,
    ) {
        // Start a transaction
        let args: Vec<String> = vec_args.iter().map(|s| s.to_string()).collect();
        let output = run_and_return_output(args, client_state).await.unwrap();
        assert_eq!(output, expected_output);
    }

    #[test]
    fn test_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client2 = Client::new(Arc::<ServerState>::default(), store, None);

            // Start a transaction
            check_command(
                client.inner(),
                vec!["multi"],
                RedisObject::Status("OK".into()),
            )
            .await;

            // No nested MULTI
            check_command(
                client.inner(),
                vec!["multi"],
                RedisObject::Error("command multi can not be used in a MULTI / EXEC block".into()),
            )
            .await;

            // Check that commands marked with "no_transaction" are not accepted
            check_command(
                client.inner(),
                vec!["hgetall"],
                RedisObject::Error(
                    "command hgetall can not be used in a MULTI / EXEC block".into(),
                ),
            )
            .await;

            // Check that commands marked with "no_transaction" are not accepted
            check_command(
                client.inner(),
                vec!["set", "k1", "v1"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // Queue some valid commands
            check_command(
                client.inner(),
                vec!["set", "k2", "v2"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 2 commands in the queue
            assert_eq!(client.inner().commands_queue().len(), 2);

            // Use the second client to confirm that the command was not executed
            check_command(client2.inner(), vec!["get", "k1"], RedisObject::NullString).await;
            check_command(client2.inner(), vec!["get", "k2"], RedisObject::NullString).await;
        });
    }

    #[test]
    fn test_discard() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(
                client.inner(),
                vec!["multi"],
                RedisObject::Status("OK".into()),
            )
            .await;

            // state should be "multi"
            assert!(client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(
                client.inner(),
                vec!["set", "k1", "v1"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                vec!["set", "k2", "v2"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // We expect `hlen` to fail, but this will not break the transaction
            check_command(
                client.inner(),
                vec!["hlen", "k2"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                vec!["set", "k3", "v3"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 4 commands in the queue
            assert_eq!(client.inner().commands_queue().len(), 4);

            check_command(
                client.inner(),
                vec!["discard"],
                RedisObject::Status("OK".into()),
            )
            .await;

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(client.inner(), vec!["get", "k1"], RedisObject::NullString).await;
            check_command(client.inner(), vec!["get", "k2"], RedisObject::NullString).await;
            check_command(client.inner(), vec!["get", "k3"], RedisObject::NullString).await;
            assert_eq!(client.inner().commands_queue().len(), 0);
        });
    }

    #[test]
    fn test_blocking_list_commands_in_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let myclient = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(
                myclient.inner(),
                vec!["multi"],
                RedisObject::Status("OK".into()),
            )
            .await;

            // state should be "multi"
            assert!(myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            check_command(
                myclient.inner(),
                vec!["blpop", "mylist", "100"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                myclient.inner(),
                vec!["brpop", "mylist", "100"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                myclient.inner(),
                vec!["brpoplpush", "mylist", "mylist2", "100"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                myclient.inner(),
                vec!["blmove", "mylist", "mylist2", "LEFT", "LEFT", "100"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                myclient.inner(),
                vec![
                    "blmpop", "100", "2", "mylist2", "mylist3", "LEFT", "COUNT", "1",
                ],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;
            assert_eq!(myclient.inner().commands_queue().len(), 5);

            check_command(
                myclient.inner(),
                vec!["exec"],
                RedisObject::Array(vec![
                    RedisObject::NullArray,
                    RedisObject::NullArray,
                    RedisObject::NullArray,
                    RedisObject::NullArray,
                    RedisObject::NullArray,
                ]),
            )
            .await;
        });
    }

    use crate::utils::RedisObject;

    #[test]
    fn test_exec() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(
                client.inner(),
                vec!["multi"],
                RedisObject::Status("OK".into()),
            )
            .await;

            // state should be "multi"
            assert!(client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(
                client.inner(),
                vec!["set", "k1", "v1"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                vec!["set", "k2", "v2"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // We expect `hlen` to fail, but this will not break the transaction
            check_command(
                client.inner(),
                vec!["hlen", "k2"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                vec!["set", "k3", "v3"],
                RedisObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 4 commands in the queue
            assert_eq!(client.inner().commands_queue().len(), 4);

            check_command(
                client.inner(),
                vec!["exec"],
                RedisObject::Array(vec![
                    RedisObject::Status("OK".into()),
                    RedisObject::Status("OK".into()),
                    RedisObject::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    RedisObject::Status("OK".into()),
                ]),
            )
            .await;

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(
                client.inner(),
                vec!["get", "k1"],
                RedisObject::Str("v1".into()),
            )
            .await;
            check_command(
                client.inner(),
                vec!["get", "k2"],
                RedisObject::Str("v2".into()),
            )
            .await;
            check_command(
                client.inner(),
                vec!["get", "k3"],
                RedisObject::Str("v3".into()),
            )
            .await;
            assert_eq!(client.inner().commands_queue().len(), 0);

            // Run another command, this time we expect it to run without "EXEC"
            check_command(
                client.inner(),
                vec!["set", "no_cached", "value"],
                RedisObject::Status("OK".into()),
            )
            .await;

            // Run another command, this time we expect it to run without "EXEC"
            check_command(
                client.inner(),
                vec!["get", "no_cached"],
                RedisObject::Str("value".into()),
            )
            .await;
        });
    }
}
