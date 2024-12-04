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
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RespBuilderV2, StorageAdapter,
    StringUtils, Telemetry, TimeUtils, ValkeyCommand, ValkeyCommandName,
};

use std::rc::Rc;
use tokio::io::AsyncWriteExt;

/// A helper class for discarding a transaction when leaving the scope
pub struct ScopedTranscation {
    client_state: Rc<ClientState>,
}

impl ScopedTranscation {
    pub fn new(client_state: Rc<ClientState>) -> Self {
        ScopedTranscation { client_state }
    }
}

impl Drop for ScopedTranscation {
    fn drop(&mut self) {
        self.client_state.discard_transaction();
    }
}

pub struct TransactionCommands {}

impl TransactionCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            ValkeyCommandName::Multi => Self::multi(client_state, command, tx).await,
            ValkeyCommandName::Discard => Self::discard(client_state, command, tx).await,
            ValkeyCommandName::Watch => Self::watch(client_state, command, tx).await,
            ValkeyCommandName::Unwatch => Self::unwatch(client_state, command, tx).await,
            _ => Err(SableError::InvalidArgument(format!(
                "Unexpected command {}",
                command.main_command()
            ))),
        }
    }

    /// Handle `EXEC` command
    pub async fn handle_exec(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 1024, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        // Make sure that when we leave this scope, the transaction for this client is discarded
        let _txn_guard = ScopedTranscation::new(client_state.clone());

        // Confirm that we are in the MULTI state
        if !client_state.is_txn_state_multi() {
            resp_writer
                .error_string(Strings::EXEC_WITHOUT_MULTI)
                .await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        // can this clien proceed with committing the txn?
        // (this can happen if a watched key was modified)
        if !client_state.can_commit_txn() {
            resp_writer.null_array().await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        // Clear the multi state
        client_state.set_txn_state_multi(false);

        // Step 1:
        // Get a list of slots that we should lock

        // Change the client state into "calculating slots"
        client_state.set_txn_state_calc_slots(true);

        let queued_commands = client_state.txn_commands_vec_cloned();
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
                    // other error occurred, propagate it
                    return Err(e);
                }
                Ok(ClientNextAction::TerminateConnection) => {
                    return Err(SableError::ConnectionClosed);
                }
                _ => {
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
        let _unused = LockManager::lock_multi_slots_exclusive(slots, client_state.clone()).await?;

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
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        // start a txn
        client_state.start_txn();

        // set the client into the "MULTI" state
        client_state.set_txn_state_multi(true);
        resp_writer.ok().await?;
        resp_writer.flush().await?;
        Ok(HandleCommandResult::ResponseSent)
    }

    /// Handle `MULTI` command
    async fn discard(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        let _txn_guard = ScopedTranscation::new(client_state.clone());

        // Confirm that we are in the MULTI state
        if !client_state.is_txn_state_multi() {
            resp_writer
                .error_string(Strings::DISCARD_WITHOUT_MULTI)
                .await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        resp_writer.ok().await?;
        resp_writer.flush().await?;
        Ok(HandleCommandResult::ResponseSent)
    }

    /// Marks the given keys to be watched for conditional execution of a transaction.
    async fn watch(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 2, resp_writer, HandleCommandResult::ResponseSent);

        // Confirm that we are NOT in MULTI state
        if client_state.is_txn_state_multi() {
            resp_writer
                .error_string(Strings::WATCH_INSIDE_MULTI)
                .await?;
            resp_writer.flush().await?;
            return Ok(HandleCommandResult::ResponseSent);
        }

        let mut iter = command.args_vec().iter();
        iter.next();

        // The actual code that marks keys as modified is done in the storage layer
        // where keys are already encoded.
        // So we need to convert the user keys into internal keys before adding them to the watch list
        let mut user_keys = Vec::<&bytes::BytesMut>::with_capacity(command.arg_count());
        for key in iter {
            user_keys.push(key);
        }

        client_state.set_watched_keys(Some(&user_keys));

        resp_writer.ok().await?;
        resp_writer.flush().await?;
        Ok(HandleCommandResult::ResponseSent)
    }

    /// Flushes all the previously watched keys for a transaction
    async fn unwatch(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut resp_writer = RespWriter::new(tx, 64, client_state.clone());
        expect_args_count_tx!(command, 1, resp_writer, HandleCommandResult::ResponseSent);

        client_state.set_watched_keys(None);

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
    use super::*;
    use crate::{
        commands::Strings, server::SableError, server::WatchedKeys, tests::run_and_return_output,
        Client, ClientState, ServerState,
    };
    use bytes::BytesMut;
    use std::rc::Rc;
    use std::sync::Arc;

    async fn check_command(
        client_state: Rc<ClientState>,
        command_str: &str,
        expected_output: ValkeyObject,
    ) {
        // Start a transaction
        let args: Vec<String> = command_str
            .split(' ')
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let output = run_and_return_output(args, client_state).await.unwrap();
        assert_eq!(output, expected_output);
    }

    #[test]
    #[serial_test::serial]
    fn test_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client2 = Client::new(Arc::<ServerState>::default(), store, None);

            // Start a transaction
            check_command(client.inner(), "multi", ValkeyObject::Status("OK".into())).await;

            // No nested MULTI
            check_command(
                client.inner(),
                "multi",
                ValkeyObject::Error(
                    "ERR command multi can not be used in a MULTI / EXEC block".into(),
                ),
            )
            .await;

            // Queue some commands
            check_command(
                client.inner(),
                "hgetall",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                "set k1 v1",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // Queue some valid commands
            check_command(
                client.inner(),
                "set k2 v2",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 2 commands in the queue
            assert_eq!(client.inner().txn_commands_vec_len(), 3);

            // Use the second client to confirm that the command was not executed
            check_command(client2.inner(), "get k1", ValkeyObject::NullString).await;
            check_command(client2.inner(), "get k2", ValkeyObject::NullString).await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_discard() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(client.inner(), "multi", ValkeyObject::Status("OK".into())).await;

            // state should be "multi"
            assert!(client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(
                client.inner(),
                "set k1 v1",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                "set k2 v2",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // We expect `hlen` to fail, but this will not break the transaction
            check_command(
                client.inner(),
                "hlen k2",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                "set k3 v3",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 4 commands in the queue
            assert_eq!(client.inner().txn_commands_vec_len(), 4);

            check_command(client.inner(), "discard", ValkeyObject::Status("OK".into())).await;

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(client.inner(), "get k1", ValkeyObject::NullString).await;
            check_command(client.inner(), "get k2", ValkeyObject::NullString).await;
            check_command(client.inner(), "get k3", ValkeyObject::NullString).await;
            assert_eq!(client.inner().txn_commands_vec_len(), 0);
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_blocking_list_commands_in_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let myclient = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(myclient.inner(), "multi", ValkeyObject::Status("OK".into())).await;

            // state should be "multi"
            assert!(myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            check_command(
                myclient.inner(),
                "blpop mylist 100",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await; // Null array

            check_command(
                myclient.inner(),
                "brpop mylist 100",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await; // Null array

            check_command(
                myclient.inner(),
                "brpoplpush mylist mylist2 100",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await; // Null string

            check_command(
                myclient.inner(),
                "blmove mylist mylist2 LEFT LEFT 100",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await; // Null string

            check_command(
                myclient.inner(),
                "blmpop 100 2 mylist2 mylist3 LEFT COUNT 1",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await; // Null array
            assert_eq!(myclient.inner().txn_commands_vec_len(), 5);

            check_command(
                myclient.inner(),
                "exec",
                ValkeyObject::Array(vec![
                    ValkeyObject::NullArray,
                    ValkeyObject::NullArray,
                    ValkeyObject::NullString,
                    ValkeyObject::NullString,
                    ValkeyObject::NullArray,
                ]),
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_blocking_zset_commands_in_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let myclient = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(myclient.inner(), "multi", ValkeyObject::Status("OK".into())).await;

            // state should be "multi"
            assert!(myclient.inner().is_txn_state_multi());
            assert!(!myclient.inner().is_txn_state_exec());
            assert!(!myclient.inner().is_txn_state_calc_slots());

            check_command(
                myclient.inner(),
                "bzmpop 30 1 mylist MAX",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;
            assert_eq!(myclient.inner().txn_commands_vec_len(), 1);

            check_command(
                myclient.inner(),
                "exec",
                ValkeyObject::Array(vec![ValkeyObject::NullArray]),
            )
            .await;
        });
    }

    use crate::utils::ValkeyObject;

    #[test]
    #[serial_test::serial]
    fn test_exec() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            // Start a transaction
            check_command(client.inner(), "multi", ValkeyObject::Status("OK".into())).await;

            // state should be "multi"
            assert!(client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(
                client.inner(),
                "set k1 v1",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                "set k2 v2",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // We expect `hlen` to fail, but this will not break the transaction
            check_command(
                client.inner(),
                "hlen k2",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            check_command(
                client.inner(),
                "set k3 v3",
                ValkeyObject::Status(Strings::QUEUED.into()),
            )
            .await;

            // we expect 4 commands in the queue
            assert_eq!(client.inner().txn_commands_vec_len(), 4);

            check_command(
                client.inner(),
                "exec",
                ValkeyObject::Array(vec![
                    ValkeyObject::Status("OK".into()),
                    ValkeyObject::Status("OK".into()),
                    ValkeyObject::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    ValkeyObject::Status("OK".into()),
                ]),
            )
            .await;

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            check_command(client.inner(), "get k1", ValkeyObject::Str("v1".into())).await;
            check_command(client.inner(), "get k2", ValkeyObject::Str("v2".into())).await;
            check_command(client.inner(), "get k3", ValkeyObject::Str("v3".into())).await;
            assert_eq!(client.inner().txn_commands_vec_len(), 0);

            // Run another command, this time we expect it to run without "EXEC"
            check_command(
                client.inner(),
                "set no_cached value",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // Run another command, this time we expect it to run without "EXEC"
            check_command(
                client.inner(),
                "get no_cached",
                ValkeyObject::Str("value".into()),
            )
            .await;
        });
    }

    #[test_case::test_case(vec![
        ("MULTI", "+OK\r\n"),
        ("MULTI", "-ERR command multi can not be used in a MULTI / EXEC block\r\n"),
        ("set tanks rein", "+QUEUED\r\n"),
        ("append tanks _orisa", "+QUEUED\r\n"),
        ("append tanks _sigma", "+QUEUED\r\n"),
        ("EXEC", "*3\r\n+OK\r\n:10\r\n:16\r\n"),
        ("get tanks", "$16\r\nrein_orisa_sigma\r\n"),
    ]; "test_multi")]
    #[test_case::test_case(vec![
        ("MULTI", "+OK\r\n"),
        ("set tanks rein", "+QUEUED\r\n"),
        ("append tanks _orisa", "+QUEUED\r\n"),
        ("append tanks _sigma", "+QUEUED\r\n"),
        ("DISCARD", "+OK\r\n"),
        ("get tanks", "$-1\r\n"),
        ("MULTI", "+OK\r\n"),
    ]; "test_discard")]
    fn test_txn_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
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

    #[test]
    #[serial_test::serial]
    fn test_watch_unwatch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client_state = client.inner();

            // set the watched keys
            check_command(
                client_state.clone(),
                "watch key1 key2",
                ValkeyObject::Status("OK".into()),
            )
            .await;
            assert_eq!(client_state.watched_user_keys_cloned().unwrap().len(), 2);
            assert_eq!(WatchedKeys::watchers_count(None), 1);

            // update the watched keys
            check_command(
                client_state.clone(),
                "watch key1 key2 key3",
                ValkeyObject::Status("OK".into()),
            )
            .await;
            assert_eq!(client_state.watched_user_keys_cloned().unwrap().len(), 3);
            assert_eq!(
                client_state.watched_user_keys_cloned().unwrap(),
                vec![
                    BytesMut::from("key1"),
                    BytesMut::from("key2"),
                    BytesMut::from("key3"),
                ]
            );

            check_command(
                client_state.clone(),
                "set key1 value",
                ValkeyObject::Status("OK".into()),
            )
            .await;
            assert!(WatchedKeys::is_user_key_modified(
                &BytesMut::from("key1"),
                0,
                None
            ));

            check_command(
                client_state.clone(),
                "unwatch",
                ValkeyObject::Status("OK".into()),
            )
            .await;
            assert_eq!(client_state.watched_user_keys_cloned().unwrap().len(), 0);
            assert_eq!(WatchedKeys::watchers_count(None), 0);
        });
    }

    #[test]
    #[serial_test::serial]
    // Create 2 clients:
    // Client 1: watch "key1", start a txn modifying "key1" and attempting to commit it
    // Client 2: modifies "key1"
    // Confirm that the txn is aborted
    fn test_exec_cancelled_with_watch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client_1 = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client_1_state = client_1.inner();

            let client_2 = Client::new(client_1_state.server_inner_state(), store.clone(), None);
            let client_2_state = client_2.inner();

            // set the watched keys
            check_command(
                client_1_state.clone(),
                "watch key1 key2",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // update the watched keys
            check_command(
                client_1_state.clone(),
                "multi",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            check_command(
                client_1_state.clone(),
                "set key1 client_1_value",
                ValkeyObject::Status("QUEUED".into()),
            )
            .await;

            // Modify the watched key from another client
            check_command(
                client_2_state.clone(),
                "Set key1 client_2_value",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // Confirm that the value was indeed modified
            check_command(
                client_2_state.clone(),
                "get key1",
                ValkeyObject::Str("client_2_value".into()),
            )
            .await;

            // Now attempt to commit client-1 transaction and confirm that it is aborted
            check_command(client_1_state.clone(), "exec", ValkeyObject::NullArray).await;

            // Confirm that no more keys are being watched for client-1
            // and the transaction object was removed (getting a `None` watched user keys indicates that
            // no `TransactionState` object was found the client)
            assert_eq!(client_1_state.watched_user_keys_cloned(), None);
            assert_eq!(WatchedKeys::watchers_count(None), 0);

            // Check that the value was not modified (use `client_1` to read the value)
            check_command(
                client_1_state.clone(),
                "get key1",
                ValkeyObject::Str("client_2_value".into()),
            )
            .await;
        });
    }

    #[test]
    #[serial_test::serial]
    // Create 2 clients:
    // Client 1: watch "key1", unwatch "key1", start a txn modifying "key1" and attempting to commit it
    // Client 2: modifies "key1"
    // Confirm that the txn is committed
    fn test_exec_not_affected_by_a_cancelled_watch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client_1 = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client_1_state = client_1.inner();

            let client_2 = Client::new(client_1_state.server_inner_state(), store.clone(), None);
            let client_2_state = client_2.inner();

            // watch key1 + key2
            check_command(
                client_1_state.clone(),
                "watch key1 key2",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // Modify the watched key from another client
            check_command(
                client_2_state.clone(),
                "set key1 client_2_value",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // Confirm that the value was indeed modified
            check_command(
                client_2_state.clone(),
                "get key1",
                ValkeyObject::Str("client_2_value".into()),
            )
            .await;

            // unwatch from client-1
            check_command(
                client_1_state.clone(),
                "unwatch",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            // start a txn (client 1)
            check_command(
                client_1_state.clone(),
                "multi",
                ValkeyObject::Status("OK".into()),
            )
            .await;

            check_command(
                client_1_state.clone(),
                "set key1 client_1_value",
                ValkeyObject::Status("QUEUED".into()),
            )
            .await;

            // Now attempt to commit client-1 transaction and confirm that it is aborted
            check_command(
                client_1_state.clone(),
                "exec",
                ValkeyObject::Array(vec![ValkeyObject::Status("OK".into())]),
            )
            .await;

            // Confirm that no more keys are being watched for client-1
            // and the transaction object was removed (getting a `None` watched user keys indicates that
            // no `TransactionState` object was found the client)
            assert_eq!(client_1_state.watched_user_keys_cloned(), None);
            assert_eq!(WatchedKeys::watchers_count(None), 0);
            assert_eq!(WatchedKeys::watched_keys_count(None), 0);

            // Check that the value was not modified
            check_command(
                client_1_state.clone(),
                "get key1",
                ValkeyObject::Str("client_1_value".into()),
            )
            .await;
        });
    }
}
