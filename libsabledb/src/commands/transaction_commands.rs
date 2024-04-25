#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type,
    client::{Client, ClientState},
    command_arg_at,
    commands::Strings,
    commands::{HandleCommandResult, StringCommands},
    io::{FileResponseSink, RespWriter},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    storage::StringsDb,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct TransactionCommands {}

impl TransactionCommands {
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
            match Client::handle_non_exec_command(client_state.clone(), cmd.clone(), tx).await {
                Err(SableError::LockCancelledTxnPrep(command_slots)) => {
                    slots.extend_from_slice(&command_slots);
                }
                Err(e) => {
                    // other error occured, propogate it
                    return Err(e);
                }
                Ok(_) => {
                    // this should not happend
                    return Err(SableError::ClientInvalidState);
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
    pub async fn handle_multi(
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
    use crate::{commands::Strings, tests::run_and_return_output, Client, ServerState};
    use std::sync::Arc;

    #[test]
    fn test_multi() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store.clone(), None);
            let client2 = Client::new(Arc::<ServerState>::default(), store, None);

            // Start a transaction
            let output = run_and_return_output(vec!["multi".to_string()], client.inner())
                .await
                .unwrap();

            assert_eq!(output.status().unwrap(), "OK");

            // No nested MULTI
            let output = run_and_return_output(vec!["multi".to_string()], client.inner())
                .await
                .unwrap();

            // we expect an error message
            assert!(output.error_string().is_ok());

            // Check that commands marked with "no_transaction" are not accepted
            let output = run_and_return_output(vec!["hgetall".to_string()], client.inner())
                .await
                .unwrap();

            // we expect an error message
            assert!(output.error_string().is_ok());

            // Check that commands marked with "no_transaction" are not accepted
            let output = run_and_return_output(
                vec!["set".to_string(), "k1".to_string(), "v1".to_string()],
                client.inner(),
            )
            .await
            .unwrap();
            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            // Check that commands marked with "no_transaction" are not accepted
            let output = run_and_return_output(
                vec!["set".to_string(), "k2".to_string(), "v2".to_string()],
                client.inner(),
            )
            .await
            .unwrap();

            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            // we expect 2 commands in the queue
            assert_eq!(client.inner().commands_queue().len(), 2);

            // Use the second client to confirm that the command was not executed
            let output = run_and_return_output(
                vec!["get".to_string(), "k1".to_string(), "v1".to_string()],
                client2.inner(),
            )
            .await
            .unwrap();

            // We expect a null string
            assert!(output.is_null_string());

            // Use the second client to confirm that the command was not executed
            let output = run_and_return_output(
                vec!["get".to_string(), "k2".to_string(), "v2".to_string()],
                client2.inner(),
            )
            .await
            .unwrap();

            // We expect a null string
            assert!(output.is_null_string());
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
            let output = run_and_return_output(vec!["multi".to_string()], client.inner())
                .await
                .unwrap();

            // state should be "multi"
            assert!(client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            assert_eq!(output.status().unwrap(), "OK");

            let output = run_and_return_output(
                vec!["set".to_string(), "k1".to_string(), "v1".to_string()],
                client.inner(),
            )
            .await
            .unwrap();

            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            let output = run_and_return_output(
                vec!["set".to_string(), "k2".to_string(), "v2".to_string()],
                client.inner(),
            )
            .await
            .unwrap();

            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            // We expect `hlen` to fail, but this will not break the transaction
            let output =
                run_and_return_output(vec!["hlen".to_string(), "k2".to_string()], client.inner())
                    .await
                    .unwrap();
            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            let output = run_and_return_output(
                vec!["set".to_string(), "k3".to_string(), "v3".to_string()],
                client.inner(),
            )
            .await
            .unwrap();

            assert_eq!(output.status().unwrap(), Strings::QUEUED);

            // we expect 4 commands in the queue
            assert_eq!(client.inner().commands_queue().len(), 4);

            let output = run_and_return_output(vec!["exec".to_string()], client.inner())
                .await
                .unwrap();
            assert_eq!(
                output,
                RedisObject::Array(vec![
                    RedisObject::Status("OK".into()),
                    RedisObject::Status("OK".into()),
                    RedisObject::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into()
                    ),
                    RedisObject::Status("OK".into()),
                ]),
            );

            assert!(!client.inner().is_txn_state_multi());
            assert!(!client.inner().is_txn_state_exec());
            assert!(!client.inner().is_txn_state_calc_slots());

            let output =
                run_and_return_output(vec!["get".to_string(), "k1".to_string()], client.inner())
                    .await
                    .unwrap();
            assert_eq!(output, RedisObject::Str("v1".into()),);
            let output =
                run_and_return_output(vec!["get".to_string(), "k2".to_string()], client.inner())
                    .await
                    .unwrap();
            assert_eq!(output, RedisObject::Str("v2".into()),);
        });
    }
}
