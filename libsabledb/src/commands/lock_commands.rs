use crate::{
    commands::{HandleCommandResult, Strings, TimeoutResponse, TryAgainResponse},
    server::ClientState,
    storage::{LockResult, UnlockResult},
    utils::RespBuilderV2,
    BytesMutUtils, SableError, ValkeyCommand, ValkeyCommandName,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct LockCommands {}

impl LockCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::Lock => Self::lock(client_state, command, response_buffer).await,
            ValkeyCommandName::Unlock => Self::unlock(client_state, command, response_buffer).await,
            _ => Err(SableError::InvalidArgument(format!(
                "Not a LOCK command {}",
                command.main_command()
            ))),
        }
    }

    /// Attempt to lock `LOCK_NAME`.
    ///
    /// If the the lock is obtained, return `OK`. If timeout is provided and the lock is being held by another client, then
    /// the client is blocked for the timeout duration (in milliseconds) until the lock is available. In case
    /// of a timeout or the resource is locked and no timeout is provided, EAGAIN error is return.
    /// Note that there is no need to create the lock, the lock is implicitly created and locked
    ///
    /// NOTE:
    /// This is a meta command, i.e. it does not conflict with data with the same name. So a user can have lock named "my-lock"
    /// and a (for example) a string with the same name: "my-lock"
    ///
    /// Syntax:
    ///
    /// ```text
    /// LOCK <LOCK_NAME> [timeout-milliseconds]
    /// ```
    async fn lock(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            2,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );
        let lock_name = command_arg_at!(command, 1);
        let slot = crate::utils::calculate_slot(&lock_name);

        if !client_state.server_inner_state().slots().is_set(slot)? {
            return Err(SableError::NotOwner(vec![slot]));
        }

        let builder = RespBuilderV2::default();
        let timeout_duration = if let Some(timeout_ms) = command.args_vec().get(2) {
            let Some(timeout_ms) = BytesMutUtils::parse::<u64>(timeout_ms) else {
                builder.error_string(
                    &mut response_buffer,
                    Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
                );
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            };
            Some(tokio::time::Duration::from_millis(timeout_ms))
        } else {
            None
        };

        if let Some(timeout_duration) = timeout_duration {
            match client_state
                .server_inner_state()
                .lock(lock_name, client_state.id())?
            {
                LockResult::Ok => {
                    client_state.locked_key_add(lock_name);
                    builder.ok(&mut response_buffer);
                    Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
                }
                LockResult::Deadlock => {
                    builder.error_string(&mut response_buffer, Strings::ERR_DEADLOCK);
                    Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
                }
                LockResult::Pending(rx) => Ok(HandleCommandResult::Blocked((
                    rx,
                    timeout_duration,
                    TimeoutResponse::Err(Strings::ERR_EAGAIN.into()),
                    // In case we managed to obtain the lock during the timeout, we only need to add it to the list of
                    // tracking locks by this client
                    TryAgainResponse::ClientAcquiredLock(lock_name.clone()),
                ))),
                _ => Err(SableError::ClientInvalidState),
            }
        } else {
            // No timeout
            match client_state
                .server_inner_state()
                .try_lock(lock_name, client_state.id())?
            {
                LockResult::Ok => {
                    client_state.locked_key_add(lock_name);
                    builder.ok(&mut response_buffer);
                    Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
                }
                LockResult::WouldBlock => {
                    // this can not happen since we are not calling "try_lock"
                    builder.error_string(&mut response_buffer, Strings::ERR_EAGAIN);
                    Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
                }
                LockResult::Deadlock => {
                    builder.error_string(&mut response_buffer, Strings::ERR_DEADLOCK);
                    Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
                }
                _ => Err(SableError::ClientInvalidState),
            }
        }
    }

    /// Attempt to un-lock `LOCK_NAME`.
    ///
    /// ```text
    /// UNLOCK <LOCK_NAME>
    /// ```
    async fn unlock(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            2,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );
        let lock_name = command_arg_at!(command, 1);
        let slot = crate::utils::calculate_slot(&lock_name);

        if !client_state.server_inner_state().slots().is_set(slot)? {
            return Err(SableError::NotOwner(vec![slot]));
        }

        let builder = RespBuilderV2::default();
        match client_state
            .server_inner_state()
            .unlock(lock_name, client_state.id())?
        {
            UnlockResult::Ok => {
                client_state.locked_key_remove(lock_name);
                builder.ok(&mut response_buffer);
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            UnlockResult::NotOwner => {
                builder.error_string(&mut response_buffer, Strings::ERR_NOT_OWNER);
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            UnlockResult::NotFound => {
                builder.error_string(&mut response_buffer, Strings::NO_SUCH_KEY);
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
        }
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

    use std::rc::Rc;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(vec![
        ("lock", "-ERR wrong number of arguments for 'lock' command\r\n"),
        ("lock mylock", "+OK\r\n"),
    ]; "test_lock")]
    #[test_case(vec![
        ("lock mylock", "+OK\r\n"),
        ("lock mylock", "-DEADLOCK lock is already owned by the calling client\r\n"),
        ("unlock mylock", "+OK\r\n"),
        ("unlock mylock", "-ERR no such key\r\n"),
    ]; "test_unlock")]
    #[test_case(vec![
        ("lock", "-ERR wrong number of arguments for 'lock' command\r\n"),
        ("lock mylock 1000", "+OK\r\n"),
    ]; "test_lock_with_timeout")]
    fn test_lock_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
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
