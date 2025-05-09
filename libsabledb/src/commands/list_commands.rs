use crate::{
    commands::{HandleCommandResult, Strings, TimeoutResponse, TryAgainResponse},
    server::ClientState,
    storage::{
        ListDb, ListFlags, ListIndexOfResult, ListInsertAtResult, ListInsertResult, ListItemAt,
        ListLenResult, ListPopResult, ListPushResult, ListRangeResult, ListRemoveResult,
        ListTrimResult,
    },
    to_number, BlockClientResult, BytesMutUtils, LockManager, RespBuilderV2, SableError,
    ValkeyCommand, ValkeyCommandName,
};
use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver as TokioReceiver;
use tokio::time::Duration;

#[allow(dead_code)]
pub struct ListCommands {}

#[allow(dead_code)]
impl ListCommands {
    /// Main entry point for all list commands
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::Lpush => {
                Self::push(
                    client_state,
                    command,
                    &mut response_buffer,
                    ListFlags::FromLeft,
                )
                .await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lpushx => {
                Self::push(
                    client_state,
                    command,
                    &mut response_buffer,
                    ListFlags::FromLeft | ListFlags::ListMustExist,
                )
                .await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Rpush => {
                Self::push(client_state, command, &mut response_buffer, ListFlags::None).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Rpushx => {
                Self::push(
                    client_state,
                    command,
                    &mut response_buffer,
                    ListFlags::ListMustExist,
                )
                .await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lpop => {
                Self::pop(
                    client_state,
                    command,
                    &mut response_buffer,
                    ListFlags::FromLeft,
                )
                .await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Rpop => {
                Self::pop(client_state, command, &mut response_buffer, ListFlags::None).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Ltrim => {
                Self::ltrim(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lrange => {
                Self::lrange(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Llen => {
                Self::llen(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lindex => {
                Self::lindex(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Linsert => {
                Self::linsert(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lset => {
                Self::lset(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lpos => {
                Self::lpos(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lrem => {
                Self::lrem(client_state, command, &mut response_buffer).await?;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ValkeyCommandName::Lmove => Self::lmove(client_state, command, response_buffer).await,
            ValkeyCommandName::Lmpop => {
                Self::lmpop(client_state, command, false, response_buffer).await
            }
            ValkeyCommandName::Rpoplpush => {
                Self::rpoplpush(client_state, command, response_buffer).await
            }
            ValkeyCommandName::Brpoplpush => {
                Self::blocking_rpoplpush(client_state, command, response_buffer).await
            }
            ValkeyCommandName::Blpop => {
                Self::blocking_pop(client_state, command, response_buffer, ListFlags::FromLeft)
                    .await
            }
            ValkeyCommandName::Brpop => {
                Self::blocking_pop(client_state, command, response_buffer, ListFlags::FromRight)
                    .await
            }
            ValkeyCommandName::Blmove => {
                Self::blocking_move(client_state, command, response_buffer).await
            }
            ValkeyCommandName::Blmpop => {
                Self::lmpop(client_state, command, true, response_buffer).await
            }
            _ => Err(SableError::InvalidArgument(format!(
                "Non List command `{}`",
                command.main_command(),
            ))),
        }
    }

    /// Insert all the specified values at the head of the list stored at key.
    /// If key does not exist, it is created as empty list before performing the
    /// push operations. When key holds a value that is not a list, an error is returned
    pub async fn push(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        let values: Vec<&BytesMut> = command.args_vec()[2..].iter().collect();
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.push(key, &values, flags)? {
            ListPushResult::Some(list_len) => {
                // Commit the changes
                db.commit()?;

                // Build the response (integer reply)
                builder.number_usize(response_buffer, list_len);

                // And wakeup any waiting clients
                client_state
                    .server_inner_state()
                    .wakeup_clients(key, values.len())
                    .await;
            }
            ListPushResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListPushResult::ListNotFound => builder.number_usize(response_buffer, 0),
        }
        Ok(())
    }

    /// Atomically returns and removes the last element (tail) of the list stored at source, and
    /// pushes the element at the first element (head) of the list stored at destination
    pub async fn rpoplpush(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            3,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);

        // same as calling "lmove src target right left"
        Self::lmove_internal(
            client_state,
            command.clone(),
            src_list_name,
            target_list_name,
            "right",
            "left",
            response_buffer,
            None,
        )
        .await
    }

    /// Atomically returns and removes the last element (tail) of the list stored at source, and
    /// pushes the element at the first element (head) of the list stored at destination
    /// Blocking version
    pub async fn blocking_rpoplpush(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            4,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let timeout = command_arg_at!(command, 3);
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            let builder = RespBuilderV2::default();
            builder.error_string(
                &mut response_buffer,
                "ERR timeout is not a float or out of range",
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it

        // same as calling "lmove src target right left"
        Self::lmove_internal(
            client_state,
            command.clone(),
            src_list_name,
            target_list_name,
            "right",
            "left",
            response_buffer,
            Some(Duration::from_millis(timeout_ms)),
        )
        .await
    }

    /// `LMOVE SRC TARGET <LEFT|RIGHT> <LEFT|RIGHT>`
    /// Atomically returns and removes the first/last element (head/tail depending on the wherefrom argument)
    /// of the list stored at source, and pushes the element at the first/last element
    /// (head/tail depending on the whereto argument) of the list stored at destination.
    pub async fn lmove(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            5,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let src_left_or_right = command_arg_at_as_str!(command, 3);
        let target_left_or_right = command_arg_at_as_str!(command, 4);
        Self::lmove_internal(
            client_state,
            command.clone(),
            src_list_name,
            target_list_name,
            &src_left_or_right,
            &target_left_or_right,
            response_buffer,
            None,
        )
        .await
    }

    /// `BLMOVE SRC TARGET <LEFT|RIGHT> <LEFT|RIGHT> timeout`
    /// Atomically returns and removes the first/last element (head/tail depending on the wherefrom argument)
    /// of the list stored at source, and pushes the element at the first/last element
    /// (head/tail depending on the whereto argument) of the list stored at destination.
    pub async fn blocking_move(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            6,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let src_left_or_right = command_arg_at_as_str!(command, 3);
        let target_left_or_right = command_arg_at_as_str!(command, 4);
        let timeout = command_arg_at!(command, 5);
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            let builder = RespBuilderV2::default();
            builder.error_string(
                &mut response_buffer,
                "ERR timeout is not a float or out of range",
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it

        Self::lmove_internal(
            client_state,
            command.clone(),
            src_list_name,
            target_list_name,
            &src_left_or_right,
            &target_left_or_right,
            response_buffer,
            Some(Duration::from_millis(timeout_ms)),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    /// Move **one** element from `src_list_name` list to `target_list_name`.
    /// If `blocking_duration` is not `None` and there are no elements to be moved in the source list
    /// the client is blocked for the duration of specified in `blocking_duration`
    async fn lmove_internal(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        src_list_name: &BytesMut,
        target_list_name: &BytesMut,
        src_left_or_right: &str,
        target_left_or_right: &str,
        mut response_buffer: BytesMut,
        blocking_duration: Option<Duration>,
    ) -> Result<HandleCommandResult, SableError> {
        let builder = RespBuilderV2::default();
        let (src_flags, target_flags) = match (src_left_or_right, target_left_or_right) {
            ("left", "left") => (ListFlags::FromLeft, ListFlags::FromLeft),
            ("right", "right") => (ListFlags::FromRight, ListFlags::FromLeft),
            ("left", "right") => (ListFlags::FromLeft, ListFlags::FromRight),
            ("right", "left") => (ListFlags::FromRight, ListFlags::FromLeft),
            (_, _) => {
                builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            }
        };

        // lock both keys
        let keys = vec![src_list_name, target_list_name];
        let _unused = LockManager::lock_multi(&keys, client_state.clone(), command.clone()).await?;

        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let moved_item = match db.pop(src_list_name, 1, src_flags)? {
            ListPopResult::WrongType => {
                builder.error_string(&mut response_buffer, Strings::WRONGTYPE);
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            }
            ListPopResult::Some(mut elements) => {
                // we are popping from the back, but since the vector is expected to be of size 1
                // it has the same effect
                let Some(item) = elements.pop() else {
                    return Err(SableError::InternalError(
                        "Expected at least 1 value in array".into(),
                    ));
                };
                item
            }
            ListPopResult::None => {
                if let Some(blocking_duration) = blocking_duration {
                    // blocking is requested
                    let keys: Vec<BytesMut> = keys.iter().map(|e| (*e).clone()).collect();
                    if let Some(rx) = Self::block_client_for_keys(client_state.clone(), &keys).await
                    {
                        return Ok(HandleCommandResult::Blocked((
                            rx,
                            blocking_duration,
                            TimeoutResponse::NullString,
                            TryAgainResponse::RunCommandAgain,
                        )));
                    }
                }
                // None blocking or failed to block -> return null string
                builder.null_string(&mut response_buffer);
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            }
        };

        // Place it in the target source
        match db.push(target_list_name, &[&moved_item], target_flags)? {
            ListPushResult::WrongType => {
                builder.error_string(&mut response_buffer, Strings::WRONGTYPE);
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
            ListPushResult::ListNotFound => {
                /* cant happen */
                Err(SableError::InternalError(
                    "Unexpected return value: `ListNotFound`".into(),
                ))
            }
            ListPushResult::Some(_) => {
                builder.bulk_string(&mut response_buffer, &moved_item);
                db.commit()?;

                // And wakeup any pending client
                client_state
                    .server_inner_state()
                    .wakeup_clients(target_list_name, 1)
                    .await;
                Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
            }
        }
    }

    /// `LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]`
    /// `BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]`
    /// Pops one or more elements from the first non-empty list key from the list of provided key names
    pub async fn lmpop(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        allow_blocking: bool,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            if allow_blocking { 4 } else { 3 },
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let builder = RespBuilderV2::default();
        let mut iter = command.args_vec().iter();
        iter.next(); // skip the command
        let (numkeys, timeout_ms) = if allow_blocking {
            let Some(timeout_secs) = command.arg_as_number::<f64>(1) else {
                builder.error_string(&mut response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            };

            let Some(numkeys) = command.arg_as_number::<usize>(2) else {
                builder.error_string(&mut response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            };
            iter.next(); // timeout
            iter.next(); // numkeys
            (numkeys, (timeout_secs * 1000.0) as usize)
        } else {
            let Some(numkeys) = command.arg_as_number::<usize>(1) else {
                builder.error_string(&mut response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            };
            iter.next(); // numkeys
            (numkeys, 0)
        };

        if numkeys == 0 {
            builder.error_string(&mut response_buffer, "ERR numkeys should be greater than 0");
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        }

        let mut keys = Vec::<&BytesMut>::with_capacity(numkeys);
        enum State {
            CollectingKeys,
            RightOrLeft,
            Count,
        }
        let mut state = State::CollectingKeys;
        let mut list_flags: Option<ListFlags> = None;
        let mut count = 1;
        while let Some(arg) = iter.next() {
            match state {
                State::CollectingKeys => {
                    keys.push(arg);
                    if keys.len() == numkeys {
                        state = State::RightOrLeft;
                    }
                }
                State::RightOrLeft => {
                    let argstr = BytesMutUtils::to_string(arg).to_lowercase();
                    match argstr.as_str() {
                        "left" => {
                            list_flags = Some(ListFlags::FromLeft);
                        }
                        "right" => {
                            list_flags = Some(ListFlags::FromRight);
                        }
                        _ => {
                            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                        }
                    }
                    state = State::Count;
                }
                State::Count => {
                    let argstr = BytesMutUtils::to_string(arg).to_lowercase();
                    match argstr.as_str() {
                        "count" => {
                            let Some(elements_to_pop) = iter.next() else {
                                builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                                return Ok(HandleCommandResult::ResponseBufferUpdated(
                                    response_buffer,
                                ));
                            };

                            let Some(elements_to_pop) =
                                BytesMutUtils::parse::<usize>(elements_to_pop)
                            else {
                                builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                                return Ok(HandleCommandResult::ResponseBufferUpdated(
                                    response_buffer,
                                ));
                            };

                            if elements_to_pop == 0 {
                                builder.error_string(
                                    &mut response_buffer,
                                    "ERR numkeys should be greater than 0",
                                );
                                return Ok(HandleCommandResult::ResponseBufferUpdated(
                                    response_buffer,
                                ));
                            }
                            count = elements_to_pop;
                            break; // Parsing is completed
                        }
                        _ => {
                            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                        }
                    }
                }
            }
        }

        if keys.len() != numkeys {
            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        }

        let Some(list_flags) = list_flags else {
            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let _unused = LockManager::lock_multi(&keys, client_state.clone(), command.clone()).await?;
        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());

        // Check the first lists
        for list_name in &keys {
            match db.pop(list_name, count, list_flags.clone())? {
                ListPopResult::WrongType => {
                    builder.error_string(&mut response_buffer, Strings::WRONGTYPE);
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                ListPopResult::Some(items_removed) => {
                    builder.add_array_len(&mut response_buffer, 2);
                    builder.add_bulk_string(&mut response_buffer, list_name);
                    builder.add_array_len(&mut response_buffer, items_removed.len());
                    for v in &items_removed {
                        builder.add_bulk_string(&mut response_buffer, v);
                    }
                    db.commit()?;
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                ListPopResult::None => {}
            }
        }

        // If we got here, there were no lists that we could "pop" items from
        let allow_blocking = allow_blocking && !client_state.is_txn_state_exec();
        if allow_blocking {
            let keys: Vec<BytesMut> = keys.iter().map(|x| (*x).clone()).collect();
            if let Some(rx) = Self::block_client_for_keys(client_state, &keys).await {
                return Ok(HandleCommandResult::Blocked((
                    rx,
                    Duration::from_millis(timeout_ms as u64),
                    TimeoutResponse::NullArrray,
                    TryAgainResponse::RunCommandAgain,
                )));
            }
        }

        // Blocking is not allowed - return null string
        builder.null_array(&mut response_buffer);
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Removes and returns the first elements of the list stored at key.
    /// By default, the command pops a single element from the beginning
    /// or the end of the list. When provided with the optional count argument, the
    /// reply will consist of up to count elements, depending on the list's length.
    pub async fn pop(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let has_count = command.arg_count() == 3;
        let count = if has_count {
            to_number!(command_arg_at!(command, 2), usize, response_buffer, Ok(()))
        } else {
            1usize
        };

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let items = match db.pop(key, count, flags)? {
            ListPopResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListPopResult::None => {
                builder_return_null_reply!(builder, response_buffer, has_count);
            }
            ListPopResult::Some(items) => items,
        };

        if !has_count {
            let item = items.first().ok_or(SableError::InternalError(
                "Expected exactly 1 item in popped items!".into(),
            ))?;
            builder.add_bulk_string(response_buffer, item);
        } else {
            // when count is provided, we always return an array, even if it is a single element
            builder.add_array_len(response_buffer, items.len());
            for item in &items {
                builder.add_bulk_string(response_buffer, item);
            }
        }
        db.commit()?;
        Ok(())
    }

    pub async fn blocking_pop(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        mut response_buffer: BytesMut,
        flags: ListFlags,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            3,
            &mut response_buffer,
            HandleCommandResult::ResponseBufferUpdated(response_buffer)
        );

        let mut iter = command.args_vec().iter().peekable(); // points to the command
        iter.next(); // blpop

        // Parse the arguments, extracting the lists + time-out
        let mut lists = Vec::<&BytesMut>::new();
        let mut timeout: Option<&BytesMut> = None;
        loop {
            let (key1, key2) = (iter.next(), iter.peek());
            match (key1, key2) {
                (Some(key1), Some(_)) => lists.push(key1),
                (Some(key1), None) => {
                    // last item, this is our time-out
                    timeout = Some(key1);
                    break;
                }
                (None, _) => break,
            }
        }

        // Sanity, should not happen but...
        let builder = RespBuilderV2::default();
        let Some(timeout) = timeout else {
            builder.error_string(
                &mut response_buffer,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    command.main_command()
                ),
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        // Check that a valid number was provided
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            builder.error_string(&mut response_buffer, Strings::ZERR_TIMEOUT_NOT_FLOAT);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        // convert to milliseconds and round it
        let timeout_ms = (timeout_secs * 1000.0) as u64;

        if lists.is_empty() {
            builder.error_string(
                &mut response_buffer,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    command.main_command()
                ),
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        }

        // Try to pop an element from one of the lists
        // if all the lists are empty, block the client
        let _unused =
            LockManager::lock_multi(&lists, client_state.clone(), command.clone()).await?;
        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());

        for list_name in &lists {
            match db.pop(list_name, 1, flags.clone())? {
                ListPopResult::WrongType => {
                    builder.error_string(&mut response_buffer, Strings::WRONGTYPE);
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                ListPopResult::Some(items_removed) => {
                    // Array of 2 elements is expected. the list name + the popped item
                    builder.add_array_len(&mut response_buffer, 2);
                    builder.add_bulk_string(&mut response_buffer, list_name);
                    let Some(item) = items_removed.first() else {
                        return Err(SableError::InternalError(
                            "Expected at least 1 value in array".into(),
                        ));
                    };
                    builder.add_bulk_string(&mut response_buffer, item);
                    db.commit()?;
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                ListPopResult::None => {}
            }
        }

        // If we reached here, it means that we failed to pop any item from any list => block the client
        let keys: Vec<BytesMut> = lists.iter().map(|e| (*e).clone()).collect();
        if let Some(rx) = Self::block_client_for_keys(client_state, &keys).await {
            // client is successfully blocked
            Ok(HandleCommandResult::Blocked((
                rx,
                std::time::Duration::from_millis(timeout_ms),
                TimeoutResponse::NullArrray,
                TryAgainResponse::RunCommandAgain,
            )))
        } else {
            // failed to block the client (e.g. active txn in the current client)
            // return null array
            builder.null_array(&mut response_buffer);
            Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
        }
    }

    /// Trim an existing list so that it will contain only the specified range of elements specified.
    /// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head),
    /// 1 the next element and so on
    pub async fn ltrim(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let key = command_arg_at!(command, 1);
        let start = to_number!(command_arg_at!(command, 2), isize, response_buffer, Ok(()));
        let end = to_number!(command_arg_at!(command, 3), isize, response_buffer, Ok(()));
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.trim(key, start, end)? {
            ListTrimResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListTrimResult::NotFound => {
                builder.ok(response_buffer);
            }
            ListTrimResult::Ok => {
                builder.ok(response_buffer);
                db.commit()?;
            }
        }
        Ok(())
    }

    /// Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes,
    /// with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
    /// These offsets can also be negative numbers indicating offsets starting at the end of the list. For example,
    /// -1 is the last element of the list, -2 the penultimate, and so on.
    pub async fn lrange(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let key = command_arg_at!(command, 1);
        let start = to_number!(command_arg_at!(command, 2), isize, response_buffer, Ok(()));
        let end = to_number!(command_arg_at!(command, 3), isize, response_buffer, Ok(()));
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.range(key, start, end)? {
            ListRangeResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListRangeResult::InvalidRange => {
                builder_return_empty_array!(builder, response_buffer);
            }
            ListRangeResult::Some(items) => {
                builder.add_array_len(response_buffer, items.len());
                for item in &items {
                    builder.add_bulk_string(response_buffer, item);
                }
            }
        }
        Ok(())
    }

    /// Returns the length of the list stored at key. If key does not exist, it is interpreted
    /// as an empty list and 0 is returned. An error is returned when the value stored at key
    /// is not a list.
    pub async fn llen(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let key = command_arg_at!(command, 1);
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        let db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.len(key)? {
            ListLenResult::Some(len) => builder.number_usize(response_buffer, len),
            ListLenResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListLenResult::NotFound => {
                builder.number_usize(response_buffer, 0);
            }
        }
        Ok(())
    }

    /// Inserts element in the list stored at key either before or after the reference value pivot.
    /// When key does not exist, it is considered an empty list and no operation is performed.
    /// An error is returned when key exists but does not hold a list value
    /// `LINSERT key <BEFORE | AFTER> pivot element`
    pub async fn linsert(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 5, response_buffer);

        let key = command_arg_at!(command, 1);
        let orientation = command_arg_at_as_str!(command, 2);
        let pivot = command_arg_at!(command, 3);
        let element = command_arg_at!(command, 4);

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        let res = match orientation.as_str() {
            "after" => db.insert_after(key, element, pivot)?,
            "before" => db.insert_before(key, element, pivot)?,
            _ => {
                builder_return_syntax_error!(builder, response_buffer);
            }
        };

        db.commit()?;
        match res {
            ListInsertResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListInsertResult::PivotNotFound => {
                builder.number_i64(response_buffer, -1);
            }
            ListInsertResult::ListNotFound => {
                builder.number_i64(response_buffer, 0);
            }
            ListInsertResult::Some(len) => {
                builder.number_usize(response_buffer, len);
            }
        }
        Ok(())
    }

    /// Returns the element at index index in the list stored at key.
    /// The index is zero-based, so 0 means the first element, 1 the second element and so on.
    /// Negative indices can be used to designate elements starting at the tail of the list.
    /// Here, -1 means the last element, -2 means the penultimate and so forth.
    /// When the value at key is not a list, an error is returned.
    pub async fn lindex(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let key = command_arg_at!(command, 1);
        let index = command_arg_at!(command, 2);
        let index = to_number!(index, isize, response_buffer, Ok(()));

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.item_at(key, index)? {
            ListItemAt::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListItemAt::ListNotFound | ListItemAt::OutOfRange => {
                builder_return_null_string!(builder, response_buffer);
            }
            ListItemAt::Some(item) => {
                builder.bulk_string(response_buffer, &item);
            }
        }
        Ok(())
    }

    /// The command returns the index of matching elements inside a Valkey list.
    /// By default, when no options are given, it will scan the list from head
    /// to tail, looking for the first match of "element". If the element is
    /// found, its index (the zero-based position in the list) is returned.
    /// Otherwise, if no match is found, nil is returned.
    pub async fn lpos(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let key = command_arg_at!(command, 1);
        let value = command_arg_at!(command, 2);

        let mut iter = command.args_vec().iter();
        iter.next(); // lpos
        iter.next(); // key
        iter.next(); // element

        let mut rank: Option<isize> = None;
        let mut count: Option<usize> = None;
        let mut maxlen: Option<usize> = None;

        // parse the command line arguments
        let builder = RespBuilderV2::default();
        while let (Some(arg), Some(value)) = (iter.next(), iter.next()) {
            let keyword_lowercase = BytesMutUtils::to_string(arg);
            match keyword_lowercase.as_str() {
                "rank" => {
                    rank = Some(to_number!(value, isize, response_buffer, Ok(())));
                }
                "count" => {
                    let value = to_number!(value, i64, response_buffer, Ok(()));
                    if value < 0 {
                        builder.error_string(response_buffer, Strings::COUNT_CANT_BE_NEGATIVE);
                        return Ok(());
                    }
                    count = Some(value.try_into().unwrap_or(1));
                }
                "maxlen" => {
                    let value = to_number!(value, i64, response_buffer, Ok(()));
                    if value < 0 {
                        builder.error_string(response_buffer, Strings::MAXLNE_CANT_BE_NEGATIVE);
                        return Ok(());
                    }
                    maxlen = Some(value.try_into().unwrap_or(0));
                }
                _ => {
                    builder_return_syntax_error!(builder, response_buffer);
                }
            }
        }

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let db = ListDb::with_storage(client_state.database(), client_state.database_id());

        match db.index_of(key, value, rank, count, maxlen)? {
            ListIndexOfResult::InvalidRank => {
                builder.error_string(response_buffer, Strings::LIST_RANK_INVALID);
                return Ok(());
            }
            ListIndexOfResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListIndexOfResult::None if count.is_some() => {
                builder_return_null_array!(builder, response_buffer);
            }
            ListIndexOfResult::None => {
                builder_return_null_string!(builder, response_buffer);
            }
            ListIndexOfResult::Some(indexes) if count.is_some() => {
                builder.add_array_len(response_buffer, indexes.len());
                for indx in indexes {
                    builder.add_number(response_buffer, indx, false);
                }
            }
            ListIndexOfResult::Some(indexes) => {
                let Some(index) = indexes.first() else {
                    return Err(SableError::InternalError(
                        "Expected at least one index".into(),
                    ));
                };
                builder.number_usize(response_buffer, *index);
            }
        }
        Ok(())
    }

    /// Sets the list element at index to element. For more information on the index argument, see `lindex`.
    /// An error is returned for out of range indexes.
    pub async fn lset(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        // extract variables from the command
        let key = command_arg_at!(command, 1);
        let index = command_arg_at!(command, 2);
        let value = command_arg_at!(command, 3);
        let index = to_number!(index, isize, response_buffer, Ok(()));

        // Lock and set
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();
        match db.update_by_index(key, value, index)? {
            ListInsertAtResult::Ok => builder.ok(response_buffer),
            ListInsertAtResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListInsertAtResult::NotFound => {
                builder.error_string(response_buffer, Strings::NO_SUCH_KEY);
                return Ok(());
            }
            ListInsertAtResult::InvalidIndex => {
                builder.error_string(response_buffer, Strings::INDEX_OUT_OF_BOUNDS);
                return Ok(());
            }
        }
        db.commit()?;
        Ok(())
    }

    /// Sets the list element at index to element. For more information on the index argument, see `lindex`.
    /// An error is returned for out of range indexes.
    pub async fn lrem(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        // extract variables from the command
        let key = command_arg_at!(command, 1);
        let count = to_number!(command_arg_at!(command, 2), isize, response_buffer, Ok(()));
        let element = command_arg_at!(command, 3);

        // Lock and set
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let mut db = ListDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        match db.remove_items(key, count, element)? {
            ListRemoveResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ListRemoveResult::Some(count) => {
                builder.number_usize(response_buffer, count);
            }
        }
        db.commit()?;
        Ok(())
    }

    /// Block `client_state` for `keys` and return the channel on which the client should wait
    async fn block_client_for_keys(
        client_state: Rc<ClientState>,
        keys: &[BytesMut],
    ) -> Option<TokioReceiver<u8>> {
        let client_state_clone = client_state.clone();
        match client_state
            .server_inner_state()
            .block_client(client_state.id(), keys, client_state_clone)
            .await
        {
            BlockClientResult::Blocked(rx) => Some(rx),
            BlockClientResult::TxnActive => {
                // can't block the client due to an active transaction
                None
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
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::{
        commands::ClientNextAction, test_assert, Client, ServerState, StorageAdapter,
        StorageOpenParams, Telemetry,
    };
    use std::rc::Rc;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(vec![
        (vec!["lpush", "lpush_mykey", "foo", "bar", "baz"], ":3\r\n"),
        (vec!["lpush", "lpush_mykey", "foo", "bar", "baz"], ":6\r\n"),
        ], "lpush"; "lpush")]
    #[test_case(vec![
        (vec!["lpushx", "lpushx_mykey", "foo"], ":0\r\n"),
        (vec!["lpush", "lpush_mykey", "foo", "bar", "baz"], ":3\r\n"),
        (vec!["lpushx", "lpush_mykey", "hello"], ":4\r\n"),
        ], "lpushx"; "lpushx")]
    #[test_case(vec![
        (vec!["rpush", "rpush_mykey", "foo", "bar", "baz"], ":3\r\n"),
        (vec!["rpush", "rpush_mykey", "foo", "bar", "baz"], ":6\r\n"),
        ], "rpush"; "rpush")]
    #[test_case(vec![
        (vec!["rpushx", "lpushx_mykey", "foo"], ":0\r\n"),
        (vec!["rpush", "lpush_mykey", "foo", "bar", "baz"], ":3\r\n"),
        (vec!["rpushx", "lpush_mykey", "hello"], ":4\r\n"),
        ], "rpushx"; "rpushx")]
    #[test_case(vec![
        (vec!["lpush", "lpop_mykey", "foo", "bar", "baz", "apple"], ":4\r\n"),
        (vec!["lpop", "lpop_mykey"], "$5\r\napple\r\n"),
        (vec!["lpop", "lpop_mykey", "3"], "*3\r\n$3\r\nbaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"),
        (vec!["lpop", "lpop_mykey"], "$-1\r\n"),
        (vec!["rpush", "lpop_mykey", "foo", "bar", "baz", "apple"], ":4\r\n"),
        (vec!["lpop", "lpop_mykey"], "$3\r\nfoo\r\n"),
        ], "lpop"; "lpop")]
    #[test_case(vec![
        (vec!["rpush", "rpop_mykey", "foo", "bar", "baz", "apple"], ":4\r\n"),
        (vec!["rpop", "rpop_mykey"], "$5\r\napple\r\n"),
        (vec!["rpop", "rpop_mykey", "3"], "*3\r\n$3\r\nbaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n"),
        (vec!["rpop", "rpop_mykey"], "$-1\r\n"),
        (vec!["lpush", "rpop_mykey", "foo", "bar", "baz", "apple"], ":4\r\n"),
        (vec!["rpop", "rpop_mykey"], "$3\r\nfoo\r\n"),
        ], "rpop"; "rpop")]
    #[test_case(vec![
        (vec!["lpush", "len_key", "foo", "bar", "baz", "apple"], ":4\r\n"),
        (vec!["llen", "len_key"], ":4\r\n"),
        (vec!["llen", "len_key2"], ":0\r\n"),
        (vec!["set", "len_key3", "value"], "+OK\r\n"),
        (vec!["llen", "len_key3"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ], "llen"; "llen")]
    #[test_case(vec![
        (vec!["rpush", "lindex_list", "hello", "world", "foo", "bar"], ":4\r\n"),
        (vec!["lindex", "lindex_list", "0"], "$5\r\nhello\r\n"),
        (vec!["lindex", "lindex_list", "-1"], "$3\r\nbar\r\n"),
        (vec!["lindex", "lindex_list", "-2"], "$3\r\nfoo\r\n"),
        (vec!["lindex", "lindex_list", "-6"], "$-1\r\n"),
        (vec!["lindex", "lindex_list", "4"], "$-1\r\n"),
        (vec!["lindex", "lindex_list", "4"], "$-1\r\n"),
        (vec!["lindex", "lindex_list", "not_a_number"], "-ERR value is not an integer or out of range\r\n"),
        ], "lindex"; "lindex")]
    #[test_case(vec![
        (vec!["lset", "no_such_list", "0", "value"], "-ERR no such key\r\n"),
        (vec!["rpush", "lset_list", "hellow", "world", "foo", "bar"], ":4\r\n"),
        (vec!["lindex", "lset_list", "0"], "$6\r\nhellow\r\n"),
        (vec!["lset", "lset_list", "0", "hello"], "+OK\r\n"),
        (vec!["lindex", "lset_list", "0"], "$5\r\nhello\r\n"),
        (vec!["lset", "lset_list", "11"], "-ERR wrong number of arguments for 'lset' command\r\n"),
        (vec!["lset", "lset_list", "11", "world"], "-index out of range\r\n"),
        (vec!["lset", "lset_list", "-4", "world"], "+OK\r\n"),
        (vec!["lset", "lset_list", "-5", "world"], "-index out of range\r\n"),
        (vec!["lset", "lset_list", "not_a_number", "world"], "-ERR value is not an integer or out of range\r\n"),
        ], "lset"; "lset")]
    #[test_case(vec![
        (vec!["rpush", "lpos_list", "x", "y", "x", "y", "x", "y", "x", "y", "x", "y"], ":10\r\n"),
        (vec!["lpos", "lpos_list", "hellow"], "$-1\r\n"),
        (vec!["lpos", "lpos_no_such_list", "hello"], "$-1\r\n"),
        (vec!["lpos", "lpos_list", "x"], ":0\r\n"),
        (vec!["lpos", "lpos_list", "x", "count", "2"], "*2\r\n:0\r\n:2\r\n"),
        (vec!["lpos", "lpos_list", "x", "count", "not_a_number"], "-ERR value is not an integer or out of range\r\n"),
        (vec!["lpos", "lpos_list", "x", "count", "0"], "*5\r\n:0\r\n:2\r\n:4\r\n:6\r\n:8\r\n"),
        (vec!["lpos", "lpos_list", "x", "count", "-1"], "-ERR COUNT can't be negative\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "0"], "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "1"], ":0\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "-1"], ":8\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "-2", "count", "2"], "*2\r\n:6\r\n:4\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "2", "count", "2"], "*2\r\n:2\r\n:4\r\n"),
        (vec!["lpos", "lpos_list", "x", "rank", "not_a_number"], "-ERR value is not an integer or out of range\r\n"),
        ], "lpos"; "lpos")]
    #[test_case(vec![
        (vec!["rpush", "ltrim_list", "one", "two", "three"], ":3\r\n"),
        (vec!["ltrim", "ltrim_list", "1", "-1"], "+OK\r\n"),
        (vec!["ltrim", "ltrim_list"], "-ERR wrong number of arguments for 'ltrim' command\r\n"),
        (vec!["llen", "ltrim_list"], ":2\r\n"),
        (vec!["lpos", "ltrim_list", "one"], "$-1\r\n"),
        (vec!["ltrim", "ltrim_list_not_existing", "1", "2"], "+OK\r\n"),
        ], "ltrim"; "ltrim")]
    #[test_case(vec![
        (vec!["lrange", "no_such_list", "0", "-1"], "*0\r\n"),
        (vec!["rpush", "lrange_list", "one", "two", "three"], ":3\r\n"),
        (vec!["lrange", "lrange_list", "1", "-1"], "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
        (vec!["lrange", "lrange_list"], "-ERR wrong number of arguments for 'lrange' command\r\n"),
        (vec!["llen", "lrange_list"], ":3\r\n"),
        (vec!["lpos", "lrange_list", "one"], ":0\r\n"),
        (vec!["lrange", "lrange_list_not_existing", "1", "2"], "*0\r\n"),
        ], "lrange"; "lrange")]
    #[test_case(vec![
        (vec!["rpush", "lmove_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "lmove_list2", "1", "2", "3"], ":3\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "left"], "$1\r\na\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "left"], "$1\r\nb\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "left"], "$1\r\nc\r\n"),
        (vec!["llen", "lmove_list1"], ":0\r\n"),
        (vec!["llen", "lmove_list2"], ":6\r\n"),
        (vec!["lrange", "lmove_list2", "0", "-1"], "*6\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
        ], "lmove1"; "lmove1")]
    #[test_case(vec![
        (vec!["rpush", "lmove_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "lmove_list2", "1", "2", "3"], ":3\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "left"], "$1\r\nc\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "left"], "$1\r\nb\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "left"], "$1\r\na\r\n"),
        (vec!["llen", "lmove_list1"], ":0\r\n"),
        (vec!["llen", "lmove_list2"], ":6\r\n"),
        (vec!["lrange", "lmove_list2", "0", "-1"], "*6\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
        ], "lmove2"; "lmove2")]
    #[test_case(vec![
        (vec!["rpush", "lmove_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "lmove_list2", "1", "2", "3"], ":3\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "right"], "$1\r\na\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "right"], "$1\r\nb\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "left", "right"], "$1\r\nc\r\n"),
        (vec!["llen", "lmove_list1"], ":0\r\n"),
        (vec!["llen", "lmove_list2"], ":6\r\n"),
        (vec!["lrange", "lmove_list2", "0", "-1"], "*6\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
        ], "lmove3"; "lmove3")]
    #[test_case(vec![
        (vec!["rpush", "lmove_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "lmove_list2", "1", "2", "3"], ":3\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "right"], "$1\r\nc\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "right"], "$1\r\nb\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "right"], "$1\r\na\r\n"),
        (vec!["llen", "lmove_list1"], ":0\r\n"),
        (vec!["llen", "lmove_list2"], ":6\r\n"),
        (vec!["lrange", "lmove_list2", "0", "-1"], "*6\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
        ], "lmove4"; "lmove4")]
    #[test_case(vec![
        (vec!["set", "lmove_list1", "a"], "+OK\r\n"),
        (vec!["set", "lmove_list2", "1"], "+OK\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "right", "right"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["lmove", "lmove_list1", "lmove_list2", "ssdsd", "right"], "-ERR syntax error\r\n"),
        ], "lmove5"; "lmove5")]
    #[test_case(vec![
        (vec!["rpush", "list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "list2", "1", "2", "3"], ":3\r\n"),
        (vec!["rpoplpush", "list1", "list2"], "$1\r\nc\r\n"),
        (vec!["llen", "list1"], ":2\r\n"),
        (vec!["llen", "list2"], ":4\r\n"),
        (vec!["lrange", "list2", "0", "-1"], "*4\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n"),
        ], "rpoplpush"; "rpoplpush")]
    #[test_case(vec![
        (vec!["rpush", "brpoplpush_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["brpoplpush", "brpoplpush_list1", "brpoplpush_new_list", "1"], "$1\r\nc\r\n"),
        (vec!["brpoplpush", "brpoplpush_list1", "brpoplpush_new_list", "1"], "$1\r\nb\r\n"),
        (vec!["brpoplpush", "brpoplpush_list1", "brpoplpush_new_list", "1"], "$1\r\na\r\n"),
        (vec!["brpoplpush", "brpoplpush_list1", "brpoplpush_new_list", "sdsd"], "-ERR timeout is not a float or out of range\r\n"),
        (vec!["llen", "brpoplpush_new_list"], ":3\r\n"),
        (vec!["lrange", "brpoplpush_new_list", "0", "-1"], "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"),
        (vec!["brpoplpush", "brpoplpush_list1", "brpoplpush_new_list", "1"], "$-1\r\n"),
        ], "brpoplpush"; "brpoplpush")]
    #[test_case(vec![
        (vec!["rpush", "blpop_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["blpop", "blpop_list2", "blpop_list1", "1"], "*2\r\n$11\r\nblpop_list1\r\n$1\r\na\r\n"),
        (vec!["blpop", "blpop_list2", "blpop_list1", "1"], "*2\r\n$11\r\nblpop_list1\r\n$1\r\nb\r\n"),
        (vec!["blpop", "blpop_list2", "blpop_list1", "1"], "*2\r\n$11\r\nblpop_list1\r\n$1\r\nc\r\n"),
        (vec!["blpop", "blpop_list2", "blpop_list1", "1"], "*-1\r\n"), // timeout
        ], "blpop"; "blpop")]
    #[test_case(vec![
        (vec!["rpush", "brpop_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\nc\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\nb\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\na\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*-1\r\n"), // timeout
        ], "brpop"; "brpop")]
    #[test_case(vec![
        (vec!["rpush", "list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "list2", "d"], ":1\r\n"),
        (vec!["lmpop", "not_anumber", "list1", "list1", "left"], "-ERR numkeys should be greater than 0\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "sdsd"], "-ERR syntax error\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "right"], "*2\r\n$5\r\nlist1\r\n*1\r\n$1\r\nc\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left"], "*2\r\n$5\r\nlist1\r\n*1\r\n$1\r\na\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT"], "-ERR syntax error\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT", "bla"], "-ERR syntax error\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT", "1"], "*2\r\n$5\r\nlist1\r\n*1\r\n$1\r\nb\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT", "1"], "*2\r\n$5\r\nlist2\r\n*1\r\n$1\r\nd\r\n"),
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT", "1"], "*-1\r\n"),
        ], "lmpop"; "lmpop")]
    #[test_case(vec![
        (vec!["rpush", "list1", "a", "b", "c"], ":3\r\n"),
        (vec!["linsert", "list1", "bla", "c", "b.1"], "-ERR syntax error\r\n"),
        (vec!["linsert", "no_such_list", "after", "c", "b.1"], ":0\r\n"),
        (vec!["linsert", "list1", "before", "c", "b.1"], ":4\r\n"),
        // [a, b, b.1, c]
        (vec!["lrange", "list1", "0", "-1"], "*4\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\nb.1\r\n$1\r\nc\r\n"),
        (vec!["linsert", "list1", "after", "c", "d"], ":5\r\n"),
        // [a, b, b.1, c, d]
        (vec!["lrange", "list1", "0", "-1"], "*5\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\nb.1\r\n$1\r\nc\r\n$1\r\nd\r\n"),
        (vec!["linsert", "list1", "before", "a", "_"], ":6\r\n"),
        // [_, a, b, b.1, c, d]
        (vec!["lrange", "list1", "0", "-1"], "*6\r\n$1\r\n_\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\nb.1\r\n$1\r\nc\r\n$1\r\nd\r\n"),
        ], "linsert"; "linsert")]
    #[test_case(vec![
        (vec!["blmove", "blmove_src", "blmove_target", "left", "left", "0.1"], "$-1\r\n"),
        (vec!["rpush", "blmove_src", "a", "b", "c"], ":3\r\n"),
        (vec!["blmove", "blmove_src", "blmove_target", "left", "left", "0.1"], "$1\r\na\r\n"), // a
        (vec!["blmove", "blmove_src", "blmove_target", "left", "left", "0.1"], "$1\r\nb\r\n"), // b
        (vec!["blmove", "blmove_src", "blmove_target", "left", "left", "0.1"], "$1\r\nc\r\n"), // c
        (vec!["lrange", "blmove_target", "0", "-1"], "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
        (vec!["blmove", "blmove_src", "blmove_target", "left", "left", "0.1"], "$-1\r\n"), // timeout
        ], "blmove"; "blmove")]
    #[test_case(vec![
        (vec!["rpush", "blmpop_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["rpush", "blmpop_list2", "d"], ":1\r\n"),
        (vec!["blmpop", "not_anumber", "blmpop_list1", "blmpop_list1", "left"], "-ERR numkeys should be greater than 0\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "sdsd"], "-ERR syntax error\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "right", "COUNT", "1"], "*2\r\n$12\r\nblmpop_list1\r\n*1\r\n$1\r\nc\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "left", "COUNT", "1"], "*2\r\n$12\r\nblmpop_list1\r\n*1\r\n$1\r\na\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "right", "COUNT", "1"], "*2\r\n$12\r\nblmpop_list1\r\n*1\r\n$1\r\nb\r\n"),
        // pop from the second list now ("d")
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "right", "COUNT", "1"], "*2\r\n$12\r\nblmpop_list2\r\n*1\r\n$1\r\nd\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "right", "COUNT", "1"], "*-1\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "left", "COUNT"], "-ERR syntax error\r\n"),
        ], "blmpop"; "blmpop")]
    fn test_list_commands(args_vec: Vec<(Vec<&'static str>, &'static str)>, test_name: &str) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args_vec {
                let mut sink = crate::tests::ResponseSink::with_name(test_name).await;
                let cmd = Rc::new(ValkeyCommand::for_test(args));
                match Client::handle_command(client.inner(), cmd.clone(), &mut sink.fp)
                    .await
                    .unwrap()
                {
                    ClientNextAction::TerminateConnection => {}
                    ClientNextAction::SendResponse(response_buffer) => {
                        if BytesMutUtils::to_string(&response_buffer).as_str() != expected_value {
                            println!("Command: {:?}", cmd);
                        }
                        assert_eq!(
                            BytesMutUtils::to_string(&response_buffer).as_str(),
                            expected_value
                        );
                    }
                    ClientNextAction::NoAction => {
                        assert_eq!(&sink.read_all().await, expected_value);
                    }
                    ClientNextAction::Wait((rx, duration, timeout_response, _)) => {
                        println!("--> got Wait for duration of {:?}", duration);
                        let sw = crate::stopwatch::StopWatch::default();
                        let response = match Client::wait_for(rx, duration).await {
                            crate::server::WaitResult::TryAgain => {
                                println!(
                                    "--> got TryAgain after {}ms",
                                    sw.elapsed_micros().unwrap() / 1000
                                );
                                crate::tests::execute_command(client.inner(), cmd.clone()).await
                            }
                            crate::server::WaitResult::Timeout => {
                                let builder = RespBuilderV2::default();
                                let mut response = BytesMut::new();
                                match timeout_response {
                                    TimeoutResponse::NullString => {
                                        builder.null_string(&mut response)
                                    }
                                    TimeoutResponse::NullArrray => {
                                        builder.null_array(&mut response)
                                    }
                                    TimeoutResponse::Number(num) => {
                                        builder.number_i64(&mut response, num)
                                    }
                                    TimeoutResponse::Ok => {
                                        builder.ok(&mut response);
                                    }
                                    TimeoutResponse::Err(msg) => {
                                        builder.error_string(&mut response, &msg);
                                    }
                                }
                                response
                            }
                        };
                        assert_eq!(BytesMutUtils::to_string(&response).as_str(), expected_value);
                    }
                }
            }
        });
    }

    #[test]
    fn test_blocking_pop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();

            let server = Arc::<ServerState>::default();
            let reader = Client::new(server.clone(), store.clone(), None);
            let writer = Client::new(server, store, None);

            const EXPECTED_RESULT: &str = "*2\r\n$23\r\ntest_blocking_pop_list1\r\n$5\r\nvalue\r\n";
            let read_cmd = Rc::new(ValkeyCommand::for_test(vec![
                "blpop",
                "test_blocking_pop_list1",
                "test_blocking_pop_list2",
                "5",
            ]));

            // we expect to get a rx + duration, if we dont "deferred_command" will panic!
            let (rx, duration, _timeout_response) =
                crate::tests::deferred_command(reader.inner(), read_cmd.clone()).await;

            // second connection: push data to the list
            let pus_cmd = Rc::new(ValkeyCommand::for_test(vec![
                "lpush",
                "test_blocking_pop_list1",
                "value",
            ]));
            let response = crate::tests::execute_command(writer.inner(), pus_cmd.clone()).await;
            assert_eq!(":1\r\n", BytesMutUtils::to_string(&response).as_str());

            // Try reading again now
            match Client::wait_for(rx, duration).await {
                crate::server::WaitResult::TryAgain => {
                    println!("consumer: got something - calling blpop again");
                    let response = crate::tests::execute_command(reader.inner(), read_cmd).await;
                    assert_eq!(
                        EXPECTED_RESULT,
                        BytesMutUtils::to_string(&response).as_str()
                    );
                }
                crate::server::WaitResult::Timeout => {
                    assert!(false, "consumer: Expected `TryAagain` not a `Timeout`!");
                }
            }
        });
    }
}
