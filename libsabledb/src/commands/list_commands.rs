use crate::{
    client::ClientState,
    commands::{ErrorStrings, HandleCommandResult},
    to_number,
    types::{BlockingCommandResult, List, ListFlags, MoveResult, MultiPopResult},
    BytesMutUtils, LockManager, RedisCommand, RedisCommandName, RespBuilderV2, SableError,
};
use bytes::BytesMut;
use std::rc::Rc;
use tokio::time::Duration;

#[allow(dead_code)]
pub struct ListCommands {}

#[allow(dead_code)]
impl ListCommands {
    /// Main entry point for all list commands
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::Lpush => {
                Self::push(client_state, command, response_buffer, ListFlags::FromLeft).await
            }
            RedisCommandName::Lpushx => {
                Self::push(
                    client_state,
                    command,
                    response_buffer,
                    ListFlags::FromLeft | ListFlags::ListMustExist,
                )
                .await
            }
            RedisCommandName::Rpush => {
                Self::push(client_state, command, response_buffer, ListFlags::None).await
            }
            RedisCommandName::Rpushx => {
                Self::push(
                    client_state,
                    command,
                    response_buffer,
                    ListFlags::ListMustExist,
                )
                .await
            }
            RedisCommandName::Lpop => {
                Self::pop(client_state, command, response_buffer, ListFlags::FromLeft).await
            }
            RedisCommandName::Rpop => {
                Self::pop(client_state, command, response_buffer, ListFlags::None).await
            }
            RedisCommandName::Ltrim => Self::ltrim(client_state, command, response_buffer).await,
            RedisCommandName::Lrange => Self::lrange(client_state, command, response_buffer).await,
            RedisCommandName::Llen => Self::llen(client_state, command, response_buffer).await,
            RedisCommandName::Lindex => Self::lindex(client_state, command, response_buffer).await,
            RedisCommandName::Linsert => {
                Self::linsert(client_state, command, response_buffer).await
            }
            RedisCommandName::Lset => Self::lset(client_state, command, response_buffer).await,
            RedisCommandName::Lpos => Self::lpos(client_state, command, response_buffer).await,
            RedisCommandName::Lrem => Self::lrem(client_state, command, response_buffer).await,
            RedisCommandName::Lmove => Self::lmove(client_state, command, response_buffer).await,
            RedisCommandName::Lmpop => {
                Self::lmpop(client_state, command, false, response_buffer).await
            }
            RedisCommandName::Rpoplpush => {
                Self::rpoplpush(client_state, command, response_buffer).await
            }
            RedisCommandName::Brpoplpush => {
                Self::blocking_rpoplpush(client_state, command, response_buffer).await
            }
            RedisCommandName::Blpop => {
                Self::blocking_pop(client_state, command, response_buffer, ListFlags::FromLeft)
                    .await
            }
            RedisCommandName::Brpop => {
                Self::blocking_pop(client_state, command, response_buffer, ListFlags::FromRight)
                    .await
            }
            RedisCommandName::Blmove => {
                Self::blocking_move(client_state, command, response_buffer).await
            }
            RedisCommandName::Blmpop => {
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
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 3, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // lpush
        iter.next(); // key

        // collect the values
        let mut values = Vec::<&BytesMut>::with_capacity(command.arg_count());
        for value in iter {
            values.push(value);
        }

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.push(key, &values, response_buffer, flags)?;

        client_state
            .server_inner_state()
            .wakeup_clients(key, values.len())
            .await;
        Ok(HandleCommandResult::Completed)
    }

    /// Atomically returns and removes the last element (tail) of the list stored at source, and
    /// pushes the element at the first element (head) of the list stored at destination
    pub async fn rpoplpush(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 3, response_buffer, HandleCommandResult::Completed);

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);

        // same as calling "lmove src target right left"
        Self::lmove_internal(
            client_state,
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
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 4, response_buffer, HandleCommandResult::Completed);

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let timeout = command_arg_at!(command, 3);
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            let builder = RespBuilderV2::default();
            builder.error_string(
                response_buffer,
                "ERR timeout is not a float or out of range",
            );
            return Ok(HandleCommandResult::Completed);
        };

        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it

        // same as calling "lmove src target right left"
        Self::lmove_internal(
            client_state,
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
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 5, response_buffer, HandleCommandResult::Completed);

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let src_left_or_right = command_arg_at_as_str!(command, 3);
        let target_left_or_right = command_arg_at_as_str!(command, 4);
        Self::lmove_internal(
            client_state,
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
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 6, response_buffer, HandleCommandResult::Completed);

        let src_list_name = command_arg_at!(command, 1);
        let target_list_name = command_arg_at!(command, 2);
        let src_left_or_right = command_arg_at_as_str!(command, 3);
        let target_left_or_right = command_arg_at_as_str!(command, 4);
        let timeout = command_arg_at!(command, 5);
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            let builder = RespBuilderV2::default();
            builder.error_string(
                response_buffer,
                "ERR timeout is not a float or out of range",
            );
            return Ok(HandleCommandResult::Completed);
        };

        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it

        Self::lmove_internal(
            client_state,
            src_list_name,
            target_list_name,
            &src_left_or_right,
            &target_left_or_right,
            response_buffer,
            Some(Duration::from_millis(timeout_ms)),
        )
        .await
    }

    async fn lmove_internal(
        client_state: Rc<ClientState>,
        src_list_name: &BytesMut,
        target_list_name: &BytesMut,
        src_left_or_right: &str,
        target_left_or_right: &str,
        response_buffer: &mut BytesMut,
        blocking_duration: Option<Duration>,
    ) -> Result<HandleCommandResult, SableError> {
        let (src_flags, target_flags) = match (src_left_or_right, target_left_or_right) {
            ("left", "left") => (ListFlags::FromLeft, ListFlags::FromLeft),
            ("right", "right") => (ListFlags::FromRight, ListFlags::FromLeft),
            ("left", "right") => (ListFlags::FromLeft, ListFlags::FromRight),
            ("right", "left") => (ListFlags::FromRight, ListFlags::FromLeft),
            (_, _) => {
                let builder = RespBuilderV2::default();
                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::Completed);
            }
        };

        // lock both keys
        let keys = vec![src_list_name, target_list_name];
        let _unused = LockManager::lock_user_keys_exclusive(&keys, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        let res = list
            .move_item(src_list_name, target_list_name, src_flags, target_flags)
            .await?;

        let builder = RespBuilderV2::default();
        match res {
            MoveResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                Ok(HandleCommandResult::Completed)
            }
            MoveResult::Some(popped_item) => {
                client_state
                    .server_inner_state()
                    .wakeup_clients(target_list_name, 1)
                    .await;
                builder.bulk_string(response_buffer, &popped_item.user_data);
                Ok(HandleCommandResult::Completed)
            }
            MoveResult::None => {
                if let Some(blocking_duration) = blocking_duration {
                    let interersting_keys: Vec<BytesMut> =
                        keys.iter().map(|e| (*e).clone()).collect();
                    let rx = client_state
                        .server_inner_state()
                        .block_client(&interersting_keys)
                        .await;
                    Ok(HandleCommandResult::Blocked((rx, blocking_duration)))
                } else {
                    builder.null_string(response_buffer);
                    Ok(HandleCommandResult::Completed)
                }
            }
        }
    }

    /// `LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]`
    /// Pops one or more elements from the first non-empty list key from the list of provided key names
    pub async fn lmpop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        allow_blocking: bool,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(
            command,
            if allow_blocking { 4 } else { 3 },
            response_buffer,
            HandleCommandResult::Completed
        );

        let builder = RespBuilderV2::default();
        let mut iter = command.args_vec().iter();
        iter.next(); // skip the command
        let (numkeys, timeout_ms) = if allow_blocking {
            let Some(timeout_secs) = command.arg_as_number::<f64>(1) else {
                builder.error_string(response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::Completed);
            };

            let Some(numkeys) = command.arg_as_number::<usize>(2) else {
                builder.error_string(response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::Completed);
            };
            iter.next(); // timeout
            iter.next(); // numkeys
            (numkeys, (timeout_secs * 1000.0) as usize)
        } else {
            let Some(numkeys) = command.arg_as_number::<usize>(1) else {
                builder.error_string(response_buffer, "ERR numkeys should be greater than 0");
                return Ok(HandleCommandResult::Completed);
            };
            iter.next(); // numkeys
            (numkeys, 0)
        };

        if numkeys == 0 {
            builder.error_string(response_buffer, "ERR numkeys should be greater than 0");
            return Ok(HandleCommandResult::Completed);
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
                            builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                            return Ok(HandleCommandResult::Completed);
                        }
                    }
                    state = State::Count;
                }
                State::Count => {
                    let argstr = BytesMutUtils::to_string(arg).to_lowercase();
                    match argstr.as_str() {
                        "count" => {
                            let Some(elements_to_pop) = iter.next() else {
                                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                                return Ok(HandleCommandResult::Completed);
                            };

                            let Some(elements_to_pop) =
                                BytesMutUtils::parse::<usize>(elements_to_pop)
                            else {
                                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                                return Ok(HandleCommandResult::Completed);
                            };

                            if elements_to_pop == 0 {
                                builder.error_string(
                                    response_buffer,
                                    "ERR count should be greater than 0",
                                );
                                return Ok(HandleCommandResult::Completed);
                            }
                            count = elements_to_pop;
                            break; // Parsing is completed
                        }
                        _ => {
                            builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                            return Ok(HandleCommandResult::Completed);
                        }
                    }
                }
            }
        }

        if keys.len() != numkeys {
            builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::Completed);
        }

        let Some(list_flags) = list_flags else {
            builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::Completed);
        };

        let _unused = LockManager::lock_user_keys_exclusive(&keys, client_state.database_id());
        let list = List::with_storage(client_state.database(), client_state.database_id());
        match list.multi_pop(&keys, count, list_flags)? {
            MultiPopResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                Ok(HandleCommandResult::Completed)
            }
            MultiPopResult::Some((list_name, values)) => {
                builder.add_array_len(response_buffer, 2);
                builder.add_bulk_string(response_buffer, &list_name);
                builder.add_array_len(response_buffer, values.len());
                for v in values {
                    builder.add_bulk_string(response_buffer, &v.user_data);
                }
                Ok(HandleCommandResult::Completed)
            }
            MultiPopResult::None => {
                // block the client here
                if allow_blocking {
                    let keys: Vec<BytesMut> = keys.iter().map(|x| (*x).clone()).collect();
                    let rx = client_state.server_inner_state().block_client(&keys).await;
                    Ok(HandleCommandResult::Blocked((
                        rx,
                        Duration::from_millis(timeout_ms as u64),
                    )))
                } else {
                    builder.null_string(response_buffer);
                    Ok(HandleCommandResult::Completed)
                }
            }
        }
    }

    /// Removes and returns the first elements of the list stored at key.
    /// By default, the command pops a single element from the beginning
    /// or the end of the list. When provided with the optional count argument, the
    /// reply will consist of up to count elements, depending on the list's length.
    pub async fn pop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 2, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let count = if command.arg_count() == 3 {
            to_number!(
                command_arg_at!(command, 2),
                usize,
                response_buffer,
                Ok(HandleCommandResult::Completed)
            )
        } else {
            1usize
        };

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.pop(key, count, response_buffer, flags)?;
        Ok(HandleCommandResult::Completed)
    }

    pub async fn blocking_pop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        flags: ListFlags,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 3, response_buffer, HandleCommandResult::Completed);

        let mut iter = command.args_vec().iter().peekable(); // points to the command
        iter.next(); // blpop

        // Parse the arguments, extracting the lists + timeout
        let mut lists = Vec::<&BytesMut>::new();
        let mut timeout: Option<&BytesMut> = None;
        loop {
            let (key1, key2) = (iter.next(), iter.peek());
            match (key1, key2) {
                (Some(key1), Some(_)) => lists.push(key1),
                (Some(key1), None) => {
                    // last item, this is our timeout
                    timeout = Some(key1);
                    break;
                }
                (None, _) => break,
            }
        }

        // Sanity, shouldn't happen but...
        let Some(timeout) = timeout else {
            let builder = RespBuilderV2::default();
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'blpop' command",
            );
            return Ok(HandleCommandResult::Completed);
        };

        if lists.is_empty() {
            let builder = RespBuilderV2::default();
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'blpop' command",
            );
            return Ok(HandleCommandResult::Completed);
        }

        // Try to pop an element from one of the lists
        // if all the lists are empty, block the client
        let _unused = LockManager::lock_user_keys_exclusive(&lists, client_state.database_id());
        let list = List::with_storage(client_state.database(), client_state.database_id());
        if let BlockingCommandResult::Ok = list.blocking_pop(&lists, 1, response_buffer, flags)? {
            Ok(HandleCommandResult::Completed)
        } else {
            // Block the client
            let keys: Vec<BytesMut> = lists.iter().map(|e| (*e).clone()).collect();
            let rx = client_state.server_inner_state().block_client(&keys).await;

            // Notify the caller
            let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
                let builder = RespBuilderV2::default();
                builder.error_string(
                    response_buffer,
                    "ERR timeout is not a float or out of range",
                );
                return Ok(HandleCommandResult::Completed);
            };
            let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it
            Ok(HandleCommandResult::Blocked((
                rx,
                std::time::Duration::from_millis(timeout_ms),
            )))
        }
    }

    /// Trim an existing list so that it will contain only the specified range of elements specified.
    /// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head),
    /// 1 the next element and so on
    pub async fn ltrim(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 4, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let start = to_number!(
            command_arg_at!(command, 2),
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );
        let end = to_number!(
            command_arg_at!(command, 3),
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.ltrim(key, start, end, response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes,
    /// with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
    /// These offsets can also be negative numbers indicating offsets starting at the end of the list. For example,
    /// -1 is the last element of the list, -2 the penultimate, and so on.
    pub async fn lrange(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 4, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let start = to_number!(
            command_arg_at!(command, 2),
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );
        let end = to_number!(
            command_arg_at!(command, 3),
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );
        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.lrange(key, start, end, response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Returns the length of the list stored at key. If key does not exist, it is interpreted
    /// as an empty list and 0 is returned. An error is returned when the value stored at key
    /// is not a list.
    pub async fn llen(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 2, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());

        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.len(key, response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Inserts element in the list stored at key either before or after the reference value pivot.
    /// When key does not exist, it is considered an empty list and no operation is performed.
    /// An error is returned when key exists but does not hold a list value
    /// `LINSERT key <BEFORE | AFTER> pivot element`
    pub async fn linsert(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 5, response_buffer, HandleCommandResult::Completed);
        let key = command_arg_at!(command, 1);
        let orientation = command_arg_at_as_str!(command, 2);
        let pivot = command_arg_at!(command, 3);
        let element = command_arg_at!(command, 4);

        let flags = match orientation.as_str() {
            "after" => ListFlags::InsertAfter,
            "before" => ListFlags::InsertBefore,
            _ => {
                let builder = RespBuilderV2::default();
                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::Completed);
            }
        };
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.linsert(key, element, pivot, response_buffer, flags)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Returns the element at index index in the list stored at key.
    /// The index is zero-based, so 0 means the first element, 1 the second element and so on.
    /// Negative indices can be used to designate elements starting at the tail of the list.
    /// Here, -1 means the last element, -2 means the penultimate and so forth.
    /// When the value at key is not a list, an error is returned.
    pub async fn lindex(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 3, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let index = command_arg_at!(command, 2);

        let list = List::with_storage(client_state.database(), client_state.database_id());
        let index = to_number!(
            index,
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );

        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());
        list.index(key, index, response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// The command returns the index of matching elements inside a Redis list.
    /// By default, when no options are given, it will scan the list from head
    /// to tail, looking for the first match of "element". If the element is
    /// found, its index (the zero-based position in the list) is returned.
    /// Otherwise, if no match is found, nil is returned.
    pub async fn lpos(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 3, response_buffer, HandleCommandResult::Completed);

        let key = command_arg_at!(command, 1);
        let value = command_arg_at!(command, 2);

        let mut iter = command.args_vec().iter();
        iter.next(); // lpos
        iter.next(); // key
        iter.next(); // element

        let mut rank: Option<i32> = None;
        let mut count: Option<usize> = None;
        let mut maxlen: Option<usize> = None;

        // parse the command line arguments
        let builder = RespBuilderV2::default();
        while let (Some(arg), Some(value)) = (iter.next(), iter.next()) {
            let keyword_lowercase = BytesMutUtils::to_string(arg);
            match keyword_lowercase.as_str() {
                "rank" => {
                    let value = to_number!(
                        value,
                        i32,
                        response_buffer,
                        Ok(HandleCommandResult::Completed)
                    );

                    // rank can not be 0
                    if value == 0 {
                        builder.error_string(response_buffer, ErrorStrings::LIST_RANK_INVALID);
                        return Ok(HandleCommandResult::Completed);
                    }
                    rank = Some(value);
                }
                "count" => {
                    let value = to_number!(
                        value,
                        i64,
                        response_buffer,
                        Ok(HandleCommandResult::Completed)
                    );
                    if value < 0 {
                        builder.error_string(response_buffer, ErrorStrings::COUNT_CANT_BE_NEGATIVE);
                        return Ok(HandleCommandResult::Completed);
                    }
                    count = Some(value.try_into().unwrap_or(1));
                }
                "maxlen" => {
                    let value = to_number!(
                        value,
                        i64,
                        response_buffer,
                        Ok(HandleCommandResult::Completed)
                    );

                    if value < 0 {
                        builder
                            .error_string(response_buffer, ErrorStrings::MAXLNE_CANT_BE_NEGATIVE);
                        return Ok(HandleCommandResult::Completed);
                    }
                    maxlen = Some(value.try_into().unwrap_or(0));
                }
                _ => {
                    builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR);
                    return Ok(HandleCommandResult::Completed);
                }
            }
        }

        let list = List::with_storage(client_state.database(), client_state.database_id());
        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());

        list.lpos(key, value, rank, count, maxlen, response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Sets the list element at index to element. For more information on the index argument, see `lindex`.
    /// An error is returned for out of range indexes.
    pub async fn lset(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 4, response_buffer, HandleCommandResult::Completed);
        // extract variables from the command
        let key = command_arg_at!(command, 1);
        let index = command_arg_at!(command, 2);
        let value = command_arg_at!(command, 3);

        let index = to_number!(
            index,
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );

        // Lock and set
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.set(key, index, value.clone(), response_buffer)?;
        Ok(HandleCommandResult::Completed)
    }

    /// Sets the list element at index to element. For more information on the index argument, see `lindex`.
    /// An error is returned for out of range indexes.
    pub async fn lrem(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        expect_args_count!(command, 4, response_buffer, HandleCommandResult::Completed);
        // extract variables from the command
        let key = command_arg_at!(command, 1);
        let count = to_number!(
            command_arg_at!(command, 2),
            i32,
            response_buffer,
            Ok(HandleCommandResult::Completed)
        );
        let element = command_arg_at!(command, 3);

        // Lock and set
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let list = List::with_storage(client_state.database(), client_state.database_id());
        list.remove(key, Some(element), count, response_buffer)?;
        Ok(HandleCommandResult::Completed)
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
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Arc;
    use test_case::test_case;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::Duration;

    fn open_store(name: &str) -> Result<StorageAdapter, SableError> {
        let _ = std::fs::create_dir_all("tests/list_commands/");
        let db_path = PathBuf::from(format!("tests/list_commands/{}.db", name));
        let _ = std::fs::remove_dir_all(db_path.clone());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);

        let mut store = StorageAdapter::default();
        store.open(open_params)?;
        Ok(store)
    }

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
        (vec!["rpush", "lrange_list", "one", "two", "three"], ":3\r\n"),
        (vec!["lrange", "lrange_list", "1", "-1"], "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
        (vec!["lrange", "lrange_list"], "-ERR wrong number of arguments for 'lrange' command\r\n"),
        (vec!["llen", "lrange_list"], ":3\r\n"),
        (vec!["lpos", "lrange_list", "one"], ":0\r\n"),
        (vec!["lrange", "lrange_list_not_existing", "1", "2"], "+OK\r\n"),
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
        (vec!["blpop", "blpop_list2", "blpop_list1", "1"], "$-1\r\n"), // timeout
        ], "blpop"; "blpop")]
    #[test_case(vec![
        (vec!["rpush", "brpop_list1", "a", "b", "c"], ":3\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\nc\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\nb\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "*2\r\n$11\r\nbrpop_list1\r\n$1\r\na\r\n"),
        (vec!["brpop", "brpop_list2", "brpop_list1", "1"], "$-1\r\n"), // timeout
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
        (vec!["lmpop", "2", "list1", "list2", "left", "COUNT", "1"], "$-1\r\n"),
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
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "right", "COUNT", "1"], "$-1\r\n"),
        (vec!["blmpop", "1", "2", "blmpop_list1", "blmpop_list2", "left", "COUNT"], "-ERR syntax error\r\n"),
        ], "blmpop"; "blmpop")]
    fn test_list_commands(args_vec: Vec<(Vec<&'static str>, &'static str)>, test_name: &str) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_store(test_name).unwrap();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args_vec {
                let mut sink = crate::tests::ResponseSink::with_name(test_name).await;
                let cmd = Rc::new(RedisCommand::for_test(args));
                match Client::handle_command(client.inner(), cmd.clone(), &mut sink.fp)
                    .await
                    .unwrap()
                {
                    ClientNextAction::TerminateConnection(_) => {}
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
                    ClientNextAction::Wait((rx, duration)) => {
                        println!("--> got Wait for duration of {:?}", duration);
                        let sw = crate::stopwatch::StopWatch::default();
                        let response = match Client::wait_for(rx, duration).await {
                            crate::client::WaitResult::TryAgain => {
                                println!(
                                    "--> got TryAgain after {}ms",
                                    sw.elapsed_micros().unwrap() / 1000
                                );
                                execute_command(client.inner(), cmd.clone()).await
                            }
                            crate::client::WaitResult::Timeout => {
                                let builder = RespBuilderV2::default();
                                let mut response = BytesMut::new();
                                builder.null_string(&mut response);
                                response
                            }
                        };
                        assert_eq!(BytesMutUtils::to_string(&response).as_str(), expected_value);
                    }
                }
            }
        });
    }

    /// Run a command that should be deferred by the server
    async fn deferred_command(
        client_state: Rc<ClientState>,
        cmd: Rc<RedisCommand>,
    ) -> (Receiver<u8>, Duration) {
        let mut sink = crate::tests::ResponseSink::with_name("deferred_command").await;
        let next_action = Client::handle_command(client_state, cmd, &mut sink.fp)
            .await
            .unwrap();
        match next_action {
            ClientNextAction::NoAction => panic!("expected to be blocked"),
            ClientNextAction::SendResponse(_) => panic!("expected to be blocked"),
            ClientNextAction::TerminateConnection(_) => panic!("expected to be blocked"),
            ClientNextAction::Wait((rx, duration)) => (rx, duration),
        }
    }

    /// Execute a command
    async fn execute_command(client_state: Rc<ClientState>, cmd: Rc<RedisCommand>) -> BytesMut {
        let mut sink = crate::tests::ResponseSink::with_name("deferred_command").await;
        let next_action = Client::handle_command(client_state, cmd.clone(), &mut sink.fp)
            .await
            .unwrap();
        match next_action {
            ClientNextAction::SendResponse(buffer) => buffer,
            ClientNextAction::NoAction => {
                BytesMutUtils::from_string(sink.read_all().await.as_str())
            }
            ClientNextAction::TerminateConnection(_) => {
                panic!("Command {:?} is not expected to be blocked", cmd)
            }
            ClientNextAction::Wait(_) => panic!("Command {:?} is not expected to be blocked", cmd),
        }
    }

    #[test]
    fn test_blocking_pop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_store("test_blocking_pop_2").unwrap();

            let server = Arc::<ServerState>::default();
            let reader = Client::new(server.clone(), store.clone(), None);
            let writer = Client::new(server, store, None);

            const EXPECTED_RESULT: &str = "*2\r\n$23\r\ntest_blocking_pop_list1\r\n$5\r\nvalue\r\n";
            let read_cmd = Rc::new(RedisCommand::for_test(vec![
                "blpop",
                "test_blocking_pop_list1",
                "test_blocking_pop_list2",
                "5",
            ]));

            // we expect to get a rx + duration, if we dont "deferred_command" will panic!
            let (rx, duration) = deferred_command(reader.inner(), read_cmd.clone()).await;

            // second connection: push data to the list
            let pus_cmd = Rc::new(RedisCommand::for_test(vec![
                "lpush",
                "test_blocking_pop_list1",
                "value",
            ]));
            let response = execute_command(writer.inner(), pus_cmd.clone()).await;
            assert_eq!(":1\r\n", BytesMutUtils::to_string(&response).as_str());

            // Try reading again now
            match Client::wait_for(rx, duration).await {
                crate::client::WaitResult::TryAgain => {
                    println!("consumer: got something - calling blpop again");
                    let response = execute_command(reader.inner(), read_cmd).await;
                    assert_eq!(
                        EXPECTED_RESULT,
                        BytesMutUtils::to_string(&response).as_str()
                    );
                }
                crate::client::WaitResult::Timeout => {
                    assert!(false, "consumer: Expected `TryAagain` not a `Timeout`!");
                }
            }
        });
    }
}
