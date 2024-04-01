use crate::{client::ClientState, commands::BaseCommands, storage::StringsDb};

use crate::{
    check_args_count, check_value_type, command_arg_at, command_arg_at_as_str,
    commands::SetFlags,
    commands::{ErrorStrings, HandleCommandResult},
    metadata::ValueTypeIs,
    parse_string_to_number,
    storage::PutFlags,
    to_number, to_number_ex, BytesMutUtils, CommonValueMetadata, LockManager, RedisCommand,
    RedisCommandName, RespBuilderV2, SableError, StringUtils, StringValueMetadata, Telemetry,
};

use bytes::BytesMut;
use num_traits::{Num, NumAssignOps};
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum SetInternalReturnValue {
    KeyExistsErr,
    KeyDoesNotExistErr,
    Success(Option<BytesMut>),
    SyntaxError,
    WrongType,
}

pub struct StringCommands {}

impl StringCommands {
    /// Main entry point for all string commands
    pub async fn handle_command(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::Append => {
                Self::append(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Set => {
                Self::set(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Get => {
                Self::get(client_state, command, response_buffer).await?;
            }
            RedisCommandName::GetDel => {
                Self::getdel(client_state, command, response_buffer).await?;
            }
            RedisCommandName::GetSet => {
                Self::getset(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Incr => {
                Self::incr(client_state, command, response_buffer).await?;
            }
            RedisCommandName::DecrBy => {
                Self::decrby(client_state, command, response_buffer).await?;
            }
            RedisCommandName::IncrBy => {
                Self::incrby(client_state, command, response_buffer).await?;
            }
            RedisCommandName::IncrByFloat => {
                Self::incrbyfloat(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Decr => {
                Self::decr(client_state, command, response_buffer).await?;
            }
            RedisCommandName::GetEx => {
                Self::getex(client_state, command, response_buffer).await?;
            }
            RedisCommandName::GetRange => {
                Self::getrange(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Lcs => {
                Self::lcs(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Mget => {
                Self::mget(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Mset => {
                Self::mset(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Msetnx => {
                Self::msetnx(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Psetex => {
                Self::psetex(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Setex => {
                Self::setex(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Setnx => {
                Self::setnx(client_state, command, response_buffer).await?;
            }
            RedisCommandName::SetRange => {
                Self::setrange(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Strlen => {
                Self::strlen(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Substr => {
                Self::substr(client_state, command, response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non string command `{}`",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::Completed)
    }

    /// Set key to hold the string value. If key already holds a value,
    /// it is overwritten, regardless of its type. Any previous time to
    /// live associated with the key is discarded on successful SET operation.
    async fn set(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let value = command_arg_at!(command, 2);

        let mut iter = command.args_vec().iter();
        iter.next(); // set
        iter.next(); // key
        iter.next(); // value

        let mut expiry: (Option<String>, Option<String>) = (None, None);
        let mut flags = SetFlags::None;
        while let Some(arg) = iter.next() {
            let arg_lowercase = BytesMutUtils::to_string(arg).to_lowercase();
            match arg_lowercase.as_str() {
                "ex" | "px" | "exat" | "pxat" => {
                    if let Some(val) = iter.next() {
                        expiry = (Some(arg_lowercase), Some(BytesMutUtils::to_string(val)));
                    } else {
                        expiry = (Some(arg_lowercase), None);
                    }
                }
                "keepttl" => flags |= SetFlags::KeepTtl,
                "xx" => flags |= SetFlags::SetIfExists,
                "nx" => flags |= SetFlags::SetIfNotExists,
                "get" => flags |= SetFlags::ReturnOldValue,
                _ => {
                    builder.error_string(response_buffer, "ERR syntax error");
                    return Ok(());
                }
            }
        }

        let result = Self::set_internal_with_locks(client_state, key, value, expiry, flags).await?;
        Self::handle_set_internal_result(result, response_buffer).await;
        Ok(())
    }

    /// If key already exists and is a string, this command appends the value at
    /// the end of the string. If key does not exist it is created and set as an
    /// empty string, so APPEND will be similar to SET in this special case.
    async fn append(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let builder = RespBuilderV2::default();

        // fetch the value
        let key = command_arg_at!(command, 1);
        let str_to_append = command_arg_at!(command, 2);

        // entering
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        if let Some((mut value, md)) = strings_db.get(key)? {
            // doing append
            check_value_type!(md, CommonValueMetadata::VALUE_STR, response_buffer);

            value.extend_from_slice(str_to_append);
            strings_db.put(key, &value, &md, PutFlags::Override)?;
            builder.number_usize(response_buffer, value.len());
        } else {
            // new value
            let metadata = StringValueMetadata::new();
            strings_db.put(key, str_to_append, &metadata, PutFlags::Override)?;
            builder.number_usize(response_buffer, str_to_append.len());
        }
        Ok(())
    }

    /// Get the value of key. If the key does not exist the special value nil is returned. An error
    /// is returned if the value stored at key is not a string, because GET only handles string
    /// values
    async fn get(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        check_args_count!(command, 2, response_buffer);
        let key = command_arg_at!(command, 1);

        // fetch the value
        let _unused = LockManager::lock_user_key_shared(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        match strings_db.get(key)? {
            Some((value, metadata)) => {
                check_value_type!(metadata, CommonValueMetadata::VALUE_STR, response_buffer);
                Telemetry::inc_db_hit();
                builder.bulk_string(response_buffer, &value);
            }
            None => {
                Telemetry::inc_db_miss();
                builder.null_string(response_buffer);
            }
        }
        Ok(())
    }

    /// Get the value of key and delete the key. This command is similar to GET,
    /// except for the fact that it also deletes the key on success (if and only if the key's value type is a string)
    async fn getdel(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        // start atomic operation here
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        if let Some((old_value, metadata)) = strings_db.get(key)? {
            check_value_type!(metadata, CommonValueMetadata::VALUE_STR, response_buffer);
            builder.bulk_string(response_buffer, &old_value);

            // delete the old value
            strings_db.delete(key)?;
        } else {
            builder.null_string(response_buffer);
        }
        Ok(())
    }

    /// Atomically sets key to value and returns the old value stored at key.
    /// Returns an error when key exists but does not hold a string value. Any
    /// previous time to live associated with the key is discarded on successful SET operation
    async fn getset(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let new_value = command_arg_at!(command, 2);

        // start atomic operation here
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        if let Some((old_value, metadata)) = strings_db.get(key)? {
            check_value_type!(metadata, CommonValueMetadata::VALUE_STR, response_buffer);
            builder.bulk_string(response_buffer, &old_value);
        } else {
            builder.null_string(response_buffer);
        }

        strings_db.put(
            key,
            new_value,
            &StringValueMetadata::new(),
            PutFlags::Override,
        )?;
        Ok(())
    }

    /// Get the value of key and optionally set its expiration. GETEX is similar to GET,
    /// but is a write command with additional options.
    async fn getex(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        let builder = RespBuilderV2::default();
        check_args_count!(command, 2, response_buffer);
        // fetch the value
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_shared(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        match strings_db.get(key)? {
            Some((value, mut metadata)) => {
                // ensure the key is of type string
                check_value_type!(metadata, CommonValueMetadata::VALUE_STR, response_buffer);

                Telemetry::inc_db_hit();
                builder.bulk_string(response_buffer, &value);

                let third_arg = command.arg_as_lowercase_string(2);
                let fourth_arg = command.arg_as_lowercase_string(3);

                match (third_arg.as_deref(), fourth_arg.as_deref()) {
                    (Some("ex"), Some(seconds)) => {
                        // seconds -- Set the specified expire time, in seconds
                        let num = parse_string_to_number!(seconds, response_buffer);
                        metadata.expiration_mut().set_ttl_seconds(num)?;
                        strings_db.put(key, &value, &metadata, PutFlags::Override)?;
                    }
                    (Some("px"), Some(milliseconds)) => {
                        // milliseconds -- Set the specified expire time, in milliseconds.
                        let num = parse_string_to_number!(milliseconds, response_buffer);
                        metadata.expiration_mut().set_ttl_millis(num)?;
                        strings_db.put(key, &value, &metadata, PutFlags::Override)?;
                    }
                    (Some("exat"), Some(unix_time_seconds)) => {
                        // timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
                        let num = parse_string_to_number!(unix_time_seconds, response_buffer);
                        metadata
                            .expiration_mut()
                            .set_expire_timestamp_seconds(num)?;
                        strings_db.put(key, &value, &metadata, PutFlags::Override)?;
                    }
                    (Some("pxat"), Some(unix_time_milliseconds)) => {
                        // timestamp-milliseconds -- Set the specified Unix time at which the
                        // key will expire, in milliseconds
                        let num = parse_string_to_number!(unix_time_milliseconds, response_buffer);
                        metadata.expiration_mut().set_expire_timestamp_millis(num)?;
                        strings_db.put(key, &value, &metadata, PutFlags::Override)?;
                    }
                    (Some("persist"), None) => {
                        // Remove the time to live associated with the key.
                        metadata.expiration_mut().set_no_expiration()?;
                        strings_db.put(key, &value, &metadata, PutFlags::Override)?;
                    }
                    (_, _) => {}
                }
            }
            None => {
                Telemetry::inc_db_miss();
                builder.null_string(response_buffer);
            }
        }
        Ok(())
    }

    /// Returns the substring of the string value stored at key, determined by the
    /// offsets start and end (both are inclusive). Negative offsets can be used in
    /// order to provide an offset starting from the end of the string. So -1 means
    /// the last character, -2 the penultimate and so forth.
    async fn getrange(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        // Check that the key exists
        let key = command_arg_at!(command, 1);
        let builder = RespBuilderV2::default();

        let _unused = LockManager::lock_user_key_shared(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        let result = strings_db.get(key)?;
        match result {
            Some((value, _)) => {
                let start = command_arg_at!(command, 2);
                let end = command_arg_at!(command, 3);
                let start_index = to_number!(start, i64, response_buffer, Ok(()));
                let end_index = to_number!(end, i64, response_buffer, Ok(()));

                // translate negative index into valid index in value
                let index_pair = BaseCommands::fix_range_indexes(&value, start_index, end_index);

                match index_pair {
                    Some((start, end)) => {
                        builder.bulk_string(response_buffer, &BytesMut::from(&value[start..end]));
                    }
                    None => {
                        builder.empty_string(response_buffer);
                    }
                }
            }
            None => {
                builder.empty_string(response_buffer);
            }
        }

        Ok(())
    }

    /// Decrements the number stored at key by one. If the key does not exist,
    /// it is set to 0 before performing the operation. An error is returned
    /// if the key contains a value of the wrong type or contains a string that can not be represented as integer.
    /// This operation is limited to 64 bit signed integers.
    async fn decr(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let result = if let Some((old_value, old_md)) = strings_db.get(key)? {
            check_value_type!(old_md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::incr_by_internal::<i64>(
                Some(&old_value),
                -1,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        } else {
            Self::incr_by_internal::<i64>(
                None,
                -1,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        };

        if let Some(result) = result {
            strings_db.put(
                key,
                &result,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// Increments the number stored at key by one. If the key does not exist,
    /// it is set to 0 before performing the operation. An error is returned
    /// if the key contains a value of the wrong type or contains a string
    /// that can not be represented as integer. This operation is limited to 64 bit signed integers.
    async fn incr(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let result = if let Some((old_value, old_md)) = strings_db.get(key)? {
            check_value_type!(old_md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::incr_by_internal::<i64>(
                Some(&old_value),
                1,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        } else {
            Self::incr_by_internal::<i64>(
                None,
                1,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        };

        if let Some(result) = result {
            strings_db.put(
                key,
                &result,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// Decrements the number stored at key by decrement. If the key does not exist,
    /// it is set to 0 before performing the operation. An error is returned if the
    /// key contains a value of the wrong type or contains a string that can not be
    /// represented as integer. This operation is limited to 64 bit signed integers.
    async fn decrby(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);
        let interval = command_arg_at!(command, 2);

        let decrement = to_number!(interval, i64, response_buffer, Ok(()));
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let result = if let Some((old_value, old_md)) = strings_db.get(key)? {
            check_value_type!(old_md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::incr_by_internal::<i64>(
                Some(&old_value),
                -decrement,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        } else {
            Self::incr_by_internal::<i64>(
                None,
                -decrement,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        };

        if let Some(result) = result {
            strings_db.put(
                key,
                &result,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// Increments the number stored at key by increment. If the key does not exist,
    /// it is set to 0 before performing the operation. An error is returned if the
    /// key contains a value of the wrong type or contains a string that can not be
    /// represented as integer. This operation is limited to 64 bit signed integers.
    async fn incrby(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);
        let interval = command_arg_at!(command, 2);

        let decrement = to_number!(interval, i64, response_buffer, Ok(()));
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let result = if let Some((old_value, old_md)) = strings_db.get(key)? {
            check_value_type!(old_md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::incr_by_internal::<i64>(
                Some(&old_value),
                decrement,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        } else {
            Self::incr_by_internal::<i64>(
                None,
                decrement,
                response_buffer,
                false,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            )
        };

        if let Some(result) = result {
            strings_db.put(
                key,
                &result,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// Increment the string representing a floating point number stored at key by the specified increment.
    /// By using a negative increment value, the result is that the value stored at the key is decremented
    /// (by the obvious properties of addition). If the key does not exist, it is set to 0 before performing
    /// the operation. An error is returned if one of the following conditions occur:
    async fn incrbyfloat(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);
        let interval = command_arg_at!(command, 2);
        let decrement = to_number_ex!(
            interval,
            f64,
            response_buffer,
            Ok(()),
            ErrorStrings::VALUE_NOT_VALID_FLOAT
        );

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let result = if let Some((old_value, old_md)) = strings_db.get(key)? {
            check_value_type!(old_md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::incr_by_internal::<f64>(
                Some(&old_value),
                decrement,
                response_buffer,
                true,
                ErrorStrings::VALUE_NOT_VALID_FLOAT,
            )
        } else {
            Self::incr_by_internal::<f64>(
                None,
                decrement,
                response_buffer,
                true,
                ErrorStrings::VALUE_NOT_VALID_FLOAT,
            )
        };

        if let Some(result) = result {
            strings_db.put(
                key,
                &result,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// The LCS command implements the longest common subsequence algorithm.
    /// Note that this is different than the longest common string algorithm, since matching characters
    /// in the string does not need to be contiguous.
    async fn lcs(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();
        let key1 = command_arg_at!(command, 1);
        let key2 = command_arg_at!(command, 2);

        let user_keys = vec![key1, key2];
        let _unused = LockManager::lock_user_keys_shared(&user_keys, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        // read the values for key1 and key2
        let Some((value1, _)) = strings_db.get(key1)? else {
            builder.empty_string(response_buffer);
            return Ok(());
        };

        let Some((value2, _)) = strings_db.get(key2)? else {
            builder.empty_string(response_buffer);
            return Ok(());
        };

        const MINMATCHLEN: &str = "minmatchlen";
        const LEN: &str = "len";
        const IDX: &str = "idx";
        #[allow(dead_code)]
        const WITHMATCHLEN: &str = "withmatchlen";

        // put all optional args after the mandatory ones in the map
        let mut extra_args_map = HashMap::<String, Option<String>>::new();
        let mut next_is_val_for_minmatchlen = false;
        for i in 3..command.arg_count() {
            let Some(extra_arg) = command.arg_as_lowercase_string(i) else {
                // this shouldn't happen
                return Err(SableError::InvalidArgument(
                    ErrorStrings::LCS_FAILED_TO_READ_EXTRA_ARG.to_string(),
                ));
            };

            if next_is_val_for_minmatchlen {
                extra_args_map.insert(MINMATCHLEN.to_string(), Some(extra_arg));
            } else if extra_arg.eq(MINMATCHLEN) {
                next_is_val_for_minmatchlen = true;
                extra_args_map.insert(extra_arg, None);
            } else {
                next_is_val_for_minmatchlen = false;
                extra_args_map.insert(extra_arg, None);
            }
        }

        // Check for conflicts
        if extra_args_map.contains_key(LEN) && extra_args_map.contains_key(IDX) {
            builder.error_string(response_buffer, ErrorStrings::LCS_LEN_AND_IDX);
            return Ok(());
        }

        // Default: no extra args
        if extra_args_map.is_empty() {
            let (lcs, _) = BytesMutUtils::lcs(&value1, &value2);
            builder.bulk_string(response_buffer, &lcs);
            return Ok(());
        }

        // Handle LEN
        if extra_args_map.contains_key(LEN) {
            let (lcs, _) = BytesMutUtils::lcs(&value1, &value2);
            builder.number::<usize>(response_buffer, lcs.len(), false);
            return Ok(());
        }

        // default: unsupported
        builder.error_string(response_buffer, ErrorStrings::LCS_UNSUPPORTED_ARGS);
        Ok(())
    }

    /// Returns the values of all specified keys. For every key that does not hold
    /// a string value or does not exist, the special value nil is returned.
    /// Because of this, the operation never fails.
    async fn mget(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let count = command.arg_count().saturating_sub(1);

        // prepare the response
        response_buffer.clear();
        builder.add_array_len(response_buffer, count);

        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name

        // build the list of keys to lock
        let mut user_keys = Vec::<&BytesMut>::with_capacity(command.arg_count());
        for key in iter {
            user_keys.push(key);
        }

        // obtain a shared lock on the keys
        let _unused = LockManager::lock_user_keys_shared(&user_keys, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        for key in user_keys.iter() {
            if let Some((value, metadata)) = strings_db.get(key)? {
                if metadata.is_type(CommonValueMetadata::VALUE_STR) {
                    builder.add_bulk_string(response_buffer, &value);
                } else {
                    // not a string value
                    builder.add_null_string(response_buffer);
                }
            } else {
                // key is not found
                builder.add_null_string(response_buffer);
            }
        }
        Ok(())
    }

    /// Sets the given keys to their respective values. `MSET` replaces existing values with new values,
    /// just as regular `SET`. See `MSETNX` if you don't want to overwrite existing values
    /// `MSET` is atomic, so all given keys are set at once. It is not possible for clients to see that
    /// some of the keys were updated while others are unchanged
    async fn mset(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 3 arguments
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();

        // we expect pairs of KEY:VALUE + MSET so always odd number
        if command.arg_count().rem_euclid(2) != 1 {
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'mset' command",
            );
            return Ok(());
        }

        // iterate over the pairs
        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name
        let mut keys_and_values =
            Vec::<(&BytesMut, &BytesMut)>::with_capacity(command.arg_count().saturating_div(2));
        let mut user_keys = Vec::<&BytesMut>::with_capacity(command.arg_count().saturating_div(2));
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            keys_and_values.push((key, value));
            user_keys.push(key);
        }

        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        strings_db.multi_put(&keys_and_values, PutFlags::Override)?;
        // can't fail
        builder.ok(response_buffer);
        Ok(())
    }

    /// Sets the given keys to their respective values. MSETNX will not perform any operation
    /// at all even if just a single key already exists.
    /// Because of this semantic MSETNX can be used in order to set different keys
    /// representing different fields of a unique logic object in a way that ensures that
    /// either all the fields or none at all are set.
    async fn msetnx(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 3 arguments
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();

        // we expect pairs of KEY:VALUE + MSET so always odd number
        if command.arg_count().rem_euclid(2) != 1 {
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'msetnx' command",
            );
            return Ok(());
        }

        // iterate over the pairs
        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name
        let mut keys_and_values =
            Vec::<(&BytesMut, &BytesMut)>::with_capacity(command.arg_count().saturating_div(2));
        let mut user_keys = Vec::<&BytesMut>::with_capacity(command.arg_count().saturating_div(2));
        while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            keys_and_values.push((key, value));
            user_keys.push(key);
        }

        // if any error occured, return `0`
        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        if strings_db.multi_put(&keys_and_values, PutFlags::PutIfNotExists)? {
            builder.number_i64(response_buffer, 1);
        } else {
            builder.number_i64(response_buffer, 0);
        }
        Ok(())
    }

    /// Set key to hold the string value and set key to timeout after a given number of seconds
    /// SETEX key seconds value
    async fn setex(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 4 arguments
        check_args_count!(command, 4, response_buffer);

        // SETEX key seconds value
        let key = command_arg_at!(command, 1);
        let timeout = command_arg_at_as_str!(command, 2);
        let value = command_arg_at!(command, 3);

        let result = Self::set_internal_with_locks(
            client_state,
            key,
            value,
            (Some("ex".to_string()), Some(timeout.to_string())),
            SetFlags::None,
        )
        .await?;
        Self::handle_set_internal_result(result, response_buffer).await;
        Ok(())
    }

    /// Set key to hold the string value and set key to timeout after a given number of milliseconds
    /// PSETEX key milliseconds value
    async fn psetex(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 4 arguments
        check_args_count!(command, 4, response_buffer);

        // PSETEX key milliseconds value
        let key = command_arg_at!(command, 1);
        let timeout = command_arg_at_as_str!(command, 2);
        let value = command_arg_at!(command, 3);

        let result = Self::set_internal_with_locks(
            client_state,
            key,
            value,
            (Some("px".to_string()), Some(timeout.to_string())),
            SetFlags::None,
        )
        .await?;
        Self::handle_set_internal_result(result, response_buffer).await;
        Ok(())
    }

    /// Set key to hold string value if key does not exist. In that case, it is equal to SET.
    /// When key already holds a value, no operation is performed. SETNX is short for "SET if Not eXists".
    async fn setnx(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let key = command_arg_at!(command, 1);
        let value = command_arg_at!(command, 2);

        let result = Self::set_internal_with_locks(
            client_state,
            key,
            value,
            (None, None),
            SetFlags::SetIfNotExists,
        )
        .await?;
        Self::handle_set_internal_result(result, response_buffer).await;
        Ok(())
    }

    /// Overwrites part of the string stored at key, starting at the specified offset, for
    /// the entire length of value. If the offset is larger than the current length of the
    /// string at key, the string is padded with zero-bytes to make offset fit. Non-existing
    /// keys are considered as empty strings, so this command will make sure it holds a
    /// string large enough to be able to set value at offset.
    async fn setrange(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let key = command_arg_at!(command, 1);
        let offset = command_arg_at!(command, 2);
        let value = command_arg_at!(command, 3);

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        let new_value = if let Some((old_value, md)) = strings_db.get(key)? {
            check_value_type!(md, CommonValueMetadata::VALUE_STR, response_buffer);
            Self::setrange_internal(key, Some(&old_value), Some(value), offset, response_buffer)
        } else {
            Self::setrange_internal(key, None, Some(value), offset, response_buffer)
        };

        if let Some(new_value) = new_value {
            strings_db.put(
                key,
                &new_value,
                &StringValueMetadata::new(),
                PutFlags::Override,
            )?;
        }
        Ok(())
    }

    /// Returns the length of the string value stored at key.
    /// An error is returned when key holds a non-string value
    async fn strlen(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_shared(key, client_state.db_id);
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);

        if let Some((value, md)) = strings_db.get(key)? {
            check_value_type!(md, CommonValueMetadata::VALUE_STR, response_buffer);
            builder.number::<usize>(response_buffer, value.len(), false);
        } else {
            builder.number_u64(response_buffer, 0);
        }
        Ok(())
    }

    /// An alias to `GETRANGE KEY START END`
    async fn substr(
        client_state: &ClientState,
        command: &RedisCommand,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        // redirect to getrange
        Self::getrange(client_state, command, response_buffer).await
    }

    /// Set `key` with `value`.
    /// `store` the underlying storage
    /// `key` the key
    /// `value` the new value
    /// `expiry`:
    ///     - `ex` seconds -- Set the specified expire time, in seconds
    ///     - `px` milliseconds -- Set the specified expire time, in milliseconds.
    ///     - `exat` timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    ///     - `pxat` timestamp-milliseconds -- Set the specified Unix time at which the key will expire
    ///         , in milliseconds.
    /// `flags` possible bitwise combination of `SetFlags` bits
    pub async fn set_internal_with_locks(
        client_state: &ClientState,
        user_key: &BytesMut,
        value: &BytesMut,
        expiry: (Option<String>, Option<String>),
        flags: SetFlags,
    ) -> Result<SetInternalReturnValue, SableError> {
        let mut metadata = StringValueMetadata::new();
        let mut return_value: Option<BytesMut> = None;

        let strings_db = StringsDb::with_storage(&client_state.store, client_state.db_id);
        // sanity
        if flags.intersects(SetFlags::SetIfNotExists) && flags.intersects(SetFlags::SetIfExists) {
            return Ok(SetInternalReturnValue::SyntaxError);
        }

        // choose the correct lock
        let _unused = if flags
            .intersects(SetFlags::SetIfNotExists | SetFlags::SetIfExists | SetFlags::ReturnOldValue)
        {
            // requires exclusive lock
            LockManager::lock_user_key_exclusive(user_key, client_state.db_id)
        } else {
            // shared lock is enough here
            LockManager::lock_user_key_shared(user_key, client_state.db_id)
        };

        if flags.intersects(
            SetFlags::ReturnOldValue
                | SetFlags::KeepTtl
                | SetFlags::SetIfExists
                | SetFlags::SetIfNotExists,
        ) {
            if let Some((old_value, old_metadata)) = strings_db.get(user_key)? {
                // key exists
                if flags.intersects(SetFlags::SetIfNotExists) {
                    // key exists, but `SetIfNotExists` is set
                    return Ok(SetInternalReturnValue::KeyExistsErr);
                }
                // keep the old ttl?
                if flags.intersects(SetFlags::KeepTtl) {
                    metadata
                        .expiration_mut()
                        .set_ttl_millis(old_metadata.expiration().ttl_in_millis()?)?;
                }

                // return the old value?
                if flags.intersects(SetFlags::ReturnOldValue) {
                    if !old_metadata.is_type(CommonValueMetadata::VALUE_STR) {
                        return Ok(SetInternalReturnValue::WrongType);
                    }
                    return_value = Some(old_value);
                }
            } else if flags.intersects(SetFlags::SetIfExists) {
                // key does not exists, but `SetIfExists` is set
                return Ok(SetInternalReturnValue::KeyDoesNotExistErr);
            }
        }

        match expiry {
            (Some(cmd), Some(val)) => {
                let Ok(num) = StringUtils::parse_str_to_number::<u64>(&val) else {
                    return Ok(SetInternalReturnValue::SyntaxError);
                };
                match cmd.as_str() {
                    "ex" => metadata.expiration_mut().set_ttl_seconds(num)?,
                    "px" => metadata.expiration_mut().set_ttl_millis(num)?,
                    "exat" => metadata
                        .expiration_mut()
                        .set_expire_timestamp_seconds(num)?,
                    "pexat" => metadata.expiration_mut().set_expire_timestamp_millis(num)?,
                    _ => return Ok(SetInternalReturnValue::SyntaxError),
                }
            }
            (None, None) => {
                // this is fine
            }
            (_, _) => {
                return Ok(SetInternalReturnValue::SyntaxError);
            }
        }

        if flags.intersects(SetFlags::SetIfNotExists) {
            strings_db.put(user_key, value, &metadata, PutFlags::PutIfNotExists)?;
        } else if flags.intersects(SetFlags::SetIfExists) {
            strings_db.put(user_key, value, &metadata, PutFlags::PutIfExists)?;
        } else {
            // shared lock is enough here
            strings_db.put(user_key, value, &metadata, PutFlags::Override)?;
        }
        Ok(SetInternalReturnValue::Success(return_value))
    }

    /// Build the response buffer based on the result from `set_internal` output
    pub async fn handle_set_internal_result(
        result: SetInternalReturnValue,
        response_buffer: &mut BytesMut,
    ) {
        let builder = RespBuilderV2::default();
        match result {
            SetInternalReturnValue::KeyDoesNotExistErr | SetInternalReturnValue::KeyExistsErr => {
                builder.null_string(response_buffer)
            }
            SetInternalReturnValue::SyntaxError => {
                builder.error_string(response_buffer, ErrorStrings::SYNTAX_ERROR)
            }
            SetInternalReturnValue::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE)
            }
            SetInternalReturnValue::Success(Some(old_value)) => {
                builder.bulk_string(response_buffer, &old_value);
            }
            SetInternalReturnValue::Success(None) => {
                builder.ok(response_buffer);
            }
        }
    }

    fn setrange_internal(
        _key: &BytesMut,
        old_value: Option<&BytesMut>,
        new_value: Option<&BytesMut>,
        offset: &BytesMut,
        response_buffer: &mut BytesMut,
    ) -> Option<BytesMut> {
        let builder = RespBuilderV2::default();

        let Some(value) = new_value else {
            builder.error_string(response_buffer, "ERR wrong number of arguments for command");
            return None;
        };

        let offset = to_number!(offset, usize, response_buffer, None);

        let mut new_value_mut = BytesMut::new();
        let new_value = if let Some(old_value) = old_value {
            // Copy the old value to our result
            new_value_mut.extend_from_slice(old_value);
            if offset + value.len() >= new_value_mut.len() {
                // extend old_value
                new_value_mut.resize(offset + value.len(), 0u8);
            }

            new_value_mut[offset..].clone_from_slice(value);

            // use the old Metadata + modified value
            &new_value_mut
        } else {
            // new Metadata + new value
            new_value_mut.resize(value.len() + offset, 0u8);
            new_value_mut[offset..].clone_from_slice(value);
            &new_value_mut
        };

        builder.number_u64(
            response_buffer,
            new_value.len().try_into().unwrap_or(u64::MAX),
        );

        Some(new_value.clone())
    }

    fn incr_by_internal<N: Num + Display + FromStr + NumAssignOps>(
        old_value: Option<&BytesMut>,
        incr_by: N,
        response_buffer: &mut BytesMut,
        is_float: bool,
        parse_error: &'static str,
    ) -> Option<BytesMut> {
        let builder = RespBuilderV2::default();

        // in case we won't find it, set it to 0
        let mut number: N = N::zero();
        if let Some(old_value) = old_value {
            let old_number = to_number_ex!(old_value, N, response_buffer, None, parse_error);
            number = old_number;
        }

        // decr by 1
        number += incr_by;

        // build the response buffer
        let number_as_bytes = BytesMutUtils::from(&number);
        builder.number::<N>(response_buffer, number, is_float);
        Some(number_as_bytes)
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
        commands::ClientNextAction, storage::StorageAdapter, Client, GenericCommands, ServerState,
        StorageOpenParams,
    };
    use std::path::PathBuf;
    use std::sync::{Arc, Once};
    use test_case::test_case;

    lazy_static::lazy_static! {
        static ref INIT: Once = Once::new();
    }

    async fn initialise_test() {
        INIT.call_once(|| {
            let _ = std::fs::remove_dir_all("tests/string_commands");
            let _ = std::fs::create_dir_all("tests/string_commands");
        });
    }

    /// Initialise the database
    async fn open_database(command_name: &str) -> StorageAdapter {
        // Cleanup the previous test folder
        initialise_test().await;

        // create random file name
        let db_file = format!("tests/string_commands/{}.db", command_name,);
        let _ = std::fs::create_dir_all("tests/string_commands");
        let db_path = PathBuf::from(&db_file);
        let _ = std::fs::remove_dir_all(&db_file);
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);
        crate::storage_rocksdb!(open_params)
    }

    // The commands below are executed in serialised manner. So each command
    // "remembers" the outcome of the previous command
    #[test_case(vec![
        (vec!["set", "set_key", "value"], "+OK\r\n"),
        (vec!["get", "set_key"], "$5\r\nvalue\r\n"),
        (vec!["append", "set_key", "value2"], ":11\r\n"),
        (vec!["get", "set_key"], "$11\r\nvaluevalue2\r\n"),
        (vec!["append", "set_key_no_such_key", "value"], ":5\r\n"),
        (vec!["get", "set_key_no_such_key"], "$5\r\nvalue\r\n"),
        ], "set_get_append"; "set get append")]
    #[test_case(vec![
        (vec!["getdel", "test_getdel_no_such_key"], "$-1\r\n"),
        (vec!["set", "test_getdel_key", "value"], "+OK\r\n"),
        (vec!["getdel", "test_getdel_key"], "$5\r\nvalue\r\n"),
        (vec!["get", "test_getdel_key"], "$-1\r\n"),
        ], "getdel"; "getdel")]
    #[test_case(vec![
        (vec!["decrby", "test_decr_by_counter", "10"], ":-10\r\n"),
        (vec!["decrby", "test_decr_by_counter", "5"], ":-15\r\n"),
        (vec!["decrby", "test_decr_by_counter", "-5"], ":-10\r\n"),
        ], "decrby"; "decrby")]
    #[test_case(vec![
        (vec!["incrby", "test_incr_by_counter", "10"], ":10\r\n"),
        (vec!["incrby", "test_incr_by_counter", "5"], ":15\r\n"),
    ], "incrby"; "incrby")]
    #[test_case(vec![
        (vec!["incr", "no_such_incr_counter"], ":1\r\n"),
        (vec!["incr", "no_such_incr_counter"], ":2\r\n"),
    ], "incr"; "incr")]
    #[test_case(vec![
        (vec!["decr", "no_such_decr_counter"], ":-1\r\n"),
        (vec!["decr", "no_such_decr_counter"], ":-2\r\n"),
    ], "decr"; "decr")]
    #[test_case(vec![
        (vec!["set", "getex_key", "value"], "+OK\r\n"),
        (vec!["getex", "getex_key", "ex", "3"], "$5\r\nvalue\r\n"),
        (vec!["ttl", "getex_key"], ":3\r\n"),
    ], "getex"; "getex")]
    #[test_case(vec![
        (vec!["set", "getrange_key", "value"], "+OK\r\n"),
        (vec!["getrange", "getrange_key", "0", "-1"], "$5\r\nvalue\r\n"),
        (vec!["getrange", "getrange_key", "0", "0"], "$1\r\nv\r\n"),
        (vec!["getrange", "getrange_key", "0", "10000"], "$5\r\nvalue\r\n"),
        (vec!["getrange", "getrange_key", "100000", "10000"], "$0\r\n\r\n"),
        (vec!["getrange", "getrange_key", "-2", "-1"], "$2\r\nue\r\n"),
        (vec!["getrange", "getrange_key", "-1000", "-1000"], "$1\r\nv\r\n"),
    ], "getrange"; "getrange")]
    #[test_case(vec![
        (vec!["getset", "getset_no_such_key", "value"], "$-1\r\n"),
        (vec!["get", "getset_no_such_key"], "$5\r\nvalue\r\n"),
        (vec!["getset", "getset_no_such_key", "newvalue"], "$5\r\nvalue\r\n"),
        (vec!["get", "getset_no_such_key"], "$8\r\nnewvalue\r\n"),
        (vec!["getset", "getset_2nd_key"], "-ERR wrong number of arguments for 'getset' command\r\n"),
    ], "getset"; "getset")]
    #[test_case(vec![
        (vec!["incrbyfloat", "incrbyfloat_no_such_key", "0.1"], ",0.1\r\n"),
        (vec!["incrbyfloat", "incrbyfloat_no_such_key", "1.2"], ",1.3\r\n"),
        (vec!["set", "incrbyfloat_string", "hello"], "+OK\r\n"),
        (vec!["incrbyfloat", "incrbyfloat_string", "9.9"], "-ERR value is not a valid float\r\n"),
        (vec!["incrbyfloat", "incrbyfloat_string"], "-ERR wrong number of arguments for 'incrbyfloat' command\r\n"),
    ], "incrbyfloat"; "incrbyfloat")]
    #[test_case(vec![
        (vec!["set", "lcs_key1", "fo12o345b67ar"], "+OK\r\n"),
        (vec!["set", "lcs_key2", "f8oo9xbyzaqwr[]"], "+OK\r\n"),
        (vec!["lcs", "lcs_key1", "lcs_key2"], "$6\r\nfoobar\r\n"),
        (vec!["lcs", "lcs_key1", "lcs_key2", "len"], ":6\r\n"),
        (vec!["lcs", "lcs_key1", "lcs_key2", "idx"], "-ERR unsupported arguments for command 'lcs'\r\n"),
    ], "lcs"; "lcs")]
    #[test_case(vec![
        (vec!["set", "mget_key1", "value"], "+OK\r\n"),
        (vec!["set", "mget_key2", "value"], "+OK\r\n"),
        (vec!["set", "mget_key3", "value"], "+OK\r\n"),
        (vec!["mget", "mget_key1", "mget_key2", "mget_no_such_key", "mget_key3"], "*4\r\n$5\r\nvalue\r\n$5\r\nvalue\r\n$-1\r\n$5\r\nvalue\r\n"),
    ], "mget"; "mget")]
    #[test_case(vec![
        (vec!["mset", "mset_key1", "value1", "mset_key2", "value2"], "+OK\r\n"),
        (vec!["get", "mset_key1"], "$6\r\nvalue1\r\n"),
        (vec!["get", "mset_key2"], "$6\r\nvalue2\r\n"),
        (vec!["mset", "mset_key3"], "-ERR wrong number of arguments for 'mset' command\r\n"),
        (vec!["mset", "mset_key1", "value1","mset_key2"], "-ERR wrong number of arguments for 'mset' command\r\n"),
    ], "mset"; "mset")]
    #[test_case(vec![
        (vec!["msetnx", "msetnx_key1", "value1", "msetnx_key2", "value2"], ":1\r\n"),
        (vec!["msetnx", "msetnx_key1", "value2", "msetnx_key3", "value3"], ":0\r\n"),
        (vec!["msetnx", "msetnx_key1"], "-ERR wrong number of arguments for 'msetnx' command\r\n"),
    ], "msetnx"; "msetnx")]
    #[test_case(vec![
        (vec!["psetex", "psetex_key1", "42", "value"], "+OK\r\n"),
        (vec!["get", "psetex_key1"], "$5\r\nvalue\r\n"),
        (vec!["psetex"], "-ERR wrong number of arguments for 'psetex' command\r\n"),
    ], "psetex"; "psetex")]
    #[test_case(vec![
        (vec!["set", "set_key1", "value"], "+OK\r\n"),
        (vec!["get", "set_key1"], "$5\r\nvalue\r\n"),
        (vec!["set", "set_key1", "value2", "nx"], "$-1\r\n"),
        (vec!["set", "set_key1", "value2", "xx"], "+OK\r\n"),
        (vec!["set", "set_key1", "value3", "GET"], "$6\r\nvalue2\r\n"),
        (vec!["set", "set_key1", "value4", "EX", "100"], "+OK\r\n"),
        (vec!["ttl", "set_key1"], ":100\r\n"),
        (vec!["set", "set_key1", "value4", "PX", "3000"], "+OK\r\n"),
        (vec!["ttl", "set_key1"], ":3\r\n"),
    ], "set"; "set")]
    #[test_case(vec![
        (vec!["setex", "setex_key1", "value"], "-ERR wrong number of arguments for 'setex' command\r\n"),
        (vec!["setex", "setex_key1", "42", "value"], "+OK\r\n"),
        (vec!["ttl", "setex_key1"], ":42\r\n"),
    ], "setex"; "setex")]
    #[test_case(vec![
        (vec!["set", "setnx_key", "value"], "+OK\r\n"),
        (vec!["setnx", "setnx_key", "value2"], "$-1\r\n"),
        (vec!["setnx", "setnx_key_2", "value2"], "+OK\r\n"),
        (vec!["get", "setnx_key"], "$5\r\nvalue\r\n"),
        (vec!["get", "setnx_key_2"], "$6\r\nvalue2\r\n"),
    ], "setnx"; "setnx")]
    #[test_case(vec![
        (vec!["setrange", "key1", "0", "hello"], ":5\r\n"),
        (vec!["get", "key1"], "$5\r\nhello\r\n"),
        (vec!["setrange", "key1", "5", " world"], ":11\r\n"),
        (vec!["get", "key1"], "$11\r\nhello world\r\n"),
        // Check that padding string works
        (vec!["setrange", "key2", "5", " world"], ":11\r\n"),
        (vec!["get", "key2"], "$11\r\n\0\0\0\0\0 world\r\n"),
    ], "setrange"; "setrange")]
    #[test_case(vec![
        (vec!["strlen", "key1"], ":0\r\n"), // key does not exist
        (vec!["set", "key1", "value"], "+OK\r\n"),
        (vec!["strlen", "key1"], ":5\r\n"),
    ], "strlen"; "strlen")]
    #[test_case(vec![
        (vec!["set", "key1", "This is a string"], "+OK\r\n"),
        (vec!["substr", "key1", "0", "3"], "$4\r\nThis\r\n"),
        (vec!["substr", "key1", "0", "-1"], "$16\r\nThis is a string\r\n"),
        (vec!["substr"], "-ERR wrong number of arguments for 'substr' command\r\n"),
        (vec!["substr", "key1"], "-ERR wrong number of arguments for 'substr' command\r\n"),
        (vec!["substr", "key1", "0"], "-ERR wrong number of arguments for 'substr' command\r\n"),
    ], "substr"; "substr")]
    fn test_string_commands(
        args_vec: Vec<(Vec<&'static str>, &'static str)>,
        test_name: &str,
    ) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            println!("opening db");
            let store = open_database(test_name).await;

            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args_vec {
                let cmd = RedisCommand::for_test(args);
                match Client::handle_command(client.inner(), cmd).await.unwrap() {
                    ClientNextAction::SendResponse(response_buffer) => {
                        assert_eq!(
                            BytesMutUtils::to_string(&response_buffer).as_str(),
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
    fn test_getex() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_database("test_getex").await;
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            let mut response_buffer = BytesMut::new();
            let mut cmd = RedisCommand::for_test(vec!["set", "test_getex_k1", "value"]);
            let _ = StringCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "+OK\r\n"
            );

            // the key has no TTL associated, report -1
            let mut cmd = RedisCommand::for_test(vec!["ttl", "test_getex_k1"]);
            let _ = GenericCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                ":-1\r\n"
            );

            let mut cmd = RedisCommand::for_test(vec!["getex", "test_getex_k1", "EX", "3"]);
            let _ = StringCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$5\r\nvalue\r\n"
            );

            std::thread::sleep(std::time::Duration::from_millis(1500)); // we round UP, so we are left with 1500 ms -> 2 seconds
            let mut cmd = RedisCommand::for_test(vec!["ttl", "test_getex_k1"]);
            let _ = GenericCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                ":2\r\n"
            );
            std::thread::sleep(std::time::Duration::from_millis(2000)); // Sleep for another 2 seconds, to expire the item

            // the key now does not exist
            let mut cmd = RedisCommand::for_test(vec!["ttl", "test_getex_k1"]);
            let _ = GenericCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                ":-2\r\n"
            )
        });
    }

    #[test]
    fn test_write_on_replica() {
        // create a WRITE command and try to execute it against replica server
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_database("test_write_on_replica").await;
            let client = Client::new(Arc::<ServerState>::default(), store, None);
            client.inner().server_state.set_replica();
            let cmd = RedisCommand::for_test(vec!["set", "test_write_on_replica", "1"]);
            match Client::handle_command(client.inner(), cmd).await.unwrap() {
                ClientNextAction::SendResponse(response_buffer) => {
                    assert_eq!(
                        BytesMutUtils::to_string(&response_buffer).as_str(),
                        &format!("-{}\r\n", ErrorStrings::WRITE_CMD_AGAINST_REPLICA)
                    );
                }
                _ => {}
            }
        });
    }

    #[test]
    fn test_key_with_timeout() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_database("key_with_timeout").await;
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            let mut response_buffer = BytesMut::new();
            let mut cmd =
                RedisCommand::for_test(vec!["psetex", "test_key_with_timeout_k", "10", "value"]);
            let _ = StringCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "+OK\r\n"
            );

            let mut cmd = RedisCommand::for_test(vec!["get", "test_key_with_timeout_k"]);
            let _ = StringCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$5\r\nvalue\r\n"
            );

            // sleep until the key expires
            std::thread::sleep(std::time::Duration::from_millis(20));

            // the key should now be expired
            let mut cmd = RedisCommand::for_test(vec!["get", "test_key_with_timeout_k"]);
            let _ = StringCommands::handle_command(client.inner(), &mut cmd, &mut response_buffer)
                .await;
            assert_eq!(
                BytesMutUtils::to_string(&response_buffer).as_str(),
                "$-1\r\n"
            );
        });
    }
}
