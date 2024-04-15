#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type,
    client::ClientState,
    command_arg_at,
    commands::{ErrorStrings, HandleCommandResult, StringCommands},
    metadata::Encoding,
    metadata::{CommonValueMetadata, HashFieldKey, HashValueMetadata},
    parse_string_to_number,
    storage::{
        GenericDb, GetHashMetadataResult, HashDb, HashDeleteResult, HashExistsResult,
        HashGetMultiResult, HashGetResult, HashLenResult, HashPutResult,
    },
    types::List,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use crate::io::RespWriter;
use crate::storage::StorageIterator;
use bytes::BytesMut;
use rand::prelude::*;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct HashCommands {}

#[derive(Eq, PartialEq)]
enum HGetAllOutput {
    Keys,
    Values,
    Both,
}

impl HashCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            RedisCommandName::Hset => {
                Self::hset(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hmset => {
                // HMSET is identical to HSET with different response (it responds with OK)
                Self::hset(client_state, command, &mut response_buffer).await?;
                if response_buffer.starts_with(b":") {
                    // incase of a success, change the response into "+OK\r\n"
                    let builder = RespBuilderV2::default();
                    builder.ok(&mut response_buffer);
                }
            }
            RedisCommandName::Hget => {
                Self::hget(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hdel => {
                Self::hdel(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hlen => {
                Self::hlen(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hexists => {
                Self::hexists(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hincrbyfloat => {
                Self::hincrbyfloat(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hincrby => {
                Self::hincrby(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Hgetall => {
                // write directly to the client
                Self::hgetall(client_state, command, tx, HGetAllOutput::Both).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Hkeys => {
                // write directly to the client
                Self::hgetall(client_state, command, tx, HGetAllOutput::Keys).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Hvals => {
                // write directly to the client
                Self::hgetall(client_state, command, tx, HGetAllOutput::Values).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Hmget => {
                // write directly to the client
                Self::hmget(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Hrandfield => {
                // write directly to the client
                Self::hrandfield(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non hash command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Sets the specified fields to their respective values in the hash stored at key.
    async fn hset(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let builder = RespBuilderV2::default();

        let mut iter = command.args_vec().iter();
        iter.next(); // skip "hset"
        iter.next(); // skips the key

        let mut field_values =
            Vec::<(&BytesMut, &BytesMut)>::with_capacity(command.arg_count().saturating_div(2));
        // hset key <field> <value> [<field><value>..]
        loop {
            let (field, value) = (iter.next(), iter.next());
            match (field, value) {
                (Some(field), Some(value)) => {
                    field_values.push((field, value));
                }
                (None, None) => break,
                (_, _) => {
                    // either we got even field:value pairs
                    // or its an error
                    builder.error_string(
                        response_buffer,
                        "ERR wrong number of arguments for 'hset' command",
                    );
                    return Ok(());
                }
            }
        }

        if field_values.is_empty() {
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'hset' command",
            );
            return Ok(());
        }

        let key = command_arg_at!(command, 1);
        // Hash write updating 2 entries + doing read, so we need to exclusive lock it
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let items_put = match hash_db.put_multi(key, &field_values)? {
            HashPutResult::Some(count) => count,
            HashPutResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
        };

        builder.number_usize(response_buffer, items_put);
        Ok(())
    }

    /// Returns the value associated with field in the hash stored at key.
    async fn hget(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();

        let key = command_arg_at!(command, 1);
        let field = command_arg_at!(command, 2);

        // Multiple db calls: exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        match hash_db.get(key, field)? {
            HashGetResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
            }
            HashGetResult::Some(value) => {
                // update telemetries
                Telemetry::inc_db_hit();
                builder.bulk_string(response_buffer, &value);
            }
            HashGetResult::NotFound | HashGetResult::FieldNotFound => {
                // update telemetries
                Telemetry::inc_db_miss();
                builder.null_string(response_buffer);
            }
        };
        Ok(())
    }

    /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash
    /// are ignored. If key does not exist, it is treated as an empty hash and this command returns 0
    async fn hdel(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // skip "hset"
        iter.next(); // skips the key

        // hdel key <field> <value> [<field><value>..]
        let mut fields = Vec::<&BytesMut>::with_capacity(command.arg_count().saturating_sub(2));
        for field in iter {
            fields.push(field);
        }

        if fields.is_empty() {
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'hdel' command",
            );
            return Ok(());
        }

        // Multiple db calls: exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let items_put = match hash_db.delete(key, &fields)? {
            HashDeleteResult::Some(count) => count,
            HashDeleteResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
        };

        builder.number_usize(response_buffer, items_put);
        Ok(())
    }

    /// Returns the number of fields contained in the hash stored at key
    async fn hlen(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        // Lock and delete
        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let count = match hash_db.len(key)? {
            HashLenResult::Some(count) => count,
            HashLenResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
        };
        builder.number_usize(response_buffer, count);
        Ok(())
    }

    /// Returns the number of fields contained in the hash stored at key
    async fn hexists(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let field = command_arg_at!(command, 2);

        // Lock and delete
        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        match hash_db.field_exists(key, field)? {
            HashExistsResult::NotExists => builder.number_usize(response_buffer, 0),
            HashExistsResult::Exists => builder.number_usize(response_buffer, 1),
            HashExistsResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE)
            }
        };
        Ok(())
    }

    /// Returns the number of fields contained in the hash stored at key
    async fn hgetall(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
        output_type: HGetAllOutput,
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);

        let max_response_buffer = client_state
            .server_inner_state()
            .options()
            .client_limits
            .client_response_buffer_size;

        let mut writer = RespWriter::new(tx, 4096, max_response_buffer);
        let key = command_arg_at!(command, 1);

        // multiple db access -> use exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());
        let hash_md = match hash_db.hash_metadata(key)? {
            GetHashMetadataResult::WrongType => {
                writer.error_string(ErrorStrings::WRONGTYPE).await?;
                writer.flush().await?;
                return Ok(());
            }
            GetHashMetadataResult::NotFound => {
                writer.empty_array().await?;
                writer.flush().await?;
                return Ok(());
            }
            GetHashMetadataResult::Some(hash_md) => hash_md,
        };

        // empty hash? empty array
        if hash_md.is_empty() {
            writer.empty_array().await?;
            writer.flush().await?;
            return Ok(());
        }

        // Write the length
        writer
            .add_array_len(
                hash_md
                    .len()
                    .saturating_mul(if output_type == HGetAllOutput::Both {
                        2
                    } else {
                        1
                    })
                    .try_into()
                    .unwrap_or(usize::MAX),
            )
            .await?;

        let prefix = Rc::new(hash_md.prefix());
        match client_state.database().create_iterator(prefix.clone())? {
            StorageIterator::RocksDb(mut rocksdb_iter) => {
                while rocksdb_iter.valid() {
                    // get the key & value
                    let Some(key) = rocksdb_iter.key() else {
                        break;
                    };

                    if !key.starts_with(prefix.as_ref()) {
                        break;
                    }

                    let Some(value) = rocksdb_iter.value() else {
                        break;
                    };

                    // extract the key from the row data
                    let hash_field_key = HashFieldKey::from_bytes(key)?;
                    match output_type {
                        HGetAllOutput::Keys => {
                            writer.add_bulk_string(hash_field_key.key()).await?;
                        }
                        HGetAllOutput::Values => {
                            writer.add_bulk_string(value).await?;
                        }
                        HGetAllOutput::Both => {
                            writer.add_bulk_string(hash_field_key.key()).await?;
                            writer.add_bulk_string(value).await?;
                        }
                    }
                    rocksdb_iter.next();
                }
            }
        };

        writer.flush().await?;
        Ok(())
    }

    /// Increments the number stored at field in the hash stored at key by increment.
    /// If key does not exist, a new key holding a hash is created. If field does not exist the value is set to 0
    /// before the operation is performed.
    /// The range of values supported by HINCRBY is limited to 64 bit signed integers.
    async fn hincrby(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let field = command_arg_at!(command, 2);
        let increment = command_arg_at!(command, 3);

        let Some(increment) = BytesMutUtils::parse::<i64>(increment) else {
            builder.error_string(
                response_buffer,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            );
            return Ok(());
        };

        // Lock and delete
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let prev_value = match hash_db.get(key, field)? {
            HashGetResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            HashGetResult::NotFound | HashGetResult::FieldNotFound => 0i64,
            HashGetResult::Some(value) => {
                let Some(value) = BytesMutUtils::parse::<i64>(&value) else {
                    builder.error_string(response_buffer, "ERR hash value is not an integer");
                    return Ok(());
                };
                value
            }
        };

        let new_value = prev_value + increment;
        builder.number::<i64>(response_buffer, new_value, false);

        // store the new value
        let new_value = BytesMutUtils::from::<i64>(&new_value);
        let _ = hash_db.put_multi(key, &[(field, &new_value)])?;
        Ok(())
    }

    /// Increments the number stored at field in the hash stored at key by increment.
    /// If key does not exist, a new key holding a hash is created. If field does not exist the value is set to 0
    /// before the operation is performed.
    /// The range of values supported by HINCRBY is limited to 64 bit signed integers.
    async fn hincrbyfloat(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let field = command_arg_at!(command, 2);
        let increment = command_arg_at!(command, 3);

        let Some(increment) = BytesMutUtils::parse::<f64>(increment) else {
            builder.error_string(
                response_buffer,
                ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            );
            return Ok(());
        };

        // Lock and delete
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let prev_value = match hash_db.get(key, field)? {
            HashGetResult::WrongType => {
                builder.error_string(response_buffer, ErrorStrings::WRONGTYPE);
                return Ok(());
            }
            HashGetResult::NotFound | HashGetResult::FieldNotFound => 0f64,
            HashGetResult::Some(value) => {
                let Some(value) = BytesMutUtils::parse::<f64>(&value) else {
                    builder.error_string(response_buffer, "ERR hash value is not an integer");
                    return Ok(());
                };
                value
            }
        };

        let new_value = prev_value + increment;
        builder.number::<f64>(response_buffer, new_value, true);

        // store the new value
        let new_value = BytesMutUtils::from::<f64>(&new_value);
        let _ = hash_db.put_multi(key, &[(field, &new_value)])?;
        Ok(())
    }

    /// Returns the values associated with the specified fields in the hash stored at key.
    /// For every field that does not exist in the hash, a nil value is returned. Because non-existing keys
    /// are treated as empty hashes, running HMGET against a non-existing key will return a list of nil values.
    async fn hmget(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 3, tx);
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // skip the command name
        iter.next(); // skip the hash key

        let max_response_buffer = client_state
            .server_inner_state()
            .options()
            .client_limits
            .client_response_buffer_size;

        // multi db access requires an exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        let mut writer = RespWriter::new(tx, 1024, max_response_buffer);
        let array_len = command.arg_count().saturating_sub(2); // reduce the command name + the hash key

        writer.add_array_len(array_len).await?;
        for field in iter {
            match hash_db.get(key, field)? {
                HashGetResult::Some(field_value) => {
                    writer.add_bulk_string(&field_value).await?;
                }
                HashGetResult::FieldNotFound | HashGetResult::NotFound => {
                    writer.add_null_string().await?;
                }
                HashGetResult::WrongType => {
                    writer.error_string(ErrorStrings::WRONGTYPE).await?;
                    break;
                }
            }
        }

        writer.flush().await?;
        Ok(())
    }

    #[allow(unused_variables)]
    /// `HRANDFIELD key [count [WITHVALUES]]`
    /// When called with just the key argument, return a random field from the hash value stored at key.
    /// If the provided count argument is positive, return an array of distinct fields. The array's length is either
    /// count or the hash's number of fields (HLEN), whichever is lower.
    /// If called with a negative count, the behavior changes and the command is allowed to return the same field
    /// multiple times. In this case, the number of returned fields is the absolute value of the specified count.
    /// The optional WITHVALUES modifier changes the reply so it includes the respective values of the randomly selected
    /// hash fields.
    async fn hrandfield(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // skip "hrandfield"
        iter.next(); // skips the key

        // Parse the arguments
        let builder = RespBuilderV2::default();
        let mut response_buffer = BytesMut::with_capacity(4096);
        let (count, with_values, allow_dups) = match (iter.next(), iter.next()) {
            (Some(count), None) => {
                let Some(count) = BytesMutUtils::parse::<i64>(count) else {
                    builder.error_string(
                        &mut response_buffer,
                        ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
                    );
                    tx.write_all(&response_buffer).await?;
                    return Ok(());
                };
                (count.abs(), false, count < 0)
            }
            (Some(count), Some(with_values)) => {
                let Some(count) = BytesMutUtils::parse::<i64>(count) else {
                    builder.error_string(
                        &mut response_buffer,
                        ErrorStrings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
                    );
                    tx.write_all(&response_buffer).await?;
                    return Ok(());
                };
                if BytesMutUtils::to_string(with_values).to_lowercase() != "withvalues" {
                    builder.error_string(&mut response_buffer, ErrorStrings::SYNTAX_ERROR);
                    tx.write_all(&response_buffer).await?;
                    return Ok(());
                }
                (count.abs(), true, count < 0)
            }
            (_, _) => (1i64, false, false),
        };

        // multiple db calls, requires exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.database_id());
        let hash_db = HashDb::with_storage(client_state.database(), client_state.database_id());

        // determine the array length
        let hash_md = match hash_db.hash_metadata(key)? {
            GetHashMetadataResult::Some(hash_md) => hash_md,
            GetHashMetadataResult::NotFound => {
                builder.null_string(&mut response_buffer);
                tx.write_all(&response_buffer).await?;
                return Ok(());
            }
            GetHashMetadataResult::WrongType => {
                builder.error_string(&mut response_buffer, ErrorStrings::WRONGTYPE);
                tx.write_all(&response_buffer).await?;
                return Ok(());
            }
        };

        // Adjust the "count"
        let count = if allow_dups {
            count
        } else {
            std::cmp::min(count, hash_md.len() as i64)
        };

        // fast bail out
        if count.eq(&0) {
            builder.empty_array(&mut response_buffer);
            tx.write_all(&response_buffer).await?;
            return Ok(());
        }

        let possible_indexes = (0..hash_md.len() as usize).collect::<Vec<usize>>();

        // select the indices we want to pick
        let mut indices = choose_multiple_values(count as usize, &possible_indexes, allow_dups)?;

        // When returning multiple items, we return an array
        if indices.len() > 1 || with_values {
            builder.add_array_len(
                &mut response_buffer,
                if with_values {
                    indices.len() * 2
                } else {
                    indices.len()
                },
            );
        }

        // create an iterator and place at at the start of the hash fields
        let mut curidx = 0usize;
        let prefix = Rc::new(hash_md.prefix());

        let max_response_buffer = client_state
            .server_inner_state()
            .options()
            .client_limits
            .client_response_buffer_size;

        // strategy: create an iterator on all the hash items and maintain a "curidx" that keeps the current visited
        // index for every element, compare it against the first item in the "chosen" vector which holds a sorted list of
        // chosen indices
        match client_state.database().create_iterator(prefix.clone())? {
            StorageIterator::RocksDb(mut rocksdb_iter) => {
                while rocksdb_iter.valid() && !indices.is_empty() {
                    // get the key & value
                    let Some(key) = rocksdb_iter.key() else {
                        break;
                    };

                    if !key.starts_with(prefix.as_ref()) {
                        break;
                    }

                    let Some(value) = rocksdb_iter.value() else {
                        break;
                    };

                    // extract the key from the row data
                    while let Some(wanted_index) = indices.front() {
                        if curidx.eq(wanted_index) {
                            let hash_field_key = HashFieldKey::from_bytes(key)?;
                            builder
                                .add_bulk_string_u8_arr(&mut response_buffer, hash_field_key.key());
                            if with_values {
                                builder.add_bulk_string_u8_arr(&mut response_buffer, value);
                            }
                            // pop the first element
                            indices.pop_front();

                            // Don't progress the iterator here,  we might have another item with the same index
                        } else {
                            break;
                        }
                    }

                    if response_buffer.len() > max_response_buffer {
                        tx.write_all(&response_buffer).await?;
                        response_buffer.clear();
                    }
                    curidx = curidx.saturating_add(1);
                    rocksdb_iter.next();
                }
            }
        };

        // flush the remainder
        if !response_buffer.is_empty() {
            tx.write_all(&response_buffer).await?;
            response_buffer.clear();
        }
        Ok(())
    }
}

/// Given list of values `options`, return up to `count` values.
/// The output is sorted.
fn choose_multiple_values(
    count: usize,
    options: &Vec<usize>,
    allow_dups: bool,
) -> Result<VecDeque<usize>, SableError> {
    let mut rng = rand::thread_rng();
    let mut chosen = Vec::<usize>::new();
    if allow_dups {
        for _ in 0..count {
            chosen.push(*options.choose(&mut rng).unwrap_or(&0));
        }
    } else {
        let mut unique_values = options.clone();
        unique_values.sort();
        unique_values.dedup();
        loop {
            if unique_values.is_empty() {
                break;
            }

            if chosen.len() == count {
                break;
            }

            let pos = rng.gen_range(0..unique_values.len());
            let Some(val) = unique_values.get(pos) else {
                return Err(SableError::OtherError(format!(
                    "Internal error: failed to read from vector (len: {}, pos: {})",
                    unique_values.len(),
                    pos
                )));
            };
            chosen.push(*val);
            unique_values.remove(pos);
        }
    }

    chosen.sort();
    let chosen: VecDeque<usize> = chosen.iter().copied().collect();
    Ok(chosen)
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
        (vec!["hset", "myhash", "field1", "value1"], ":1\r\n"),
        // fields already exists in hash
        (vec!["hset", "myhash", "field1", "value1", "field1", "value1"], ":0\r\n"),
        (vec!["hset", "myhash", "field1", "value1", "field2"], "-ERR wrong number of arguments for 'hset' command\r\n"),
        (vec!["hset", "myhash", "field2", "value2"], ":1\r\n"),
        (vec!["hset", "myhash", "f3", "v1"], ":1\r\n"),
        (vec!["hset", "myhash", "f3", "v2"], ":0\r\n"),
        (vec!["hset", "myhash", "f3", "v3"], ":0\r\n"),
        (vec!["hget", "myhash", "f3"], "$2\r\nv3\r\n"), // expect the last update
        (vec!["hset", "myhash"], "-ERR wrong number of arguments for 'hset' command\r\n"),
    ], "test_hset"; "test_hset")]
    #[test_case(vec![
        (vec!["hset", "myhash", "field1", "value1", "field2", "value2"], ":2\r\n"),
        (vec!["hget", "myhash", "field1"], "$6\r\nvalue1\r\n"),
        (vec!["hget", "myhash", "field2"], "$6\r\nvalue2\r\n"),
        (vec!["hget", "myhash", "nosuchfield"], "$-1\r\n"),
    ], "test_hget"; "test_hget")]
    #[test_case(vec![
        (vec!["hset", "myhash", "field1", "value1", "field2", "value2"], ":2\r\n"),
        (vec!["hdel", "myhash1"], "-ERR wrong number of arguments for 'hdel' command\r\n"),
        (vec!["hdel", "myhash"], "-ERR wrong number of arguments for 'hdel' command\r\n"),
        (vec!["hdel", "nosuchhash", "field1"], ":0\r\n"),
        (vec!["hdel", "myhash", "field1", "field1", "field1"], ":1\r\n"),
        (vec!["hset", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"], ":3\r\n"),
        (vec!["hdel", "myhash", "f1", "f2", "f3"], ":3\r\n"),
    ], "test_hdel"; "test_hdel")]
    #[test_case(vec![
        (vec!["hset", "myhash", "field1", "value1", "field2", "value2"], ":2\r\n"),
        (vec!["hlen", "myhash"], ":2\r\n"),
        (vec!["hlen", "nosuchhash"], ":0\r\n"),
        (vec!["hlen"], "-ERR wrong number of arguments for 'hlen' command\r\n"),
    ], "test_hlen"; "test_hlen")]
    #[test_case(vec![
        (vec!["hset", "myhash", "field1", "value1", "field2", "value2"], ":2\r\n"),
        (vec!["set", "str_key", "field1"], "+OK\r\n"),
        (vec!["hexists", "myhash", "nosuchfield"], ":0\r\n"),
        (vec!["hexists", "str_key"], "-ERR wrong number of arguments for 'hexists' command\r\n"),
        (vec!["hexists", "str_key", "field"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hexists", "no_such_hash", "field1"], ":0\r\n"),
        (vec!["hexists", "myhash", "field1", ], ":1\r\n"),
    ], "test_hexists"; "test_hexists")]
    #[test_case(vec![
        (vec!["hset", "myhash", "1", "2", "a", "b", "c", "d"], ":3\r\n"),
        (vec!["hgetall", "myhash"], "*6\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"),
        (vec!["hgetall", "no_such_hash"], "*0\r\n"),
        (vec!["set", "not_a_hash", "value"], "+OK\r\n"),
        (vec!["hgetall", "not_a_hash"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hgetall"], "-ERR wrong number of arguments for 'hgetall' command\r\n"),
    ], "test_hgetall"; "test_hgetall")]
    #[test_case(vec![
        (vec!["hincrby"], "-ERR wrong number of arguments for 'hincrby' command\r\n"),
        (vec!["hincrby", "myhash"], "-ERR wrong number of arguments for 'hincrby' command\r\n"),
        (vec!["hincrby", "myhash", "field"], "-ERR wrong number of arguments for 'hincrby' command\r\n"),
        (vec!["hincrby", "myhash", "field", "1"], ":1\r\n"),
        (vec!["hincrby", "myhash", "field", "1"], ":2\r\n"),
        (vec!["hincrby", "myhash", "field", "1.0"], "-ERR value is not an integer or out of range\r\n"),
        (vec!["set", "string", "field"], "+OK\r\n"),
        (vec!["hincrby", "string", "field", "1"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    ], "test_hincrby"; "test_hincrby")]
    #[test_case(vec![
        (vec!["hincrbyfloat"], "-ERR wrong number of arguments for 'hincrbyfloat' command\r\n"),
        (vec!["hincrbyfloat", "myhash"], "-ERR wrong number of arguments for 'hincrbyfloat' command\r\n"),
        (vec!["hincrbyfloat", "myhash", "field"], "-ERR wrong number of arguments for 'hincrbyfloat' command\r\n"),
        (vec!["hincrbyfloat", "myhash", "field", "1"], ",1\r\n"),
        (vec!["hincrbyfloat", "myhash", "field", "1"], ",2\r\n"),
        (vec!["hincrbyfloat", "myhash", "field", "1.0"], ",3\r\n"),
        (vec!["set", "string", "field"], "+OK\r\n"),
        (vec!["hincrbyfloat", "string", "field", "1"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    ], "test_hincrbyfloat"; "test_hincrbyfloat")]
    #[test_case(vec![
        (vec!["hset", "myhash", "1", "2", "a", "b", "c", "d"], ":3\r\n"),
        (vec!["hkeys", "myhash"], "*3\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\nc\r\n"),
        (vec!["hkeys", "no_such_hash"], "*0\r\n"),
        (vec!["set", "not_a_hash", "value"], "+OK\r\n"),
        (vec!["hkeys", "not_a_hash"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hkeys"], "-ERR wrong number of arguments for 'hkeys' command\r\n"),
    ], "test_hkeys"; "test_hkeys")]
    #[test_case(vec![
        (vec!["hset", "myhash", "1", "2", "a", "b", "c", "d"], ":3\r\n"),
        (vec!["hvals", "myhash"], "*3\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\nd\r\n"),
        (vec!["hvals", "no_such_hash"], "*0\r\n"),
        (vec!["set", "not_a_hash", "value"], "+OK\r\n"),
        (vec!["hvals", "not_a_hash"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    ], "test_hvals"; "test_hvals")]
    #[test_case(vec![
        (vec!["set", "str_key", "value"], "+OK\r\n"),
        (vec!["hmset", "str_key", "field", "value"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hmset", "myhash", "field1", "value", "field2", "value"], "+OK\r\n"),
        (vec!["hmset", "myhash", "field1"], "-ERR wrong number of arguments for 'hmset' command\r\n"),
    ], "test_hmset"; "test_hmset")]
    #[test_case(vec![
        (vec!["set", "str_key", "value"], "+OK\r\n"),
        (vec!["hmget", "str_key", "field"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hmset", "myhash", "field1", "value1", "field2", "value2"], "+OK\r\n"),
        (vec!["hmget", "myhash"], "-ERR wrong number of arguments for 'hmget' command\r\n"),
        (vec!["hmget", "myhash", "field1", "field2"], "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"),
        (vec!["hmget", "myhash", "field1"], "*1\r\n$6\r\nvalue1\r\n"),
    ], "test_hmget"; "test_hmget")]
    #[test_case(vec![
        (vec!["set", "str_key", "value"], "+OK\r\n"),
        (vec!["hrandfield", "str_key"], "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        (vec!["hrandfield", "nosuchhash"], "$-1\r\n"),
        (vec!["hset", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"], ":3\r\n"),
        // since we have hash with 3 fields and we want 3 fields, we will get them
        (vec!["hrandfield", "myhash", "3"], "*3\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n"),
        (vec!["hrandfield", "myhash", "15"], "*3\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n"),
        (vec!["hrandfield", "myhash", "3", "withvalues"], "*6\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n"),
        (vec!["hrandfield", "myhash", "8", "withvalues"], "*6\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n"),
        // Different type of responses
        (vec!["hset", "myhash_1_item", "f1", "v1"], ":1\r\n"),
        (vec!["hrandfield", "myhash_1_item", "1", "withvalues"], "*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n"),
        (vec!["hrandfield", "myhash_1_item"], "$2\r\nf1\r\n"),
    ], "test_hrandfield"; "test_hrandfield")]
    fn test_hash_commands(
        args: Vec<(Vec<&'static str>, &'static str)>,
        test_name: &str,
    ) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args {
                let mut sink = crate::tests::ResponseSink::with_name(test_name).await;
                let cmd = Rc::new(RedisCommand::for_test(args));
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
    fn test_rng_selection() {
        let options = vec![1, 2, 2, 2, 3, 4, 5, 6, 7, 7, 7];
        let selections = choose_multiple_values(8, &options, false).unwrap();
        assert_eq!(selections.len(), 7);

        let selections = choose_multiple_values(8, &options, true).unwrap();
        assert_eq!(selections.len(), 8);
    }
}
