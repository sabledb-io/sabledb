use crate::{
    check_args_count, command_arg_at,
    commands::{HandleCommandResult, Strings},
    metadata::SetMemberKey,
    server::ClientState,
    storage::ScanCursor,
    storage::{
        FindSetResult, FindSmallestResult, IteratorAdapter, SetDb, SetDeleteResult,
        SetExistsResult, SetLenResult, SetPutResult,
    },
    utils::RespBuilderV2,
    BytesMutUtils, LockManager, RedisCommand, RedisCommandName, SableError,
};

use crate::io::RespWriter;
use bytes::BytesMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

#[derive(PartialEq, Debug)]
enum IterateResult {
    Ok,
    WrongType,
    NotFound,
}

#[derive(PartialEq, Debug)]
enum IterateCallbackResult {
    Continue,
    Break,
}

#[derive(PartialEq, Debug)]
enum DiffResult {
    Some(Rc<RefCell<BTreeSet<BytesMut>>>),
    WrongType,
}

#[derive(PartialEq, Debug)]
enum IntersectResult {
    Some(Vec<BytesMut>),
    WrongType,
}

#[derive(PartialEq, Debug)]
enum UnionResult {
    Some(Vec<BytesMut>),
    WrongType,
}

#[derive(PartialEq, Debug)]
enum PickRandomIndexResult {
    Some(VecDeque<usize>),
    WrongType,
    NotFound,
}

/// Return the set metadata
macro_rules! writer_get_set_metadata_or_empty_array {
    ($set_db:expr, $key:expr, $writer:expr) => {{
        let set = match $set_db.find_set($key)? {
            FindSetResult::WrongType => {
                writer_return_wrong_type!($writer);
            }
            FindSetResult::NotFound => {
                writer_return_empty_array!($writer);
            }
            FindSetResult::Some(md) => md,
        };
        set
    }};
}

/// Return the set metadata
macro_rules! writer_get_set_metadata_or_null {
    ($set_db:expr, $key:expr, $writer:expr, $return_array:expr) => {{
        let set = match $set_db.find_set($key)? {
            FindSetResult::WrongType => {
                writer_return_wrong_type!($writer);
            }
            FindSetResult::NotFound => {
                writer_return_null_reply!($writer, $return_array);
            }
            FindSetResult::Some(md) => md,
        };
        set
    }};
}

pub struct SetCommands {}

impl SetCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            RedisCommandName::Sadd => {
                Self::sadd(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Scard => {
                Self::scard(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sdiff => {
                Self::sdiff(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sdiffstore => {
                Self::sdiffstore(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sinter => {
                Self::sinter(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sintercard => {
                Self::sintercard(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sinterstore => {
                Self::sinterstore(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sismember => {
                Self::sismember(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Smismember => {
                Self::smismember(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Smove => {
                Self::smove(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Smembers => {
                // Since - potentially - we could have a large set, stream the responses and don't build them
                // up in the memory first
                Self::smembers(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Srem => {
                Self::srem(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Spop => {
                // Since - potentially - we could have a large set, stream the responses and don't build them
                // up in the memory first
                Self::spop(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Srandmember => {
                // Since - potentially - we could have a large set, stream the responses and don't build them
                // up in the memory first
                Self::srandmember(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Sscan => {
                Self::sscan(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sunion => {
                Self::sunion(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Sunionstore => {
                Self::sunionstore(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "SET command '{}' is not supported",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Add the specified members to the set stored at key. Specified members that are already a member of this set are
    /// ignored. If key does not exist, a new set is created before adding the specified members.
    async fn sadd(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // skip "SADD"
        iter.next(); // skips the key

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        // Collect the members to add to the SET
        let mut members = Vec::<&BytesMut>::with_capacity(command.arg_count().saturating_sub(2));
        for member in iter {
            members.push(member);
        }

        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();

        let items_added = match set_db.put_multi(key, &members)? {
            SetPutResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetPutResult::Some(count) => count,
        };

        // Apply the changes
        set_db.commit()?;

        // Return the number of items actually added
        builder.number_usize(response_buffer, items_added);
        Ok(())
    }

    /// Returns the set cardinality (number of elements) of the set stored at key.
    async fn scard(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let key = command_arg_at!(command, 1);
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        match set_db.len(key)? {
            SetLenResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetLenResult::Some(len) => builder.number_usize(response_buffer, len),
        }
        Ok(())
    }

    /// Returns the members of the set resulting from the difference between the first set and all the successive sets
    /// `SDIFF key [key ...]`
    async fn sdiff(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let main_set = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // Skips the command SDIFF
        iter.next(); // Skips the main set name

        // Read-only lock (on all keys)
        let all_keys: Vec<&BytesMut> = command.args_vec()[1..].iter().collect();
        let _unused =
            LockManager::lock_multi(&all_keys, client_state.clone(), command.clone()).await?;

        let builder = RespBuilderV2::default();
        let other_sets: Vec<&BytesMut> = iter.collect();
        let diff_result = match Self::diff(client_state.clone(), main_set, &other_sets)? {
            DiffResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            DiffResult::Some(res) => res,
        };

        // Print the results
        builder.add_array_len(response_buffer, diff_result.borrow().len());
        for member in diff_result.borrow().iter() {
            builder.add_bulk_string(response_buffer, member);
        }
        Ok(())
    }

    /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
    /// If destination already exists, it is overwritten
    /// `SDIFFSTORE destination key [key ...]`
    async fn sdiffstore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let destination = command_arg_at!(command, 1);
        let main_set = command_arg_at!(command, 2);

        let mut iter = command.args_vec().iter();
        iter.next(); // Skips the command SDIFF
        iter.next(); // Skips the destination
        iter.next(); // Skips the main set name

        // Write lock (on all keys)
        let all_keys: Vec<&BytesMut> = command.args_vec()[1..].iter().collect();
        let _unused =
            LockManager::lock_multi(&all_keys, client_state.clone(), command.clone()).await?;
        let builder = RespBuilderV2::default();
        let other_sets: Vec<&BytesMut> = iter.collect();
        let diff_result = match Self::diff(client_state.clone(), main_set, &other_sets)? {
            DiffResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            DiffResult::Some(res) => res,
        };

        // Store the result in destination
        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let members: Vec<BytesMut> = diff_result.borrow().iter().cloned().collect();
        let members: Vec<&BytesMut> = members.iter().collect();
        let count = set_db.put_multi_overwrite(destination, &members)?;
        set_db.commit()?;

        // Return the number of items added
        builder.number_usize(response_buffer, count);
        Ok(())
    }

    /// Returns the members of the set resulting from the intersection of all the given sets
    /// `SINTER key [key ...]`
    async fn sinter(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let mut iter = command.args_vec().iter();
        iter.next(); // Skips the command SINTER

        // Read lock (on all keys)
        let all_keys: Vec<&BytesMut> = command.args_vec()[1..].iter().collect();
        let _unused =
            LockManager::lock_multi(&all_keys, client_state.clone(), command.clone()).await?;

        let builder = RespBuilderV2::default();
        let result = match Self::intersect(client_state.clone(), &all_keys)? {
            IntersectResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectResult::Some(res) => res,
        };

        // Store the result in destination
        let members: Vec<&BytesMut> = result.iter().collect();

        // Return the number of items added
        builder.add_array_len(response_buffer, members.len());
        for member in members {
            builder.add_bulk_string(response_buffer, member);
        }
        Ok(())
    }

    /// Returns the members of the set resulting from the intersection of all the given sets
    /// `SINTERCARD numkeys key [key ...] [LIMIT limit]`
    async fn sintercard(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let num_keys = command_arg_at!(command, 1);
        let builder = RespBuilderV2::default();
        let Some(mut num_keys) = BytesMutUtils::parse::<usize>(num_keys) else {
            builder.error_string(response_buffer, "ERR numkeys should be greater than 0");
            return Ok(());
        };

        if num_keys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }

        let mut iter = command.args_vec().iter();
        iter.next(); // Skips the command SINTERCARD
        iter.next(); // Skips the numkeys

        // Collect numkeys from the command line
        let mut keys = Vec::<&BytesMut>::new();
        for key in iter.by_ref() {
            keys.push(key);
            num_keys = num_keys.saturating_sub(1);
            if num_keys == 0 {
                break;
            }
        }

        if num_keys != 0 {
            builder.error_string(response_buffer, Strings::ERR_NUMKEYS);
            return Ok(());
        }

        // Read lock (on all keys)
        let limit = match (iter.next(), iter.next()) {
            (Some(limit_keyword), Some(limit)) if Self::check_limit(limit_keyword, limit) => {
                // this should not fail, as it passed the call to check_limit
                BytesMutUtils::parse::<usize>(limit).ok_or(SableError::OtherError(
                    "failed to convert limit into number".into(),
                ))?
            }
            (None, None) => usize::MAX,
            (_, _) => {
                // anything else is just a syntax error
                builder_return_syntax_error!(builder, response_buffer);
            }
        };

        // Lock the keys
        let _unused = LockManager::lock_multi(&keys, client_state.clone(), command.clone()).await?;

        let result = match Self::intersect(client_state.clone(), &keys)? {
            IntersectResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectResult::Some(res) => res,
        };

        // Store the result in destination
        let members: Vec<&BytesMut> = result.iter().collect();

        // Return the number of items added or the limit
        builder.number_usize(
            response_buffer,
            if members.len() > limit {
                limit
            } else {
                members.len()
            },
        );
        Ok(())
    }

    /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
    /// If destination already exists, it is overwritten.
    /// `SINTERSTORE destination key [key ...]`
    async fn sinterstore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let destination = command_arg_at!(command, 1);
        let mut iter = command.args_vec().iter();
        iter.next(); // Skips the command SINTERSTORE
        iter.next(); // Skips the destination

        // Read lock (on all keys, including the destination)
        let all_lock_keys: Vec<&BytesMut> = command.args_vec()[1..].iter().collect();
        let _unused =
            LockManager::lock_multi(&all_lock_keys, client_state.clone(), command.clone()).await?;

        let all_keys: Vec<&BytesMut> = command.args_vec()[2..].iter().collect();
        let builder = RespBuilderV2::default();
        let result = match Self::intersect(client_state.clone(), &all_keys)? {
            IntersectResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectResult::Some(res) => res,
        };

        // Store the result in destination
        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let members: Vec<&BytesMut> = result.iter().collect();
        let count = set_db.put_multi_overwrite(destination, &members)?;
        set_db.commit()?;

        // Return the number of items added
        builder.number_usize(response_buffer, count);
        Ok(())
    }

    /// Returns if member is a member of the set stored at key
    /// `SISMEMBER key member`
    async fn sismember(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);
        let member = command_arg_at!(command, 2);

        // read-lock
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;

        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        let exists = match set_db.member_exists(key, member)? {
            SetExistsResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetExistsResult::Exists => 1usize,
            SetExistsResult::NotExists => 0usize,
        };

        builder.number_usize(response_buffer, exists);
        Ok(())
    }

    /// Returns if member is a member of the set stored at key
    /// `SISMEMBER key member`
    async fn smismember(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        // read-lock
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        let mut iter = command.args_vec().iter();
        iter.next(); // skips the command
        iter.next(); // skips the key

        let mut result = Vec::<usize>::with_capacity(command.arg_count().saturating_sub(2));
        for member in iter {
            let exists = match set_db.member_exists(key, member)? {
                SetExistsResult::WrongType => {
                    builder_return_wrong_type!(builder, response_buffer);
                }
                SetExistsResult::Exists => 1usize,
                SetExistsResult::NotExists => 0usize,
            };
            result.push(exists);
        }

        builder.add_array_len(response_buffer, result.len());
        for exists in result {
            builder.add_number(response_buffer, exists, false);
        }
        Ok(())
    }

    /// Returns all the members of the set value stored at key
    /// `SMEMBERS key`
    async fn smembers(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);
        let key = command_arg_at!(command, 1);

        // read-lock
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let mut writer = RespWriter::new(tx, 1024, client_state.clone());
        let set = writer_get_set_metadata_or_empty_array!(set_db, key, writer);

        // Create an iterator over all the set members and stream them
        let set_prefix = set.prefix();

        // create an iterator that points to the start of the set elements
        writer.add_array_len(set.len() as usize).await?;
        let mut db_iter = client_state.database().create_iterator(&set_prefix)?;
        let mut count = 0u64;
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                break;
            }

            let member = SetMemberKey::from_bytes(key)?;
            writer.add_bulk_string(member.key()).await?;
            count = count.saturating_add(1);
            db_iter.next();
        }

        if count != set.len() {
            // internal error: the metadata does not match the number of items read
            // return a SableError here to close the connection
            let errmsg =
                format!(
                        "Number of set ('{:?}') items read ({}), does not match the expected items count ({}) ",
                        key,
                        count,
                        set.len()
                );
            return Err(SableError::Corrupted(errmsg));
        }

        // flush an remaining items from the writer
        writer.flush().await?;
        Ok(())
    }

    /// Move member from the set at source to the set at destination. This operation is atomic. In every given moment
    /// the element will appear to be a member of source or destination for other clients
    /// `SMOVE source destination member`
    async fn smove(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let source = command_arg_at!(command, 1);
        let destination = command_arg_at!(command, 2);
        let member = command_arg_at!(command, 3);

        // write-lock
        let _unused = LockManager::lock_multi(
            &[source, destination],
            client_state.clone(),
            command.clone(),
        )
        .await?;
        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();

        // Delete from the source
        match set_db.delete(source, &[member])? {
            SetDeleteResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetDeleteResult::Some(0) => {
                // does not exist
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            SetDeleteResult::Some(_) => {}
        }

        // Add to the target
        match set_db.put_multi(destination, &[member])? {
            SetPutResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetPutResult::Some(_) => {}
        }

        set_db.commit()?;
        builder.number_usize(response_buffer, 1);
        Ok(())
    }

    /// Removes and returns one or more random members from the set value store at key
    /// `SPOP key [count]`
    async fn spop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);
        let set_name = command_arg_at!(command, 1);

        let mut writer = RespWriter::new(tx, 1024, client_state.clone());
        let (count, return_array) = if let Some(count) = command.args_vec().get(2) {
            let Some(count) = BytesMutUtils::parse::<usize>(count) else {
                writer
                    .error_string(Strings::ZERR_VALUE_MUST_BE_POSITIVE)
                    .await?;
                writer.flush().await?;
                return Ok(());
            };
            (count, true)
        } else {
            (1usize, false)
        };

        let _lock = LockManager::lock(set_name, client_state.clone(), command.clone()).await?;
        let mut indexes =
            match Self::pick_random_indexes(client_state.clone(), set_name, count, false)? {
                PickRandomIndexResult::WrongType => {
                    writer_return_wrong_type!(writer);
                }
                PickRandomIndexResult::NotFound => {
                    writer_return_null_reply!(writer, return_array);
                }
                PickRandomIndexResult::Some(indexes) => indexes,
            };

        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let md = writer_get_set_metadata_or_null!(set_db, set_name, writer, return_array);

        let set_prefix = md.prefix();
        let mut db_iter = client_state.database().create_iterator(&set_prefix)?;

        let mut curidx = 0usize;

        if return_array {
            writer.add_array_len(indexes.len()).await?;
        }
        while db_iter.valid() && !indexes.is_empty() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                break;
            }

            if let Some(top_index) = indexes.front() {
                if *top_index == curidx {
                    // pop it
                    indexes.pop_front();
                    // write this key
                    let item = SetMemberKey::from_bytes(key)?;
                    writer.add_bulk_string(item.key()).await?;
                    // delete it from the set
                    set_db.delete(set_name, &[&BytesMut::from(item.key())])?;
                }
            }
            curidx = curidx.saturating_add(1);
            db_iter.next();
        }

        // Commit the changes
        set_db.commit()?;
        writer.flush().await?;
        Ok(())
    }

    /// When called with just the key argument, return a random element from the set value stored at key.
    /// If the provided count argument is positive, return an array of distinct elements. The array's length is either
    /// count or the set's cardinality (SCARD), whichever is lower.
    /// If called with a negative count, the behavior changes and the command is allowed to return the same element
    /// multiple times. In this case, the number of returned elements is the absolute value of the specified count
    /// `SRANDMEMBER key [count]`
    async fn srandmember(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);
        let set_name = command_arg_at!(command, 1);

        let mut writer = RespWriter::new(tx, 1024, client_state.clone());
        let (count, return_array, allow_dups) = if let Some(count) = command.args_vec().get(2) {
            let Some(count) = BytesMutUtils::parse::<i32>(count) else {
                writer
                    .error_string(Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE)
                    .await?;
                writer.flush().await?;
                return Ok(());
            };
            (count.unsigned_abs() as usize, true, count < 0)
        } else {
            (1usize, false, false)
        };

        let _lock = LockManager::lock(set_name, client_state.clone(), command.clone()).await?;
        let mut indexes =
            match Self::pick_random_indexes(client_state.clone(), set_name, count, allow_dups)? {
                PickRandomIndexResult::WrongType => {
                    writer_return_wrong_type!(writer);
                }
                PickRandomIndexResult::NotFound => {
                    writer_return_null_reply!(writer, return_array);
                }
                PickRandomIndexResult::Some(indexes) => indexes,
            };

        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let md = writer_get_set_metadata_or_null!(set_db, set_name, writer, return_array);

        let set_prefix = md.prefix();
        let mut db_iter = client_state.database().create_iterator(&set_prefix)?;

        let mut curidx = 0usize;

        if return_array {
            writer.add_array_len(indexes.len()).await?;
        }

        while db_iter.valid() && !indexes.is_empty() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                break;
            }

            let item = SetMemberKey::from_bytes(key)?;
            while let Some(wanted_index) = indexes.front() {
                if curidx.eq(wanted_index) {
                    writer.add_bulk_string(item.key()).await?;
                    // pop the first element
                    indexes.pop_front();
                    // Don't progress the iterator here,  we might have another item with the same index
                } else {
                    break;
                }
            }

            curidx = curidx.saturating_add(1);
            db_iter.next();
        }

        // Commit the changes
        writer.flush().await?;
        Ok(())
    }

    /// Remove the specified members from the set stored at key. Specified members that are not a member of this set
    /// are ignored. If key does not exist, it is treated as an empty set and this command returns 0.
    /// `SREM key member [member ...]`
    async fn srem(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        // obtains a write lock
        let _lock = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let members: Vec<&BytesMut> = command.args_vec()[2..].iter().collect();

        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        let count = match set_db.delete(key, &members)? {
            SetDeleteResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            SetDeleteResult::Some(count) => count,
        };
        set_db.commit()?;
        builder.number_usize(response_buffer, count);
        Ok(())
    }

    /// `SSCAN key cursor [MATCH pattern] [COUNT count]`
    async fn sscan(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let set_name = command_arg_at!(command, 1);
        let cursor_id = command_arg_at!(command, 2);

        let builder = RespBuilderV2::default();
        let Some(cursor_id) = BytesMutUtils::parse::<u64>(cursor_id) else {
            builder.error_string(response_buffer, Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE);
            return Ok(());
        };

        // parse the arguments
        let mut iter = command.args_vec().iter();
        iter.next(); // sscan
        iter.next(); // key
        iter.next(); // cursor

        let mut count = 10usize;
        let mut search_pattern: Option<&BytesMut> = None;
        while let Some(arg) = iter.next() {
            let arg = BytesMutUtils::to_string(arg).to_lowercase();
            match arg.as_str() {
                "match" => {
                    let Some(pattern) = iter.next() else {
                        builder_return_syntax_error!(builder, response_buffer);
                    };

                    // TODO: parse the pattern and make sure it is valid
                    search_pattern = Some(pattern);
                }
                "count" => {
                    let Some(n) = iter.next() else {
                        builder_return_syntax_error!(builder, response_buffer);
                    };
                    // parse `n`
                    let Some(n) = BytesMutUtils::parse::<usize>(n) else {
                        builder_return_value_not_int!(builder, response_buffer);
                    };

                    // TODO: scan number of items should be configurable
                    count = if n == 0 { 10usize } else { n };
                }
                _ => {
                    builder_return_syntax_error!(builder, response_buffer);
                }
            }
        }

        // multiple db calls, requires exclusive lock
        let _unused = LockManager::lock(set_name, client_state.clone(), command.clone()).await?;
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());

        let set = match set_db.find_set(set_name)? {
            FindSetResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            FindSetResult::NotFound => {
                builder.add_array_len(response_buffer, 2);
                builder.add_number(response_buffer, 0, false);
                builder.add_empty_array(response_buffer);
                return Ok(());
            }
            FindSetResult::Some(md) => md,
        };

        // Find a cursor with the given ID or create a new one (if cursor ID is `0`)
        // otherwise, return respond with an error
        let Some(cursor) = client_state.cursor_or(cursor_id, || Rc::new(ScanCursor::default()))
        else {
            builder.error_string(
                response_buffer,
                format!("ERR: Invalid cursor id {}", cursor_id).as_str(),
            );
            return Ok(());
        };

        // Build the prefix matcher
        let matcher = search_pattern.map(|search_pattern| {
            wildmatch::WildMatch::new(
                BytesMutUtils::to_string(search_pattern)
                    .to_string()
                    .as_str(),
            )
        });

        // Build the prefix
        let iter_start_pos = if let Some(saved_prefix) = cursor.prefix() {
            BytesMut::from(saved_prefix)
        } else {
            set.prefix()
        };

        let set_prefix = set.prefix();

        let mut results = Vec::<BytesMut>::with_capacity(count);
        let mut db_iter = client_state.database().create_iterator(&iter_start_pos)?;
        while db_iter.valid() && count > 0 {
            // get the key & value
            let Some(key) = db_iter.key() else {
                break;
            };

            // Go over this hash items only
            if !key.starts_with(&set_prefix) {
                break;
            }

            // extract the key from the row data
            let member = SetMemberKey::from_bytes(key)?;
            let item_user_key = member.key();

            // If we got a matcher, use it
            if let Some(matcher) = &matcher {
                let key_str = BytesMutUtils::to_string(item_user_key);
                if matcher.matches(key_str.as_str()) {
                    results.push(BytesMut::from(item_user_key));
                    count = count.saturating_sub(1);
                }
            } else {
                results.push(BytesMut::from(item_user_key));
                count = count.saturating_sub(1);
            }
            db_iter.next();
        }

        let cursor_id = if db_iter.valid() && count == 0 {
            // read the next key to be used as the next starting point for next iteration
            let Some(key) = db_iter.key() else {
                // this is an error
                builder.error_string(
                    response_buffer,
                    "ERR internal error: failed to read key from the database",
                );
                return Ok(());
            };

            if key.starts_with(&set_prefix) {
                // store the next cursor
                let cursor = Rc::new(cursor.progress(BytesMut::from(key)));
                // and store the cursor state
                client_state.set_cursor(cursor.clone());
                cursor.id()
            } else {
                // there are more items, but they don't belong to this hash
                client_state.remove_cursor(cursor.id());
                0u64
            }
        } else {
            // reached the end
            // delete the cursor
            client_state.remove_cursor(cursor.id());
            0u64
        };

        // write the response
        builder.add_array_len(response_buffer, 2);
        builder.add_number(response_buffer, cursor_id, false);
        builder.add_array_len(response_buffer, results.len());
        for member in results.iter() {
            builder.add_bulk_string(response_buffer, member);
        }
        Ok(())
    }

    /// `SUNION key [key ...]`
    async fn sunion(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let keys: Vec<&BytesMut> = command.args_vec()[1..].iter().collect();
        let _lock = LockManager::lock_multi(&keys, client_state.clone(), command.clone()).await?;
        let builder = RespBuilderV2::default();
        let members = match Self::set_union(client_state, &keys)? {
            UnionResult::Some(members) => members,
            UnionResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
        };
        builder.add_array_len(response_buffer, members.len());
        for member in &members {
            builder.add_bulk_string(response_buffer, member);
        }
        Ok(())
    }

    /// `SUNIONSTORE destination key [key ...]`
    async fn sunionstore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let destination = command_arg_at!(command, 1);
        let keys: Vec<&BytesMut> = command.args_vec()[2..].iter().collect();
        let _lock = LockManager::lock_multi(&keys, client_state.clone(), command.clone()).await?;
        let builder = RespBuilderV2::default();
        let members = match Self::set_union(client_state.clone(), &keys)? {
            UnionResult::Some(members) => members,
            UnionResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
        };

        let members: Vec<&BytesMut> = members.iter().collect();
        let mut set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        set_db.put_multi_overwrite(destination, &members)?;
        set_db.commit()?;
        builder.number_usize(response_buffer, members.len());
        Ok(())
    }

    // ===
    // Internal API
    // ===

    fn diff(
        client_state: Rc<ClientState>,
        main_set: &BytesMut,
        other_sets: &[&BytesMut],
    ) -> Result<DiffResult, SableError> {
        // Collect the primary SET members
        // load the keys of the main set into the memory (Use `BTreeSet` to keep the items sorted)
        let result_set = Rc::new(RefCell::new(BTreeSet::<BytesMut>::new()));
        // create an iterator that points to no where
        let mut db_iter = client_state
            .database()
            .create_iterator(&BytesMut::default())?;

        let main_items_clone = result_set.clone();
        let iter_result = Self::iterate_by_member_and_apply(
            client_state.clone(),
            &mut db_iter,
            main_set,
            move |member| {
                main_items_clone.borrow_mut().insert(BytesMut::from(member));
                Ok(IterateCallbackResult::Continue)
            },
        )?;

        if iter_result == IterateResult::WrongType {
            return Ok(DiffResult::WrongType);
        }

        for set_name in other_sets {
            let main_items_clone = result_set.clone();
            let iter_result = Self::iterate_by_member_and_apply(
                client_state.clone(),
                &mut db_iter,
                set_name,
                move |member| {
                    // If there are no more items in the primary set, stop looping
                    if main_items_clone.borrow().is_empty() {
                        return Ok(IterateCallbackResult::Break);
                    }
                    // Remove the member if it exists in the primary set
                    main_items_clone.borrow_mut().remove(member);
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            if iter_result == IterateResult::WrongType {
                return Ok(DiffResult::WrongType);
            }
        }

        Ok(DiffResult::Some(result_set))
    }

    fn intersect(
        client_state: Rc<ClientState>,
        all_sets: &[&BytesMut],
    ) -> Result<IntersectResult, SableError> {
        // Find the smallest set first
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let result_set = Rc::new(RefCell::new(BTreeMap::<BytesMut, usize>::new()));
        let base_set = match set_db.find_smallest(all_sets)? {
            FindSmallestResult::NotFound => {
                return Ok(IntersectResult::Some(Vec::<BytesMut>::default()));
            }
            FindSmallestResult::WrongType => return Ok(IntersectResult::WrongType),
            FindSmallestResult::Some(set_name) => set_name,
        };

        // create an iterator that points to no where
        let mut db_iter = client_state
            .database()
            .create_iterator(&BytesMut::default())?;

        // Read the base set first and collect all its members
        let main_items_clone = result_set.clone();
        let iter_result = Self::iterate_by_member_and_apply(
            client_state.clone(),
            &mut db_iter,
            base_set,
            move |member| {
                main_items_clone
                    .borrow_mut()
                    .insert(BytesMut::from(member), 1);
                Ok(IterateCallbackResult::Continue)
            },
        )?;

        if iter_result == IterateResult::WrongType {
            return Ok(IntersectResult::WrongType);
        }

        // Visit the remaining sets
        for set_name in all_sets {
            if base_set.eq(set_name) {
                continue;
            }

            let main_items_clone = result_set.clone();
            let iter_result = Self::iterate_by_member_and_apply(
                client_state.clone(),
                &mut db_iter,
                set_name,
                move |member| {
                    // If there are no more items in the primary set, stop looping
                    if main_items_clone.borrow().is_empty() {
                        return Ok(IterateCallbackResult::Break);
                    }

                    if let Some(val) = main_items_clone.borrow_mut().get_mut(member) {
                        // increase the reference count of this member
                        *val = val.saturating_add(1);
                    }
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            if iter_result == IterateResult::WrongType {
                return Ok(IntersectResult::WrongType);
            }
        }

        // collect all items that have a reference count that is equal to the number of sets
        let count = all_sets.len();
        let intersection: Vec<BytesMut> = result_set
            .borrow()
            .iter()
            .filter(|(_, refcount)| **refcount == count)
            .map(|(set_name, _)| set_name.clone())
            .collect();
        Ok(IntersectResult::Some(intersection))
    }

    /// Helper method for `SUNION` and `SUNIONESTORE` commands
    fn set_union(
        client_state: Rc<ClientState>,
        all_sets: &[&BytesMut],
    ) -> Result<UnionResult, SableError> {
        let result_set = Rc::new(RefCell::new(BTreeSet::<BytesMut>::new()));

        // create an iterator that points to no where
        let mut db_iter = client_state
            .database()
            .create_iterator(&BytesMut::default())?;

        // Visit the sets
        for set_name in all_sets {
            let result_set_cloned = result_set.clone();
            let iter_result = Self::iterate_by_member_and_apply(
                client_state.clone(),
                &mut db_iter,
                set_name,
                move |member| {
                    result_set_cloned
                        .borrow_mut()
                        .insert(BytesMut::from(member));
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            if iter_result == IterateResult::WrongType {
                return Ok(UnionResult::WrongType);
            }
        }

        let intersection: Vec<BytesMut> = result_set.borrow().iter().cloned().collect();
        Ok(UnionResult::Some(intersection))
    }

    /// Iterate over all items of `set_name` and apply callback on them
    fn iterate_by_member_and_apply<F>(
        client_state: Rc<ClientState>,
        db_iter: &mut IteratorAdapter,
        set_name: &BytesMut,
        mut callback: F,
    ) -> Result<IterateResult, SableError>
    where
        F: FnMut(&[u8]) -> Result<IterateCallbackResult, SableError>,
    {
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let set = match set_db.find_set(set_name)? {
            FindSetResult::WrongType => return Ok(IterateResult::WrongType),
            FindSetResult::NotFound => return Ok(IterateResult::NotFound),
            FindSetResult::Some(md) => md,
        };

        let set_prefix = set.prefix();
        db_iter.seek(&set_prefix);
        while db_iter.valid() {
            let Some(key) = db_iter.key() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                break;
            }

            let item = SetMemberKey::from_bytes(key)?;
            match callback(item.key())? {
                IterateCallbackResult::Continue => {}
                IterateCallbackResult::Break => break,
            }
            db_iter.next();
        }
        Ok(IterateResult::Ok)
    }

    /// Parse `LIMIT value` and confirm its syntax is correct
    fn check_limit(limit_keyword: &BytesMut, limit_value: &BytesMut) -> bool {
        limit_keyword.eq_ignore_ascii_case(b"limit")
            && BytesMutUtils::parse::<usize>(limit_value).is_some()
    }

    /// Pick up to `count` indexes from the set represented by `set_name`
    /// the returned indexes vector is sorted
    fn pick_random_indexes(
        client_state: Rc<ClientState>,
        set_name: &BytesMut,
        count: usize,
        allow_dups: bool,
    ) -> Result<PickRandomIndexResult, SableError> {
        let set_db = SetDb::with_storage(client_state.database(), client_state.database_id());
        let set = match set_db.find_set(set_name)? {
            FindSetResult::WrongType => return Ok(PickRandomIndexResult::WrongType),
            FindSetResult::NotFound => return Ok(PickRandomIndexResult::NotFound),
            FindSetResult::Some(md) => md,
        };

        let possible_indexes = (0..set.len() as usize).collect::<Vec<usize>>();
        Ok(PickRandomIndexResult::Some(
            crate::utils::choose_multiple_values(count, &possible_indexes, allow_dups)?,
        ))
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

    //use crate::tests::run_and_return_output;
    use std::rc::Rc;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(vec![
        ("sadd myset", "-ERR wrong number of arguments for 'sadd' command\r\n"),
        ("sadd myset 1 2 3 4", ":4\r\n"),
        ("sadd myset 1 2 3 4", ":0\r\n"),
        ("sadd myset 1 2 3 4 5", ":1\r\n"),
    ]; "test_sadd")]
    #[test_case(vec![
        ("sadd myset 1 2 3 4", ":4\r\n"),
        ("scard", "-ERR wrong number of arguments for 'scard' command\r\n"),
        ("scard nosuchset", ":0\r\n"),
        ("scard myset", ":4\r\n"),
    ]; "test_scard")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sdiff", "-ERR wrong number of arguments for 'sdiff' command\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sadd set2 1 2", ":2\r\n"),
        ("sadd set3 3", ":1\r\n"),
        ("sadd set4 4", ":1\r\n"),
        ("sdiff set1 set2 set3", "*1\r\n$1\r\n4\r\n"),
        ("sdiff set1 strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sdiff strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sdiff set1 set2 set3 set4", "*0\r\n"),
    ]; "test_sdiff")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sdiffstore", "-ERR wrong number of arguments for 'sdiffstore' command\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sadd set2 1 2", ":2\r\n"),
        ("sadd set3 3", ":1\r\n"),
        ("sadd set4 4", ":1\r\n"),
        ("sdiffstore dst1 set1 set2 set3", ":1\r\n"),
        ("scard dst1", ":1\r\n"),
        ("sdiffstore dst1 set1 set2", ":2\r\n"),
        ("scard dst1", ":2\r\n"),
        ("sdiffstore dst1 set1 set2 set3 set4", ":0\r\n"),
        ("scard dst1", ":0\r\n"),
        // strkey - a string - is overwritten
        ("sdiffstore strkey set1 set2", ":2\r\n"),
        ("scard strkey", ":2\r\n"),
    ]; "test_sdiffstore")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sinter", "-ERR wrong number of arguments for 'sinter' command\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sadd set2 1 2", ":2\r\n"),
        ("sadd set3 3", ":1\r\n"),
        ("sadd set4 4", ":1\r\n"),
        ("sadd set5 5 6 7 8", ":4\r\n"),
        ("sinter set1 set2", "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
        ("sinter set1 set3", "*1\r\n$1\r\n3\r\n"),
        ("sinter set1 set5", "*0\r\n"),
        ("sinter set1", "*4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n"),
        ("sinter set1 strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
    ]; "test_sinter")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sintercard", "-ERR wrong number of arguments for 'sintercard' command\r\n"),
        ("sintercard a", "-ERR wrong number of arguments for 'sintercard' command\r\n"),
        ("sintercard a s1", "-ERR numkeys should be greater than 0\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sadd set2 1 2", ":2\r\n"),
        ("sadd set3 3", ":1\r\n"),
        ("sadd set4 4", ":1\r\n"),
        ("sadd set5 5 6 7 8", ":4\r\n"),
        ("sintercard 2 set1 set2", ":2\r\n"),
        ("sintercard 2 set1 set3", ":1\r\n"),
        ("sintercard 2 set1 set5", ":0\r\n"),
        ("sintercard 1 set1", ":4\r\n"),
        ("sintercard 2 set1", "-ERR Number of keys can't be greater than number of args\r\n"),
        ("sintercard 2 set1 set2 LIMIT", "-ERR syntax error\r\n"),
        ("sintercard 2 set1 set2 sdsds", "-ERR syntax error\r\n"),
        ("sintercard 2 set1 set2 sdsds 1", "-ERR syntax error\r\n"),
        ("sintercard 2 set1 set2 LIMIT 1", ":1\r\n"),
        ("sintercard 2 set1 set2 LIMIT 5", ":2\r\n"),
    ]; "test_sintercard")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sinterstore", "-ERR wrong number of arguments for 'sinterstore' command\r\n"),
        ("sinterstore dst", "-ERR wrong number of arguments for 'sinterstore' command\r\n"),
        ("sinterstore dst strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sadd set2 1 2", ":2\r\n"),
        ("sadd set3 3", ":1\r\n"),
        ("sadd set4 4", ":1\r\n"),
        ("sadd set5 5 6 7 8", ":4\r\n"),
        ("sinterstore dst set1 set2", ":2\r\n"),
        ("scard dst", ":2\r\n"),
        ("sinterstore dst set1", ":4\r\n"),
        ("scard dst", ":4\r\n"),
    ]; "test_sinterstore")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sismember", "-ERR wrong number of arguments for 'sismember' command\r\n"),
        ("sismember key", "-ERR wrong number of arguments for 'sismember' command\r\n"),
        ("sismember strkey member", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("sismember set1 1", ":1\r\n"),
        ("sismember set1 5", ":0\r\n"),
        ("sismember nosuchkey 1", ":0\r\n"),
    ]; "test_sismember")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("smismember", "-ERR wrong number of arguments for 'smismember' command\r\n"),
        ("smismember key", "-ERR wrong number of arguments for 'smismember' command\r\n"),
        ("smismember strkey member", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sadd set1 1 2 3 4", ":4\r\n"),
        ("smismember set1 1 2 8", "*3\r\n:1\r\n:1\r\n:0\r\n"),
        ("smismember nosuchkey 1 2 8", "*3\r\n:0\r\n:0\r\n:0\r\n"),
    ]; "test_smismember")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("smembers", "-ERR wrong number of arguments for 'smembers' command\r\n"),
        ("smembers strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sadd set1 one two three four", ":4\r\n"),
        ("smembers set1", "*4\r\n$4\r\nfour\r\n$3\r\none\r\n$5\r\nthree\r\n$3\r\ntwo\r\n"),
        ("smembers nosuchkey", "*0\r\n"),
    ]; "test_smembers")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("smove s1 s2", "-ERR wrong number of arguments for 'smove' command\r\n"),
        ("sadd s1 a c", ":2\r\n"),
        ("sadd s2 a b", ":2\r\n"),
        ("smove s1 strkey a", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("smove strkey s2 a", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("smove s1 s2 a", ":1\r\n"),
        ("scard s1", ":1\r\n"),
        ("scard s2", ":2\r\n"),
        ("smove s1 s2 c", ":1\r\n"),
        ("scard s1", ":0\r\n"),
        ("scard s2", ":3\r\n"),
    ]; "test_smove")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("spop strkey 1", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("spop s1 5", "*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
        ("spop s1", "$-1\r\n"),
        ("spop s1 1", "*-1\r\n"),
    ]; "test_spop")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("spop strkey 1", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("srandmember s1 5", "*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
        ("spop s1 5", "*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"), // empty the set
        ("srandmember s1", "$-1\r\n"),
        ("srandmember s1 1", "*-1\r\n"),
    ]; "test_srandmember")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("srem strkey", "-ERR wrong number of arguments for 'srem' command\r\n"),
        ("srem strkey 1", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("srem s1 1 2", ":2\r\n"),
        ("smembers s1", "*3\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
        ("srem s1 1 2 3 4 5", ":3\r\n"),
        ("scard s1", ":0\r\n"),
    ]; "test_srem")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("sscan s1", "-ERR wrong number of arguments for 'sscan' command\r\n"),
        ("sscan s1 cursor_id", "-ERR value is not an integer or out of range\r\n"),
        ("sscan strkey 0 count 5", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sscan s1 0 count 5", "*2\r\n:0\r\n*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
        ("sscan s1 0 count 100", "*2\r\n:0\r\n*5\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"),
    ]; "test_sscan")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("sadd s2 1 2 3 4 5 6 7 8 9 0", ":10\r\n"),
        ("sunion s1 strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sunion", "-ERR wrong number of arguments for 'sunion' command\r\n"),
        ("sunion s1 s2", "*10\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n$1\r\n9\r\n"),
    ]; "test_sunion")]
    #[test_case(vec![
        ("set strkey value", "+OK\r\n"),
        ("set dst value", "+OK\r\n"),
        ("sadd s1 1 2 3 4 5", ":5\r\n"),
        ("sadd s2 1 2 3 4 5 6 7 8 9 0", ":10\r\n"),
        ("sunionstore dst s1 strkey", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("sunionstore dst", "-ERR wrong number of arguments for 'sunionstore' command\r\n"),
        ("sunionstore dst s1 s2", ":10\r\n"),
        ("smembers dst", "*10\r\n$1\r\n0\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n$1\r\n6\r\n$1\r\n7\r\n$1\r\n8\r\n$1\r\n9\r\n"),
    ]; "test_sunionstore")]
    fn test_set_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args {
                let mut sink = crate::io::FileResponseSink::new().await.unwrap();
                let args = args.split(' ').collect();
                let cmd = Rc::new(RedisCommand::for_test(args));
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
