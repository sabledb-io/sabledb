use crate::commands::Strings;
use crate::{
    commands::HandleCommandResult,
    metadata::CommonValueMetadata,
    metadata::{KeyType, ValueType},
    server::ClientState,
    storage::{GenericDb, ScanCursor},
    utils::{PatternMatcher, RespBuilderV2},
    BytesMutUtils, LockManager, PrimaryKeyMetadata, SableError, U8ArrayBuilder, ValkeyCommand,
    ValkeyCommandName,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct GenericCommands {}

impl GenericCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            ValkeyCommandName::Ttl => {
                Self::ttl(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Del => {
                Self::del(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Exists => {
                Self::exists(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Expire => {
                Self::expire(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Keys => {
                Self::keys(client_state, command, &mut response_buffer).await?;
            }
            ValkeyCommandName::Scan => {
                Self::scan(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non generic command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string,
    /// the individual complexity remains O(1) and the deletion of the element keys is done in a background thread
    async fn del(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name

        let mut deleted_items = 0usize;
        let db_id = client_state.database_id();
        let mut generic_db = GenericDb::with_storage(client_state.database(), db_id);
        for user_key in iter {
            // obtain the lock per key
            let _unused =
                LockManager::lock(user_key, client_state.clone(), command.clone()).await?;
            if generic_db.contains(user_key)? {
                generic_db.delete(user_key, false)?;
                deleted_items = deleted_items.saturating_add(1);
            }
        }
        generic_db.commit()?;

        if deleted_items > 0 {
            // commit changes
            generic_db.commit()?;

            // if user wishes to remove the item NOW, trigger an eviction
            if !client_state
                .server_inner_state()
                .options()
                .read()
                .expect("read error")
                .cron
                .instant_delete
            {
                // trigger eviction
                // TODO: do we want to trigger eviction for a single key only?
                client_state
                    .server_inner_state()
                    .send_evictor(crate::CronMessage::Evict)
                    .await?;
            }
        }

        let builder = RespBuilderV2::default();
        builder.number::<usize>(response_buffer, deleted_items, false);
        Ok(())
    }

    /// Returns the remaining time to live of a key that has a timeout.
    /// This introspection capability allows a Valkey client to check how
    /// many seconds a given key will continue to be part of the dataset.
    async fn ttl(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let mut generic_db =
            GenericDb::with_storage(client_state.database(), client_state.database_id());
        if let Some((_, value_metadata)) = generic_db.get(key)? {
            if !value_metadata.expiration().has_ttl() {
                // No timeout
                builder.number_i64(response_buffer, -1);
            } else {
                builder.number_u64(
                    response_buffer,
                    value_metadata.expiration().ttl_in_seconds()?,
                );
            }
        } else {
            // The command returns -2 if the key does not exist.
            builder.number_i64(response_buffer, -2);
        }
        Ok(())
    }

    /// Returns if key exists.
    /// The user should be aware that if the same existing key is mentioned in the arguments multiple times,
    /// it will be counted multiple times. So if somekey exists, EXISTS somekey somekey will return 2.
    async fn exists(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 2 items: EXISTS <KEY1> [..]
        check_args_count!(command, 2, response_buffer);

        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name
        let mut items_found = 0usize;
        let db_id = client_state.database_id();

        let generic_db = GenericDb::with_storage(client_state.database(), db_id);
        for user_key in iter {
            if generic_db.contains(user_key)? {
                items_found = items_found.saturating_add(1);
            }
        }

        let builder = RespBuilderV2::default();
        builder.number_usize(response_buffer, items_found);
        Ok(())
    }

    async fn expire(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // at least 3 arguments
        check_args_count!(command, 3, response_buffer);
        let builder = RespBuilderV2::default();

        // EXPIRE key seconds [NX | XX | GT | LT]
        let key = command_arg_at!(command, 1);
        let seconds = command_arg_at!(command, 2);

        // Convert into seconds
        let Some(seconds) = BytesMutUtils::parse::<u64>(seconds) else {
            builder.error_string(response_buffer, Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE);
            return Ok(());
        };

        let db_id = client_state.database_id();
        let _unused = LockManager::lock(key, client_state.clone(), command.clone()).await?;
        let mut generic_db = GenericDb::with_storage(client_state.database(), db_id);

        // Make sure the key exists in the database
        let Some(mut expiration) = generic_db.get_expiration(key)? else {
            builder.number_usize(response_buffer, 0);
            return Ok(());
        };

        // If no other param was provided, set the ttl in seconds and leave
        let Some(arg) = command.arg(3) else {
            expiration.set_ttl_seconds(seconds)?;
            generic_db.put_expiration(key, &expiration, true)?;
            builder.number_usize(response_buffer, 1);
            return Ok(());
        };

        let arg = BytesMutUtils::to_string(arg).to_lowercase();
        match arg.as_str() {
            "nx" => {
                // NX -- Set expiry only when the key has no expiry
                if !expiration.has_ttl() {
                    expiration.set_ttl_seconds(seconds)?;
                    generic_db.put_expiration(key, &expiration, true)?;
                    builder.number_usize(response_buffer, 1);
                } else {
                    builder.number_usize(response_buffer, 0);
                }
            }
            "xx" => {
                // XX -- Set expiry only when the key has an existing expiry
                if expiration.has_ttl() {
                    expiration.set_ttl_seconds(seconds)?;
                    generic_db.put_expiration(key, &expiration, true)?;
                    builder.number_usize(response_buffer, 1);
                } else {
                    builder.number_usize(response_buffer, 0);
                }
            }
            "gt" => {
                // GT -- Set expiry only when the new expiry is greater than current one
                if seconds > expiration.ttl_in_seconds()? {
                    expiration.set_ttl_seconds(seconds)?;
                    generic_db.put_expiration(key, &expiration, true)?;
                    builder.number_usize(response_buffer, 1);
                } else {
                    builder.number_usize(response_buffer, 0);
                }
            }
            "lt" => {
                // LT -- Set expiry only when the new expiry is less than current one
                if seconds < expiration.ttl_in_seconds()? {
                    expiration.set_ttl_seconds(seconds)?;
                    generic_db.put_expiration(key, &expiration, true)?;
                    builder.number_usize(response_buffer, 1);
                } else {
                    builder.number_usize(response_buffer, 0);
                }
            }
            option => {
                builder.error_string(
                    response_buffer,
                    format!("ERR Unsupported option {}", option).as_str(),
                );
            }
        }
        Ok(())
    }

    /// Returns all keys for the current database that matches a given pattern
    async fn keys(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        // NOTE: this function iterate through all keys in the database and
        // applies glob search pattern on each item. In order to implement this
        // in a single pass, we build the output in the memory. We might consider
        // changing this in the future (configurable?) to do this in 2 passes:
        // the first pass will count the number of matches and the second pass
        // will stream the results
        check_args_count!(command, 2, response_buffer);

        let pattern = command_arg_at!(command, 1);

        // Build the prefix matcher
        let matcher =
            wildmatch::WildMatch::new(BytesMutUtils::to_string(pattern).to_string().as_str());

        // create iterator
        let mut prefix = BytesMut::new();
        let mut builder = U8ArrayBuilder::with_buffer(&mut prefix);
        builder.write_key_type(KeyType::PrimaryKey);
        builder.write_u16(client_state.database_id());
        let mut dbiter = client_state.database().create_iterator(&prefix)?;
        let mut matching_keys = Vec::<BytesMut>::new();
        while dbiter.valid() {
            let Some(key) = dbiter.key() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            // Since this operation can be lengthy, yield back to the tokio
            // runtime after every 1000 keys added to the result set
            if matching_keys.len().rem_euclid(1000) == 0 {
                tokio::task::yield_now().await;
            }

            if key.len() < PrimaryKeyMetadata::SIZE {
                return Err(SableError::OtherError(format!(
                    "Invalid key size read. Key size is expected to be at least {} bytes",
                    PrimaryKeyMetadata::SIZE
                )));
            }

            // get the user key from the primary key
            let user_key = &key[PrimaryKeyMetadata::SIZE..];
            if matcher.matches(BytesMutUtils::to_string(user_key).as_str()) {
                matching_keys.push(BytesMut::from(user_key));
            }
            dbiter.next();
        }

        let builder = RespBuilderV2::default();
        builder.add_array_len(response_buffer, matching_keys.len());
        for key in &matching_keys {
            builder.add_bulk_string(response_buffer, key);
        }
        Ok(())
    }

    /// The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are used in order to incrementally
    /// iterate over a collection of elements.
    ///
    /// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`
    async fn scan(
        client_state: Rc<ClientState>,
        command: Rc<ValkeyCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let cursor_id = command_arg_at!(command, 1);

        let builder = RespBuilderV2::default();
        let Some(cursor_id) = BytesMutUtils::parse::<u64>(cursor_id) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        // parse optional arguments
        let mut iter = command.args_vec().iter();
        iter.next(); // SCAN
        iter.next(); // cursor

        let mut pattern: Option<&BytesMut> = None;
        let mut count = 10usize;
        let mut obj_type: Option<ValueType> = None;
        while let Some(keyword) = iter.next() {
            let Some(value) = iter.next() else {
                builder_return_syntax_error!(builder, response_buffer);
            };

            let keyword = BytesMutUtils::to_string(keyword).to_lowercase();
            match keyword.as_str() {
                "match" if pattern.is_none() => {
                    pattern = Some(value);
                }
                "count" => {
                    count = if let Some(count) = BytesMutUtils::parse::<usize>(value) {
                        if count > 0 {
                            count
                        } else {
                            10
                        }
                    } else {
                        builder_return_value_not_int!(builder, response_buffer);
                    };
                }
                "type" if obj_type.is_none() => {
                    obj_type = Some(ValueType::from(value));
                }
                _ => {
                    builder_return_syntax_error!(builder, response_buffer);
                }
            }
        }

        // Get or create a cursor
        let Some(cursor) = client_state.cursor_or(cursor_id, || Rc::new(ScanCursor::default()))
        else {
            builder.error_string(
                response_buffer,
                format!("ERR: Invalid cursor id {}", cursor_id).as_str(),
            );
            return Ok(());
        };

        // Create the prefix iterator
        let primary_keys_prefix = PrimaryKeyMetadata::first_key_prefix(client_state.database_id());
        let prefix = if let Some(saved_prefix) = cursor.prefix() {
            BytesMut::from(saved_prefix)
        } else {
            primary_keys_prefix.clone()
        };

        // Create the pattern matcher
        let matcher = if let Some(pattern) = pattern {
            PatternMatcher::builder().wildcard(pattern).build()
        } else {
            PatternMatcher::builder().pass_through().build()
        };

        let mut results = Vec::<BytesMut>::with_capacity(count);
        let mut db_iter = client_state.database().create_iterator(&prefix)?;
        while db_iter.valid() {
            let Some((key, val)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&primary_keys_prefix) {
                // Not a primary key
                break;
            }

            if let Some(user_key) = Self::should_collect(key, val, &matcher, &obj_type)? {
                results.push(user_key);
            }
            db_iter.next();

            if results.len() >= count {
                // we got all the elements that we wanted
                break;
            }
        }

        // Prepare cursor for next iteration
        let cursor_id =
            match cursor.create_next_cursor_for_prefix(&mut db_iter, &primary_keys_prefix) {
                Err(e) => {
                    builder.error_string(response_buffer, &format!("ERR internal error. {}", e));
                    return Ok(());
                }
                Ok(None) => {
                    // reached the end
                    client_state.remove_cursor(cursor.id());
                    0u64
                }
                Ok(Some(new_cursor)) => {
                    client_state.set_cursor(new_cursor.clone());
                    new_cursor.id()
                }
            };

        builder.add_array_len(response_buffer, 2);
        builder.add_number(response_buffer, cursor_id, false);
        builder.add_array_len(response_buffer, results.len());
        for key in &results {
            builder.add_bulk_string(response_buffer, key);
        }
        Ok(())
    }

    /// Scan helper function: return true if the `encoded_key` / `encoded_value` pair are candidates for the scan command
    fn should_collect(
        encoded_key: &[u8],
        encoded_value: &[u8],
        matcher: &PatternMatcher,
        requested_obj_type: &Option<ValueType>,
    ) -> Result<Option<BytesMut>, SableError> {
        let user_key = PrimaryKeyMetadata::from_raw(encoded_key)?
            .user_key()
            .clone();

        // Filter by object type if needed
        if let Some(requested_obj_type) = requested_obj_type {
            let cmd = CommonValueMetadata::try_from(encoded_value)?;
            if requested_obj_type.ne(&cmd.value_type()) {
                return Ok(None);
            }
        }

        Ok(if matcher.matches(&user_key) {
            Some(user_key)
        } else {
            None
        })
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
        ("set mystr myvalue", "+OK\r\n"),
        ("set mystr2 myvalue2", "+OK\r\n"),
        ("lpush mylist_1 a b c", ":3\r\n"),
        ("lpush mylist_2 a b c", ":3\r\n"),
        ("del mystr mystr2 mylist_1 mylist_2", ":4\r\n"),
        ("get mystr", "$-1\r\n"),
        ("get mystr2", "$-1\r\n"),
        ("llen mylist_1", ":0\r\n"),
        ("llen mylist_2", ":0\r\n"),
        ("del mylist_2", ":0\r\n"),
    ]; "test_del")]
    #[test_case(vec![
        ("set mykey1 myvalue", "+OK\r\n"),
        ("set mykey2 myvalue1", "+OK\r\n"),
        ("exists mykey1 mykey2", ":2\r\n"),
        ("exists mykey1 mykey2 mykey1", ":3\r\n"),
        ("exists no_such_key mykey2 mykey1", ":2\r\n"),
    ]; "test_exists")]
    #[test_case(vec![
        ("set mykey1 myvalue", "+OK\r\n"),
        ("expire mykey1 100", ":1\r\n"),
        ("get mykey1", "$7\r\nmyvalue\r\n"),
        ("set mykey2 myvalue EX 100", "+OK\r\n"),
        ("expire mykey2 90 GT", ":0\r\n"),
        ("expire mykey2 120 GT", ":1\r\n"),
        ("get mykey2", "$7\r\nmyvalue\r\n"),
        ("set mykey3 myvalue EX 100", "+OK\r\n"),
        ("expire mykey3 123 LT", ":0\r\n"),
        ("expire mykey3 90 LT", ":1\r\n"),
        ("get mykey3", "$7\r\nmyvalue\r\n"),
        ("set mykey4 myvalue EX 100", "+OK\r\n"),
        ("expire mykey4 120 NX", ":0\r\n"),
        ("expire mykey4 120 XX", ":1\r\n"),
        ("set mykey5 myvalue", "+OK\r\n"),
        ("expire mykey5 120 XX", ":0\r\n"),
        ("expire mykey5 120 NX", ":1\r\n"),
    ]; "test_expire")]
    #[test_case(vec![
        ("select 0", "+OK\r\n"),
        ("set k1 b", "+OK\r\n"),
        ("set k2 d", "+OK\r\n"),
        ("set k3 f", "+OK\r\n"),
        ("hset myhash 1 2 3 4 5 6", ":3\r\n"),
        ("keys *", "*4\r\n$2\r\nk2\r\n$2\r\nk3\r\n$6\r\nmyhash\r\n$2\r\nk1\r\n"),
        ("select 1", "+OK\r\n"),
        ("keys *", "*0\r\n"),
        ("keys", "-ERR wrong number of arguments for 'keys' command\r\n"),
        ("select 0", "+OK\r\n"),
        ("keys k*", "*3\r\n$2\r\nk2\r\n$2\r\nk3\r\n$2\r\nk1\r\n"),
        ("keys ??", "*3\r\n$2\r\nk2\r\n$2\r\nk3\r\n$2\r\nk1\r\n"),
        ("keys myhash", "*1\r\n$6\r\nmyhash\r\n"),
    ]; "test_keys")]
    #[test_case(vec![
        ("set k1 b", "+OK\r\n"),
        ("set k2 d", "+OK\r\n"),
        ("set k3 f", "+OK\r\n"),
        ("set k4 f", "+OK\r\n"),
        ("hset myhash 1 2 3 4 5 6", ":3\r\n"),
        // fetch all keys
        ("scan 0 COUNT 10", "*2\r\n:0\r\n*5\r\n$2\r\nk2\r\n$2\r\nk3\r\n$2\r\nk4\r\n$6\r\nmyhash\r\n$2\r\nk1\r\n"),
        // fetch all string keys ("myhash" is excluded)
        ("scan 0 COUNT 10 TYPE string", "*2\r\n:0\r\n*4\r\n$2\r\nk2\r\n$2\r\nk3\r\n$2\r\nk4\r\n$2\r\nk1\r\n"),
        // fetch all hash keys (only "myhash")
        ("scan 0 COUNT 10 TYPE hash", "*2\r\n:0\r\n*1\r\n$6\r\nmyhash\r\n"),
        // All keys starting with k regardless of their type
        ("scan 0 COUNT 10 MATCH k*", "*2\r\n:0\r\n*4\r\n$2\r\nk2\r\n$2\r\nk3\r\n$2\r\nk4\r\n$2\r\nk1\r\n"),
        ("scan 0 COUNT 10 MATCH *ha*", "*2\r\n:0\r\n*1\r\n$6\r\nmyhash\r\n")
    ]; "test_scan")]
    fn test_generic_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
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
