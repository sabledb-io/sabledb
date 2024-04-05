#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type,
    client::ClientState,
    command_arg_at,
    commands::{HandleCommandResult, StringCommands},
    metadata::CommonValueMetadata,
    parse_string_to_number,
    storage::StringsDb,
    types::List,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    RespBuilderV2, SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use bytes::BytesMut;
use std::rc::Rc;

pub struct GenericCommands {}

impl GenericCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        match command.metadata().name() {
            RedisCommandName::Ttl => {
                Self::ttl(client_state, command, response_buffer).await?;
            }
            RedisCommandName::Del => {
                Self::del(client_state, command, response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non generic command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::Completed)
    }

    /// O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string,
    /// the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted
    /// set or hash. Removing a single key that holds a string value is O(1).
    async fn del(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let mut iter = command.args_vec().iter();
        let _ = iter.next(); // skip the first param which is the command name

        let mut deleted_items = 0usize;
        let db_id = client_state.database_id();
        for user_key in iter {
            // obtain the lock per key
            let _unused = LockManager::lock_user_key_exclusive(user_key, db_id);
            let key_type =
                Self::query_key_type(client_state.clone(), command.clone(), user_key).await?;
            match key_type {
                Some(CommonValueMetadata::VALUE_STR) => {
                    let strings_db = StringsDb::with_storage(&client_state.store, db_id);
                    strings_db.delete(user_key)?;
                    deleted_items = deleted_items.saturating_add(1);
                }
                Some(CommonValueMetadata::VALUE_LIST) => {
                    let list = List::with_storage(&client_state.store, db_id);
                    list.remove(
                        user_key,
                        None, // remove all items
                        i32::MAX,
                        response_buffer,
                    )?;
                    deleted_items = deleted_items.saturating_add(1);
                }
                Some(unknown_type) => {
                    tracing::warn!(
                        "Deleting unknown type found in database for key `{:?}`. type=`{}`",
                        user_key,
                        unknown_type
                    );
                    let strings_db = StringsDb::with_storage(&client_state.store, db_id);
                    strings_db.delete(user_key)?;
                    deleted_items = deleted_items.saturating_add(1);
                }
                _ => {}
            }
        }

        let builder = RespBuilderV2::default();
        builder.number::<usize>(response_buffer, deleted_items, false);
        Ok(())
    }

    /// Returns the remaining time to live of a key that has a timeout.
    /// This introspection capability allows a Redis client to check how
    /// many seconds a given key will continue to be part of the dataset.
    async fn ttl(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_shared(key, client_state.database_id());
        let strings_db = StringsDb::with_storage(&client_state.store, client_state.database_id());
        if let Some((_, value_metadata)) = strings_db.get(key)? {
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

    /// Load entry from the database, don't care about the value type
    async fn query_key_type(
        client_state: Rc<ClientState>,
        _command: Rc<RedisCommand>,
        user_key: &BytesMut,
    ) -> Result<Option<u8>, SableError> {
        let internal_key =
            PrimaryKeyMetadata::new_primary_key(user_key, client_state.database_id());
        let raw_value = client_state.store.get(&internal_key)?;

        if let Some(raw_value) = raw_value {
            // determine the type
            let Some(item_type) = raw_value.first() else {
                return Err(SableError::InvalidArgument("empty value".to_string()));
            };

            match *item_type {
                CommonValueMetadata::VALUE_STR => {
                    // string type
                    return Ok(Some(CommonValueMetadata::VALUE_STR));
                }
                CommonValueMetadata::VALUE_LIST => {
                    // List type
                    return Ok(Some(CommonValueMetadata::VALUE_LIST));
                }
                _ => {
                    return Ok(None);
                }
            }
        }
        Ok(None)
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
        commands::ClientNextAction, storage::StorageAdapter, Client, ServerState, StorageOpenParams,
    };
    use std::sync::Arc;
    use std::rc::Rc;
    use std::path::PathBuf;
    use std::sync::Once;
    use test_case::test_case;

    lazy_static::lazy_static! {
        static ref INIT: Once = Once::new();
    }

    async fn initialise_test() {
        INIT.call_once(|| {
            let _ = std::fs::remove_dir_all("tests/generic_commands");
            let _ = std::fs::create_dir_all("tests/generic_commands");
        });
    }

    /// Initialise the database
    async fn open_database(command_name: &str) -> StorageAdapter {
        // Cleanup the previous test folder
        initialise_test().await;

        // create random file name
        let db_file = format!("tests/generic_commands/{}.db", command_name,);
        let _ = std::fs::create_dir_all("tests/generic_commands");
        let db_path = PathBuf::from(&db_file);
        let _ = std::fs::remove_dir_all(&db_file);
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);
        crate::storage_rocksdb!(open_params)
    }

    #[test_case(vec![
        (vec!["set", "mystr", "myvalue"], "+OK\r\n"),
        (vec!["set", "mystr2", "myvalue2"], "+OK\r\n"),
        (vec!["lpush", "mylist_1", "a", "b", "c"], ":3\r\n"),
        (vec!["lpush", "mylist_2", "a", "b", "c"], ":3\r\n"),
        (vec!["del", "mystr", "mystr2", "mylist_1", "mylist_2"], ":4\r\n"),
        (vec!["get", "mystr"], "$-1\r\n"),
        (vec!["get", "mystr2"], "$-1\r\n"),
        (vec!["llen", "mylist_1"], ":0\r\n"),
        (vec!["llen", "mylist_2"], ":0\r\n"),
        (vec!["del", "mylist_2"], ":0\r\n"),
    ]; "test_del")]
    fn test_del(args_vec: Vec<(Vec<&'static str>, &'static str)>) -> Result<(), SableError> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = open_database("test_delete_items_of_various_types").await;
            let client = Client::new(Arc::<ServerState>::default(), store, None);

            for (args, expected_value) in args_vec {
                let cmd = Rc::new(RedisCommand::for_test(args));
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
}
