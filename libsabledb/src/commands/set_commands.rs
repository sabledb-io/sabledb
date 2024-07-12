#[allow(unused_imports)]
use crate::{
    check_args_count, check_value_type, command_arg_at,
    commands::{HandleCommandResult, StringCommands, Strings},
    metadata::{CommonValueMetadata, HashFieldKey, HashValueMetadata},
    parse_string_to_number,
    server::ClientState,
    storage::{
        GetHashMetadataResult, HashDeleteResult, HashExistsResult, HashGetMultiResult,
        HashGetResult, HashLenResult, ScanCursor, SetDb, SetPutResult,
    },
    types::List,
    utils::RespBuilderV2,
    BytesMutUtils, Expiration, LockManager, PrimaryKeyMetadata, RedisCommand, RedisCommandName,
    SableError, StorageAdapter, StringUtils, Telemetry, TimeUtils,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct SetCommands {}

impl SetCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            RedisCommandName::Sadd => {
                Self::sadd(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "None SET command {}",
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
