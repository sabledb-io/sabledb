use crate::{
    check_args_count,
    client::ClientState,
    command_arg_at,
    commands::{HandleCommandResult, Strings},
    storage::{ZAddFlags, ZSetAddMemberResult, ZSetDb, ZSetGetScoreResult, ZSetLenResult},
    utils::RespBuilderV2,
    BytesMutUtils, LockManager, RedisCommand, RedisCommandName, SableError,
};

use bytes::BytesMut;
use std::rc::Rc;
use tokio::io::AsyncWriteExt;

pub struct ZSetCommands {}
impl ZSetCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        _tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<HandleCommandResult, SableError> {
        let mut response_buffer = BytesMut::with_capacity(256);
        match command.metadata().name() {
            RedisCommandName::Zadd => {
                Self::zadd(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zcard => {
                Self::zcard(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zincrby => {
                Self::zincrby(client_state, command, &mut response_buffer).await?;
            }
            _ => {
                return Err(SableError::InvalidArgument(format!(
                    "Non ZSet command {}",
                    command.main_command()
                )));
            }
        }
        Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer))
    }

    /// Returns the sorted set cardinality (number of elements) of the sorted set stored at key
    async fn zcard(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);
        let key = command_arg_at!(command, 1);
        let _unused = LockManager::lock_user_key_shared(key, client_state.clone())?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        match zset_db.len(key)? {
            ZSetLenResult::Some(len) => {
                builder.number_usize(response_buffer, len);
            }
            ZSetLenResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
            }
        }
        Ok(())
    }

    /// Adds all the specified members with the specified scores to the sorted set stored at key. It is possible to
    /// specify multiple score / member pairs. If a specified member is already a member of the sorted set, the score is
    /// updated and the element reinserted at the right position to ensure the correct ordering.
    ///
    /// If key does not exist, a new sorted set with the specified members as sole members is created, like if the
    /// sorted set was empty. If the key exists but does not hold a sorted set, an error is returned.
    ///
    /// The score values should be the string representation of a double precision floating point number.
    /// `+inf` and `-inf` values are valid values as well.
    async fn zadd(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);
        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // zadd
        iter.next(); // key

        let mut flags = ZAddFlags::None;
        let mut steps = 2usize;
        for opt in iter.by_ref() {
            let opt_lowercase = BytesMutUtils::to_string(opt).to_lowercase();
            match opt_lowercase.as_str() {
                "nx" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Nx, true);
                }
                "xx" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Xx, true);
                }
                "gt" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Gt, true);
                }
                "lt" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Lt, true);
                }
                "ch" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Ch, true);
                }
                "incr" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZAddFlags::Incr, true);
                }
                _ => {
                    break;
                }
            }
        }

        // sanity
        if flags.contains(ZAddFlags::Nx | ZAddFlags::Xx) {
            builder.error_string(
                response_buffer,
                "ERR XX and NX options at the same time are not compatible",
            );
            return Ok(());
        }

        if flags.contains(ZAddFlags::Lt | ZAddFlags::Gt) {
            builder.error_string(
                response_buffer,
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            );
            return Ok(());
        }

        if flags.intersects(ZAddFlags::Nx) && flags.intersects(ZAddFlags::Lt | ZAddFlags::Gt) {
            builder.error_string(
                response_buffer,
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            );
            return Ok(());
        }

        let mut iter = command.args_vec().iter();
        // Skip the parts that were already parsed
        for _ in 0..steps {
            iter.next();
        }

        // collect the score/member pairs
        let mut pairs = Vec::<(f64, &BytesMut)>::new();
        loop {
            let (score, member) = (iter.next(), iter.next());
            match (score, member) {
                (Some(score), Some(member)) => {
                    // parse the score into float
                    let Some(score) = Self::parse_score(score) else {
                        builder.error_string(response_buffer, Strings::VALUE_NOT_VALID_FLOAT);
                        return Ok(());
                    };
                    pairs.push((score, member));
                }
                (None, None) => break,
                (_, _) => {
                    builder.error_string(response_buffer, Strings::SYNTAX_ERROR);
                    return Ok(());
                }
            }
        }

        if pairs.is_empty() {
            builder.error_string(
                response_buffer,
                "ERR wrong number of arguments for 'zadd' command",
            );
            return Ok(());
        }

        if flags.intersects(ZAddFlags::Incr) && pairs.len() != 1 {
            builder.error_string(
                response_buffer,
                "ERR INCR option supports a single increment-element pair",
            );
            return Ok(());
        }

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let mut items_added = 0usize;
        for (score, member) in pairs {
            match zset_db.add(key, member, score, &flags)? {
                ZSetAddMemberResult::Some(incr) => items_added = items_added.saturating_add(incr),
                ZSetAddMemberResult::WrongType => {
                    builder.error_string(response_buffer, Strings::WRONGTYPE);
                    return Ok(());
                }
            }
        }

        // commit the changes
        zset_db.commit()?;
        builder.number_usize(response_buffer, items_added);
        Ok(())
    }

    /// `ZINCRBY key increment member`
    /// Increments the score of member in the sorted set stored at key by increment. If member does not exist in the
    /// sorted set, it is added with increment as its score (as if its previous score was 0.0). If key does not exist,
    /// a new sorted set with the specified member as its sole member is created.
    async fn zincrby(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let key = command_arg_at!(command, 1);
        let incrby = command_arg_at!(command, 2);
        let member = command_arg_at!(command, 3);

        let builder = RespBuilderV2::default();
        let Some(incrby) = Self::parse_score(incrby) else {
            builder.error_string(response_buffer, Strings::VALUE_NOT_VALID_FLOAT);
            return Ok(());
        };

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        let flags = ZAddFlags::Incr;

        match zset_db.add(key, member, incrby, &flags)? {
            ZSetAddMemberResult::WrongType => {
                builder.error_string(response_buffer, Strings::WRONGTYPE);
            }
            ZSetAddMemberResult::Some(_) => {
                // get the score
                match zset_db.get_score(key, member)? {
                    ZSetGetScoreResult::WrongType | ZSetGetScoreResult::NotFound => {
                        // this should not happen (type is chcked earlier + we just insert / updated this member)
                        return Err(SableError::ClientInvalidState);
                    }
                    ZSetGetScoreResult::Score(sc) => {
                        let score = format!("{:.2}", sc);
                        builder.bulk_string(response_buffer, score.as_bytes());
                    }
                }
            }
        }
        // commit the changes
        zset_db.commit()?;
        Ok(())
    }

    // Internal functions
    fn parse_score(score: &BytesMut) -> Option<f64> {
        let value_as_number = String::from_utf8_lossy(&score[..]).to_lowercase();
        match value_as_number.as_str() {
            "+inf" => Some(f64::MAX),
            "-inf" => Some(f64::MIN),
            _ => BytesMutUtils::parse::<f64>(score),
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

    //use crate::tests::run_and_return_output;
    use std::rc::Rc;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(vec![
        ("zadd myset gt nx 10 member", "-ERR GT, LT, and/or NX options at the same time are not compatible\r\n"),
        ("zadd myset gt lt 10 member", "-ERR GT, LT, and/or NX options at the same time are not compatible\r\n"),
        ("zadd myset nx xx 10 member", "-ERR XX and NX options at the same time are not compatible\r\n"),
        ("zadd myset 10 rein 20 dva 30 sigma", ":3\r\n"),
        ("zadd myset XX CH 20 rein 30 rein", ":2\r\n"), // 2 updates
        ("zadd myset ch 10 rein 20 rein", ":2\r\n"), // 2 updates again
        ("zadd myset NX ch 40 rein 50 rein", ":0\r\n"), // 0 updates (because of NX)
        ("zadd myset 10 rein 20 rein", ":0\r\n"), // 2 updates but without CH
        ("zadd myset 10 roadhog", ":1\r\n"), // new entry
        ("zadd myset CH INCR 5 roadhog", ":1\r\n"), // incremenet value for roadhog
        ("zcard myset", ":4\r\n"), // 4 tanks
    ]; "test_zadd")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zcard mystr", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zcard myset", ":0\r\n"),
        ("zadd myset 10 rein 20 dva", ":2\r\n"),
        ("zadd myset 10 orisa 20 sigma", ":2\r\n"),
        ("zcard myset", ":4\r\n"),
    ]; "test_zcard")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zincrby mystr 1 value", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zincrby mystr 1", "-ERR wrong number of arguments for 'zincrby' command\r\n"),
        ("zincrby myset 1 value", "$4\r\n1.00\r\n"),
        ("zincrby myset 3.5 value", "$4\r\n4.50\r\n"),
    ]; "test_zincrby")]
    fn test_zset_commands(args: Vec<(&'static str, &'static str)>) -> Result<(), SableError> {
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
