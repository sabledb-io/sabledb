use crate::{
    commands::{HandleCommandResult, Strings, TimeoutResponse},
    io::RespWriter,
    metadata::{ZSetMemberItem, ZSetScoreItem},
    server::ClientState,
    storage::{
        ZSetAddMemberResult, ZSetDb, ZSetDeleteMemberResult, ZSetGetMetadataResult,
        ZSetGetScoreResult, ZSetGetSmallestResult, ZSetLenResult, ZWriteFlags,
    },
    utils,
    utils::RespBuilderV2,
    BlockClientResult, BytesMutUtils, LockManager, RedisCommand, RedisCommandName, SableError,
};
use bytes::BytesMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
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

#[derive(Clone, Debug, PartialEq, Copy)]
enum AggregationMethod {
    Sum,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq)]
enum IntersectError {
    ArgCount,
    SyntaxError,
    EmptyArray,
    WrongType,
    Ok(Rc<RefCell<BTreeMap<BytesMut, f64>>>),
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
enum LexIndex<'a> {
    Include(&'a [u8]),
    Exclude(&'a [u8]),
    Min,
    Max,
    Invalid,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
enum RankIndex {
    Value(isize),
    SyntaxError,
}

#[derive(Clone, Debug, PartialEq)]
enum FindKeyWithValueResult<NumberT> {
    Value(NumberT),
    SyntaxError,
}

#[derive(Clone, Debug, PartialEq)]
enum LimitAndOffsetResult {
    Value((usize, usize)),
    SyntaxError,
}

#[derive(Clone, Debug, PartialEq)]
enum MinOrMaxResult {
    Min,
    Max,
    None,
}

enum UpperLimitState {
    NotFound,
    Found,
}

#[derive(Clone, Debug, PartialEq)]
enum TryPopResult {
    None,
    Some(Vec<(BytesMut, BytesMut)>),
    WrongType,
}

#[derive(Clone, Debug, PartialEq)]
/// Defines the output type of the ZRANGE* various methods
enum ActionType {
    /// Print the output result set
    Print,
    /// The result set is stored should be stored in destination (argument at position 1)
    Store,
    /// Remove all the members found
    #[allow(dead_code)]
    Remove,
}

/// Callback for handling the output of the Zrange* family of commands
/// - The client state
/// - The command
/// - The result set (can be empty)
/// - Does the output should include the score?
/// - The response buffer
type OutputHandler = fn(
    Rc<ClientState>,
    Rc<RedisCommand>,
    Vec<(BytesMut, f64)>,
    bool,
    &mut BytesMut,
) -> Result<(), SableError>;

/// Default output handler: write the set to the `response_buffer`
fn output_writer_handler(
    _client_state: Rc<ClientState>,
    _command: Rc<RedisCommand>,
    result_set: Vec<(BytesMut, f64)>,
    with_scores: bool,
    response_buffer: &mut BytesMut,
) -> Result<(), SableError> {
    let builder = RespBuilderV2::default();
    let response_len = if with_scores {
        result_set.len().saturating_mul(2)
    } else {
        result_set.len()
    };

    builder.add_array_len(response_buffer, response_len);
    for (member, score) in result_set {
        builder.add_bulk_string(response_buffer, &member);
        if with_scores {
            builder.add_bulk_string(response_buffer, format!("{score:.2}").as_bytes());
        }
    }
    Ok(())
}

/// Default output handler: store the output in a new destination
fn output_store_handler(
    client_state: Rc<ClientState>,
    command: Rc<RedisCommand>,
    result_set: Vec<(BytesMut, f64)>,
    _with_scores: bool,
    response_buffer: &mut BytesMut,
) -> Result<(), SableError> {
    let Some(dst) = command.args_vec().get(1) else {
        let builder = RespBuilderV2::default();
        builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
    };

    let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

    // Delete any previous entry we had there and create a new set
    zset_db.delete(dst, false)?;
    let mut count: usize = 0;
    for (member, score) in &result_set {
        zset_db.add(dst, member, *score, &ZWriteFlags::None, false)?;
        if count >= 1000 {
            // commit every 1000 adds (to avoid too much memory used)
            zset_db.commit()?;
            count = 0;
        }
        count = count.saturating_add(1);
    }

    zset_db.commit()?;
    let builder = RespBuilderV2::default();
    builder.number_usize(response_buffer, result_set.len());
    Ok(())
}

/// Default remove handler: delete all items from `result_set`. Return the number of items deleted
fn output_remove_handler(
    client_state: Rc<ClientState>,
    command: Rc<RedisCommand>,
    result_set: Vec<(BytesMut, f64)>,
    _with_scores: bool,
    response_buffer: &mut BytesMut,
) -> Result<(), SableError> {
    let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
    let key = command_arg_at!(command, 1);

    let builder = RespBuilderV2::default();
    let mut count: usize = 0;
    for (member, _) in &result_set {
        match zset_db.delete_member(key, member, false)? {
            ZSetDeleteMemberResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ZSetDeleteMemberResult::Ok => {
                count = count.saturating_add(1);
            }
            ZSetDeleteMemberResult::SetNotFound => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            ZSetDeleteMemberResult::MemberNotFound => {}
        }
    }

    zset_db.commit()?;
    builder.number_usize(response_buffer, count);
    Ok(())
}

pub struct ZSetCommands {}

#[allow(dead_code)]
impl ZSetCommands {
    pub async fn handle_command(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
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
            RedisCommandName::Zcount => {
                Self::zcount(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zdiff => {
                Self::zdiff(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zdiffstore => {
                Self::zdiffstore(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zinter => {
                Self::zinter(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zintercard => {
                Self::zintercard(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zinterstore => {
                Self::zinterstore(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zlexcount => {
                Self::zlexcount(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zmpop => {
                Self::zmpop(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Bzmpop => {
                return Self::bzmpop(client_state, command, response_buffer).await;
            }
            RedisCommandName::Zmscore => {
                Self::zmscore(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zpopmax => {
                Self::zpop_min_or_max(client_state, command, &mut response_buffer, false).await?;
            }
            RedisCommandName::Zpopmin => {
                Self::zpop_min_or_max(client_state, command, &mut response_buffer, true).await?;
            }
            RedisCommandName::Bzpopmax => {
                return Self::blocking_zpop_min_or_max(
                    client_state,
                    command,
                    response_buffer,
                    false,
                )
                .await;
            }
            RedisCommandName::Bzpopmin => {
                return Self::blocking_zpop_min_or_max(
                    client_state,
                    command,
                    response_buffer,
                    true,
                )
                .await;
            }
            RedisCommandName::Zrandmember => {
                // write directly to the client
                Self::zrandmember(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
            }
            RedisCommandName::Zrange => {
                // write directly to the client
                Self::zrange(
                    client_state,
                    command,
                    &mut response_buffer,
                    output_writer_handler,
                )
                .await?;
            }
            RedisCommandName::Zrangestore => {
                // store the output into a new destination
                Self::zrangestore(
                    client_state,
                    command,
                    &mut response_buffer,
                    output_store_handler,
                )
                .await?;
            }
            RedisCommandName::Zrangebyscore => {
                expect_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );
                Self::zrangebyscore(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Print,
                    false,
                    output_writer_handler,
                )
                .await?;
            }
            RedisCommandName::Zrevrangebyscore => {
                expect_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );

                Self::zrangebyscore(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Print,
                    true,
                    output_writer_handler,
                )
                .await?;
            }
            RedisCommandName::Zrangebylex => {
                expect_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );
                Self::zrangebylex(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Print,
                    false,
                    output_writer_handler,
                )
                .await?;
            }
            RedisCommandName::Zrevrangebylex => {
                expect_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );
                Self::zrangebylex(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Print,
                    true,
                    output_writer_handler,
                )
                .await?;
            }
            RedisCommandName::Zrank => {
                Self::zrank(client_state, command, &mut response_buffer, false).await?;
            }
            RedisCommandName::Zrem => {
                Self::zrem(client_state, command, &mut response_buffer).await?;
            }
            RedisCommandName::Zremrangebylex => {
                expect_exact_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );
                Self::zrangebylex(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Remove,
                    false,
                    output_remove_handler,
                )
                .await?;
            }
            RedisCommandName::Zremrangebyrank => {
                expect_exact_args_count!(
                    command,
                    4,
                    &mut response_buffer,
                    HandleCommandResult::ResponseBufferUpdated(response_buffer)
                );
                Self::zrangebyrank(
                    client_state,
                    command,
                    &mut response_buffer,
                    ActionType::Remove,
                    false,
                    output_remove_handler,
                )
                .await?;
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
        let _unused = LockManager::lock_user_key_shared(key, client_state.clone()).await?;
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

        let mut flags = ZWriteFlags::None;
        let mut steps = 2usize;
        for opt in iter.by_ref() {
            let opt_lowercase = BytesMutUtils::to_string(opt).to_lowercase();
            match opt_lowercase.as_str() {
                "nx" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Nx, true);
                }
                "xx" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Xx, true);
                }
                "gt" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Gt, true);
                }
                "lt" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Lt, true);
                }
                "ch" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Ch, true);
                }
                "incr" => {
                    steps = steps.saturating_add(1);
                    flags.set(ZWriteFlags::Incr, true);
                }
                _ => {
                    break;
                }
            }
        }

        // sanity
        if flags.contains(ZWriteFlags::Nx | ZWriteFlags::Xx) {
            builder.error_string(
                response_buffer,
                "ERR XX and NX options at the same time are not compatible",
            );
            return Ok(());
        }

        if flags.contains(ZWriteFlags::Lt | ZWriteFlags::Gt) {
            builder.error_string(
                response_buffer,
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            );
            return Ok(());
        }

        if flags.intersects(ZWriteFlags::Nx) && flags.intersects(ZWriteFlags::Lt | ZWriteFlags::Gt)
        {
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

        if flags.intersects(ZWriteFlags::Incr) && pairs.len() != 1 {
            builder.error_string(
                response_buffer,
                "ERR INCR option supports a single increment-element pair",
            );
            return Ok(());
        }

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let mut items_added = 0usize;
        for (score, member) in &pairs {
            match zset_db.add(key, member, *score, &flags, false)? {
                ZSetAddMemberResult::Some(incr) => items_added = items_added.saturating_add(incr),
                ZSetAddMemberResult::WrongType => {
                    builder.error_string(response_buffer, Strings::WRONGTYPE);
                    return Ok(());
                }
            }
        }

        // commit the changes
        zset_db.commit()?;

        // Wakeup up to pairs.len() clients waiting on `key`
        client_state
            .server_inner_state()
            .wakeup_clients(key, pairs.len())
            .await;
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

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        let flags = ZWriteFlags::Incr;

        match zset_db.add(key, member, incrby, &flags, false)? {
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

    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
    /// Returns all the elements in the sorted set at key with a score between min and max (including elements with score
    /// equal to min or max). The elements are considered to be ordered from low to high scores
    async fn zcount(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let key = command_arg_at!(command, 1);
        let min = command_arg_at!(command, 2);
        let max = command_arg_at!(command, 3);

        let builder = RespBuilderV2::default();
        let Some((start_score, include_start_score)) = Self::parse_score_index(min) else {
            builder_return_min_max_not_float!(builder, response_buffer);
        };

        let Some((end_score, include_end_score)) = Self::parse_score_index(max) else {
            builder_return_min_max_not_float!(builder, response_buffer);
        };

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = zset_md_or_0_builder!(zset_db, key, builder, response_buffer);

        // empty set? empty array
        if md.is_empty() {
            builder.number_usize(response_buffer, 0);
            return Ok(());
        }

        // Determine the starting score
        let prefix = md.prefix_by_score(None);
        let mut db_iter = client_state.database().create_iterator(&prefix)?;
        if !db_iter.valid() {
            // invalud iterator
            builder_return_number!(builder, response_buffer, 0);
        }

        // Find the first item in the set that complies with the start condition
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                builder_return_number!(builder, response_buffer, 0);
            };

            if !key.starts_with(&prefix) {
                builder_return_number!(builder, response_buffer, 0);
            }

            let set_item = ZSetScoreItem::from_bytes(key)?;
            if (include_start_score && set_item.score() >= start_score)
                || (!include_start_score && set_item.score() > start_score)
            {
                let prefix = md.prefix_by_score(Some(set_item.score()));
                // place the iterator on the range start
                db_iter = client_state.database().create_iterator(&prefix)?;
                break;
            }
            db_iter.next();
        }

        // All items must start with `zset_prefix` regardless of the user conditions
        let zset_prefix = md.prefix_by_score(None);

        let mut matched_entries = 0usize;
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&zset_prefix) {
                // reached the set limit
                break;
            }

            let field = ZSetScoreItem::from_bytes(key)?;

            // Check end condition
            if (include_end_score && field.score() > end_score)
                || (!include_end_score && field.score() >= end_score)
            {
                break;
            }

            // Store the member + score
            matched_entries = matched_entries.saturating_add(1);
            db_iter.next();
        }

        builder_return_number!(builder, response_buffer, matched_entries);
    }

    /// `ZDIFF numkeys key [key ...] [WITHSCORES]`
    /// Computes the difference between the first and all successive input sorted sets
    async fn zdiff(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let numkeys = command_arg_at!(command, 1);
        let builder = RespBuilderV2::default();
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        if numkeys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }
        let with_scores = matches!(command.args_vec().last(), Some(value) if value.to_ascii_uppercase().eq(b"WITHSCORES"));

        // The keys are located starting from position [2..] of the command vector,
        // however if the last token in the vector is "WITHVALUES" it will be [2..len - 1]
        let keys_vec = if with_scores {
            let keys_vec = &command.args_vec()[2usize..];
            let Some((_last, keys_vec)) = keys_vec.split_last() else {
                builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
            };
            keys_vec
        } else {
            &command.args_vec()[2usize..]
        };

        // Now that we have the number of keys, make sure that we have all the keys we need
        if numkeys != keys_vec.len() {
            builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
        }

        let iter = keys_vec.iter();
        let user_keys: Vec<&BytesMut> = iter.collect();
        if user_keys.len() == 1 {
            builder_return_empty_array!(builder, response_buffer);
        }

        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        let mut iter = user_keys.iter();
        let Some(main_key) = iter.next() else {
            builder_return_syntax_error!(builder, response_buffer);
        };

        // load the keys of the main set into the memory (Use `BTreeSet` to keep the items sorted)
        let result_set = Rc::new(RefCell::new(BTreeMap::<BytesMut, f64>::new()));

        // collect the main set members
        let main_items_clone = result_set.clone();
        let iter_result = Self::iterate_by_member_and_apply(
            client_state.clone(),
            main_key,
            move |member, score| {
                main_items_clone
                    .borrow_mut()
                    .insert(BytesMut::from(member), score);
                Ok(IterateCallbackResult::Continue)
            },
        )?;

        if iter_result == IterateResult::WrongType {
            builder_return_wrong_type!(builder, response_buffer);
        }

        // loop over the other sets, removing duplicate items from the main set
        for set_name in iter {
            if result_set.borrow().is_empty() {
                // No need to continue
                break;
            }
            let main_items_clone = result_set.clone();
            let iter_result = Self::iterate_by_member_and_apply(
                client_state.clone(),
                set_name,
                move |member, _score| {
                    if main_items_clone.borrow().contains_key(member) {
                        // remove `member` from the result
                        main_items_clone.borrow_mut().remove(member);
                        if main_items_clone.borrow().is_empty() {
                            return Ok(IterateCallbackResult::Break);
                        }
                    }
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            if iter_result == IterateResult::WrongType {
                builder_return_wrong_type!(builder, response_buffer);
            }
        }

        let array_len = result_set.borrow().len();
        builder.add_array_len(
            response_buffer,
            if with_scores {
                array_len.saturating_mul(2)
            } else {
                array_len
            },
        );
        for (key, score) in result_set.borrow().iter() {
            builder.add_bulk_string(response_buffer, key);
            if with_scores {
                builder.add_bulk_string(response_buffer, format!("{score:.2}").as_bytes());
            }
        }
        Ok(())
    }

    /// `ZDIFFSTORE destination numkeys key [key ...]`
    /// Computes the difference between the first and all successive input sorted sets and stores the result in
    /// destination. The total number of input keys is specified by numkeys
    async fn zdiffstore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let destination = command_arg_at!(command, 1);
        let builder = RespBuilderV2::default();
        let numkeys = command_arg_at!(command, 2);
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        if numkeys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }

        // The keys are located starting from position [3..] of the command vector,
        // however if the last token in the vector is "WITHVALUES" it will be [2..len - 1]
        let keys_vec = &command.args_vec()[3usize..];

        // Now that we have the number of keys, make sure that we have all the keys we need
        if numkeys != keys_vec.len() {
            builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
        }

        let iter = keys_vec.iter();
        let mut user_keys: Vec<&BytesMut> = iter.collect();
        if user_keys.len() == 1 {
            builder_return_empty_array!(builder, response_buffer);
        }
        user_keys.push(destination);

        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        let mut iter = user_keys.iter();
        let Some(main_key) = iter.next() else {
            builder_return_syntax_error!(builder, response_buffer);
        };

        // load the keys of the main set into the memory (Use `BTreeSet` to keep the items sorted)
        let result_set = Rc::new(RefCell::new(BTreeMap::<BytesMut, f64>::new()));

        // collect the main set members
        let main_items_clone = result_set.clone();
        let iter_result = Self::iterate_by_member_and_apply(
            client_state.clone(),
            main_key,
            move |member, score| {
                main_items_clone
                    .borrow_mut()
                    .insert(BytesMut::from(member), score);
                Ok(IterateCallbackResult::Continue)
            },
        )?;

        if iter_result == IterateResult::WrongType {
            builder_return_wrong_type!(builder, response_buffer);
        }

        // loop over the other sets, removing duplicate items from the main set
        for set_name in iter {
            if result_set.borrow().is_empty() {
                // No need to continue
                break;
            }
            let main_items_clone = result_set.clone();
            let iter_result = Self::iterate_by_member_and_apply(
                client_state.clone(),
                set_name,
                move |member, _score| {
                    if main_items_clone.borrow().contains_key(member) {
                        // remove `member` from the result
                        main_items_clone.borrow_mut().remove(member);
                        if main_items_clone.borrow().is_empty() {
                            return Ok(IterateCallbackResult::Break);
                        }
                    }
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            if iter_result == IterateResult::WrongType {
                builder_return_wrong_type!(builder, response_buffer);
            }
        }

        // Zdiffstore overrides
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        zset_db.delete(destination, false)?;
        for (key, score) in result_set.borrow().iter() {
            zset_db.add(destination, key, *score, &ZWriteFlags::None, false)?;
        }
        zset_db.commit()?;
        builder.number_usize(response_buffer, result_set.borrow().len());
        Ok(())
    }

    /// `ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]`
    /// Computes the intersection of numkeys sorted sets given by the specified keys
    ///
    /// Using the `WEIGHTS` option, it is possible to specify a multiplication factor for each input sorted set.
    /// This means that the score of every element in every input sorted set is multiplied by this factor before
    /// being passed to the aggregation function. When WEIGHTS is not given, the multiplication factors default to 1.
    ///
    /// With the `AGGREGATE` option, it is possible to specify how the results of the union are aggregated.
    /// This option defaults to `SUM`, where the score of an element is summed across the inputs where it exists.
    /// When this option is set to either MIN or MAX, the resulting set will contain the minimum or maximum score of an
    /// element across the inputs where it exists.
    async fn zinter(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let builder = RespBuilderV2::default();
        let numkeys = command_arg_at!(command, 1);
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        if numkeys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }

        let reserved_words: HashSet<&'static str> =
            ["WEIGHTS", "AGGREGATE", "SUM", "MIN", "MAX", "WITHSCORES"]
                .iter()
                .copied()
                .collect();
        let Ok(user_keys) =
            Self::parse_keys_to_lock(command.clone(), 2, numkeys, &reserved_words, None)
        else {
            builder_return_syntax_error!(builder, response_buffer);
        };

        let user_keys: Vec<&BytesMut> = user_keys.iter().collect();
        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        let result_set = match Self::intersect(client_state.clone(), command.clone(), 2, numkeys)? {
            IntersectError::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            IntersectError::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectError::EmptyArray => {
                builder_return_empty_array!(builder, response_buffer);
            }
            IntersectError::ArgCount => {
                builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
            }
            IntersectError::Ok(result) => result,
        };

        // do we want scores?
        let with_scores = Self::withscores(command.clone());

        // Finally, generate the output
        builder.add_array_len(
            response_buffer,
            if with_scores {
                result_set.borrow().len() * 2usize
            } else {
                result_set.borrow().len()
            },
        );

        for (member, score) in result_set.borrow().iter() {
            builder.add_bulk_string(response_buffer, member);
            if with_scores {
                builder.add_bulk_string(response_buffer, format!("{:.2}", score).as_bytes())
            }
        }
        Ok(())
    }

    /// `ZINTERCARD numkeys key [key ...] [LIMIT limit]`
    /// This command is similar to ZINTER, but instead of returning the result set, it returns just the cardinality of the result
    async fn zintercard(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let builder = RespBuilderV2::default();
        let numkeys = command_arg_at!(command, 1);
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        if numkeys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }

        let reserved_words: HashSet<&'static str> = ["LIMIT"].iter().copied().collect();
        let Ok(user_keys) =
            Self::parse_keys_to_lock(command.clone(), 2, numkeys, &reserved_words, None)
        else {
            builder_return_syntax_error!(builder, response_buffer);
        };
        let user_keys: Vec<&BytesMut> = user_keys.iter().collect();
        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        let result = match Self::intersect(client_state.clone(), command.clone(), 2, numkeys)? {
            IntersectError::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            IntersectError::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectError::EmptyArray => {
                builder_return_empty_array!(builder, response_buffer);
            }
            IntersectError::ArgCount => {
                builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
            }
            IntersectError::Ok(result) => result,
        };

        // Do we have a limit?
        let limit = match Self::get_limit(command.clone()) {
            FindKeyWithValueResult::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            FindKeyWithValueResult::Value(limit) => limit,
        };

        // Finally, write the result
        builder.number_usize(
            response_buffer,
            if result.borrow().len() > limit {
                limit
            } else {
                result.borrow().len()
            },
        );
        Ok(())
    }

    /// `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]`
    /// This command is similar to ZINTER, but instead of returning the result set, it returns just the cardinality of the result
    async fn zinterstore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let builder = RespBuilderV2::default();
        let destination = command_arg_at!(command, 1);
        let numkeys = command_arg_at!(command, 2);
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        if numkeys == 0 {
            builder_return_at_least_1_key!(builder, response_buffer, command);
        }

        let reserved_words: HashSet<&'static str> = ["WEIGHTS", "AGGREGATE", "SUM", "MIN", "MAX"]
            .iter()
            .copied()
            .collect();

        let Ok(user_keys) = Self::parse_keys_to_lock(
            command.clone(),
            3,
            numkeys,
            &reserved_words,
            Some(destination),
        ) else {
            builder_return_syntax_error!(builder, response_buffer);
        };
        let user_keys: Vec<&BytesMut> = user_keys.iter().collect();

        // Lock the database. The lock must be done here (it has to do with how txn are working)
        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        let result = match Self::intersect(client_state.clone(), command.clone(), 3, numkeys)? {
            IntersectError::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            IntersectError::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            IntersectError::EmptyArray => {
                builder_return_empty_array!(builder, response_buffer);
            }
            IntersectError::ArgCount => {
                builder_return_wrong_args_count!(builder, response_buffer, command.main_command());
            }
            IntersectError::Ok(result) => result,
        };

        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        zset_db.delete(destination, false)?;
        for (key, score) in result.borrow().iter() {
            zset_db.add(destination, key, *score, &ZWriteFlags::None, false)?;
        }
        zset_db.commit()?;
        builder.number_usize(response_buffer, result.borrow().len());
        Ok(())
    }

    /// `ZLEXCOUNT key min max`
    /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
    /// ordering, this command returns the number of elements in the sorted set at key with a value between min and max
    async fn zlexcount(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let key = command_arg_at!(command, 1);
        let min = command_arg_at!(command, 2);
        let max = command_arg_at!(command, 3);

        let builder = RespBuilderV2::default();
        let min = Self::parse_lex_index(min.as_ref());
        let max = Self::parse_lex_index(max.as_ref());

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = match zset_db.get_metadata(key)? {
            ZSetGetMetadataResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ZSetGetMetadataResult::NotFound => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            ZSetGetMetadataResult::Some(md) => md,
        };

        let mut db_iter = match min {
            LexIndex::Invalid => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            LexIndex::Include(prefix) => {
                let prefix = md.prefix_by_member(Some(prefix));
                client_state.database().create_iterator(&prefix)?
            }
            LexIndex::Exclude(prefix) => {
                let prefix = md.prefix_by_member(Some(prefix));
                let mut db_iter = client_state.database().create_iterator(&prefix)?;
                db_iter.next(); // skip this entry
                db_iter
            }
            LexIndex::Max => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            LexIndex::Min => {
                let prefix = md.prefix_by_member(None);
                client_state.database().create_iterator(&prefix)?
            }
        };

        // Setup the upper limit
        let upper_limit = match max {
            LexIndex::Invalid => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            LexIndex::Include(prefix) => Some((md.prefix_by_member(Some(prefix)), true)),
            LexIndex::Exclude(prefix) => Some((md.prefix_by_member(Some(prefix)), false)),
            LexIndex::Max => None,
            LexIndex::Min => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
        };

        let set_prefix = md.prefix_by_member(None);
        let mut state = UpperLimitState::NotFound;
        let mut count = 0usize;
        while db_iter.valid() {
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                // Key does not belong to this set
                break;
            }

            if let Some((end_prefix, include_it)) = &upper_limit {
                if !Self::can_iter_continue(key, end_prefix.as_ref(), *include_it, &mut state) {
                    break;
                }
            }
            count = count.saturating_add(1);
            db_iter.next();
        }

        builder.number_usize(response_buffer, count);
        Ok(())
    }

    /// `ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]`
    /// Pops one or more elements, that are member-score pairs, from the first non-empty sorted set in the provided list
    /// of key names
    async fn zmpop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let builder = RespBuilderV2::default();
        let numkeys = command_arg_at!(command, 1);
        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder_return_value_not_int!(builder, response_buffer);
        };

        let reserved_words: HashSet<&'static str> =
            ["COUNT", "MIN", "MAX"].iter().copied().collect();
        let Ok(keys_to_lock) =
            Self::parse_keys_to_lock(command.clone(), 2, numkeys, &reserved_words, None)
        else {
            builder_return_syntax_error!(builder, response_buffer);
        };

        let count = match Self::get_count(command.clone(), 1) {
            FindKeyWithValueResult::Value(count) => count,
            FindKeyWithValueResult::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
        };

        let min_members = match Self::get_min_or_max(command.clone()) {
            MinOrMaxResult::None => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            MinOrMaxResult::Max => false,
            MinOrMaxResult::Min => true,
        };

        let user_keys: Vec<&BytesMut> = keys_to_lock.iter().collect();
        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        for key in &user_keys {
            match Self::try_pop(client_state.clone(), key, count, min_members)? {
                TryPopResult::Some(items) => {
                    // build response
                    builder.add_array_len(response_buffer, 2);
                    builder.add_bulk_string(response_buffer, key);
                    builder.add_array_len(response_buffer, items.len());
                    for (member, score) in &items {
                        builder.add_array_len(response_buffer, 2);
                        builder.add_bulk_string(response_buffer, member);
                        builder.add_bulk_string(response_buffer, score);
                    }
                    return Ok(());
                }
                TryPopResult::None => {}
                TryPopResult::WrongType => {
                    builder_return_wrong_type!(builder, response_buffer);
                }
            }
        }
        // if we reached here, nothing was popped
        builder.null_array(response_buffer);
        Ok(())
    }

    /// `BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]`
    /// `BZMPOP` is the blocking variant of `ZMPOP`.
    /// Pops one or more elements, that are member-score pairs, from the first non-empty sorted set in the provided list
    /// of key names
    async fn bzmpop(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        mut response_buffer: BytesMut,
    ) -> Result<HandleCommandResult, SableError> {
        let builder = RespBuilderV2::default();
        if !command.expect_args_count(5) {
            let builder = RespBuilderV2::default();
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                command.main_command()
            );
            builder.error_string(&mut response_buffer, &errmsg);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        }

        let numkeys = command_arg_at!(command, 2);
        let timeout = command_arg_at!(command, 1);
        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout) else {
            builder.error_string(
                &mut response_buffer,
                "ERR timeout is not a float or out of range",
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let Some(numkeys) = BytesMutUtils::parse::<usize>(numkeys) else {
            builder.error_string(
                &mut response_buffer,
                Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE,
            );
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let reserved_words: HashSet<&'static str> =
            ["COUNT", "MIN", "MAX"].iter().copied().collect();
        let Ok(keys_to_lock) =
            Self::parse_keys_to_lock(command.clone(), 3, numkeys, &reserved_words, None)
        else {
            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let count = match Self::get_count(command.clone(), 1) {
            FindKeyWithValueResult::Value(count) => count,
            FindKeyWithValueResult::SyntaxError => {
                builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            }
        };

        let min_members = match Self::get_min_or_max(command.clone()) {
            MinOrMaxResult::None => {
                builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
            }
            MinOrMaxResult::Max => false,
            MinOrMaxResult::Min => true,
        };

        let user_keys: Vec<&BytesMut> = keys_to_lock.iter().collect();
        let _unused =
            LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone()).await?;

        for key in &user_keys {
            match Self::try_pop(client_state.clone(), key, count, min_members)? {
                TryPopResult::Some(items) => {
                    // build response
                    builder.add_array_len(&mut response_buffer, 2);
                    builder.add_bulk_string(&mut response_buffer, key);
                    builder.add_array_len(&mut response_buffer, items.len());
                    for (member, score) in &items {
                        builder.add_array_len(&mut response_buffer, 2);
                        builder.add_bulk_string(&mut response_buffer, member);
                        builder.add_bulk_string(&mut response_buffer, score);
                    }
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                TryPopResult::None => {}
                TryPopResult::WrongType => {
                    builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
            }
        }

        // Block the client or return null array. If txn is active the macro will return null array
        let rx =
            block_client_for_keys_return_null_array!(client_state, &keys_to_lock, response_buffer);
        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it
        Ok(HandleCommandResult::Blocked((
            rx,
            std::time::Duration::from_millis(timeout_ms),
            TimeoutResponse::NullArrray,
        )))
    }

    /// `ZMSCORE key member [member ...]`
    /// Returns the scores associated with the specified members in the sorted set stored at key. For every member that
    /// does not exist in the sorted set, a nil value is returned.
    async fn zmscore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // zmscore
        iter.next(); // key

        let mut members = Vec::<&BytesMut>::new();
        for member in iter {
            members.push(member);
        }

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let builder = RespBuilderV2::default();
        builder.add_array_len(response_buffer, members.len());
        for member in members {
            match zset_db.get_score(key, member)? {
                ZSetGetScoreResult::WrongType => {
                    builder_return_wrong_type!(builder, response_buffer);
                }
                ZSetGetScoreResult::NotFound => {
                    builder.add_null_string(response_buffer);
                }
                ZSetGetScoreResult::Score(score) => {
                    builder.add_bulk_string(response_buffer, format!("{:.2}", score).as_bytes());
                }
            }
        }
        Ok(())
    }

    /// `ZPOPMAX key [count]` / `ZPOPMIN key [count]`
    /// Removes and returns up to count members with the highest/minimal scores in the sorted set stored at key
    /// When left unspecified, the default value for count is 1. Specifying a count value that is higher than the
    /// sorted set's cardinality will not produce an error
    async fn zpop_min_or_max(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        pop_min: bool,
    ) -> Result<(), SableError> {
        check_args_count!(command, 2, response_buffer);

        let key = command_arg_at!(command, 1);
        let builder = RespBuilderV2::default();

        let count = match command.arg_count() {
            2 => 1usize,
            3 => {
                let count = command_arg_at_as_str!(command, 2);
                let Ok(count) = count.parse::<usize>() else {
                    builder.error_string(response_buffer, Strings::ZERR_VALUE_MUST_BE_POSITIVE);
                    return Ok(());
                };
                count
            }
            _ => {
                builder_return_syntax_error!(builder, response_buffer);
            }
        };

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;

        match Self::try_pop(client_state.clone(), key, count, pop_min)? {
            TryPopResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            TryPopResult::None => {
                builder_return_empty_array!(builder, response_buffer);
            }
            TryPopResult::Some(result) => {
                builder.add_array_len(response_buffer, result.len().saturating_mul(2));
                for (member, score) in &result {
                    builder.add_bulk_string(response_buffer, member);
                    builder.add_bulk_string(response_buffer, score);
                }
            }
        }
        Ok(())
    }

    /// `BZPOPMIN key [key ...] timeout` / `BZPOPMAX key [key ...] timeout`
    /// The blocking variant
    async fn blocking_zpop_min_or_max(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        mut response_buffer: BytesMut,
        pop_min: bool,
    ) -> Result<HandleCommandResult, SableError> {
        if !command.expect_args_count(3) {
            let builder = RespBuilderV2::default();
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                command.main_command()
            );
            builder.error_string(&mut response_buffer, &errmsg);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        }

        let builder = RespBuilderV2::default();
        let Some(timeout_secs) = command.args_vec().last() else {
            builder.error_string(&mut response_buffer, Strings::SYNTAX_ERROR);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let Some(timeout_secs) = BytesMutUtils::parse::<f64>(timeout_secs) else {
            builder.error_string(&mut response_buffer, Strings::ZERR_TIMEOUT_NOT_FLOAT);
            return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
        };

        let end_index = command.args_vec().len().saturating_sub(1);
        let keys = &command.args_vec()[1usize..end_index];

        let keys_to_lock: Vec<&BytesMut> = keys.iter().collect();
        let _unused =
            LockManager::lock_user_keys_exclusive(&keys_to_lock, client_state.clone()).await?;

        for key in keys {
            match Self::try_pop(client_state.clone(), key, 1, pop_min)? {
                TryPopResult::WrongType => {
                    builder.error_string(&mut response_buffer, Strings::WRONGTYPE);
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
                TryPopResult::None => { /* try other keys */ }
                TryPopResult::Some(result) => {
                    builder.add_array_len(&mut response_buffer, 3);
                    builder.add_bulk_string(&mut response_buffer, key);
                    let Some((member, score)) = result.first() else {
                        // can't really happen...
                        return Err(SableError::ClientInvalidState);
                    };
                    builder.add_bulk_string(&mut response_buffer, member);
                    builder.add_bulk_string(&mut response_buffer, score);
                    return Ok(HandleCommandResult::ResponseBufferUpdated(response_buffer));
                }
            }
        }

        // If we reached here, we need to block the client

        // Block the client or return null array. If txn is active the macro will return null array
        let rx = block_client_for_keys_return_null_array!(client_state, keys, response_buffer);

        let timeout_ms = (timeout_secs * 1000.0) as u64; // convert to milliseconds and round it
        Ok(HandleCommandResult::Blocked((
            rx,
            std::time::Duration::from_millis(timeout_ms),
            TimeoutResponse::NullArrray,
        )))
    }

    /// Try to pop `count` members from `key` set.
    /// If `lowest_score_items` is `true`, remove the members with lowest score
    fn try_pop(
        client_state: Rc<ClientState>,
        key: &BytesMut,
        count: usize,
        items_with_low_score: bool,
    ) -> Result<TryPopResult, SableError> {
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        let md = match zset_db.get_metadata(key)? {
            ZSetGetMetadataResult::Some(md) => md,
            ZSetGetMetadataResult::NotFound => {
                return Ok(TryPopResult::None);
            }
            ZSetGetMetadataResult::WrongType => {
                return Ok(TryPopResult::WrongType);
            }
        };

        let count = std::cmp::min(count, md.len() as usize);
        let prefix = md.prefix_by_score(None);

        // items with lowest scores are placed at the start
        let mut db_iter = if items_with_low_score {
            client_state.database().create_iterator(&prefix)?
        } else {
            let upper_bound = md.score_upper_bound_prefix();
            client_state
                .database()
                .create_reverse_iterator(&upper_bound)?
        };

        let mut result = Vec::<(BytesMut, BytesMut)>::new();
        while db_iter.valid() {
            if count == result.len() {
                break;
            }

            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            let score_member = ZSetScoreItem::from_bytes(key)?;
            let score_value = format!("{:.2}", score_member.score());
            result.push((score_member.member().into(), score_value.as_str().into()));
            db_iter.next();
        }

        if !result.is_empty() {
            for (member, _) in &result {
                zset_db.delete_member(key, member, false)?;
            }
            zset_db.commit()?;
        }
        Ok(TryPopResult::Some(result))
    }

    /// Check whether we reached the upper limit `end_prefix`.
    /// If `end_prefix_included` is `true`, we allow this prefix to be included
    /// If `end_prefix_included` is `false`, this function return false, at the first
    /// `end_prefix` found
    fn can_iter_continue(
        current_key: &[u8],
        end_prefix: &[u8],
        end_prefix_included: bool,
        state: &mut UpperLimitState,
    ) -> bool {
        match state {
            UpperLimitState::NotFound => {
                if current_key.lt(end_prefix) {
                    true
                } else if current_key.eq(end_prefix) {
                    *state = UpperLimitState::Found;
                    // We found the upper limit, "upper limit reached" is
                    // now determined based on whether or not we should include it
                    // i.e if the upper limit is NOT included, then we reached the upper
                    // limit boundary
                    end_prefix_included
                } else {
                    // We passed the upper limit
                    *state = UpperLimitState::Found;
                    false
                }
            }
            UpperLimitState::Found => current_key.eq(end_prefix),
        }
    }

    /// `ZRANDMEMBER key [count [WITHSCORES]]`
    async fn zrandmember(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 2, tx);
        let key = command_arg_at!(command, 1);

        let mut iter = command.args_vec().iter();
        iter.next(); // skip "ZRANDMEMBER"
        iter.next(); // skips the key

        let mut writer = RespWriter::new(tx, 1024, client_state.clone());

        // Parse the arguments
        let (count, with_scores, allow_dups) = match (iter.next(), iter.next()) {
            (Some(count), None) => {
                let Some(count) = BytesMutUtils::parse::<i64>(count) else {
                    writer_return_value_not_int!(writer);
                };
                (count.abs(), false, count < 0)
            }
            (Some(count), Some(with_scores)) => {
                let Some(count) = BytesMutUtils::parse::<i64>(count) else {
                    writer_return_value_not_int!(writer);
                };
                if BytesMutUtils::to_string(with_scores).to_lowercase() != "withscores" {
                    writer_return_syntax_error!(writer);
                }
                (count.abs(), true, count < 0)
            }
            (_, _) => (1i64, false, false),
        };

        // multiple db calls, requires exclusive lock
        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        // determine the array length
        let md = match zset_db.get_metadata(key)? {
            ZSetGetMetadataResult::Some(md) => md,
            ZSetGetMetadataResult::NotFound => {
                writer_return_null_string!(writer);
            }
            ZSetGetMetadataResult::WrongType => {
                writer_return_wrong_type!(writer);
            }
        };

        // Adjust the "count"
        let count = if allow_dups {
            count
        } else {
            std::cmp::min(count, md.len() as i64)
        };

        // fast bail out
        if count.eq(&0) {
            writer_return_empty_array!(writer);
        }

        let possible_indexes: Vec<usize> = (0..md.len() as usize).collect();

        // select the indices we want to pick (indices is sorted, descending order)
        let mut indices =
            utils::choose_multiple_values(count as usize, &possible_indexes, allow_dups)?;

        // When returning multiple items, we return an array
        if indices.len() > 1 || with_scores {
            writer
                .add_array_len(if with_scores {
                    indices.len() * 2
                } else {
                    indices.len()
                })
                .await?;
        }

        // Create an iterator and place at at the start of the set members
        let mut curidx = 0usize;
        let prefix = md.prefix_by_member(None);

        // strategy: create an iterator on all the hash items and maintain a "curidx" that keeps the current visited
        // index for every element, compare it against the first item in the "chosen" vector which holds a sorted list of
        // chosen indices
        let mut db_iter = client_state.database().create_iterator(&prefix)?;

        while db_iter.valid() && !indices.is_empty() {
            // get the key & value
            let Some((key, value)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            // extract the key from the row data
            while let Some(wanted_index) = indices.front() {
                if curidx.eq(wanted_index) {
                    let member_field = ZSetMemberItem::from_bytes(key)?;
                    writer.add_bulk_string(member_field.member()).await?;
                    if with_scores {
                        let score = zset_db.score_from_bytes(value)?;
                        writer
                            .add_bulk_string(format!("{:.2}", score).as_bytes())
                            .await?;
                    }
                    // pop the first element
                    indices.pop_front();
                    // Don't progress the iterator here,  we might have another item with the same index
                } else {
                    break;
                }
            }
            curidx = curidx.saturating_add(1);
            db_iter.next();
        }
        writer.flush().await?;
        Ok(())
    }

    /// `ZRANGESTORE dst key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
    /// Returns the specified range of elements in the sorted set stored at <key>
    async fn zrangestore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        output_handler: OutputHandler,
    ) -> Result<(), SableError> {
        check_args_count!(command, 5, response_buffer);

        let reverse = Self::has_optional_arg(command.clone(), "rev", 5);
        if Self::has_optional_arg(command.clone(), "byscore", 5) {
            Self::zrangebyscore(
                client_state,
                command,
                response_buffer,
                ActionType::Store,
                reverse,
                output_handler,
            )
            .await
        } else if Self::has_optional_arg(command.clone(), "bylex", 5) {
            Self::zrangebylex(
                client_state,
                command,
                response_buffer,
                ActionType::Store,
                reverse,
                output_handler,
            )
            .await
        } else {
            Self::zrangebyrank(
                client_state,
                command,
                response_buffer,
                ActionType::Store,
                reverse,
                output_handler,
            )
            .await
        }
    }

    /// `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
    /// Returns the specified range of elements in the sorted set stored at <key>
    async fn zrange(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        output_handler: OutputHandler,
    ) -> Result<(), SableError> {
        check_args_count!(command, 4, response_buffer);

        let reverse = Self::has_optional_arg(command.clone(), "rev", 4);
        if Self::has_optional_arg(command.clone(), "byscore", 4) {
            Self::zrangebyscore(
                client_state,
                command,
                response_buffer,
                ActionType::Print,
                reverse,
                output_handler,
            )
            .await
        } else if Self::has_optional_arg(command.clone(), "bylex", 4) {
            Self::zrangebylex(
                client_state,
                command,
                response_buffer,
                ActionType::Print,
                reverse,
                output_handler,
            )
            .await
        } else {
            Self::zrangebyrank(
                client_state,
                command,
                response_buffer,
                ActionType::Print,
                reverse,
                output_handler,
            )
            .await
        }
    }

    /// `ZRANGEBYLEX key min max [LIMIT offset count]`
    /// Returns all the elements in the sorted set at key with a score between min and max (including elements with score
    /// equal to min or max). The elements are considered to be ordered from low to high scores
    async fn zrangebylex(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        action_type: ActionType,
        reverse: bool,
        output_handler: OutputHandler,
    ) -> Result<(), SableError> {
        let (first_key_pos, dest) = match action_type {
            ActionType::Print | ActionType::Remove => (1, None),
            ActionType::Store => (2, Some(command_arg_at!(command, 1))),
        };

        let key = command_arg_at!(command, first_key_pos);
        let min = command_arg_at!(command, first_key_pos + 1);
        let max = command_arg_at!(command, first_key_pos + 2);

        let builder = RespBuilderV2::default();
        let mut min = Self::parse_lex_index(min.as_ref());
        let mut max = Self::parse_lex_index(max.as_ref());
        if reverse {
            std::mem::swap(&mut min, &mut max);
        }

        // If we have a destination key, lock it as well
        let keys_to_lock = if let Some(dest) = dest {
            vec![key, dest]
        } else {
            vec![key]
        };
        let _unused =
            LockManager::lock_user_keys_exclusive(&keys_to_lock, client_state.clone()).await?;

        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = match zset_db.get_metadata(key)? {
            ZSetGetMetadataResult::WrongType => {
                builder_return_wrong_type!(builder, response_buffer);
            }
            ZSetGetMetadataResult::NotFound => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            ZSetGetMetadataResult::Some(md) => md,
        };

        let client_state_cloned = client_state.clone();
        let mut db_iter = match min {
            LexIndex::Invalid => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            LexIndex::Include(prefix) => {
                let prefix = md.prefix_by_member(Some(prefix));
                client_state_cloned.database().create_iterator(&prefix)?
            }
            LexIndex::Exclude(prefix) => {
                let prefix = md.prefix_by_member(Some(prefix));
                let mut db_iter = client_state_cloned.database().create_iterator(&prefix)?;
                db_iter.next(); // skip this entry
                db_iter
            }
            LexIndex::Max => {
                builder_return_empty_array!(builder, response_buffer);
            }
            LexIndex::Min => {
                let prefix = md.prefix_by_member(None);
                client_state_cloned.database().create_iterator(&prefix)?
            }
        };

        // Setup the upper index
        let upper_limit = match max {
            LexIndex::Invalid => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            LexIndex::Include(prefix) => Some((md.prefix_by_member(Some(prefix)), true)),
            LexIndex::Exclude(prefix) => Some((md.prefix_by_member(Some(prefix)), false)),
            LexIndex::Max => None,
            LexIndex::Min => {
                builder_return_empty_array!(builder, response_buffer);
            }
        };

        let (mut offset, count) =
            match Self::parse_offset_and_limit(command.clone(), md.len() as usize) {
                LimitAndOffsetResult::SyntaxError => {
                    builder_return_syntax_error!(builder, response_buffer);
                }
                LimitAndOffsetResult::Value((offset, count)) => (offset, count),
            };

        let zset_prefix = md.prefix_by_member(None);
        let mut result_set = Vec::<(BytesMut, f64)>::new();
        let mut state = UpperLimitState::NotFound;
        while db_iter.valid() {
            let Some((key, value)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&zset_prefix) {
                break;
            }

            if let Some((end_prefix, include_it)) = &upper_limit {
                if !Self::can_iter_continue(key, end_prefix.as_ref(), *include_it, &mut state) {
                    break;
                }
            }

            // Store the member + score
            let field = ZSetMemberItem::from_bytes(key)?;
            result_set.push((
                BytesMut::from(field.member()),
                zset_db.score_from_bytes(value)?,
            ));
            db_iter.next();
        }

        if reverse {
            result_set.reverse();
        }

        // Apply the OFFSET limit (remove first `offset` elements)
        while offset > 0 && !result_set.is_empty() {
            let _ = result_set.remove(0);
            offset = offset.saturating_sub(1);
        }

        // shrink the result set to fit the "LIMIT OFFSET COUNT" restriction
        result_set.truncate(count);

        output_handler(client_state, command, result_set, false, response_buffer)?;
        Ok(())
    }

    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
    /// Returns all the elements in the sorted set at key with a score between min and max (including elements with
    /// score equal to min or max). The elements are considered to be ordered from low to high scores
    async fn zrangebyscore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        action_type: ActionType,
        reverse: bool,
        output_handler: OutputHandler,
    ) -> Result<(), SableError> {
        let (first_key_pos, dest) = match action_type {
            ActionType::Print | ActionType::Remove => (1, None),
            ActionType::Store => (2, Some(command_arg_at!(command, 1))),
        };

        let key = command_arg_at!(command, first_key_pos);
        let min = command_arg_at!(command, first_key_pos + 1);
        let max = command_arg_at!(command, first_key_pos + 2);

        let builder = RespBuilderV2::default();
        let Some((mut start_score, mut include_start_score)) = Self::parse_score_index(min) else {
            builder_return_min_max_not_float!(builder, response_buffer);
        };

        let Some((mut end_score, mut include_end_score)) = Self::parse_score_index(max) else {
            builder_return_min_max_not_float!(builder, response_buffer);
        };

        // If we have a destination key, lock it as well
        let keys_to_lock = if let Some(dest) = dest {
            vec![key, dest]
        } else {
            vec![key]
        };
        let _unused =
            LockManager::lock_user_keys_exclusive(&keys_to_lock, client_state.clone()).await?;

        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = zset_md_or_nil_builder!(zset_db, key, builder, response_buffer);

        let with_scores = Self::has_optional_arg(command.clone(), "withscores", first_key_pos + 3);
        let (mut offset, count) =
            match Self::parse_offset_and_limit(command.clone(), md.len() as usize) {
                LimitAndOffsetResult::Value((offset, count)) => (offset, count),
                LimitAndOffsetResult::SyntaxError => {
                    builder_return_syntax_error!(builder, response_buffer);
                }
            };

        // empty set? empty array
        if md.is_empty() {
            builder_return_empty_array!(builder, response_buffer);
        }

        if reverse {
            std::mem::swap(&mut start_score, &mut end_score);
            std::mem::swap(&mut include_start_score, &mut include_end_score);
        }

        // Determine the starting score
        let prefix = md.prefix_by_score(None);
        let client_state_cloned = client_state.clone();
        let mut db_iter = client_state_cloned.database().create_iterator(&prefix)?;

        if !db_iter.valid() {
            // invalud iterator
            builder_return_empty_array!(builder, response_buffer);
        }

        // Find the first item in the set that complies with the start condition
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                builder_return_empty_array!(builder, response_buffer);
            };

            if !key.starts_with(&prefix) {
                builder_return_empty_array!(builder, response_buffer);
            }

            let cur_item_score = ZSetScoreItem::from_bytes(key)?;
            if (include_start_score && cur_item_score.score() >= start_score)
                || (!include_start_score && cur_item_score.score() > start_score)
            {
                let prefix = md.prefix_by_score(Some(cur_item_score.score()));
                // place the iterator on the range start
                db_iter = client_state_cloned.database().create_iterator(&prefix)?;
                break;
            }
            db_iter.next();
        }

        // All items must start with `zset_prefix` regardless of the user conditions
        let zset_prefix = md.prefix_by_score(None);

        let mut result_set = Vec::<(BytesMut, f64)>::new();
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&zset_prefix) {
                // reached the set limit
                break;
            }

            let field = ZSetScoreItem::from_bytes(key)?;

            // Check end condition
            if (include_end_score && field.score() > end_score)
                || (!include_end_score && field.score() >= end_score)
            {
                break;
            }

            result_set.push((BytesMut::from(field.member()), field.score()));
            db_iter.next();
        }

        if reverse {
            result_set.reverse();
        }

        // Apply the OFFSET limit (remove first `offset` elements)
        while offset > 0 && !result_set.is_empty() {
            let _ = result_set.remove(0);
            offset = offset.saturating_sub(1);
        }

        // Shrink the result set to fit the "LIMIT COUNT" restriction
        result_set.truncate(count);

        // Call the handler
        output_handler(
            client_state,
            command,
            result_set,
            with_scores,
            response_buffer,
        )?;

        Ok(())
    }

    /// `ZRANGEBYRANK key min max [WITHSCORES]`
    /// This function does not really exists, its basically `ZRANGE` without any `BY*` property
    ///
    /// Returns all the elements in the sorted set at key with a rank. Items are sorted by rank
    async fn zrangebyrank(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        action_type: ActionType,
        reverse: bool,
        output_handler: OutputHandler,
    ) -> Result<(), SableError> {
        let (first_key_pos, dest) = match action_type {
            ActionType::Print | ActionType::Remove => (1, None),
            ActionType::Store => (2, Some(command_arg_at!(command, 1))),
        };

        let key = command_arg_at!(command, first_key_pos);
        let mut min = command_arg_at!(command, first_key_pos + 1).clone();
        let mut max = command_arg_at!(command, first_key_pos + 2).clone();

        let builder = RespBuilderV2::default();

        // If we have a destination key, lock it as well
        let keys_to_lock = if let Some(dest) = dest {
            vec![key, dest]
        } else {
            vec![key]
        };
        let _unused =
            LockManager::lock_user_keys_exclusive(&keys_to_lock, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = zset_md_or_nil_builder!(zset_db, key, builder, response_buffer);

        // empty set? empty array
        if md.is_empty() {
            builder_return_empty_array!(builder, response_buffer);
        }

        if reverse {
            // range is <max> <min>
            std::mem::swap(&mut min, &mut max);
        }

        // parse the start / stop
        let start_idx = match Self::parse_rank_index(&min) {
            RankIndex::SyntaxError => {
                builder_return_min_max_not_float!(builder, response_buffer);
            }
            RankIndex::Value(index) => index,
        };

        let end_idx = match Self::parse_rank_index(&max) {
            RankIndex::SyntaxError => {
                builder_return_min_max_not_float!(builder, response_buffer);
            }
            RankIndex::Value(index) => index,
        };

        let llen = md.len() as isize;
        let mut start_idx = Self::fix_range_index(start_idx, reverse, llen);
        let mut end_idx = Self::fix_range_index(end_idx, reverse, llen);

        if start_idx > end_idx || end_idx < 0 || start_idx >= llen {
            builder_return_empty_array!(builder, response_buffer);
        }

        if start_idx < 0 {
            start_idx = 0;
        }

        if end_idx >= llen {
            end_idx = llen - 1;
        }

        let with_scores = Self::has_optional_arg(command.clone(), "withscores", first_key_pos + 3);
        if Self::has_optional_arg(command.clone(), "limit", first_key_pos + 3) {
            builder.error_string(
                response_buffer,
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
            );
            return Ok(());
        }

        // Determine the starting score
        let set_prefix = md.prefix_by_score(None);
        let client_state_cloned = client_state.clone();
        let mut db_iter = client_state_cloned
            .database()
            .create_iterator(&set_prefix)?;

        if !db_iter.valid() {
            // invalid iterator
            builder_return_empty_array!(builder, response_buffer);
        }

        let mut cur_idx = 0isize;

        // Find the first index (inclusive range)
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                builder_return_empty_array!(builder, response_buffer);
            };

            if !key.starts_with(&set_prefix) {
                builder_return_empty_array!(builder, response_buffer);
            }

            if cur_idx == start_idx {
                break;
            } else {
                cur_idx = cur_idx.saturating_add(1);
            }
            db_iter.next();
        }

        // Start reading the values
        let mut result_set = Vec::<(BytesMut, f64)>::new();
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&set_prefix) {
                // reached the set limit
                break;
            }

            if cur_idx > end_idx {
                break;
            }

            let field = ZSetScoreItem::from_bytes(key)?;
            result_set.push((BytesMut::from(field.member()), field.score()));
            db_iter.next();
            cur_idx = cur_idx.saturating_add(1);
        }

        if reverse {
            result_set.reverse();
        }

        // Call the handler
        output_handler(
            client_state,
            command,
            result_set,
            with_scores,
            response_buffer,
        )?;
        Ok(())
    }

    /// `ZRANK key member [WITHSCORE]`
    /// Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
    /// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
    async fn zrank(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
        reverse: bool,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);

        let builder = RespBuilderV2::default();
        let key = command_arg_at!(command, 1);
        let member = command_arg_at!(command, 2);
        let with_scores = if let Some(withscore) = command.args_vec().get(3) {
            if BytesMutUtils::to_string(withscore)
                .to_lowercase()
                .eq("withscore")
            {
                true
            } else {
                builder_return_syntax_error!(builder, response_buffer);
            }
        } else {
            false
        };

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = if with_scores {
            zset_md_or_nil_array_builder!(zset_db, key, builder, response_buffer)
        } else {
            zset_md_or_nil_string_builder!(zset_db, key, builder, response_buffer)
        };

        let zset_prefix = md.prefix_by_score(None);
        let mut db_iter = if reverse {
            let upper_bound = md.score_upper_bound_prefix();
            client_state
                .database()
                .create_reverse_iterator(&upper_bound)?
        } else {
            client_state.database().create_iterator(&zset_prefix)?
        };

        let mut rank: usize = if reverse {
            md.len().saturating_sub(1) as usize
        } else {
            0
        };

        while db_iter.valid() {
            let Some((key, _)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&zset_prefix) {
                // could not find a match
                break;
            }

            let item = ZSetScoreItem::from_bytes(key)?;
            if item.member().eq(member) {
                if with_scores {
                    // note that we return an array of different types here
                    builder.add_array_len(response_buffer, 2);
                    builder.add_number::<usize>(response_buffer, rank, false);
                    builder.add_bulk_string(
                        response_buffer,
                        format!("{:.2}", item.score()).as_bytes(),
                    );
                } else {
                    builder.number_usize(response_buffer, rank);
                }
                return Ok(());
            }

            if reverse {
                let Some(res) = rank.checked_sub(1) else {
                    break;
                };
                rank = res;
            } else {
                rank = rank.saturating_add(1);
                if rank >= md.len() as usize {
                    break;
                }
            }
            db_iter.next();
        }

        // if we reached here, no match was found
        if with_scores {
            builder.null_array(response_buffer);
        } else {
            builder.null_string(response_buffer);
        }
        Ok(())
    }

    /// `ZREM key member [member ...]`
    /// Removes the specified members from the sorted set stored at key. Non existing members are ignored. An error is
    /// returned when key exists and does not hold a sorted set.
    async fn zrem(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        response_buffer: &mut BytesMut,
    ) -> Result<(), SableError> {
        check_args_count!(command, 3, response_buffer);
        let key = command_arg_at!(command, 1);

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone()).await?;
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        let builder = RespBuilderV2::default();

        let mut iter = command.args_vec().iter();
        iter.next(); // command
        iter.next(); // key

        let mut items_deleted: usize = 0;
        for member in iter {
            match zset_db.delete_member(key, member, false)? {
                ZSetDeleteMemberResult::WrongType => {
                    builder_return_wrong_type!(builder, response_buffer);
                }
                ZSetDeleteMemberResult::Ok => {
                    items_deleted = items_deleted.saturating_add(1);
                }
                ZSetDeleteMemberResult::SetNotFound => {
                    builder.number_usize(response_buffer, items_deleted);
                    return Ok(());
                }
                ZSetDeleteMemberResult::MemberNotFound => {}
            }
        }

        if items_deleted > 0 {
            zset_db.commit()?;
        }
        builder.number_usize(response_buffer, items_deleted);
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

    /// `index` can be `-inf` or `+inf`, denoting the negative and positive infinities, respectively.
    /// This means that you are not required to know the highest or lowest score in the sorted set to get all elements
    /// from or up to a certain score. By default, the score intervals specified by <start> and <stop> are closed
    /// (inclusive). It is possible to specify an open interval (exclusive) by prefixing the score with the character `(`
    ///
    /// Returns
    /// If parsed correctly, returns a tupple with the index value and whether or not it is included in the range
    /// (`result.0 => score value`)
    /// (`result.1 => inclusive?`)
    fn parse_score_index(index: &BytesMut) -> Option<(f64, bool)> {
        let exclude_index = index.starts_with(b"(");
        let mod_index = if exclude_index {
            &index[1..] // skip the `(` char
        } else {
            &index[..]
        };

        Self::parse_score(&BytesMut::from(mod_index)).map(|val| (val, !exclude_index))
    }

    /// Parse rank index. If index is negative, translate it to the positive index using
    /// the current set length
    fn parse_rank_index(index: &BytesMut) -> RankIndex {
        let Some(index) = BytesMutUtils::parse::<isize>(index) else {
            return RankIndex::SyntaxError;
        };

        RankIndex::Value(index)
    }

    fn fix_range_index(index: isize, reverse: bool, llen: isize) -> isize {
        if reverse {
            // 0 is the last element, 1 is second last element and so on
            if index < 0 {
                index.abs() - 1
            } else {
                llen - index - 1
            }
        } else if index < 0 {
            llen + index
        } else {
            index
        }
    }

    /// Valid `index` must start with `(` (exclude) or `[` (include), in order to specify whether the range
    /// interval is exclusive or inclusive, respectively.
    /// The special values of `+` or `-` for start and stop have the special meaning or positively infinite
    /// and negatively infinite strings
    fn parse_lex_index(index: &[u8]) -> LexIndex {
        if index.is_empty() {
            return LexIndex::Invalid;
        }

        if index == b"+" {
            return LexIndex::Max;
        }

        if index == b"-" {
            return LexIndex::Min;
        }

        if index.len() < 2 {
            return LexIndex::Invalid;
        }

        let prefix = index[0];
        let remainder = &index[1..];

        match prefix {
            b'(' => LexIndex::Exclude(remainder),
            b'[' => LexIndex::Include(remainder),
            _ => LexIndex::Invalid,
        }
    }

    /// Iterate over all items of `set_name` and apply callback on them
    fn iterate_by_member_and_apply<F>(
        client_state: Rc<ClientState>,
        set_name: &BytesMut,
        mut callback: F,
    ) -> Result<IterateResult, SableError>
    where
        F: FnMut(&[u8], f64) -> Result<IterateCallbackResult, SableError>,
    {
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());
        let md = match zset_db.get_metadata(set_name)? {
            ZSetGetMetadataResult::WrongType => return Ok(IterateResult::WrongType),
            ZSetGetMetadataResult::NotFound => return Ok(IterateResult::NotFound),
            ZSetGetMetadataResult::Some(md) => md,
        };

        let prefix = md.prefix_by_member(None);
        let mut db_iter = client_state.database().create_iterator(&prefix)?;
        while db_iter.valid() {
            let Some((key, value)) = db_iter.key_value() else {
                break;
            };

            if !key.starts_with(&prefix) {
                break;
            }

            let item = ZSetMemberItem::from_bytes(key)?;
            match callback(item.member(), zset_db.score_from_bytes(value)?)? {
                IterateCallbackResult::Continue => {}
                IterateCallbackResult::Break => break,
            }
            db_iter.next();
        }
        Ok(IterateResult::Ok)
    }

    /// Return the keys to lock
    fn parse_keys_to_lock(
        command: Rc<RedisCommand>,
        first_key_pos: usize,
        numkeys: usize,
        reserved_words: &HashSet<&'static str>,
        destination: Option<&BytesMut>,
    ) -> Result<Vec<BytesMut>, String> {
        let mut iter = command.args_vec().iter();
        let mut words_to_skip = first_key_pos;

        let reserved_words: HashSet<String> =
            reserved_words.iter().map(|w| w.to_lowercase()).collect();
        while words_to_skip > 0 {
            iter.next();
            words_to_skip = words_to_skip.saturating_sub(1);
        }

        let mut keys = Vec::<BytesMut>::with_capacity(numkeys + 1);
        for _ in 0..numkeys {
            if let Some(key) = iter.next() {
                let key_lowercase = BytesMutUtils::to_string(key).to_lowercase();
                if reserved_words.contains(key_lowercase.as_str()) {
                    // if the key is a known keyword, return false
                    return Err("syntax error".into());
                }
                keys.push(key.clone());
            } else {
                break;
            }
        }

        if let Some(destination) = destination {
            keys.push(destination.clone());
        }
        Ok(keys)
    }

    /// Common function for parsing command line arguments that uses the following format:
    /// <CMD> .. numkeys <key1> ... <keyN> ...
    fn intersect(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        first_key_pos: usize,
        numkeys: usize,
    ) -> Result<IntersectError, SableError> {
        // Now that we know the keys count, we can further strict the expected arguments requirement
        if !command.expect_args_count(first_key_pos + numkeys) {
            return Ok(IntersectError::ArgCount);
        }

        let in_keys = &command.args_vec()[first_key_pos..first_key_pos.saturating_add(numkeys)];
        let mut input_keys: Vec<(&BytesMut, usize)> = in_keys.iter().map(|k| (k, 1)).collect();

        // Parse the remaining command args
        let mut parsed_args = HashMap::<&'static str, KeyWord>::new();
        parsed_args.insert("weights", KeyWord::new("weights", input_keys.len()));
        parsed_args.insert("aggregate", KeyWord::new("aggregate", 1));
        parsed_args.insert("withscores", KeyWord::new("withscores", 0));
        parsed_args.insert("limit", KeyWord::new("limit", 1));

        if let Err(msg) = Self::parse_optional_args(
            command.clone(),
            numkeys.saturating_add(first_key_pos),
            &mut parsed_args,
        ) {
            tracing::debug!("failed to parse command: {:?}. {}", command.args_vec(), msg);
            return Ok(IntersectError::SyntaxError);
        }

        // Determine the score aggreation method
        let agg_method = Self::aggregation_method(&parsed_args);

        if !Self::assign_weight(&parsed_args, &mut input_keys)? {
            tracing::debug!(
                "failed to assign weights for command: {:?}",
                command.args_vec()
            );
            return Ok(IntersectError::SyntaxError);
        }

        let keys_to_lock: Vec<&BytesMut> = input_keys.iter().map(|(k, _w)| (*k)).collect();
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        // For best performance, locate the smallest set. This will be our base working set
        // everything is compared against this set
        let smallest_set_idx = match zset_db.find_smallest(&keys_to_lock)? {
            ZSetGetSmallestResult::None => {
                return Ok(IntersectError::EmptyArray);
            }
            ZSetGetSmallestResult::WrongType => {
                return Ok(IntersectError::WrongType);
            }
            ZSetGetSmallestResult::Some(idx) => idx,
        };

        let Some((smallest_set_name, smallest_set_weight)) = input_keys.get(smallest_set_idx)
        else {
            return Ok(IntersectError::EmptyArray);
        };

        // Read the smalleset set items - and store them as the result set (we use BTreeMap to produce a sorted output)
        let result_set = Rc::new(RefCell::new(BTreeMap::<BytesMut, f64>::new()));
        let result_set_clone = result_set.clone();
        let _ = Self::iterate_by_member_and_apply(
            client_state.clone(),
            smallest_set_name,
            move |member, score| {
                result_set_clone
                    .borrow_mut()
                    .insert(BytesMut::from(member), *smallest_set_weight as f64 * score);
                Ok(IterateCallbackResult::Continue)
            },
        )?;

        // Read the remainder of the sets, while we keep intersecting with the result set
        for (set_name, weight) in &input_keys {
            if set_name.eq(smallest_set_name) {
                continue;
            }

            let result_set_clone = result_set.clone();
            if result_set_clone.borrow().is_empty() {
                break;
            }

            // Keep track of items that were not visited in the result set during this iteration
            // these items, should be removed from the final result set
            // We start by assuming that all items were NOT visited
            let not_visited: HashSet<BytesMut> = result_set_clone
                .borrow()
                .iter()
                .map(|(k, _w)| k.clone())
                .collect();

            let not_visited = Rc::new(RefCell::new(not_visited));
            let not_visited_clone = not_visited.clone();

            // read the current set
            let _ = Self::iterate_by_member_and_apply(
                client_state.clone(),
                set_name,
                move |member, score| {
                    // first thing we do: adjust this item score by the multiplier
                    let score = *weight as f64 * score;
                    // Check to see if this member exists in the result set
                    let mut res_set_mut = result_set_clone.borrow_mut();
                    if let Some(cur_member_score) = res_set_mut.get(member) {
                        // Aggregate the score based on the given method (SUM, MAX, MIN)
                        let new_score = Self::agg_func(agg_method, score, *cur_member_score);
                        // Finally, update the result set
                        res_set_mut.insert(BytesMut::from(member), new_score);
                        // remove "member" from the "not_visited" set
                        not_visited_clone.borrow_mut().remove(member);
                    }
                    Ok(IterateCallbackResult::Continue)
                },
            )?;

            // All items that still exist in the "not_visited" set, should be removed from the final output
            for member in not_visited.borrow().iter() {
                result_set.borrow_mut().remove(member);
            }
        }
        Ok(IntersectError::Ok(result_set))
    }

    // Perform aggregation on `score1` and `score2` based on the requested method
    fn agg_func(method: AggregationMethod, score1: f64, score2: f64) -> f64 {
        match method {
            AggregationMethod::Sum => score1 + score2,
            AggregationMethod::Min => {
                if score1 < score2 {
                    score1
                } else {
                    score2
                }
            }
            AggregationMethod::Max => {
                if score1 > score2 {
                    score1
                } else {
                    score2
                }
            }
        }
    }

    /// Return the aggregation method from a parsed keywords
    fn aggregation_method(parsed_args: &HashMap<&'static str, KeyWord>) -> AggregationMethod {
        if let Some(aggregation) = parsed_args.get("aggregate") {
            if aggregation.is_found() {
                match aggregation.value_lowercase(0).as_str() {
                    "sum" => AggregationMethod::Sum,
                    "max" => AggregationMethod::Max,
                    "min" => AggregationMethod::Min,
                    _ => AggregationMethod::Sum,
                }
            } else {
                AggregationMethod::Sum
            }
        } else {
            AggregationMethod::Sum
        }
    }

    /// Return the LIMIT <value> from the command line
    fn get_limit(command: Rc<RedisCommand>) -> FindKeyWithValueResult<usize> {
        Self::find_keyval_or::<usize>(command, "limit", usize::MAX)
    }

    /// Return the `LIMIT <offset> <count>` from the command line
    fn parse_offset_and_limit(
        command: Rc<RedisCommand>,
        default_value: usize,
    ) -> LimitAndOffsetResult {
        let mut iter = command.args_vec().iter();
        while let Some(arg) = iter.next() {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq("limit") {
                if let (Some(offset), Some(limit)) = (iter.next(), iter.next()) {
                    let Some(offset) = BytesMutUtils::parse::<usize>(offset) else {
                        return LimitAndOffsetResult::SyntaxError;
                    };

                    let Some(limit) = BytesMutUtils::parse::<i64>(limit) else {
                        return LimitAndOffsetResult::SyntaxError;
                    };

                    let count = match limit {
                        num if num < 0 => usize::MAX,
                        _ => limit as usize,
                    };

                    return LimitAndOffsetResult::Value((offset, count));
                } else {
                    return LimitAndOffsetResult::SyntaxError;
                }
            }
        }
        LimitAndOffsetResult::Value((0, default_value))
    }

    /// Return the `COUNT <value>` from the command line
    fn get_count(command: Rc<RedisCommand>, default_value: usize) -> FindKeyWithValueResult<usize> {
        Self::find_keyval_or::<usize>(command, "count", default_value)
    }

    fn find_keyval_or<NumberT: std::str::FromStr>(
        command: Rc<RedisCommand>,
        key_name: &'static str,
        default_value: NumberT,
    ) -> FindKeyWithValueResult<NumberT> {
        let mut iter = command.args_vec().iter();
        while let Some(arg) = iter.next() {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq(key_name) {
                let Some(val) = iter.next() else {
                    return FindKeyWithValueResult::SyntaxError;
                };

                let Some(val) = BytesMutUtils::parse::<NumberT>(val) else {
                    return FindKeyWithValueResult::SyntaxError;
                };

                return FindKeyWithValueResult::Value(val);
            }
        }
        FindKeyWithValueResult::Value(default_value)
    }

    /// Find an optional argument in the command. An "argument" is a reserved
    /// word without value. e.g. `WITHSCORES`
    fn has_optional_arg(
        command: Rc<RedisCommand>,
        arg_name: &'static str,
        start_from: usize,
    ) -> bool {
        // parse the remaining arguments
        let mut iter = command.args_vec().iter();
        let mut start_from = start_from;

        // skip the requested elements
        while start_from > 0 {
            start_from = start_from.saturating_sub(1);
            iter.next();
        }

        let arg_name_lowercase = arg_name.to_lowercase();
        for arg in iter {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq(&arg_name_lowercase) {
                return true;
            }
        }
        false
    }

    /// Locate `MIN` or `MAX` in the command line
    fn get_min_or_max(command: Rc<RedisCommand>) -> MinOrMaxResult {
        let iter = command.args_vec().iter();
        for arg in iter {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            match token_lowercase.as_str() {
                "min" => {
                    return MinOrMaxResult::Min;
                }
                "max" => {
                    return MinOrMaxResult::Max;
                }
                _ => {}
            }
        }
        MinOrMaxResult::None
    }

    /// Return the LIMIT <value> from the command line
    fn withscores(command: Rc<RedisCommand>) -> bool {
        let iter = command.args_vec().iter();
        for arg in iter {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq("withscores") {
                return true;
            }
        }
        false
    }

    /// Return the WITHSCORES values from a parsed arguments
    fn assign_weight(
        parsed_args: &HashMap<&'static str, KeyWord>,
        keys: &mut [(&BytesMut, usize)],
    ) -> Result<bool, SableError> {
        let Some(weights) = parsed_args.get("weights") else {
            return Ok(true);
        };

        if !weights.is_found() || weights.tokens.len() != keys.len() {
            return Ok(true);
        }

        let mut index = 0usize;
        for val in &weights.tokens {
            let Some(weight) = BytesMutUtils::parse::<usize>(val) else {
                return Ok(false);
            };
            if let Some((_, w)) = keys.get_mut(index) {
                *w = weight;
            } else {
                // should not happen...
                return Err(SableError::ClientInvalidState);
            }
            index = index.saturating_add(1);
        }
        Ok(true)
    }

    /// Parse command line arguments that uses the following syntax
    ///
    /// ```no_compile
    /// <KEYWORD1> <Arg1>...<ArgN> <KEYWORD2> <Arg1>...<ArgN> ..
    /// ```
    /// Params:
    ///
    /// The arguments to parse are taken from `command`, if `start_from` is greater than `0`
    /// the function will skip the first `start_from` arguments before it starts parsing.
    /// Allowed `keywords` are passed in the `keywords` hash mam, this map is also update upon
    /// successful parsing
    fn parse_optional_args(
        command: Rc<RedisCommand>,
        start_from: usize,
        keywords: &mut HashMap<&'static str, KeyWord>,
    ) -> Result<(), String> {
        // parse the remaining arguments
        let mut iter = command.args_vec().iter();
        let mut start_from = start_from;

        // skip the requested elements
        while start_from > 0 {
            start_from = start_from.saturating_sub(1);
            iter.next();
        }

        let mut current_keyword = String::new();
        for token in iter {
            let token_lowercase = String::from_utf8_lossy(&token[..]).to_lowercase();
            if keywords.contains_key(token_lowercase.as_str()) {
                if current_keyword.is_empty() {
                    // first time, the token must be a known keyword
                    current_keyword = token_lowercase.as_str().to_string();
                    let Some(current) = keywords.get_mut(current_keyword.as_str()) else {
                        return Err(format!("{current_keyword} not found in allowed keywords"));
                    };
                    current.set_found();
                } else {
                    let Some(current) = keywords.get(current_keyword.as_str()) else {
                        return Err(format!("{current_keyword} not found in allowed keywords"));
                    };

                    if !current.is_completed() {
                        // cant switch keyword before completing the current one
                        return Err(format!("wrong number of arguments for {current_keyword}"));
                    }
                    current_keyword = token_lowercase.as_str().to_string();
                    let Some(current) = keywords.get_mut(current_keyword.as_str()) else {
                        return Err(format!("{current_keyword} not found in allowed keywords"));
                    };
                    current.set_found();
                }
            } else if current_keyword.is_empty() {
                // The token is not a keyword and we don't have a keyword to associate this token to
                return Err(format!("expected keyword, found {:?}", token));
            } else {
                // Associate the token with the current keyword
                let Some(current) = keywords.get_mut(current_keyword.as_str()) else {
                    return Err(format!("{current_keyword} not found in allowed keywords"));
                };
                if current.is_completed() {
                    return Err(format!("too many arguments for keyword {current_keyword}"));
                }
                current.add_token(token.clone());
            }
        }

        // Make sure that all keywords found are marked as "is_completed"
        for kw in keywords.values() {
            if kw.is_found() && !kw.is_completed() {
                return Err("syntax error".into());
            }
        }

        Ok(())
    }
}

#[allow(dead_code)]
struct KeyWord {
    /// The keyword name (should be in lowercase)
    keyword: String,
    expected_tokens: usize,
    tokens: Vec<BytesMut>,
    found: bool,
}

#[allow(dead_code)]
impl KeyWord {
    pub fn new(keyword: &'static str, expected_tokens: usize) -> Self {
        KeyWord {
            keyword: keyword.to_lowercase(),
            expected_tokens,
            tokens: Vec::<BytesMut>::new(),
            found: false,
        }
    }

    pub fn is_found(&self) -> bool {
        self.found
    }

    pub fn is_completed(&self) -> bool {
        self.expected_tokens == self.tokens.len()
    }

    pub fn set_found(&mut self) {
        self.found = true
    }

    pub fn add_token(&mut self, token: BytesMut) {
        self.tokens.push(token);
    }

    pub fn tokens(&self) -> &Vec<BytesMut> {
        &self.tokens
    }

    pub fn value_lowercase(&self, index: usize) -> String {
        let Some(val) = self.tokens.get(index) else {
            return String::new();
        };
        BytesMutUtils::to_string(val.as_ref()).to_lowercase()
    }

    pub fn value_usize_or(&self, index: usize, default_value: usize) -> usize {
        let Some(val) = self.tokens.get(index) else {
            return default_value;
        };
        BytesMutUtils::parse::<usize>(val).unwrap_or(default_value)
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

    #[test]
    fn test_parse_optional_args() {
        let args = "zinter 2 a b WEIGHTS 1 2".split(' ').collect();
        let cmd = Rc::new(RedisCommand::for_test(args));
        let mut keywords = HashMap::<&'static str, KeyWord>::new();
        keywords.insert("weights", KeyWord::new("weights", 2));
        let res = ZSetCommands::parse_optional_args(cmd, 3, &mut keywords);
        assert!(res.is_err());
        let errstr = res.unwrap_err();
        assert!(errstr.contains("expected keyword"));

        let args = "zinter 2 a b WEIGHTS 1 2".split(' ').collect();
        let cmd = Rc::new(RedisCommand::for_test(args));
        let mut keywords = HashMap::<&'static str, KeyWord>::new();
        keywords.insert("weights", KeyWord::new("weights", 2));
        assert!(ZSetCommands::parse_optional_args(cmd, 4, &mut keywords).is_ok());

        assert!(keywords.get("weights").unwrap().is_found());
        assert_eq!(keywords.get("weights").unwrap().tokens().len(), 2);

        let args = "zinter 2 a b WEIGHTS 1 2 WITHSCORES AGGREGATE MAX"
            .split(' ')
            .collect();

        let cmd = Rc::new(RedisCommand::for_test(args));
        let mut keywords = HashMap::<&'static str, KeyWord>::new();
        keywords.insert("weights", KeyWord::new("weights", 2));
        keywords.insert("withscores", KeyWord::new("withscores", 0));
        keywords.insert("aggregate", KeyWord::new("aggregate", 1));
        assert!(ZSetCommands::parse_optional_args(cmd, 4, &mut keywords).is_ok());
        assert!(keywords.get("weights").unwrap().is_found());
        assert_eq!(keywords.get("weights").unwrap().tokens().len(), 2);
        assert!(keywords.get("aggregate").unwrap().is_found());
        assert_eq!(keywords.get("aggregate").unwrap().tokens().len(), 1);
        assert_eq!(
            keywords.get("aggregate").unwrap().tokens().first().unwrap(),
            "MAX"
        );

        let args = "zinter 2 a b WEIGHTS 1 2 3 WITHSCORES AGGREGATE MAX"
            .split(' ')
            .collect();

        let cmd = Rc::new(RedisCommand::for_test(args));
        let mut keywords = HashMap::<&'static str, KeyWord>::new();
        keywords.insert("weights", KeyWord::new("weights", 2));
        keywords.insert("withscores", KeyWord::new("withscores", 0));
        keywords.insert("aggregate", KeyWord::new("aggregate", 1));
        let result = ZSetCommands::parse_optional_args(cmd, 4, &mut keywords);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too many arguments"));
    }

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
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zrangebyscore mystr 1 2", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd tanks 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        // everything without knowing their ranks
        ("zrangebyscore tanks -inf +inf", "*6\r\n$3\r\ndva\r\n$4\r\nrein\r\n$5\r\norisa\r\n$5\r\nsigma\r\n$5\r\nmauga\r\n$3\r\nram\r\n"),
        ("zrangebyscore tanks -inf 1", "*2\r\n$3\r\ndva\r\n$4\r\nrein\r\n"),
        ("zrangebyscore tanks -inf (1", "*0\r\n"),
        ("zrangebyscore tanks 1 2", "*4\r\n$3\r\ndva\r\n$4\r\nrein\r\n$5\r\norisa\r\n$5\r\nsigma\r\n"),
        ("zrangebyscore tanks 1 (2", "*2\r\n$3\r\ndva\r\n$4\r\nrein\r\n"),
        ("zrangebyscore tanks (1 (1", "*0\r\n"),
        ("zrangebyscore tanks (1 1", "*0\r\n"),
        ("zrangebyscore tanks 10 1", "*0\r\n"),
        ("zrangebyscore tanks 1 3", "*6\r\n$3\r\ndva\r\n$4\r\nrein\r\n$5\r\norisa\r\n$5\r\nsigma\r\n$5\r\nmauga\r\n$3\r\nram\r\n"),
        ("zrangebyscore tanks 1 3 WITHSCORES", "*12\r\n$3\r\ndva\r\n$4\r\n1.00\r\n$4\r\nrein\r\n$4\r\n1.00\r\n$5\r\norisa\r\n$4\r\n2.00\r\n$5\r\nsigma\r\n$4\r\n2.00\r\n$5\r\nmauga\r\n$4\r\n3.00\r\n$3\r\nram\r\n$4\r\n3.00\r\n"),
        // exclude dva
        ("zrangebyscore tanks 1 3 LIMIT 1 5", "*5\r\n$4\r\nrein\r\n$5\r\norisa\r\n$5\r\nsigma\r\n$5\r\nmauga\r\n$3\r\nram\r\n"),
        // only rein
        ("zrangebyscore tanks 1 3 LIMIT 1 1", "*1\r\n$4\r\nrein\r\n"),
        // all of score 1 but dva & rein (empty array)
        ("zrangebyscore tanks 1 1 LIMIT 2 -1", "*0\r\n"),
    ]; "test_zrangebyscore")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zrangebylex mystr 1 2", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 0 a 0 b 0 c 0 d 0 e 0 f 0 g", ":7\r\n"),
        ("ZADD myzset2 0 foo 0 zap 0 zip 0 ALPHA 0 alpha", ":5\r\n"),
        ("ZADD myzset2 0 aaaa 0 b 0 c 0 d 0 e", ":5\r\n"),
        ("ZRANGEBYLEX myzset2 [alpha [omega", "*6\r\n$5\r\nalpha\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$3\r\nfoo\r\n"),
        ("ZRANGEBYLEX myzset - (c", "*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
        ("ZRANGEBYLEX myzset [aaa (g", "*5\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n"),
        ("ZRANGEBYLEX myzset + -", "*0\r\n"),
        ("ZRANGEBYLEX myzset - +", "*7\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n$1\r\ng\r\n"),
        ("ZRANGEBYLEX myzset - + LIMIT 1 2", "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"),
    ]; "test_zrangebylex")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zrevrangebylex mystr 1 2", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 0 a 0 b 0 c 0 d 0 e 0 f 0 g", ":7\r\n"),
        ("ZREVRANGEBYLEX myzset [c -", "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
        ("ZREVRANGEBYLEX myzset (c -", "*2\r\n$1\r\nb\r\n$1\r\na\r\n"),
        ("ZREVRANGEBYLEX myzset (g [aaa", "*5\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n"),
        ("ZREVRANGEBYLEX myzset - +", "*0\r\n"),
        ("ZREVRANGEBYLEX myzset + -", "*7\r\n$1\r\ng\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
        ("ZREVRANGEBYLEX myzset + - LIMIT 1 -2", "*6\r\n$1\r\nf\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"),
    ]; "test_zrevrangebylex")]
    #[test_case(vec![
        //("set mystr value", "+OK\r\n"),
        //("zrevrangebyscore mystr 1 2", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd tanks 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        // everything without knowing their ranks
        ("zrevrangebyscore tanks -inf +inf", "*0\r\n"),
        ("zrevrangebyscore tanks -inf 1", "*0\r\n"),
        ("zrevrangebyscore tanks -inf (1", "*0\r\n"),
        ("zrevrangebyscore tanks 1 2", "*0\r\n"),
        ("zrevrangebyscore tanks 2 1", "*4\r\n$5\r\nsigma\r\n$5\r\norisa\r\n$4\r\nrein\r\n$3\r\ndva\r\n"),
        ("zrevrangebyscore tanks (2 1", "*2\r\n$4\r\nrein\r\n$3\r\ndva\r\n"),
        ("zrevrangebyscore tanks 3 1 WITHSCORES", "*12\r\n$3\r\nram\r\n$4\r\n3.00\r\n$5\r\nmauga\r\n$4\r\n3.00\r\n$5\r\nsigma\r\n$4\r\n2.00\r\n$5\r\norisa\r\n$4\r\n2.00\r\n$4\r\nrein\r\n$4\r\n1.00\r\n$3\r\ndva\r\n$4\r\n1.00\r\n"),
        // exclude sigma
        ("zrevrangebyscore tanks 2 1 LIMIT 1 5", "*3\r\n$5\r\norisa\r\n$4\r\nrein\r\n$3\r\ndva\r\n"),
        //// only mauga
        ("zrevrangebyscore tanks 3 1 LIMIT 1 1", "*1\r\n$5\r\nmauga\r\n"),
        ("zrevrangebyscore tanks 3 1 LIMIT 1 -1", "*5\r\n$5\r\nmauga\r\n$5\r\nsigma\r\n$5\r\norisa\r\n$4\r\nrein\r\n$3\r\ndva\r\n"),
        ("zrevrangebyscore tanks 3 1 LIMIT 1 -1 withScores", "*10\r\n$5\r\nmauga\r\n$4\r\n3.00\r\n$5\r\nsigma\r\n$4\r\n2.00\r\n$5\r\norisa\r\n$4\r\n2.00\r\n$4\r\nrein\r\n$4\r\n1.00\r\n$3\r\ndva\r\n$4\r\n1.00\r\n"),
        //// all of score 1 but dva & rein (empty array)
        ("zrevrangebyscore tanks 1 1 LIMIT 2 -1", "*0\r\n"),
    ]; "test_zrevrangebyscore")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zcount mystr 1 2", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zcount mystr 1", "-ERR wrong number of arguments for 'zcount' command\r\n"),
        ("zadd tanks 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zcount no_such_zset -inf +inf", ":0\r\n"),
        ("zcount tanks -inf +inf", ":6\r\n"),
        ("zcount tanks (1 (3", ":2\r\n"),
        ("zcount tanks 1 3", ":6\r\n"),
        ("zcount tanks 1 (3", ":4\r\n"),
        ("zcount tanks (1 3", ":4\r\n"),
        ("zcount tanks 10 8", ":0\r\n"),
    ]; "test_zcount")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zadd tanks_1 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zadd tanks_2 1 rein 1 roadhog 1 doomfist 5 mei", ":4\r\n"),
        ("zadd tanks_3 2 orisa 3 ram", ":2\r\n"),
        ("zdiff 2 tanks_1 mystr", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zdiff 2 mystr tanks_1", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zdiff 2 tanks_1 tanks_2", "*5\r\n$3\r\ndva\r\n$5\r\nmauga\r\n$5\r\norisa\r\n$3\r\nram\r\n$5\r\nsigma\r\n"),
        ("zdiff 2 tanks_2 tanks_1", "*3\r\n$8\r\ndoomfist\r\n$3\r\nmei\r\n$7\r\nroadhog\r\n"),
        ("zdiff 3 tanks_1 tanks_2 tanks_3", "*3\r\n$3\r\ndva\r\n$5\r\nmauga\r\n$5\r\nsigma\r\n"),
        ("zdiff 3 tanks_1 tanks_2 tanks_3 WITHSCORES", "*6\r\n$3\r\ndva\r\n$4\r\n1.00\r\n$5\r\nmauga\r\n$4\r\n3.00\r\n$5\r\nsigma\r\n$4\r\n2.00\r\n"),
    ]; "test_zdiff")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zadd tanks_1 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zadd tanks_2 1 rein 1 roadhog 1 doomfist 5 mei", ":4\r\n"),
        ("zadd tanks_3 2 orisa 3 ram", ":2\r\n"),
        ("zdiffstore diff_2_1 2 tanks_2 tanks_1", ":3\r\n"),
        ("zcard diff_2_1", ":3\r\n"),
        // to get the content, we just diff it against a non existing set
        ("zdiff 2 diff_2_1 no_such_zset", "*3\r\n$8\r\ndoomfist\r\n$3\r\nmei\r\n$7\r\nroadhog\r\n"),
        ("zdiffstore diff_3_none 2 tanks_3 no_such_zset", ":2\r\n"),
        ("zdiff 2 diff_3_none no_such_zset", "*2\r\n$5\r\norisa\r\n$3\r\nram\r\n"),
    ]; "test_zdiffstore")]
    #[test_case(vec![
        ("zadd tanks_1 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zadd tanks_2 1 rein 1 roadhog 1 doomfist 5 mei", ":4\r\n"),
        ("zadd tanks_3 2 orisa 3 ram", ":2\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3", "*0\r\n"),
        ("zadd tanks_3 1 rein", ":1\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3", "*1\r\n$4\r\nrein\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WITHSCORES", "*2\r\n$4\r\nrein\r\n$4\r\n3.00\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5", "-ERR syntax error\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5", "*1\r\n$4\r\nrein\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 WITHSCORES", "*2\r\n$4\r\nrein\r\n$5\r\n15.00\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 WITHSCORES AGGREGATE MIN", "*2\r\n$4\r\nrein\r\n$4\r\n5.00\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 WITHSCORES AGGREGATE MAX", "*2\r\n$4\r\nrein\r\n$4\r\n5.00\r\n"),
        ("zinter 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 WITHSCORES AGGREGATE SUM", "*2\r\n$4\r\nrein\r\n$5\r\n15.00\r\n"),
        ("zinter 2 tanks_1 no_such_set WEIGHTS 5 5 5 WITHSCORES AGGREGATE SUM", "-ERR syntax error\r\n"),
        ("zinter 2 tanks_1 no_such_set WEIGHTS 5 5 WITHSCORES AGGREGATE SUM", "*0\r\n"),
    ]; "test_zinter")]
    #[test_case(vec![
        ("zadd tanks_1 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zadd tanks_2 1 rein 1 roadhog 1 doomfist 5 mei", ":4\r\n"),
        ("zadd tanks_3 2 orisa 3 ram 5 mei", ":3\r\n"),
        ("zintercard 3 tanks_1 tanks_2 tanks_3", ":0\r\n"),
        ("zintercard 2 tanks_1 tanks_3", ":2\r\n"),
        ("zadd tanks_3 1 rein", ":1\r\n"),
        ("zintercard 2 tanks_1 tanks_3", ":3\r\n"),
        ("zintercard 2 tanks_1 tanks_3 LIMIT 1", ":1\r\n"),
        ("zintercard 2 tanks_1 no_such_set LIMIT", "-ERR syntax error\r\n"),
        ("zintercard 2 tanks_1 no_such_set LIMIT 1", ":0\r\n"),
    ]; "test_zintercard")]
    #[test_case(vec![
        ("zadd tanks_1 1 rein 1 dva 2 orisa 2 sigma 3 mauga 3 ram", ":6\r\n"),
        ("zadd tanks_2 1 rein 1 roadhog 1 doomfist 5 mei", ":4\r\n"),
        ("zadd tanks_3 2 orisa 3 ram", ":2\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3", ":0\r\n"),
        ("zadd tanks_3 1 rein", ":1\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3", ":1\r\n"),
        ("zdiff 2 new_set no_such_zset WITHSCORES", "*2\r\n$4\r\nrein\r\n$4\r\n3.00\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5", "-ERR syntax error\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3 WEIGHTS 1 2 3", ":1\r\n"),
        ("zdiff 2 new_set no_such_zset WITHSCORES", "*2\r\n$4\r\nrein\r\n$4\r\n6.00\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5", ":1\r\n"),
        ("zdiff 2 new_set no_such_zset WITHSCORES", "*2\r\n$4\r\nrein\r\n$5\r\n15.00\r\n"),
        ("zinterstore new_set 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 AGGREGATE MIN", ":1\r\n"),
        ("zdiff 2 new_set no_such_zset WITHSCORES", "*2\r\n$4\r\nrein\r\n$4\r\n5.00\r\n"),
        ("zinterstore 3 tanks_1 tanks_2 tanks_3 WEIGHTS 5 5 5 AGGREGATE MIN", "-ERR value is not an integer or out of range\r\n"),
        ("zinterstore new_set 2 tanks_1 no_such_set WEIGHTS 5 5 5 AGGREGATE SUM", "-ERR syntax error\r\n"),
        ("zinterstore new_set 2 tanks_1 no_such_set WEIGHTS 5 5 AGGREGATE SUM", ":0\r\n"),
    ]; "test_zinterstore")]
    #[test_case(vec![
        ("zadd myzset0 0 a 0 b 0 c 0 d 0 e", ":5\r\n"),
        ("zadd myzset1 0 a 0 b 0 c 0 d 0 e", ":5\r\n"),
        ("zadd myzset2 0 a 0 b 0 c 0 d 0 e", ":5\r\n"),
        ("zadd myzset1 0 f 0 g", ":2\r\n"),
        ("zlexcount myzset1 - +", ":7\r\n"),
        ("zlexcount myzset1 [b [f", ":5\r\n"),
        ("zlexcount myzset1 [b (f", ":4\r\n"),
        ("zlexcount myzset1 (b (f", ":3\r\n"),
        ("zlexcount myzset1 b (f", "-ERR syntax error\r\n"),
        ("zlexcount myzset1 (b f", "-ERR syntax error\r\n"),
    ]; "test_zlexcount")]
    #[test_case(vec![
        ("ZMPOP 1 notsuchkey MIN", "*-1\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZMPOP 1 myzset MAX", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$5\r\nthree\r\n$4\r\n3.00\r\n"),
        ("ZMPOP 1 myzset MAX", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$3\r\ntwo\r\n$4\r\n2.00\r\n"),
        ("ZMPOP 1 myzset MAX", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$3\r\none\r\n$4\r\n1.00\r\n"),
        ("ZMPOP 1 myzset MAX", "*-1\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZMPOP 1 myzset MIN", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$3\r\none\r\n$4\r\n1.00\r\n"),
        ("ZMPOP 1 myzset MIN", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$3\r\ntwo\r\n$4\r\n2.00\r\n"),
        ("ZMPOP 1 myzset MIN", "*2\r\n$6\r\nmyzset\r\n*1\r\n*2\r\n$5\r\nthree\r\n$4\r\n3.00\r\n"),
        ("ZMPOP 1 myzset MAX", "*-1\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZMPOP 1 myzset MAX COUNT 4", "*2\r\n$6\r\nmyzset\r\n*3\r\n*2\r\n$5\r\nthree\r\n$4\r\n3.00\r\n*2\r\n$3\r\ntwo\r\n$4\r\n2.00\r\n*2\r\n$3\r\none\r\n$4\r\n1.00\r\n"),
        ("ZMPOP 1 myzset MAX", "*-1\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZMPOP 1 myzset MIN COUNT 4", "*2\r\n$6\r\nmyzset\r\n*3\r\n*2\r\n$3\r\none\r\n$4\r\n1.00\r\n*2\r\n$3\r\ntwo\r\n$4\r\n2.00\r\n*2\r\n$5\r\nthree\r\n$4\r\n3.00\r\n"),
    ]; "test_zmpop")]
    #[test_case(vec![
        ("zmscore notsuchkey a b", "*2\r\n$-1\r\n$-1\r\n"),
        ("set strkey value", "+OK\r\n"),
        ("zmscore strkey value", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd myset 1 rein 2 dva 3 sigma 4 roadhog", ":4\r\n"),
        ("zmscore myset rein dva sigma no_such_tank roadhog", "*5\r\n$4\r\n1.00\r\n$4\r\n2.00\r\n$4\r\n3.00\r\n$-1\r\n$4\r\n4.00\r\n"),
    ]; "test_zmscore")]
    #[test_case(vec![
        ("zpopmin notsuchkey 5", "*0\r\n"),
        ("set strkey value", "+OK\r\n"),
        ("zpopmin strkey 5", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd myset 1 rein 2 dva 3 sigma 4 roadhog", ":4\r\n"),
        ("zpopmin myset 1", "*2\r\n$4\r\nrein\r\n$4\r\n1.00\r\n"),
        ("zpopmin myset 5", "*6\r\n$3\r\ndva\r\n$4\r\n2.00\r\n$5\r\nsigma\r\n$4\r\n3.00\r\n$7\r\nroadhog\r\n$4\r\n4.00\r\n"),
    ]; "test_zpopmin")]
    #[test_case(vec![
        ("zpopmax notsuchkey1 5", "*0\r\n"),
        ("set strkey value", "+OK\r\n"),
        ("zpopmax strkey 5", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd myset 1 rein 2 dva 3 sigma 4 roadhog", ":4\r\n"),
        ("zpopmax myset 1", "*2\r\n$7\r\nroadhog\r\n$4\r\n4.00\r\n"),
        ("zpopmax myset 5", "*6\r\n$5\r\nsigma\r\n$4\r\n3.00\r\n$3\r\ndva\r\n$4\r\n2.00\r\n$4\r\nrein\r\n$4\r\n1.00\r\n"),
    ]; "test_zpopmax")]
    #[test_case(vec![
        ("zrandmember notsuchkey1", "$-1\r\n"),
        ("set strkey value", "+OK\r\n"),
        ("zrandmember strkey 5", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("zadd myset 1 rein 2 dva 3 sigma 4 roadhog", ":4\r\n"),
        ("zrandmember myset a", "-ERR value is not an integer or out of range\r\n"),
        ("zrandmember myset withscores", "-ERR value is not an integer or out of range\r\n"),
        ("zrandmember myset 10 00", "-ERR syntax error\r\n"),
        ("zrandmember myset 4", "*4\r\n$3\r\ndva\r\n$4\r\nrein\r\n$7\r\nroadhog\r\n$5\r\nsigma\r\n"),
        ("zrandmember myset 4 withscores", "*8\r\n$3\r\ndva\r\n$4\r\n2.00\r\n$4\r\nrein\r\n$4\r\n1.00\r\n$7\r\nroadhog\r\n$4\r\n4.00\r\n$5\r\nsigma\r\n$4\r\n3.00\r\n"),
    ]; "test_zrandmember")]
    #[test_case(vec![
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZRANGE myzset 0 -1", "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
        ("ZRANGE myzset 2 3", "*1\r\n$5\r\nthree\r\n"),
        ("ZRANGE myzset -2 -1", "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
        ("ZRANGE myzset -2 -1 LIMIT 0 1", "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX\r\n"),
        ("ZRANGE myzset -2 -1", "*2\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"),
        ("ZRANGE myzset -2 -1 REV", "*2\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
        ("ZRANGE myzset 0 -1 REV", "*3\r\n$5\r\nthree\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
        ("ZRANGE myzset 0 2 REV", "*3\r\n$5\r\nthree\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
        ("ZRANGE myzset 0 10 REV", "*3\r\n$5\r\nthree\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
        ("ZRANGE myzset -10 -7 REV", "*0\r\n"),
        ("ZRANGE myzset -5 2 REV", "*3\r\n$5\r\nthree\r\n$3\r\ntwo\r\n$3\r\none\r\n"),
    ]; "test_zrangerank")]
    #[test_case(vec![
        ("ZADD srczset 1 one 2 two 3 three 4 four", ":4\r\n"),
        ("ZRANGESTORE dstzset srczset 2 -1", ":2\r\n"),
        ("ZRANGE dstzset 0 -1", "*2\r\n$5\r\nthree\r\n$4\r\nfour\r\n"),
    ]; "test_zrangestore")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("ZRANK mystr bla", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZRANK myzset", "-ERR wrong number of arguments for 'zrank' command\r\n"),
        ("ZRANK myzset three", ":2\r\n"),
        ("ZRANK myzset four", "$-1\r\n"),
        ("ZRANK myzset three WITHSCORE", "*2\r\n:2\r\n$4\r\n3.00\r\n"),
        ("ZRANK myzset four WITHSCORE", "*-1\r\n"),
        ("zrank no_such_set one", "$-1\r\n"),
        ("zrank no_such_set one WITHSCORE", "*-1\r\n"),
    ]; "test_zrank")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zrem mystr bla", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 1 one 2 two 3 three", ":3\r\n"),
        ("ZREM myzset two", ":1\r\n"),
        ("ZRANGE myzset 0 -1 WITHSCORES", "*4\r\n$3\r\none\r\n$4\r\n1.00\r\n$5\r\nthree\r\n$4\r\n3.00\r\n"),
        ("zrem sddsd a", ":0\r\n"),
    ]; "test_zrem")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("zremrangebylex mystr min", "-ERR wrong number of arguments for 'zremrangebylex' command\r\n"),
        ("zremrangebylex mystr min max", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 0 aaaa 0 b 0 c 0 d 0 e", ":5\r\n"),
        ("ZADD myzset 0 foo 0 zap 0 zip 0 ALPHA 0 alpha", ":5\r\n"),
        ("ZRANGE myzset 0 -1", "*10\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$5\r\nalpha\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$3\r\nfoo\r\n$3\r\nzap\r\n$3\r\nzip\r\n"),
        ("ZREMRANGEBYLEX myzset [alpha [omega", ":6\r\n"),
        ("ZRANGE myzset 0 -1", "*4\r\n$5\r\nALPHA\r\n$4\r\naaaa\r\n$3\r\nzap\r\n$3\r\nzip\r\n"),
    ]; "test_zremrangebylex")]
    #[test_case(vec![
        ("set mystr value", "+OK\r\n"),
        ("ZREMRANGEBYRANK mystr min 1 3", "-ERR wrong number of arguments for 'zremrangebyrank' command\r\n"),
        ("ZREMRANGEBYRANK mystr min max", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
        ("ZADD myzset 1 one", ":1\r\n"),
        ("ZADD myzset 2 two", ":1\r\n"),
        ("ZADD myzset 3 three", ":1\r\n"),
        ("ZREMRANGEBYRANK myzset 0 1", ":2\r\n"),
        ("ZRANGE myzset 0 -1 WITHSCORES", "*2\r\n$5\r\nthree\r\n$4\r\n3.00\r\n"),
    ]; "test_zremrangebyrank")]
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

    #[test_case("bzpopmax myset 30", "*3\r\n$5\r\nmyset\r\n$5\r\norisa\r\n$4\r\n2.00\r\n"; "test_bzpopmax")]
    #[test_case("bzpopmin myset 30", "*3\r\n$5\r\nmyset\r\n$4\r\nrein\r\n$4\r\n1.00\r\n"; "test_bzpopmin")]
    #[test_case("bzmpop 30 1 myset MAX", "*2\r\n$5\r\nmyset\r\n*1\r\n*2\r\n$5\r\norisa\r\n$4\r\n2.00\r\n"; "test_bzmpop")]
    fn test_zset_blocking_commands(command: &'static str, expected_result: &'static str) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (_guard, store) = crate::tests::open_store();

            let server = Arc::<ServerState>::default();
            let reader = Client::new(server.clone(), store.clone(), None);
            let writer = Client::new(server, store, None);

            let args = command.split(' ').collect();
            let read_cmd = Rc::new(RedisCommand::for_test(args));

            // we expect to get a rx + duration, if we dont "deferred_command" will panic!
            let (rx, duration, _timeout_response) =
                crate::tests::deferred_command(reader.inner(), read_cmd.clone()).await;

            // second connection: push data to the list
            let pus_cmd = Rc::new(RedisCommand::for_test(vec![
                "zadd", "myset", "1", "rein", "2", "orisa",
            ]));
            let response = crate::tests::execute_command(writer.inner(), pus_cmd.clone()).await;
            assert_eq!(":2\r\n", BytesMutUtils::to_string(&response).as_str());

            // Try reading again now
            match Client::wait_for(rx, duration).await {
                crate::server::WaitResult::TryAgain => {
                    let response = crate::tests::execute_command(reader.inner(), read_cmd).await;
                    assert_eq!(
                        expected_result,
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
