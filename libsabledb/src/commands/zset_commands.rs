use crate::{
    commands::{HandleCommandResult, Strings},
    io::RespWriter,
    metadata::{ZSetMemberItem, ZSetScoreItem},
    server::ClientState,
    storage::{
        ZSetAddMemberResult, ZSetDb, ZSetGetMetadataResult, ZSetGetScoreResult,
        ZSetGetSmallestResult, ZSetLenResult, ZWriteFlags,
    },
    utils::RespBuilderV2,
    BytesMutUtils, LockManager, RedisCommand, RedisCommandName, SableError,
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

#[derive(Clone, Debug, PartialEq)]
enum GetLimitResult {
    Limit(usize),
    SyntaxError,
}

#[derive(Clone, Debug, PartialEq)]
enum GetCountResult {
    Count(usize),
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
            RedisCommandName::Zrangebyscore => {
                Self::zrangebyscore(client_state, command, tx).await?;
                return Ok(HandleCommandResult::ResponseSent);
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

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
        let mut zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let mut items_added = 0usize;
        for (score, member) in pairs {
            match zset_db.add(key, member, score, &flags, false)? {
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

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = zset_md_or_0_builder!(zset_db, key, builder, response_buffer);

        // empty set? empty array
        if md.is_empty() {
            builder.number_usize(response_buffer, 0);
            return Ok(());
        }

        // Determine the starting score
        let prefix = md.prefix_by_score(None);
        let mut db_iter = client_state.database().create_iterator(Some(&prefix))?;
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
                db_iter = client_state.database().create_iterator(Some(&prefix))?;
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

        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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

        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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
        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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
        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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
            GetLimitResult::SyntaxError => {
                builder_return_syntax_error!(builder, response_buffer);
            }
            GetLimitResult::Limit(limit) => limit,
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
        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
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
                client_state.database().create_iterator(Some(&prefix))?
            }
            LexIndex::Exclude(prefix) => {
                let prefix = md.prefix_by_member(Some(prefix));
                let mut db_iter = client_state.database().create_iterator(Some(&prefix))?;
                db_iter.next(); // skip this entry
                db_iter
            }
            LexIndex::Max => {
                builder.number_usize(response_buffer, 0);
                return Ok(());
            }
            LexIndex::Min => {
                let prefix = md.prefix_by_member(None);
                client_state.database().create_iterator(Some(&prefix))?
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
            GetCountResult::Count(count) => count,
            GetCountResult::SyntaxError => {
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
        let _unused = LockManager::lock_user_keys_exclusive(&user_keys, client_state.clone())?;

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
            client_state.database().create_iterator(Some(&prefix))?
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
                if !current_key.starts_with(end_prefix) {
                    true
                } else {
                    *state = UpperLimitState::Found;
                    // We found the upper limit, "upper limit reached" is
                    // now determined based on whether or not we should include it
                    // i.e if the upper limit is NOT included, then we reached the upper
                    // limit boundary
                    end_prefix_included
                }
            }
            UpperLimitState::Found => current_key.starts_with(end_prefix),
        }
    }

    /// `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
    /// Returns all the elements in the sorted set at key with a score between min and max (including elements with score
    /// equal to min or max). The elements are considered to be ordered from low to high scores
    async fn zrangebyscore(
        client_state: Rc<ClientState>,
        command: Rc<RedisCommand>,
        tx: &mut (impl AsyncWriteExt + std::marker::Unpin),
    ) -> Result<(), SableError> {
        check_args_count_tx!(command, 4, tx);

        let key = command_arg_at!(command, 1);
        let min = command_arg_at!(command, 2);
        let max = command_arg_at!(command, 3);

        let mut writer = RespWriter::new(tx, 4096, client_state.clone());
        let Some((start_score, include_start_score)) = Self::parse_score_index(min) else {
            writer.error_string(Strings::ZERR_MIN_MAX_NOT_FLOAT).await?;
            writer.flush().await?;
            return Ok(());
        };

        let Some((end_score, include_end_score)) = Self::parse_score_index(max) else {
            writer.error_string(Strings::ZERR_MIN_MAX_NOT_FLOAT).await?;
            writer.flush().await?;
            return Ok(());
        };

        // parse the remaining arguments
        let mut iter = command.args_vec().iter();

        // Skip mandatory arguments
        iter.next(); // ZRANGEBYSCORE
        iter.next(); // key
        iter.next(); // min
        iter.next(); // max

        let mut with_scores = false;
        let mut offset = 0u64;
        let mut count = u64::MAX;
        while let Some(arg) = iter.next() {
            let arg_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            match arg_lowercase.as_str() {
                "withscores" => {
                    with_scores = true;
                }
                "limit" => {
                    // The following 2 arguments should the offset and limit
                    let (Some(start_index), Some(limit)) = (iter.next(), iter.next()) else {
                        writer_return_syntax_error!(writer);
                    };

                    let Some(start_index) = BytesMutUtils::parse::<u64>(start_index) else {
                        writer_return_value_not_int!(writer);
                    };

                    let Some(limit) = BytesMutUtils::parse::<i64>(limit) else {
                        writer_return_value_not_int!(writer);
                    };

                    // Negative value means: all items
                    count = match limit {
                        0 => {
                            writer_return_empty_array!(writer);
                        }
                        num if num < 0 => u64::MAX,
                        _ => limit as u64,
                    };
                    offset = start_index;
                }
                _ => {
                    writer_return_syntax_error!(writer);
                }
            }
        }

        let _unused = LockManager::lock_user_key_exclusive(key, client_state.clone())?;
        let zset_db = ZSetDb::with_storage(client_state.database(), client_state.database_id());

        let md = zset_md_or_nil!(zset_db, key, writer);

        // empty set? empty array
        if md.is_empty() {
            writer_return_empty_array!(writer);
        }

        // Determine the starting score
        let prefix = md.prefix_by_score(None);
        let mut db_iter = client_state.database().create_iterator(Some(&prefix))?;
        if !db_iter.valid() {
            // invalud iterator
            writer_return_empty_array!(writer);
        }

        // Find the first item in the set that complies with the start condition
        while db_iter.valid() {
            // get the key & value
            let Some((key, _)) = db_iter.key_value() else {
                writer_return_empty_array!(writer);
            };

            if !key.starts_with(&prefix) {
                writer_return_empty_array!(writer);
            }

            let set_item = ZSetScoreItem::from_bytes(key)?;
            if (include_start_score && set_item.score() >= start_score)
                || (!include_start_score && set_item.score() > start_score)
            {
                let prefix = md.prefix_by_score(Some(set_item.score()));
                // place the iterator on the range start
                db_iter = client_state.database().create_iterator(Some(&prefix))?;
                break;
            }
            db_iter.next();
        }

        // Apply the LIMIT <OFFSET> <COUNT> condition
        while offset > 0 && db_iter.valid() {
            db_iter.next();
            offset = offset.saturating_sub(1);
        }

        // Sanity checks
        if offset != 0 || !db_iter.valid() {
            writer_return_empty_array!(writer);
        }

        enum MatchValue {
            Member(BytesMut),
            Score(f64),
        }

        // All items must start with `zset_prefix` regardless of the user conditions
        let zset_prefix = md.prefix_by_score(None);

        let mut response = Vec::<MatchValue>::new();
        while db_iter.valid() && count > 0 {
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
            response.push(MatchValue::Member(BytesMut::from(field.member())));
            if with_scores {
                response.push(MatchValue::Score(field.score()));
            }
            db_iter.next();
            count = count.saturating_sub(1);
        }

        if response.is_empty() {
            writer_return_empty_array!(writer);
        } else {
            writer.add_array_len(response.len()).await?;
            for v in response {
                match v {
                    MatchValue::Score(score) => {
                        writer
                            .add_bulk_string(format!("{score:.2}").as_bytes())
                            .await?
                    }
                    MatchValue::Member(member) => writer.add_bulk_string(&member).await?,
                }
            }
            writer.flush().await?;
        }
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
        let mut db_iter = client_state.database().create_iterator(Some(&prefix))?;
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
    fn get_limit(command: Rc<RedisCommand>) -> GetLimitResult {
        let mut iter = command.args_vec().iter();
        while let Some(arg) = iter.next() {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq("limit") {
                let Some(val) = iter.next() else {
                    return GetLimitResult::SyntaxError;
                };

                let Some(val) = BytesMutUtils::parse::<usize>(val) else {
                    return GetLimitResult::SyntaxError;
                };

                return GetLimitResult::Limit(val);
            }
        }
        GetLimitResult::Limit(usize::MAX)
    }

    /// Return the `COUNT <value>` from the command line
    fn get_count(command: Rc<RedisCommand>, default_value: usize) -> GetCountResult {
        let mut iter = command.args_vec().iter();
        while let Some(arg) = iter.next() {
            let token_lowercase = String::from_utf8_lossy(&arg[..]).to_lowercase();
            if token_lowercase.eq("count") {
                let Some(val) = iter.next() else {
                    return GetCountResult::SyntaxError;
                };

                let Some(val) = BytesMutUtils::parse::<usize>(val) else {
                    return GetCountResult::SyntaxError;
                };

                return GetCountResult::Count(val);
            }
        }
        GetCountResult::Count(default_value)
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