use libsabledb::RedisObject;

/// Response validators

pub fn validate_status_ok(redis_obj: RedisObject) -> bool {
    matches!(redis_obj, RedisObject::Status(msg) if msg.eq("OK"))
}

pub fn validate_status_pong(redis_obj: RedisObject) -> bool {
    matches!(redis_obj, RedisObject::Status(msg) if msg.eq("PONG"))
}

pub fn validate_number(redis_obj: RedisObject) -> bool {
    matches!(redis_obj, RedisObject::Integer(_))
}

pub fn validate_null_string_or_number(redis_obj: RedisObject) -> bool {
    match redis_obj {
        RedisObject::NullString | RedisObject::Integer(_) => {
            crate::stats::incr_hits();
            true
        }
        other => {
            tracing::error!("Unexpected response. `{:?}`", other);
            false
        }
    }
}

pub fn validate_bulk_str(redis_obj: RedisObject) -> bool {
    match redis_obj {
        RedisObject::NullString => true,
        RedisObject::Str(_) => {
            crate::stats::incr_hits();
            true
        }
        other => {
            tracing::error!("Unexpected response. `{:?}`", other);
            false
        }
    }
}
