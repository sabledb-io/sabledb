use crate::redis_client::{RedisClient, StreamType};
use crate::{bench_utils, sb_options::Options, stats};
use libsabledb::{stopwatch::StopWatch, RedisObject};

/// Run the `set` test case
pub async fn run_set(
    mut stream: StreamType,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = RedisClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let sw = StopWatch::default();
        client.set(&mut stream, &key, &payload).await?;
        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `get` test case
pub async fn run_get(
    mut stream: StreamType,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = RedisClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let sw = StopWatch::default();
        if let RedisObject::Str(_) = client.get(&mut stream, &key).await? {
            stats::incr_hits();
        }
        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `ping` test case
pub async fn run_ping(
    mut stream: StreamType,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let mut client = RedisClient::default();
    for _ in 0..count {
        let sw = StopWatch::default();
        client.ping(&mut stream).await?;
        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `incr` test case
pub async fn run_incr(
    mut stream: StreamType,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = RedisClient::default();
    let key = bench_utils::generate_key(key_size, key_range);
    for _ in 0..count {
        let sw = StopWatch::default();
        client.incr(&mut stream, &key, 1).await?;

        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `set` test case
pub async fn run_push(
    mut stream: StreamType,
    right: bool,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = RedisClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let sw = StopWatch::default();
        client.push(&mut stream, &key, &payload, right).await?;
        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `set` test case
pub async fn run_pop(
    mut stream: StreamType,
    right: bool,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = RedisClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let sw = StopWatch::default();
        if let RedisObject::Str(_) = client.pop(&mut stream, &key, right).await? {
            stats::incr_hits();
        }

        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}

/// Run the `hset` test case
pub async fn run_hset(
    mut stream: StreamType,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = RedisClient::default();
    for i in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let field = bytes::BytesMut::from(format!("field_{}", i).as_str());
        let sw = StopWatch::default();
        client.hset(&mut stream, &key, &field, &payload).await?;

        stats::record_latency(sw.elapsed_micros()?.try_into().unwrap_or(u64::MAX));
        stats::incr_requests();
    }
    Ok(())
}
