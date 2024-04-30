use crate::redis_client::{SBClient, ValidationFunction};
use crate::{bench_utils, sb_options::Options};
use bytes::BytesMut;
use tokio::sync::mpsc::Sender;

/// Run the `set` test case
pub async fn run_set(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = SBClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        client.set(tx, &key, &payload).await?;
    }
    Ok(())
}

/// Run the `get` test case
pub async fn run_get(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = SBClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        client.get(tx, &key).await?;
    }
    Ok(())
}

/// Run the `ping` test case
pub async fn run_ping(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let mut client = SBClient::default();
    for _ in 0..count {
        client.ping(tx).await?;
    }
    Ok(())
}

/// Run the `incr` test case
pub async fn run_incr(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = SBClient::default();
    let key = bench_utils::generate_key(key_size, key_range);
    for _ in 0..count {
        client.incr(tx, &key, 1).await?;
    }
    Ok(())
}

/// Run the `set` test case
pub async fn run_push(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    right: bool,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = SBClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        client.push(tx, &key, &payload, right).await?;
    }
    Ok(())
}

/// Run the `set` test case
pub async fn run_pop(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    right: bool,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let mut client = SBClient::default();
    for _ in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        client.pop(tx, &key, right).await?;
    }
    Ok(())
}

/// Run the `hset` test case
pub async fn run_hset(
    tx: &mut Sender<(BytesMut, ValidationFunction)>,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = opts.client_requests();
    let key_size = opts.key_size;
    let key_range = opts.key_range;
    let payload = bench_utils::generate_payload(opts.data_size);
    let mut client = SBClient::default();
    for i in 0..count {
        let key = bench_utils::generate_key(key_size, key_range);
        let field = bytes::BytesMut::from(format!("field_{}", i).as_str());
        client.hset(tx, &key, &field, &payload).await?;
    }
    Ok(())
}
