use bytes::BytesMut;
use libsabledb::BytesMutUtils;
use rand::{distributions::Alphanumeric, Rng};
use std::sync::atomic::AtomicUsize;

lazy_static::lazy_static! {
    static ref COUNTER: AtomicUsize = AtomicUsize::default();
}

/// Generate random string of length `len`
pub fn generate_payload(len: usize) -> BytesMut {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    BytesMutUtils::from_string(&s)
}

/// Generate random string of length `len` and in the range of `key_range`
pub fn generate_key(len: usize, key_range: usize) -> BytesMut {
    let number = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if number >= key_range {
        COUNTER.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    let right_string = BytesMutUtils::from::<usize>(&number);
    let mut left_string = BytesMutUtils::from(&"0".repeat(len.saturating_sub(right_string.len())));
    left_string.extend_from_slice(&right_string);
    left_string
}
