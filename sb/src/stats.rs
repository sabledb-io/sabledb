use hdrhistogram::Histogram;
use indicatif::ProgressBar;
use lazy_static::lazy_static;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

lazy_static! {
    static ref REQUESTS_PROCESSED: AtomicUsize = AtomicUsize::new(0);
    static ref SETGET_SET_CLIENTS: AtomicUsize = AtomicUsize::new(0);
    static ref SETGET_GET_CLIENTS: AtomicUsize = AtomicUsize::new(0);
    static ref HITS: AtomicUsize = AtomicUsize::new(0);
    static ref RUNNING_THREADS: AtomicUsize = AtomicUsize::new(0);
    // possible values:
    // 100us -> 10 minutes
    static ref HIST: Mutex<Histogram<u64>>
        = Mutex::new(Histogram::<u64>::new_with_bounds(1, 600000000, 2).unwrap());
    static ref PROGRESS: ProgressBar = ProgressBar::new(10);
}

/// Increment the total number of requests by `count`
pub fn incr_requests(count: usize) {
    REQUESTS_PROCESSED.fetch_add(count, Ordering::Relaxed);
    PROGRESS.inc(count as u64);
}

/// Increment the total number of requests by 1
pub fn incr_hits() {
    HITS.fetch_add(1, Ordering::Relaxed);
}

/// Return the total requests processed
pub fn requests_processed() -> usize {
    REQUESTS_PROCESSED.load(Ordering::Relaxed)
}

/// Return the total hits
pub fn total_hits() -> usize {
    HITS.load(Ordering::Relaxed)
}

/// Increment the number of running threads by 1
pub fn incr_threads_running() {
    RUNNING_THREADS.fetch_add(1, Ordering::Relaxed);
}

/// Reduce the number of running threads by 1
pub fn decr_threads_running() {
    RUNNING_THREADS.fetch_sub(1, Ordering::Relaxed);
}

pub fn record_latency(val: u64) {
    let mut guard = HIST.lock().expect("lock");
    if let Err(e) = guard.record(val) {
        tracing::error!("Failed to record histogram. {:?}", e);
    }
}

pub fn print_latency() {
    let guard = HIST.lock().expect("lock");
    println!(
        r#"    Latency: [min: {}ms, p50: {}ms, p90: {}ms, p95: {}ms, p99: {}ms, p99.5: {}ms, p99.9: {}ms, max: {}ms]"#,
        guard.min() as f64 / 1000.0,
        guard.value_at_quantile(0.5) as f64 / 1000.0,
        guard.value_at_quantile(0.9) as f64 / 1000.0,
        guard.value_at_quantile(0.95) as f64 / 1000.0,
        guard.value_at_quantile(0.99) as f64 / 1000.0,
        guard.value_at_quantile(0.995) as f64 / 1000.0,
        guard.value_at_quantile(0.999) as f64 / 1000.0,
        guard.max() as f64 / 1000.0,
    );
}

pub fn finish_progress() {
    PROGRESS.finish();
}

pub fn finalise_progress_setup(len: u64) {
    PROGRESS.set_length(len);
    PROGRESS.set_style(
        indicatif::ProgressStyle::with_template(
            "{spinner:.red} [Progress: {percent}%] {wide_bar:.green/green } [{elapsed_precise}] ({eta})",
        )
        .expect("finalise_progress"),
    );
}

pub fn incr_setget_set_tasks(count: usize) {
    SETGET_SET_CLIENTS.fetch_add(count, Ordering::Relaxed);
}

pub fn incr_setget_get_tasks(count: usize) {
    SETGET_GET_CLIENTS.fetch_add(count, Ordering::Relaxed);
}

/// Return the number SET tasks launched when the "setget" test was selected
pub fn setget_set_tasks() -> usize {
    SETGET_SET_CLIENTS.load(Ordering::Relaxed)
}

/// Return the number GET tasks launched when the "setget" test was selected
pub fn setget_get_tasks() -> usize {
    SETGET_GET_CLIENTS.load(Ordering::Relaxed)
}
