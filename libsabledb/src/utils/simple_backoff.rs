use crate::TimeUtils;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct SimpleBackoff {
    /// Each failure multiplies the delay by this value
    multiplier: u64,
    /// Current duration to wait
    wait_duration: AtomicU64,
    /// Last op timestamp
    last_tick: AtomicU64,
    /// Minimal value for backoff, in milliseconds
    min_delay: u64,
    /// Maximum value for backoff, in milliseconds
    max_delay: u64,
    /// Are we in error state?
    error_state: AtomicBool,
}

impl SimpleBackoff {
    pub fn with_range(min_delay_ms: u64, max_delay_ms: u64) -> Self {
        Self {
            multiplier: 2,
            wait_duration: AtomicU64::new(0),
            last_tick: AtomicU64::new(TimeUtils::epoch_ms().unwrap_or_default()),
            min_delay: min_delay_ms,
            max_delay: max_delay_ms,
            error_state: AtomicBool::new(false),
        }
    }

    /// Return how long to should the caller wait until an operation can be tried
    pub fn can_try(&self) -> u64 {
        let curts = TimeUtils::epoch_ms().unwrap_or_default();
        if curts.saturating_sub(self.last_tick.load(Ordering::Relaxed))
            > self.wait_duration.load(Ordering::Relaxed)
        {
            self.last_tick.store(curts, Ordering::Relaxed);
            0
        } else {
            curts.saturating_sub(self.last_tick.load(Ordering::Relaxed))
        }
    }

    /// An error occurred, increase the wait duration up until we reach `max_delay`
    pub fn incr_error(&self) {
        // Change the state to error
        if !self.error_state.load(Ordering::Relaxed) {
            self.error_state.store(true, Ordering::Relaxed);
            // set the wait duration to the minimum delay
            self.wait_duration.store(self.min_delay, Ordering::Relaxed);
        } else {
            // multiply the wait duration
            self.wait_duration.store(
                self.wait_duration
                    .load(Ordering::Relaxed)
                    .saturating_mul(self.multiplier),
                Ordering::Relaxed,
            );
        }

        if self.wait_duration.load(Ordering::Relaxed) > self.max_delay {
            self.wait_duration.store(self.max_delay, Ordering::Relaxed);
        }
    }

    /// Caller should call this method after a successful operation
    pub fn reset(&self) {
        self.wait_duration.store(0, Ordering::Relaxed);
        self.error_state.store(false, Ordering::Relaxed);
        self.last_tick
            .store(TimeUtils::epoch_ms().unwrap_or_default(), Ordering::Relaxed);
    }
}
