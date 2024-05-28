use crate::SableError;

pub enum TickInterval {
    Seconds(u64),
    Milliseconds(u64),
}

pub struct Ticker {
    last_tick: u64,
    tick_interval: TickInterval,
}

impl Ticker {
    pub fn new(tick_interval: TickInterval) -> Self {
        Self {
            last_tick: 0,
            tick_interval,
        }
    }

    /// Check whether a "tick" occured. If a tick occured,
    /// this method also updates the "last_tick" timestamp
    pub fn try_tick(&mut self) -> Result<bool, SableError> {
        let (curts, interval) = match self.tick_interval {
            TickInterval::Seconds(secs) => (crate::TimeUtils::epoch_seconds()?, secs),
            TickInterval::Milliseconds(millis) => (crate::TimeUtils::epoch_ms()?, millis),
        };

        if curts.saturating_sub(self.last_tick) > interval {
            self.last_tick = curts;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
