use crate::{SableError, Telemetry};

pub struct StopWatch {
    start: u128,
}

#[derive(Default)]
pub struct IoDurationStopWatch {
    stop_watch: StopWatch,
}

impl StopWatch {
    fn now_as_micros() -> Result<u128, SableError> {
        let Ok(timestamp_micros) =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        else {
            return Err(SableError::OtherError(
                "failed to retrieve std::time::UNIX_EPOCH".to_string(),
            ));
        };
        Ok(timestamp_micros.as_micros())
    }

    pub fn elapsed_micros(&self) -> Result<u128, SableError> {
        let now = Self::now_as_micros()?;
        Ok(now.saturating_sub(self.start))
    }
}

impl Default for StopWatch {
    fn default() -> Self {
        StopWatch {
            start: Self::now_as_micros().unwrap_or_default(),
        }
    }
}

impl Drop for IoDurationStopWatch {
    fn drop(&mut self) {
        if let Ok(elapsed) = self.stop_watch.elapsed_micros() {
            Telemetry::inc_total_io_duration(elapsed);
        }
    }
}
