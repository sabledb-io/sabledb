pub mod client;
pub mod commands;
pub mod error_codes;
pub mod io;
pub mod metadata;
pub mod replication;
pub mod request_parser;
pub mod resp_builder_v2;
pub mod server;
pub mod server_options;
pub mod shard_locker;
pub mod stopwatch;
pub mod storage;
pub mod telemetry;
pub mod tls;
pub mod transport;
pub mod types;
pub mod utils;
pub mod worker;
pub mod worker_manager;

pub use client::Client;
pub use commands::{
    ClientCommands, GenericCommands, HashCommands, ListCommands, RedisCommand, RedisCommandName,
    ServerCommands, StringCommands,
};
pub use error_codes::{ParserError, SableError};
pub use metadata::{CommonValueMetadata, Expiration, PrimaryKeyMetadata, StringValueMetadata};
pub use request_parser::RequestParser;
pub use resp_builder_v2::RespBuilderV2;
pub use server::{Server, ServerState};
pub use server_options::ServerOptions;
pub use shard_locker::LockManager;
pub use stopwatch::IoDurationStopWatch;
pub use storage::{BatchUpdate, DbWriteCache, StorageAdapter, StorageOpenParams, StorageRocksDb};
pub use telemetry::Telemetry;
pub use transport::Transport;
pub use utils::{BytesMutUtils, StringUtils, TimeUtils, U8ArrayBuilder, U8ArrayReader};
pub use worker::{Worker, WorkerContext, WorkerMessage};
pub use worker_manager::WorkerManager;

/// Parse string into `usize` with suffix support
#[macro_export]
macro_rules! parse_number {
    ($value:expr, $number_type:ty) => {{
        let (num, suffix) = match $value.find(|c: char| !(c.is_digit(10) || c == '-')) {
            Some(pos) => (&$value[..pos], Some($value[pos..].to_lowercase())),
            None => (&$value[..], None),
        };

        let num = $crate::StringUtils::parse_str_to_number::<$number_type>(num)?;
        match suffix.as_deref() {
            Some("mb") => num.saturating_mul(1 << 20),
            Some("kb") => num.saturating_mul(1 << 10),
            Some("gb") => num.saturating_mul(1 << 30),
            Some("k") => num.saturating_mul(1000),
            Some("m") => num.saturating_mul(1_000_000),
            Some("g") => num.saturating_mul(1_000_000_000),
            Some(sfx) => return Err(SableError::InvalidArgument(format!(
                "Failed parsing `{}`. Unsupported suffix `{}`. Supported suffix are:  gb, mb, kb, g, m, k",
                $value, sfx
            ))),
            None => num,
        }
    }};
}

#[macro_export]
macro_rules! ini_usize {
    ($value:expr) => {{
        let Ok(num_usize) = $value.parse::<usize>() else {
            return Err(SableError::InvalidArgument(format!(
                "failed to convert INI value `{}` to usize",
                $value
            )));
        };
        num_usize
    }};
}

#[macro_export]
macro_rules! ini_isize {
    ($value:expr) => {{
        let Ok(num_usize) = $value.parse::<isize>() else {
            return Err(SableError::InvalidArgument(format!(
                "failed to convert INI value `{}` to isize",
                $value
            )));
        };
        num_usize
    }};
}

#[macro_export]
macro_rules! ini_bool {
    ($value:expr) => {{
        let Ok(num_usize) = $value.parse::<bool>() else {
            return Err(SableError::InvalidArgument(format!(
                "failed to convert INI value `{}` to bool",
                $value
            )));
        };
        num_usize
    }};
}

thread_local! {
    pub static LAST_ERROR_TS: std::cell::RefCell<u64> = const { std::cell::RefCell::new(0u64) };
}

#[macro_export]
/// Log message with throttling in order to avoid flooding the log file
macro_rules! error_with_throttling {
    ($delay_seconds:expr, $($arg:tt)*) => {{
        let current_time = $crate::utils::current_time($crate::utils::CurrentTimeResolution::Seconds);
         $crate::LAST_ERROR_TS.with(|last_ts| {
            let last_logged_ts = *last_ts.borrow();
            if current_time - last_logged_ts >= $delay_seconds {
                *last_ts.borrow_mut() = current_time;
                tracing::error!($($arg)*);
                true
            } else {
                false
            }
        })
    }}
}

//  _    _ _   _ _____ _______      _______ ______  _____ _______ _____ _   _  _____
// | |  | | \ | |_   _|__   __|    |__   __|  ____|/ ____|__   __|_   _| \ | |/ ____|
// | |  | |  \| | | |    | |    _     | |  | |__  | (___    | |    | | |  \| | |  __|
// | |  | | . ` | | |    | |   / \    | |  |  __|  \___ \   | |    | | | . ` | | |_ |
// | |__| | |\  |_| |_   | |   \_/    | |  | |____ ____) |  | |   _| |_| |\  | |__| |
//  \____/|_| \_|_____|  |_|          |_|  |______|_____/   |_|  |_____|_| \_|\_____|
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_with_throttling() {
        // first time, we expect this message to be logged
        let logged = error_with_throttling!(1, "first message");
        assert!(logged);

        // This message should not be logged
        let logged = error_with_throttling!(1, "second message");
        assert!(!logged);

        // sleep for 1.5 seconds
        std::thread::sleep(std::time::Duration::from_millis(1500));

        // this time, the message should be logged
        let logged = error_with_throttling!(1, "third message");
        assert!(logged);
    }

    #[test_case::test_case("100gb", 100 << 30; "convert usize gb")]
    #[test_case::test_case("100mb", 100 << 20; "convert usize mb")]
    #[test_case::test_case("100kb", 100 << 10; "convert usize kb")]
    #[test_case::test_case("1k", 1000; "convert usize k")]
    #[test_case::test_case("1m", 1_000_000; "convert usize m")]
    fn test_string_to_usize_conversion(
        value_as_str: &str,
        expected_value: usize,
    ) -> Result<(), SableError> {
        assert_eq!(parse_number!(value_as_str, usize), expected_value);
        Ok(())
    }

    #[test_case::test_case("-100gb", -(100 << 30); "convert isize gb")]
    #[test_case::test_case("-100mb", -(100 << 20); "convert isize mb")]
    #[test_case::test_case("-100kb", -(100 << 10); "convert isize kb")]
    #[test_case::test_case("-1k", -1000; "convert isize k")]
    #[test_case::test_case("-1m", -1_000_000; "convert isize m")]
    fn test_string_to_isize_conversion(
        value_as_str: &str,
        expected_value: isize,
    ) -> Result<(), SableError> {
        assert_eq!(parse_number!(value_as_str, isize), expected_value);
        Ok(())
    }
}
