pub mod commands;
pub mod io;
pub mod metadata;
pub mod net;
pub mod replication;
pub mod server;
pub mod storage;
pub mod types;
pub mod utils;
pub mod integtests;

pub use commands::{
    ClientCommands, GenericCommands, HashCommands, ListCommands, RedisCommand, RedisCommandName,
    ServerCommands, SetCommands, StringCommands, TransactionCommands, ZSetCommands,
};
pub use metadata::{CommonValueMetadata, Expiration, PrimaryKeyMetadata, StringValueMetadata};
pub use net::Transport;
pub use server::Telemetry;
pub use server::WorkerManager;
pub use server::*;
pub use server::{BlockClientResult, Server, ServerState};
pub use server::{Worker, WorkerContext, WorkerMessage};
pub use storage::{BatchUpdate, DbWriteCache, StorageAdapter, StorageOpenParams, StorageRocksDb};
pub use utils::resp_builder_v2::RespBuilderV2;
pub use utils::resp_response_parser_v2::{RedisObject, RespResponseParserV2, ResponseParseResult};
pub use utils::*;

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
macro_rules! ini_read_prop {
    ($ini_file:expr, $sect:expr, $name:expr) => {{
        let Some(properties) = $ini_file.section(Some($sect)) else {
            return Ok(());
        };

        let Some(val) = properties.get($name) else {
            return Ok(());
        };
        val
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

#[macro_export]
macro_rules! impl_to_u8_builder_for {
    ($type_name:ident) => {
        // The macro will expand into the contents of this block.
        paste::item! {
            impl $crate::utils::ToU8Writer for $type_name {
                fn to_writer(&self, builder: &mut $crate::utils::U8ArrayBuilder) {
                    builder.[< write_ $type_name >](*self);
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_to_u8_builder_for_atomic {
    ($type_name:ident, $simple_type_name:ident) => {
        // The macro will expand into the contents of this block.
        paste::item! {
            impl $crate::utils::ToU8Writer for $type_name {
                fn to_writer(&self, builder: &mut $crate::utils::U8ArrayBuilder) {
                    let val = self.load(std::sync::atomic::Ordering::Relaxed);
                    builder.[< write_ $simple_type_name >](val);
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_from_u8_reader_for {
    ($type_name:ident) => {
        // The macro will expand into the contents of this block.
        paste::item! {
            impl $crate::utils::FromU8Reader for $type_name {
                type Item = $type_name;
                fn from_reader(reader: &mut $crate::utils::U8ArrayReader) -> Option<Self::Item> {
                    reader.[< read_ $type_name >]()
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_from_u8_reader_for_atomic {
    ($type_name:ident, $simple_type_name:ident) => {
        // The macro will expand into the contents of this block.
        paste::item! {
            impl $crate::utils::FromU8Reader for $type_name {
                type Item = $type_name;
                fn from_reader(reader: &mut $crate::utils::U8ArrayReader) -> Option<Self::Item> {
                    let val = reader.[< read_ $simple_type_name >]()?;
                    Some($type_name::new(val))
                }
            }
        }
    };
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
    use crate::commands::{ClientNextAction, TimeoutResponse};
    use bytes::BytesMut;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::{atomic, atomic::Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::Duration;

    #[allow(dead_code)]
    pub struct ResponseSink {
        temp_file: crate::io::TempFile,
        pub fp: tokio::fs::File,
    }

    lazy_static::lazy_static! {
        static ref COUNTER: atomic::AtomicU64
            = atomic::AtomicU64::new(crate::TimeUtils::epoch_micros().unwrap_or(0));
    }

    impl ResponseSink {
        pub async fn with_name(name: &str) -> Self {
            let temp_file = crate::io::TempFile::with_name(name);
            let fp = tokio::fs::File::create(&temp_file.fullpath())
                .await
                .unwrap();
            ResponseSink { temp_file, fp }
        }

        pub async fn read_all(&mut self) -> String {
            self.read_all_with_size(4096).await
        }

        pub async fn read_all_with_size(&mut self, size: usize) -> String {
            self.fp.sync_all().await.unwrap();
            let mut fp = tokio::fs::File::open(&self.temp_file.fullpath())
                .await
                .unwrap();

            let mut buffer = bytes::BytesMut::with_capacity(size);
            fp.read_buf(&mut buffer).await.unwrap();
            crate::BytesMutUtils::to_string(&buffer)
        }
    }

    /// Deleter directory when dropped
    pub struct DirDeleter {
        dirpath: String,
    }

    impl DirDeleter {
        pub fn with_path(dirpath: String) -> Self {
            DirDeleter { dirpath }
        }
    }

    impl Drop for DirDeleter {
        fn drop(&mut self) {
            if let Ok(md) = std::fs::metadata(self.dirpath.clone()) {
                if md.is_dir() {
                    std::fs::remove_dir_all(self.dirpath.clone()).unwrap();
                }
            }
        }
    }

    /// Run a command that should be deferred by the server
    pub async fn deferred_command(
        client_state: Rc<ClientState>,
        cmd: Rc<RedisCommand>,
    ) -> (Receiver<u8>, Duration, TimeoutResponse) {
        let mut sink = crate::io::FileResponseSink::new().await.unwrap();
        let next_action = Client::handle_command(client_state, cmd, &mut sink.fp)
            .await
            .unwrap();
        match next_action {
            ClientNextAction::NoAction => panic!("NoAction: expected to be blocked"),
            ClientNextAction::SendResponse(_) => panic!("SendResponse: expected to be blocked"),
            ClientNextAction::TerminateConnection => {
                panic!("TerminateConnection: expected to be blocked")
            }
            ClientNextAction::Wait((rx, duration, timout_response)) => {
                (rx, duration, timout_response)
            }
        }
    }

    /// Execute a command
    pub async fn execute_command(client_state: Rc<ClientState>, cmd: Rc<RedisCommand>) -> BytesMut {
        let mut sink = crate::io::FileResponseSink::new().await.unwrap();
        let next_action = Client::handle_command(client_state, cmd.clone(), &mut sink.fp)
            .await
            .unwrap();
        match next_action {
            ClientNextAction::SendResponse(buffer) => buffer,
            ClientNextAction::NoAction => {
                BytesMutUtils::from_string(sink.read_all_as_string().await.unwrap().as_str())
            }
            ClientNextAction::TerminateConnection => {
                panic!("Command {:?} is not expected to be blocked", cmd)
            }
            ClientNextAction::Wait(_) => panic!("Command {:?} is not expected to be blocked", cmd),
        }
    }

    // Provide a convenient API for opening a unique database
    pub fn open_store() -> (DirDeleter, StorageAdapter) {
        let database_base_dir = format!(
            "{}/sabledb_tests",
            std::env::temp_dir().to_path_buf().display()
        );

        let database_base_dir = database_base_dir.replace('\\', "/");
        let database_base_dir = database_base_dir.replace("//", "/");

        // make sure we don't creat a folder at the root directory
        assert_ne!(database_base_dir, "/sabledb_tests");

        let database_fullpath = format!(
            "{}/testdb_{}.db",
            database_base_dir,
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        let _ = std::fs::create_dir_all(database_base_dir.as_str());
        let db_path = PathBuf::from(database_fullpath.as_str());
        let open_params = StorageOpenParams::default()
            .set_compression(false)
            .set_cache_size(64)
            .set_path(&db_path)
            .set_wal_disabled(true);

        let mut store = StorageAdapter::default();
        store.open(open_params).unwrap();
        (DirDeleter::with_path(database_fullpath), store)
    }

    pub async fn run_and_return_output(
        cmd_args: Vec<String>,
        client_state: std::rc::Rc<ClientState>,
    ) -> Result<RedisObject, SableError> {
        let mut sink = crate::tests::ResponseSink::with_name("run_and_return_output").await;
        let cmd = std::rc::Rc::new(RedisCommand::for_test2(cmd_args));

        match Client::handle_command(client_state, cmd, &mut sink.fp)
            .await
            .unwrap()
        {
            crate::commands::ClientNextAction::NoAction => {
                match RespResponseParserV2::parse_response(
                    sink.read_all().await.as_str().as_bytes(),
                )
                .unwrap()
                {
                    ResponseParseResult::Ok((_, obj)) => {
                        return Ok(obj);
                    }
                    _ => {
                        return Err(SableError::OtherError("parsing error".into()));
                    }
                }
            }
            _ => {
                return Err(SableError::NotFound);
            }
        }
    }

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
