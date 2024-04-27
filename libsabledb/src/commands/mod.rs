#[macro_export]
macro_rules! expect_args_count {
    ($cmd:expr, $expected_args_count:expr, $response_buffer:expr, $return_value:expr) => {
        if !$cmd.expect_args_count($expected_args_count) {
            let builder = RespBuilderV2::default();
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                $cmd.main_command()
            );
            builder.error_string($response_buffer, &errmsg);
            return Ok($return_value);
        }
    };
}

#[macro_export]
macro_rules! expect_args_count_tx {
    ($cmd:expr, $expected_args_count:expr, $resp_writer:expr, $return_value:expr) => {
        if !$cmd.expect_args_count($expected_args_count) {
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                $cmd.main_command()
            );
            $resp_writer.error_string(&errmsg).await?;
            $resp_writer.flush().await?;
            return Ok($return_value);
        }
    };
}

#[macro_export]
macro_rules! check_args_count {
    ($cmd:expr, $expected_args_count:expr, $response_buffer:expr) => {
        expect_args_count!($cmd, $expected_args_count, $response_buffer, ())
    };
}

#[macro_export]
macro_rules! check_args_count_tx {
    ($cmd:expr, $expected_args_count:expr, $tx:expr) => {{
        let builder = RespBuilderV2::default();
        let mut response_buffer = bytes::BytesMut::with_capacity(128);
        if !$cmd.expect_args_count($expected_args_count) {
            builder.error_string(
                &mut response_buffer,
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    $cmd.main_command()
                )
                .as_str(),
            );
            $tx.write_all(response_buffer.as_ref()).await?;
            return Ok(());
        }
    }};
}

#[macro_export]
macro_rules! check_value_type {
    ($key_md:expr, $expected_type:expr, $response_buffer:expr) => {
        if !$key_md.is_type($expected_type) {
            let builder = RespBuilderV2::default();
            builder.error_string($response_buffer, Strings::WRONGTYPE);
            return Ok(());
        }
    };
}

#[macro_export]
macro_rules! parse_string_to_number {
    ($val:expr, $response_buffer:expr) => {{
        let Ok(res) = StringUtils::parse_str_to_number::<u64>($val) else {
            let builder = RespBuilderV2::default();
            builder.error_string($response_buffer, Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE);
            return Ok(());
        };
        res
    }};
}

#[macro_export]
/// Parse `$val` into a number of type `$number_type`.
/// On a successful parsing, return the parsed number, otherwise
/// build RESP parsing error message of type `$err_str`
/// and return `$err_val`
macro_rules! to_number_ex {
    ($val:expr, $number_type:ty, $response_buffer:expr, $err_val:expr, $err_str:expr) => {{
        let Some(res) = BytesMutUtils::parse::<$number_type>($val) else {
            let builder = RespBuilderV2::default();
            builder.error_string($response_buffer, $err_str);
            return $err_val;
        };
        res
    }};
}

#[macro_export]
/// Parse `$val` into a number of type `$number_type`.
/// On success parsing, return the parsed number, otherwise
/// build RESP parsing error message of type `VALUE_NOT_AN_INT_OR_OUT_OF_RANGE`
/// and return `$err_val`
macro_rules! to_number {
    ($val:expr, $number_type:ty, $response_buffer:expr, $err_val:expr) => {{
        to_number_ex!(
            $val,
            $number_type,
            $response_buffer,
            $err_val,
            Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE
        )
    }};
}

#[macro_export]
/// Return the command argument at position `pos` as `&BytesMut`
macro_rules! command_arg_at {
    ($cmd:expr, $pos:expr) => {{
        let Some(cmdarg) = $cmd.arg($pos) else {
            return Err(SableError::InvalidArgument(
                "requested argument is out of bounds".to_string(),
            ));
        };
        cmdarg
    }};
}

#[macro_export]
/// Return the command argument at position `pos` as lowercase `String`
macro_rules! command_arg_at_as_str {
    ($cmd:expr, $pos:expr) => {{
        let Some(cmdarg) = $cmd.arg_as_lowercase_string($pos) else {
            return Err(SableError::InvalidArgument(
                Strings::OUT_OF_BOUNDS.to_string(),
            ));
        };
        cmdarg
    }};
}

#[macro_export]
macro_rules! test_assert {
    ($expected:expr, $actual:expr) => {{
        if $actual != $expected {
            println!(
                "{}:{}: Expected: {:?} != Actual: {:?}",
                file!(),
                line!(),
                $actual,
                $expected
            );
            std::process::exit(1);
        }
    }};
}

bitflags::bitflags! {
pub struct SetFlags: u32  {
    const None = 0;
    /// Only set the key if it does not already exist
    const SetIfExists = 1 << 0;
    /// Only set the key if it already exists
    const SetIfNotExists = 1 << 1;
    /// Return the old string stored at key, or nil if key did not exist.
    /// An error is returned and SET aborted if the value stored at key is not a string.
    const ReturnOldValue = 1 << 2;
    /// Retain the time to live associated with the key
    const KeepTtl = 1 << 3;
}
}

#[derive(Debug)]
pub enum TimeoutResponse {
    NullString,
    NullArrray,
    Number(i64),
}

/// Possible return value for a "process_command" function
#[derive(Debug)]
#[allow(dead_code)]
pub enum HandleCommandResult {
    ResponseBufferUpdated(bytes::BytesMut),
    Blocked((Receiver<u8>, Duration, TimeoutResponse)),
    ResponseSent,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ClientNextAction {
    SendResponse(bytes::BytesMut),
    Wait((Receiver<u8>, Duration, TimeoutResponse)),
    TerminateConnection,
    /// Response was already sent to the client
    NoAction,
}

mod base_commands;
mod client_commands;
mod command;
mod commander;
mod generic_commands;
mod hash_commands;
mod list_commands;
mod server_commands;
mod string_commands;
mod strings;
mod transaction_commands;
mod zset_commands;

pub use crate::commands::strings::Strings;
pub use base_commands::BaseCommands;
pub use client_commands::ClientCommands;
pub use command::commands_manager;
pub use command::RedisCommand;
pub use commander::{CommandMetadata, CommandsManager, RedisCommandName};
pub use generic_commands::GenericCommands;
pub use hash_commands::HashCommands;
pub use list_commands::ListCommands;
pub use server_commands::ServerCommands;
pub use string_commands::StringCommands;
pub use transaction_commands::TransactionCommands;
pub use zset_commands::ZSetCommands;

use tokio::{sync::mpsc::Receiver, time::Duration};
