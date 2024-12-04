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
macro_rules! expect_exact_args_count {
    ($cmd:expr, $expected_args_count:expr, $response_buffer:expr, $return_value:expr) => {
        if $cmd.arg_count() != $expected_args_count {
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
macro_rules! check_optional_arg_at_pos {
    ($cmd:expr, $pos:expr, $expected_val:expr, $response_buffer:expr) => {
        if let Some(arg) = $cmd.arg($pos) {
            if BytesMutUtils::to_string(arg).to_lowercase() != $expected_val {
                let builder = RespBuilderV2::default();
                builder.error_string(&mut $response_buffer, Strings::SYNTAX_ERROR);
                return Ok(HandleCommandResult::ResponseBufferUpdated($response_buffer));
            }
        }
    };
}

#[macro_export]
macro_rules! expect_args_count_writer {
    ($cmd:expr, $expected_args_count:expr, $writer:expr, $return_value:expr) => {
        if !$cmd.expect_args_count($expected_args_count) {
            let errmsg = format!(
                "ERR wrong number of arguments for '{}' command",
                $cmd.main_command()
            );
            $writer.error_string(&errmsg).await?;
            $writer.flush().await?;
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
macro_rules! check_args_count_writer {
    ($cmd:expr, $expected_args_count:expr, $writer:expr) => {
        expect_args_count_writer!($cmd, $expected_args_count, $writer, ())
    };
}

#[macro_export]
macro_rules! writer_return_wrong_args_count {
    ($writer:expr, $cmd_name:expr) => {
        $writer
            .error_string(
                format!("ERR wrong number of arguments for '{}' command", $cmd_name).as_str(),
            )
            .await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_wrong_args_count {
    ($builder:expr, $response_buffer:expr, $cmd_name:expr) => {
        $builder.error_string(
            $response_buffer,
            format!("ERR wrong number of arguments for '{}' command", $cmd_name).as_str(),
        );
        return Ok(());
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
///
/// On successful parsing, return the parsed number, otherwise
/// build RESP parsing error message of type `VALUE_NOT_AN_INT_OR_OUT_OF_RANGE`
/// and return `$err_val`
///
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

// Macros for shortening code
#[macro_export]
macro_rules! writer_return_empty_array {
    ($writer:expr) => {
        $writer.empty_array().await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_null_string {
    ($writer:expr) => {
        $writer.null_string().await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_null_array {
    ($writer:expr) => {
        $writer.null_array().await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_null_reply {
    ($writer:expr, $return_array:expr) => {
        if $return_array {
            $writer.null_array().await?;
        } else {
            $writer.null_string().await?;
        }
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_wrong_type {
    ($writer:expr) => {
        $writer.error_string(Strings::WRONGTYPE).await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_syntax_error {
    ($writer:expr) => {
        $writer.error_string(Strings::SYNTAX_ERROR).await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_value_not_int {
    ($writer:expr) => {
        $writer
            .error_string(Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE)
            .await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_min_max_not_float {
    ($writer:expr) => {
        $writer
            .error_string(Strings::ZERR_MIN_MAX_NOT_FLOAT)
            .await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_empty_array {
    ($builder:expr, $response_buffer:expr) => {
        $builder.empty_array($response_buffer);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_null_array {
    ($builder:expr, $response_buffer:expr) => {
        $builder.null_array($response_buffer);
        return Ok(());
    };
}

/// Return a null response.
///
/// Params:
/// - `$builder` and instance of `RespBuilderV2`
/// - `$response_buffer` the response will be written into this buffer
/// - `$arr_null` if set to true, a null array is returned, else return a null string
#[macro_export]
macro_rules! builder_return_null_reply {
    ($builder:expr, $response_buffer:expr, $arr_null:expr) => {
        if $arr_null {
            $builder.null_array($response_buffer);
        } else {
            $builder.null_string($response_buffer);
        }
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_null_string {
    ($builder:expr, $response_buffer:expr) => {
        $builder.null_string($response_buffer);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_syntax_error {
    ($builder:expr, $response_buffer:expr) => {
        $builder.error_string($response_buffer, Strings::SYNTAX_ERROR);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_wrong_type {
    ($builder:expr, $response_buffer:expr) => {
        $builder.error_string($response_buffer, Strings::WRONGTYPE);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_value_not_int {
    ($builder:expr, $response_buffer:expr) => {
        $builder.error_string($response_buffer, Strings::VALUE_NOT_AN_INT_OR_OUT_OF_RANGE);
        return Ok(());
    };
}

#[macro_export]
macro_rules! writer_return_at_least_1_key {
    ($writer:expr, $command:expr) => {
        $writer
            .error_string(
                format!(
                    "ERR at least 1 input key is needed for '{}' command",
                    $command.main_command()
                )
                .as_str(),
            )
            .await?;
        $writer.flush().await?;
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_at_least_1_key {
    ($builder:expr, $response_buffer:expr, $command:expr) => {
        $builder.error_string(
            $response_buffer,
            format!(
                "ERR at least 1 input key is needed for '{}' command",
                $command.main_command()
            )
            .as_str(),
        );
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_keys_must_be_gt_0 {
    ($builder:expr, $response_buffer:expr) => {
        $builder.error_string($response_buffer, Strings::ERR_NUM_KEYS_MUST_BE_GT_ZERO);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_min_max_not_float {
    ($builder:expr, $response_buffer:expr) => {
        $builder.error_string($response_buffer, Strings::ZERR_MIN_MAX_NOT_FLOAT);
        return Ok(());
    };
}

#[macro_export]
macro_rules! builder_return_number {
    ($builder:expr, $response_buffer:expr, $num:expr) => {
        $builder.number_usize($response_buffer, $num);
        return Ok(());
    };
}

#[macro_export]
macro_rules! zset_md_or_nil {
    ($zset_db:expr, $key:expr,  $writer:expr) => {{
        let md = match $zset_db.find_set($key)? {
            $crate::storage::FindZSetResult::WrongType => {
                $writer.error_string(Strings::WRONGTYPE).await?;
                $writer.flush().await?;
                return Ok(());
            }
            $crate::storage::FindZSetResult::NotFound => {
                $writer.empty_array().await?;
                $writer.flush().await?;
                return Ok(());
            }
            $crate::storage::FindZSetResult::Some(md) => md,
        };
        md
    }};
}

#[macro_export]
macro_rules! zset_md_or_nil_builder {
    ($zset_db:expr, $key:expr,  $builder:expr, $response_buffer:expr) => {{
        let md = match $zset_db.find_set($key)? {
            $crate::storage::FindZSetResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            $crate::storage::FindZSetResult::NotFound => {
                $builder.empty_array($response_buffer);
                return Ok(());
            }
            $crate::storage::FindZSetResult::Some(md) => md,
        };
        md
    }};
}

#[macro_export]
macro_rules! zset_md_or_nil_string_builder {
    ($zset_db:expr, $key:expr,  $builder:expr, $response_buffer:expr) => {{
        let md = match $zset_db.find_set($key)? {
            $crate::storage::FindZSetResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            $crate::storage::FindZSetResult::NotFound => {
                $builder.null_string($response_buffer);
                return Ok(());
            }
            $crate::storage::FindZSetResult::Some(md) => md,
        };
        md
    }};
}

#[macro_export]
macro_rules! zset_md_or_nil_array_builder {
    ($zset_db:expr, $key:expr,  $builder:expr, $response_buffer:expr) => {{
        let md = match $zset_db.find_set($key)? {
            $crate::storage::FindZSetResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            $crate::storage::FindZSetResult::NotFound => {
                $builder.null_array($response_buffer);
                return Ok(());
            }
            $crate::storage::FindZSetResult::Some(md) => md,
        };
        md
    }};
}

#[macro_export]
macro_rules! zset_md_or_0_builder {
    ($zset_db:expr, $key:expr,  $builder:expr, $response_buffer:expr) => {{
        let md = match $zset_db.find_set($key)? {
            $crate::storage::FindZSetResult::WrongType => {
                $builder.error_string($response_buffer, Strings::WRONGTYPE);
                return Ok(());
            }
            $crate::storage::FindZSetResult::NotFound => {
                $builder.number_usize($response_buffer, 0);
                return Ok(());
            }
            $crate::storage::FindZSetResult::Some(md) => md,
        };
        md
    }};
}

/// Block `$client_state` for keys `$interersting_keys`. In case of failure to block
/// the client, return `NullArray`
macro_rules! block_client_for_keys_return_null_array {
    ($client_state:expr, $interersting_keys:expr, $response_buffer:expr) => {{
        if $client_state.is_txn_state_exec() {
            // while in txn, we do not block the client
            let builder = RespBuilderV2::default();
            builder.null_array(&mut $response_buffer);
            return Ok(HandleCommandResult::ResponseBufferUpdated($response_buffer));
        }

        let client_state_clone = $client_state.clone();
        let rx = match $client_state
            .server_inner_state()
            .block_client($client_state.id(), &$interersting_keys, client_state_clone)
            .await
        {
            BlockClientResult::Blocked(rx) => rx,
            BlockClientResult::TxnActive => {
                // can't block the client due to an active transaction
                let builder = RespBuilderV2::default();
                builder.null_array(&mut $response_buffer);
                return Ok(HandleCommandResult::ResponseBufferUpdated($response_buffer));
            }
        };
        rx
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
mod set_commands;
mod string_commands;
mod strings;
mod transaction_commands;
mod zset_commands;

pub use crate::commands::strings::Strings;
pub use base_commands::BaseCommands;
pub use client_commands::ClientCommands;
pub use command::commands_manager;
pub use command::ValkeyCommand;
pub use commander::{CommandMetadata, CommandsManager, ValkeyCommandName};
pub use generic_commands::GenericCommands;
pub use hash_commands::HashCommands;
pub use list_commands::ListCommands;
pub use server_commands::ServerCommands;
pub use set_commands::SetCommands;
pub use string_commands::StringCommands;
pub use transaction_commands::TransactionCommands;
pub use zset_commands::ZSetCommands;

use tokio::{sync::mpsc::Receiver, time::Duration};
