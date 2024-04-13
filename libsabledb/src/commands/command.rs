use crate::commands::{CommandMetadata, CommandsManager};
use crate::{BytesMutUtils, SableError};
use bytes::BytesMut;
use std::str::FromStr;

lazy_static::lazy_static! {
    static ref COMMANDS_MGR: CommandsManager = CommandsManager::default();
}

/// Generate a RESPv2 output from the command table. Used by the `command` command
pub fn commands_manager() -> &'static CommandsManager {
    &COMMANDS_MGR
}

#[derive(Default, Debug, Clone)]
pub struct RedisCommand {
    /// the raw command arguments
    args: Vec<BytesMut>,

    /// the command name. in lowercase
    /// e.g. if `set KEY VALUE` is the full command, `set` is the `command_name`
    command_name: String,

    /// The command type
    command_metadata: CommandMetadata,
}

impl RedisCommand {
    #[cfg(test)]
    pub fn for_test(args: Vec<&'static str>) -> Self {
        let args: Vec<BytesMut> = args.iter().map(|s| BytesMut::from(s.as_bytes())).collect();
        Self::new(args).unwrap()
    }

    /// Construct `RedisCommandData` from raw parsed data
    pub fn new(args: Vec<BytesMut>) -> Result<Self, SableError> {
        let Some(command_name) = args.first() else {
            return Err(SableError::EmptyCommand);
        };

        let command_name = String::from_utf8_lossy(command_name).to_lowercase();
        let metadata = COMMANDS_MGR.metadata(command_name.as_str());

        Ok(RedisCommand {
            args,
            command_name,
            command_metadata: metadata,
        })
    }

    /// return the primary command
    pub fn main_command(&self) -> &String {
        &self.command_name
    }

    /// Return argument at position `pos` as a lowercase
    /// `String`
    pub fn arg_as_lowercase_string(&self, pos: usize) -> Option<String> {
        let arg = self.args.get(pos)?;
        Some(BytesMutUtils::to_string(arg).to_lowercase())
    }

    /// Return argument at position `pos`
    pub fn arg(&self, pos: usize) -> Option<&BytesMut> {
        self.args.get(pos)
    }

    /// Return number of arguments
    pub fn arg_count(&self) -> usize {
        self.args.len()
    }

    /// Return the command metadata
    #[allow(dead_code)]
    pub fn metadata(&self) -> &CommandMetadata {
        &self.command_metadata
    }

    pub fn expect_args_count(&self, count: usize) -> bool {
        self.arg_count() >= count
    }

    /// Replace value at `index`
    pub fn set_arg_at(&mut self, index: usize, value: &BytesMut) {
        if let Some(val) = self.args.get_mut(index) {
            *val = value.clone();
        }
    }

    pub fn args_vec(&self) -> &Vec<BytesMut> {
        &self.args
    }

    /// Return argument at position `pos` as `T`
    /// In any case of error, return `None`
    pub fn arg_as_number<T: FromStr>(&self, pos: usize) -> Option<T> {
        let arg = self.args.get(pos)?;
        let num = BytesMutUtils::parse::<T>(arg)?;
        Some(num)
    }
}
