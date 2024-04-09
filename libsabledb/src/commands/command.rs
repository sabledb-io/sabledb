use crate::{BytesMutUtils, SableError};
use bytes::BytesMut;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Clone, Debug, Default)]
pub enum RedisCommandName {
    Append,
    Decr,
    DecrBy,
    IncrBy,
    IncrByFloat,
    Incr,
    #[default]
    Set,
    Get,
    Mget,
    Mset,
    Msetnx,
    GetDel,
    GetSet,
    GetEx,
    GetRange,
    Lcs,
    Ping,
    Cmd,
    Config,
    Info,
    Psetex,
    Setex,
    Setnx,
    SetRange,
    Strlen,
    Substr,
    // List commands
    Lpush,
    Lpushx,
    Rpush,
    Rpushx,
    Lpop,
    Rpop,
    Llen,
    Lindex,
    Linsert,
    Lset,
    Lpos,
    Ltrim,
    Lrange,
    Lrem,
    Lmove,
    Rpoplpush,
    Brpoplpush,
    Blpop,
    Brpop,
    Lmpop,
    Blmpop,
    Blmove,
    // Client commands
    Client,
    Select,
    // Server commands
    ReplicaOf,
    SlaveOf,
    // Generic commands
    Ttl,
    Del,
    Exists,
    Expire,
    // Hash commands
    Hset,
    Hget,
    Hdel,
    Hlen,
    NotSupported(String),
}

bitflags::bitflags! {
#[derive(Default, Debug, Clone)]
pub struct RedisCommandFlags: u32  {
    const None = 0;
    /// A read command
    const Read = 1 << 0;
    /// A write command
    const Write = 1 << 1;
    /// Administration command
    const Admin = 1 << 2;
    /// @connection command
    const Connection = 1 << 3;
}
}

#[derive(Default, Debug, Clone)]
pub struct CommandMetadata {
    cmd_name: RedisCommandName,
    cmd_flags: RedisCommandFlags,
}

impl CommandMetadata {
    pub fn new(cmd_name: RedisCommandName, cmd_flags: RedisCommandFlags) -> Self {
        CommandMetadata {
            cmd_name,
            cmd_flags,
        }
    }

    pub fn name(&self) -> &RedisCommandName {
        &self.cmd_name
    }

    #[allow(dead_code)]
    pub fn flags(&self) -> &RedisCommandFlags {
        &self.cmd_flags
    }

    /// Is this command a "Write" command?
    pub fn is_write_command(&self) -> bool {
        self.cmd_flags.intersects(RedisCommandFlags::Write)
    }
}

// READONLY You can't write against a read only replica.
lazy_static::lazy_static! {
    static ref COMMAND_TABLE: HashMap<&'static str, CommandMetadata> = HashMap::from([
        ("ping", CommandMetadata::new(RedisCommandName::Ping, RedisCommandFlags::Read)),
        ("command", CommandMetadata::new(RedisCommandName::Cmd, RedisCommandFlags::Read)),
        ("config", CommandMetadata::new(RedisCommandName::Config, RedisCommandFlags::Read)),
        ("info", CommandMetadata::new(RedisCommandName::Info, RedisCommandFlags::Read)),
        // string commands
        ("append", CommandMetadata::new(RedisCommandName::Append, RedisCommandFlags::Write)),
        ("decr", CommandMetadata::new(RedisCommandName::Decr, RedisCommandFlags::Write)),
        ("decrby", CommandMetadata::new(RedisCommandName::DecrBy, RedisCommandFlags::Write)),
        ("incr", CommandMetadata::new(RedisCommandName::Incr, RedisCommandFlags::Write)),
        ("incrby", CommandMetadata::new(RedisCommandName::IncrBy, RedisCommandFlags::Write)),
        ("incrbyfloat", CommandMetadata::new(RedisCommandName::IncrByFloat, RedisCommandFlags::Write)),
    ("set", CommandMetadata::new(RedisCommandName::Set, RedisCommandFlags::Write)),
        ("get", CommandMetadata::new(RedisCommandName::Get, RedisCommandFlags::Read)),
        ("getdel", CommandMetadata::new(RedisCommandName::GetDel, RedisCommandFlags::Write)),
        ("getset", CommandMetadata::new(RedisCommandName::GetSet, RedisCommandFlags::Write)),
        ("getex", CommandMetadata::new(RedisCommandName::GetEx, RedisCommandFlags::Write)),
        ("getrange", CommandMetadata::new(RedisCommandName::GetRange, RedisCommandFlags::Read)),
        ("lcs", CommandMetadata::new(RedisCommandName::Lcs, RedisCommandFlags::Read)),
        ("mget", CommandMetadata::new(RedisCommandName::Mget, RedisCommandFlags::Read)),
        ("mset", CommandMetadata::new(RedisCommandName::Mset, RedisCommandFlags::Write)),
        ("msetnx", CommandMetadata::new(RedisCommandName::Msetnx, RedisCommandFlags::Read)),
        ("psetex", CommandMetadata::new(RedisCommandName::Psetex, RedisCommandFlags::Write)),
        ("setex", CommandMetadata::new(RedisCommandName::Setex, RedisCommandFlags::Write)),
        ("setnx", CommandMetadata::new(RedisCommandName::Setnx, RedisCommandFlags::Write)),
        ("setrange", CommandMetadata::new(RedisCommandName::SetRange, RedisCommandFlags::Write)),
        ("strlen", CommandMetadata::new(RedisCommandName::Strlen, RedisCommandFlags::Read)),
        ("substr", CommandMetadata::new(RedisCommandName::Substr, RedisCommandFlags::Read)),
        // list commands
        ("lpush", CommandMetadata::new(RedisCommandName::Lpush, RedisCommandFlags::Write)),
        ("lpushx", CommandMetadata::new(RedisCommandName::Lpushx, RedisCommandFlags::Write)),
        ("rpush", CommandMetadata::new(RedisCommandName::Rpush, RedisCommandFlags::Write)),
        ("rpushx", CommandMetadata::new(RedisCommandName::Rpushx, RedisCommandFlags::Write)),
        ("lpop", CommandMetadata::new(RedisCommandName::Lpop, RedisCommandFlags::Write)),
        ("rpop", CommandMetadata::new(RedisCommandName::Rpop, RedisCommandFlags::Write)),
        ("llen", CommandMetadata::new(RedisCommandName::Llen, RedisCommandFlags::Read)),
        ("lindex", CommandMetadata::new(RedisCommandName::Lindex, RedisCommandFlags::Read)),
        ("linsert", CommandMetadata::new(RedisCommandName::Linsert, RedisCommandFlags::Write)),
        ("lset", CommandMetadata::new(RedisCommandName::Lset, RedisCommandFlags::Write)),
        ("lpos", CommandMetadata::new(RedisCommandName::Lpos, RedisCommandFlags::Read)),
        ("ltrim", CommandMetadata::new(RedisCommandName::Ltrim, RedisCommandFlags::Write)),
        ("lrange", CommandMetadata::new(RedisCommandName::Lrange, RedisCommandFlags::Read)),
        ("lrem", CommandMetadata::new(RedisCommandName::Lrem, RedisCommandFlags::Write)),
        ("lmove", CommandMetadata::new(RedisCommandName::Lmove, RedisCommandFlags::Write)),
        ("rpoplpush", CommandMetadata::new(RedisCommandName::Rpoplpush, RedisCommandFlags::Write)),
        ("brpoplpush", CommandMetadata::new(RedisCommandName::Brpoplpush, RedisCommandFlags::Write)),
        ("blpop", CommandMetadata::new(RedisCommandName::Blpop, RedisCommandFlags::Write)),
        ("lmpop", CommandMetadata::new(RedisCommandName::Lmpop, RedisCommandFlags::Write)),
        ("blmove", CommandMetadata::new(RedisCommandName::Blmove, RedisCommandFlags::Write)),
        ("blmpop", CommandMetadata::new(RedisCommandName::Blmpop, RedisCommandFlags::Write)),
        ("brpop", CommandMetadata::new(RedisCommandName::Brpop, RedisCommandFlags::Write)),
        // Client commands
        ("client", CommandMetadata::new(RedisCommandName::Client, RedisCommandFlags::Connection)),
        ("select", CommandMetadata::new(RedisCommandName::Select, RedisCommandFlags::Connection)),
        // Server commands
        ("replicaof", CommandMetadata::new(RedisCommandName::ReplicaOf, RedisCommandFlags::Admin)),
        ("slaveof", CommandMetadata::new(RedisCommandName::SlaveOf, RedisCommandFlags::Admin)),
        // generic commands
        ("ttl", CommandMetadata::new(RedisCommandName::Ttl, RedisCommandFlags::Read)),
        ("del", CommandMetadata::new(RedisCommandName::Del, RedisCommandFlags::Write)),
        ("exists", CommandMetadata::new(RedisCommandName::Exists, RedisCommandFlags::Read)),
        ("expire", CommandMetadata::new(RedisCommandName::Expire, RedisCommandFlags::Write)),
        // Hash commands
        ("hset", CommandMetadata::new(RedisCommandName::Hset, RedisCommandFlags::Write)),
        ("hget", CommandMetadata::new(RedisCommandName::Hget, RedisCommandFlags::Read)),
        ("hdel", CommandMetadata::new(RedisCommandName::Hdel, RedisCommandFlags::Write)),
        ("hlen", CommandMetadata::new(RedisCommandName::Hlen, RedisCommandFlags::Read)),
    ]);
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
        let metadata = match COMMAND_TABLE.get(command_name.as_str()) {
            Some(t) => t.clone(),
            None => CommandMetadata::new(
                RedisCommandName::NotSupported(format!("unsupported command {}", &command_name)),
                RedisCommandFlags::None,
            ),
        };

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
