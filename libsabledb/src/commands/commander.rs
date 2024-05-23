use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use strum_macros::EnumString;

#[derive(Default, Debug, Clone, EnumString)]
pub enum RedisCommandFlags {
    #[default]
    None = 0,
    /// A read command
    #[strum(serialize = "read")]
    Read = 1 << 0,
    /// A write command
    #[strum(serialize = "write")]
    Write = 1 << 1,
    /// Administration command
    #[strum(serialize = "admin")]
    Admin = 1 << 2,
    /// @connection command
    #[strum(serialize = "connection")]
    Connection = 1 << 3,
    /// Command might block the client
    #[strum(serialize = "blocking")]
    Blocking = 1 << 4,
    /// This command is now allowed inside a transaction block (`MULTI` / `EXEC`)
    #[strum(serialize = "notransaction")]
    NoTxn = 1 << 5,
}

#[derive(Clone, Debug, Default, EnumString, PartialEq, Eq)]
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
    Config,
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
    Info,
    Command,
    FlushAll,
    FlushDb,
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
    Hexists,
    Hgetall,
    Hincrby,
    Hincrbyfloat,
    Hkeys,
    Hvals,
    Hmget,
    Hmset,
    Hrandfield,
    Hscan,
    Hsetnx,
    Hstrlen,
    // Transaction
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
    // ZSet commands
    Zadd,
    Zcard,
    Zincrby,
    Zcount,
    Zdiff,
    Zdiffstore,
    Zinter,
    Zintercard,
    Zinterstore,
    Zlexcount,
    Zmpop,
    Bzmpop,
    Zmscore,
    Zpopmax,
    Zpopmin,
    Bzpopmax,
    Bzpopmin,
    Zrandmember,
    Zrange,
    Zrangebyscore,
    Zrevrangebyscore,
    Zrangebylex,
    Zrevrangebylex,
    Zrangestore,
    Zrank,
    Zrem,
    Zremrangebylex,
    Zremrangebyrank,
    Zremrangebyscore,
    Zrevrange,
    Zrevrank,
    Zunion,
    Zunionstore,
    Zscore,
    Zscan,
    NotSupported(String),
}

pub struct CommandsManager {
    cmds: HashMap<&'static str, Arc<CommandMetadata>>,
}

impl CommandsManager {
    /// Return the metadata for a command
    pub fn metadata(&self, cmdname: &str) -> Arc<CommandMetadata> {
        match self.cmds.get(cmdname) {
            Some(t) => t.clone(),
            None => Arc::new(CommandMetadata::new(RedisCommandName::NotSupported(
                format!("unsupported command {}", cmdname),
            ))),
        }
    }

    /// Return the entire command table into RESPv2 response
    pub fn cmmand_output(&self) -> BytesMut {
        let builder = crate::RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(4096);

        builder.add_array_len(&mut buffer, self.cmds.len());
        for cmd_md in self.cmds.values() {
            builder.add_resp_string(&mut buffer, &cmd_md.to_resp_v2());
        }
        buffer
    }

    /// Return the entire command table into RESPv2 response
    pub fn cmmand_docs_output(&self) -> BytesMut {
        let builder = crate::RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(4096);

        builder.add_array_len(&mut buffer, self.cmds.len() * 2);
        for name in self.cmds.keys() {
            builder.add_bulk_string(&mut buffer, name.as_bytes());
            builder.add_empty_array(&mut buffer);
        }
        buffer
    }

    /// Return the commands table
    pub fn all_commands(&self) -> &HashMap<&'static str, Arc<CommandMetadata>> {
        &self.cmds
    }
}

#[derive(Default, Debug, Clone)]
pub struct CommandMetadata {
    cmd_name: RedisCommandName,
    cmd_flags: u64,
    /// Arity is the number of arguments a command expects. It follows a simple pattern:
    /// A positive integer means a fixed number of arguments.
    /// A negative integer means a minimal number of arguments.
    /// Command arity always includes the command's name itself (and the subcommand when applicable)
    arity: i16,
    first_key: i16,
    last_key: i16,
    step: u16,
}

impl CommandMetadata {
    pub fn new(cmd_name: RedisCommandName) -> Self {
        CommandMetadata {
            cmd_name,
            cmd_flags: RedisCommandFlags::None as u64,
            arity: 2,
            first_key: 1,
            last_key: 1,
            step: 1,
        }
    }

    /// Arity is the number of arguments a command expects. It follows a simple pattern:
    /// - A positive integer means a fixed number of arguments.
    /// - A negative integer means a minimal number of arguments.
    /// Command arity always includes the command's name itself (and the subcommand when applicable).
    pub fn with_arity(mut self, arity: i16) -> Self {
        self.arity = arity;
        self
    }
    /// The step, or increment, between the first key and the position of the next key.
    pub fn with_step(mut self, step: u16) -> Self {
        self.step = step;
        self
    }

    /// The position of the command's first key name argument. For most commands, the first key's position is 1.
    /// Position 0 is always the command name itself.
    pub fn with_first_key(mut self, first_key: i16) -> Self {
        self.first_key = first_key;
        self
    }

    /// The position of the command's last key name argument. Redis commands usually accept one, two or multiple
    /// number of keys.Commands that accept a single key have both first key and last key set to 1.
    /// Commands that accept two key name arguments, e.g. BRPOPLPUSH, SMOVE and RENAME, have this value set to
    /// the position of their second key. Multi-key commands that accept an arbitrary number of keys,
    /// such as MSET, use the value -1.
    pub fn with_last_key(mut self, last_key: i16) -> Self {
        self.last_key = last_key;
        self
    }

    /// This command might block the client
    pub fn blocking(mut self) -> Self {
        self.set_flag(RedisCommandFlags::Blocking);
        self
    }

    /// This command might block the client
    pub fn no_transaction(mut self) -> Self {
        self.set_flag(RedisCommandFlags::NoTxn);
        self
    }

    /// This command is a "write" command
    pub fn write(mut self) -> Self {
        self.set_flag(RedisCommandFlags::Write);
        self
    }

    /// This command performs "read" on the database
    pub fn read_only(mut self) -> Self {
        self.set_flag(RedisCommandFlags::Read);
        self
    }

    /// An administrator command
    pub fn admin(mut self) -> Self {
        self.set_flag(RedisCommandFlags::Admin);
        self
    }

    /// This command falls under the @connection category
    pub fn connection(mut self) -> Self {
        self.cmd_flags |= RedisCommandFlags::Connection as u64;
        self
    }

    pub fn name(&self) -> &RedisCommandName {
        &self.cmd_name
    }

    /// Is this command a "Write" command?
    pub fn is_write_command(&self) -> bool {
        self.has_flag(RedisCommandFlags::Write)
    }

    /// Is this command a "Write" command?
    pub fn is_read_only_command(&self) -> bool {
        self.has_flag(RedisCommandFlags::Read)
    }

    pub fn is_blocking(&self) -> bool {
        self.has_flag(RedisCommandFlags::Blocking)
    }

    pub fn is_notxn(&self) -> bool {
        self.has_flag(RedisCommandFlags::NoTxn)
    }

    pub fn to_resp_v2(&self) -> BytesMut {
        let builder = crate::RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(64);

        let mut flags = Vec::<&str>::new();
        if self.has_flag(RedisCommandFlags::Read) {
            flags.push("readonly");
        }
        if self.has_flag(RedisCommandFlags::Write) {
            flags.push("write");
        }
        if self.has_flag(RedisCommandFlags::Blocking) {
            flags.push("blocking");
        }
        if self.has_flag(RedisCommandFlags::Admin) {
            flags.push("admin");
        }
        if self.has_flag(RedisCommandFlags::Connection) {
            flags.push("connection");
        }

        if self.has_flag(RedisCommandFlags::NoTxn) {
            flags.push("notransaction");
        }

        let cmdname = BytesMut::from(format!("{:?}", self.cmd_name).to_lowercase().as_str());

        // convert this object into RESP
        builder.add_array_len(&mut buffer, 10);
        builder.add_bulk_string(&mut buffer, &cmdname); // command name
        builder.add_number::<i16>(&mut buffer, self.arity, false); // arity
        builder.add_strings(&mut buffer, &flags); // array of flags
        builder.add_number::<i16>(&mut buffer, self.first_key, false); // first key
        builder.add_number::<i16>(&mut buffer, self.last_key, false); // last key
        builder.add_number::<u16>(&mut buffer, self.step, false); // step between keys
        builder.add_array_len(&mut buffer, 0); // ACL
        builder.add_array_len(&mut buffer, 0); // Tips
        builder.add_array_len(&mut buffer, 0); // Key specs
        builder.add_array_len(&mut buffer, 0); // Sub commands
        buffer
    }

    fn set_flag(&mut self, flag: RedisCommandFlags) {
        self.cmd_flags |= flag as u64
    }

    fn has_flag(&self, flag: RedisCommandFlags) -> bool {
        let res = self.cmd_flags & flag.clone() as u64;
        res == flag as u64
    }
}

impl Default for CommandsManager {
    fn default() -> Self {
        let cmds: HashMap<&str, CommandMetadata> = HashMap::from([
            (
                "config",
                CommandMetadata::new(RedisCommandName::Config)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "info",
                CommandMetadata::new(RedisCommandName::Info)
                    .read_only()
                    .with_arity(-1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            // string commands
            (
                "append",
                CommandMetadata::new(RedisCommandName::Append)
                    .write()
                    .with_arity(3),
            ),
            (
                "decr",
                CommandMetadata::new(RedisCommandName::Decr)
                    .write()
                    .with_arity(2),
            ),
            (
                "decrby",
                CommandMetadata::new(RedisCommandName::DecrBy)
                    .write()
                    .with_arity(3),
            ),
            (
                "incr",
                CommandMetadata::new(RedisCommandName::Incr)
                    .write()
                    .with_arity(2),
            ),
            (
                "incrby",
                CommandMetadata::new(RedisCommandName::IncrBy)
                    .write()
                    .with_arity(3),
            ),
            (
                "incrbyfloat",
                CommandMetadata::new(RedisCommandName::IncrByFloat)
                    .write()
                    .with_arity(3),
            ),
            (
                "set",
                CommandMetadata::new(RedisCommandName::Set)
                    .write()
                    .with_arity(3),
            ),
            (
                "get",
                CommandMetadata::new(RedisCommandName::Get)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "getdel",
                CommandMetadata::new(RedisCommandName::GetDel)
                    .write()
                    .with_arity(2),
            ),
            (
                "getset",
                CommandMetadata::new(RedisCommandName::GetSet)
                    .write()
                    .with_arity(3),
            ),
            (
                "getex",
                CommandMetadata::new(RedisCommandName::GetEx)
                    .write()
                    .with_arity(-2),
            ),
            (
                "getrange",
                CommandMetadata::new(RedisCommandName::GetRange)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "lcs",
                CommandMetadata::new(RedisCommandName::Lcs)
                    .read_only()
                    .with_arity(-3)
                    .with_last_key(2),
            ),
            (
                "mget",
                CommandMetadata::new(RedisCommandName::Mget)
                    .read_only()
                    .with_arity(-2)
                    .with_last_key(-1),
            ),
            (
                "mset",
                CommandMetadata::new(RedisCommandName::Mset)
                    .write()
                    .with_arity(-3)
                    .with_last_key(-1)
                    .with_step(2),
            ),
            (
                "msetnx",
                CommandMetadata::new(RedisCommandName::Msetnx)
                    .write()
                    .with_arity(-3)
                    .with_last_key(-1)
                    .with_step(2),
            ),
            (
                "psetex",
                CommandMetadata::new(RedisCommandName::Psetex)
                    .write()
                    .with_arity(4),
            ),
            (
                "setex",
                CommandMetadata::new(RedisCommandName::Setex)
                    .write()
                    .with_arity(4),
            ),
            (
                "setnx",
                CommandMetadata::new(RedisCommandName::Setnx)
                    .write()
                    .with_arity(3),
            ),
            (
                "setrange",
                CommandMetadata::new(RedisCommandName::SetRange)
                    .write()
                    .with_arity(4),
            ),
            (
                "strlen",
                CommandMetadata::new(RedisCommandName::Strlen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "substr",
                CommandMetadata::new(RedisCommandName::Substr)
                    .read_only()
                    .with_arity(4),
            ),
            // list commands
            (
                "lpush",
                CommandMetadata::new(RedisCommandName::Lpush)
                    .write()
                    .with_arity(-3),
            ),
            (
                "lpushx",
                CommandMetadata::new(RedisCommandName::Lpushx)
                    .write()
                    .with_arity(-3),
            ),
            (
                "rpush",
                CommandMetadata::new(RedisCommandName::Rpush)
                    .write()
                    .with_arity(-3),
            ),
            (
                "rpushx",
                CommandMetadata::new(RedisCommandName::Rpushx)
                    .write()
                    .with_arity(-3),
            ),
            (
                "lpop",
                CommandMetadata::new(RedisCommandName::Lpop)
                    .write()
                    .with_arity(-2),
            ),
            (
                "rpop",
                CommandMetadata::new(RedisCommandName::Rpop)
                    .write()
                    .with_arity(-2),
            ),
            (
                "llen",
                CommandMetadata::new(RedisCommandName::Llen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "lindex",
                CommandMetadata::new(RedisCommandName::Lindex)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "linsert",
                CommandMetadata::new(RedisCommandName::Linsert)
                    .write()
                    .with_arity(5),
            ),
            (
                "lset",
                CommandMetadata::new(RedisCommandName::Lset)
                    .write()
                    .with_arity(4),
            ),
            (
                "lpos",
                CommandMetadata::new(RedisCommandName::Lpos)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "ltrim",
                CommandMetadata::new(RedisCommandName::Ltrim)
                    .write()
                    .with_arity(4),
            ),
            (
                "lrange",
                CommandMetadata::new(RedisCommandName::Lrange)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "lrem",
                CommandMetadata::new(RedisCommandName::Lrem)
                    .write()
                    .with_arity(4),
            ),
            (
                "lmove",
                CommandMetadata::new(RedisCommandName::Lmove)
                    .write()
                    .with_arity(5)
                    .with_last_key(2),
            ),
            (
                "rpoplpush",
                CommandMetadata::new(RedisCommandName::Rpoplpush)
                    .write()
                    .with_arity(3)
                    .with_last_key(2),
            ),
            (
                "lmpop",
                CommandMetadata::new(RedisCommandName::Lmpop)
                    .write()
                    .with_arity(-4)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "brpoplpush",
                CommandMetadata::new(RedisCommandName::Brpoplpush)
                    .write()
                    .blocking()
                    .with_arity(4)
                    .with_last_key(2),
            ),
            (
                "blpop",
                CommandMetadata::new(RedisCommandName::Blpop)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_last_key(-2),
            ),
            (
                "blmove",
                CommandMetadata::new(RedisCommandName::Blmove)
                    .write()
                    .blocking()
                    .with_arity(6)
                    .with_last_key(2),
            ),
            (
                "blmpop",
                CommandMetadata::new(RedisCommandName::Blmpop)
                    .write()
                    .blocking()
                    .with_arity(-5)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "brpop",
                CommandMetadata::new(RedisCommandName::Brpop)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_last_key(-2),
            ),
            // Client commands
            (
                "client",
                CommandMetadata::new(RedisCommandName::Client)
                    .connection()
                    .no_transaction(),
            ),
            (
                "select",
                CommandMetadata::new(RedisCommandName::Select)
                    .connection()
                    .with_arity(2)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            // Server commands
            (
                "replicaof",
                CommandMetadata::new(RedisCommandName::ReplicaOf)
                    .admin()
                    .with_arity(3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "slaveof",
                CommandMetadata::new(RedisCommandName::SlaveOf)
                    .admin()
                    .with_arity(3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "ping",
                CommandMetadata::new(RedisCommandName::Ping)
                    .read_only()
                    .with_arity(-1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "command",
                CommandMetadata::new(RedisCommandName::Command)
                    .with_arity(-1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            // generic commands
            (
                "ttl",
                CommandMetadata::new(RedisCommandName::Ttl)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "del",
                CommandMetadata::new(RedisCommandName::Del)
                    .write()
                    .with_arity(-2)
                    .with_last_key(-1),
            ),
            (
                "exists",
                CommandMetadata::new(RedisCommandName::Exists)
                    .read_only()
                    .with_arity(-2)
                    .with_last_key(-1),
            ),
            (
                "expire",
                CommandMetadata::new(RedisCommandName::Expire)
                    .write()
                    .with_arity(-3),
            ),
            // Hash commands
            (
                "hset",
                CommandMetadata::new(RedisCommandName::Hset)
                    .write()
                    .with_arity(-4),
            ),
            (
                "hmset",
                CommandMetadata::new(RedisCommandName::Hmset)
                    .write()
                    .with_arity(-4),
            ),
            (
                "hget",
                CommandMetadata::new(RedisCommandName::Hget)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "hdel",
                CommandMetadata::new(RedisCommandName::Hdel)
                    .write()
                    .with_arity(-3),
            ),
            (
                "hlen",
                CommandMetadata::new(RedisCommandName::Hlen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hexists",
                CommandMetadata::new(RedisCommandName::Hexists)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "hgetall",
                CommandMetadata::new(RedisCommandName::Hgetall)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hincrbyfloat",
                CommandMetadata::new(RedisCommandName::Hincrbyfloat)
                    .write()
                    .with_arity(4),
            ),
            (
                "hincrby",
                CommandMetadata::new(RedisCommandName::Hincrby)
                    .write()
                    .with_arity(4),
            ),
            (
                "hkeys",
                CommandMetadata::new(RedisCommandName::Hkeys)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hvals",
                CommandMetadata::new(RedisCommandName::Hvals)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hmget",
                CommandMetadata::new(RedisCommandName::Hmget)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "hrandfield",
                CommandMetadata::new(RedisCommandName::Hrandfield)
                    .read_only()
                    .with_arity(-2),
            ),
            (
                "hscan",
                CommandMetadata::new(RedisCommandName::Hscan)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "hsetnx",
                CommandMetadata::new(RedisCommandName::Hsetnx)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "hstrlen",
                CommandMetadata::new(RedisCommandName::Hstrlen)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "multi",
                CommandMetadata::new(RedisCommandName::Multi)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "exec",
                CommandMetadata::new(RedisCommandName::Exec)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "discard",
                CommandMetadata::new(RedisCommandName::Discard)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "watch",
                CommandMetadata::new(RedisCommandName::Watch)
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .no_transaction(),
            ),
            (
                "unwatch",
                CommandMetadata::new(RedisCommandName::Unwatch)
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_first_key(0)
                    .no_transaction(),
            ),
            (
                "zadd",
                CommandMetadata::new(RedisCommandName::Zadd)
                    .write()
                    .with_arity(-4),
            ),
            (
                "zcard",
                CommandMetadata::new(RedisCommandName::Zcard)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "zincrby",
                CommandMetadata::new(RedisCommandName::Zincrby)
                    .write()
                    .with_arity(4),
            ),
            (
                "zrangebyscore",
                CommandMetadata::new(RedisCommandName::Zrangebyscore)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrangebyscore",
                CommandMetadata::new(RedisCommandName::Zrevrangebyscore)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrangebylex",
                CommandMetadata::new(RedisCommandName::Zrangebylex)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrangebylex",
                CommandMetadata::new(RedisCommandName::Zrevrangebylex)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zcount",
                CommandMetadata::new(RedisCommandName::Zcount)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "zdiff",
                CommandMetadata::new(RedisCommandName::Zdiff)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "zdiffstore",
                CommandMetadata::new(RedisCommandName::Zdiffstore)
                    .write()
                    .with_arity(-4)
                    .with_first_key(1)
                    .with_last_key(1)
                    .with_step(1),
            ),
            (
                "zinter",
                CommandMetadata::new(RedisCommandName::Zinter)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "zintercard",
                CommandMetadata::new(RedisCommandName::Zintercard)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "zinterstore",
                CommandMetadata::new(RedisCommandName::Zinterstore)
                    .write()
                    .with_arity(-4),
            ),
            (
                "zlexcount",
                CommandMetadata::new(RedisCommandName::Zlexcount)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "zmpop",
                CommandMetadata::new(RedisCommandName::Zmpop)
                    .write()
                    .with_arity(-4)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "bzmpop",
                CommandMetadata::new(RedisCommandName::Bzmpop)
                    .write()
                    .blocking()
                    .with_arity(-5)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "zmscore",
                CommandMetadata::new(RedisCommandName::Zmscore)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zpopmax",
                CommandMetadata::new(RedisCommandName::Zpopmax)
                    .write()
                    .with_arity(-2),
            ),
            (
                "zpopmin",
                CommandMetadata::new(RedisCommandName::Zpopmin)
                    .write()
                    .with_arity(-2),
            ),
            (
                "bzpopmax",
                CommandMetadata::new(RedisCommandName::Bzpopmax)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-2)
                    .with_step(1),
            ),
            (
                "bzpopmin",
                CommandMetadata::new(RedisCommandName::Bzpopmin)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-2)
                    .with_step(1),
            ),
            (
                "zrandmember",
                CommandMetadata::new(RedisCommandName::Zrandmember)
                    .read_only()
                    .with_arity(-2),
            ),
            (
                "zrange",
                CommandMetadata::new(RedisCommandName::Zrange)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrangestore",
                CommandMetadata::new(RedisCommandName::Zrangestore)
                    .write()
                    .with_arity(-5)
                    .with_first_key(1)
                    .with_last_key(2)
                    .with_step(1),
            ),
            (
                "zrank",
                CommandMetadata::new(RedisCommandName::Zrank)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zrem",
                CommandMetadata::new(RedisCommandName::Zrem)
                    .write()
                    .with_arity(-3),
            ),
            (
                "zremrangebylex",
                CommandMetadata::new(RedisCommandName::Zremrangebylex)
                    .write()
                    .with_arity(4),
            ),
            (
                "zremrangebyrank",
                CommandMetadata::new(RedisCommandName::Zremrangebyrank)
                    .write()
                    .with_arity(4),
            ),
            (
                "zremrangebyscore",
                CommandMetadata::new(RedisCommandName::Zremrangebyscore)
                    .write()
                    .with_arity(4),
            ),
            (
                "zrevrange",
                CommandMetadata::new(RedisCommandName::Zrevrange)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrank",
                CommandMetadata::new(RedisCommandName::Zrevrank)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zunion",
                CommandMetadata::new(RedisCommandName::Zunion)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "zunionstore",
                CommandMetadata::new(RedisCommandName::Zunionstore)
                    .write()
                    .with_arity(-4),
            ),
            (
                "zscore",
                CommandMetadata::new(RedisCommandName::Zscore)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "zscan",
                CommandMetadata::new(RedisCommandName::Zscan)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "flushall",
                CommandMetadata::new(RedisCommandName::FlushAll)
                    .write()
                    .with_arity(-1)
                    .no_transaction(),
            ),
            (
                "flushdb",
                CommandMetadata::new(RedisCommandName::FlushDb)
                    .write()
                    .with_arity(-1)
                    .no_transaction(),
            ),
        ]);

        let cmds: HashMap<&str, Arc<CommandMetadata>> = cmds
            .iter()
            .map(|(k, v)| ((*k), Arc::new(v.clone())))
            .collect();
        CommandsManager { cmds }
    }
}
