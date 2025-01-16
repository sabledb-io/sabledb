use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use strum_macros::EnumString;

#[derive(Default, Debug, Clone, EnumString)]
pub enum ValkeyCommandFlags {
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
    /// This command operates on multiple keys
    #[strum(serialize = "multikey")]
    MultiKey = 1 << 6,
}

#[derive(Clone, Debug, Default, EnumString, PartialEq, Eq)]
pub enum ValkeyCommandName {
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
    DbSize,
    // Generic commands
    Ttl,
    Del,
    Exists,
    Expire,
    Keys,
    Scan,
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
    // Set commands
    Sadd,
    Scard,
    Sdiff,
    Sdiffstore,
    Sinter,
    Sintercard,
    Sinterstore,
    Sismember,
    Smismember,
    Smembers,
    Smove,
    Spop,
    Srandmember,
    Srem,
    Sscan,
    Sunion,
    Sunionstore,
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
            None => Arc::new(CommandMetadata::new(ValkeyCommandName::NotSupported(
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
    cmd_name: ValkeyCommandName,
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
    pub fn new(cmd_name: ValkeyCommandName) -> Self {
        CommandMetadata {
            cmd_name,
            cmd_flags: ValkeyCommandFlags::None as u64,
            arity: 2,
            first_key: 1,
            last_key: 1,
            step: 1,
        }
    }

    /// Arity is the number of arguments a command expects.
    ///
    /// It follows a simple pattern:
    /// - A positive integer means a fixed number of arguments.
    /// - A negative integer means a minimal number of arguments.
    ///
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

    /// The position of the command's last key name argument. Valkey commands usually accept one, two or multiple
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
        self.set_flag(ValkeyCommandFlags::Blocking);
        self
    }

    /// This command is not allowed in transaction
    pub fn no_transaction(mut self) -> Self {
        self.set_flag(ValkeyCommandFlags::NoTxn);
        self
    }

    /// This command operates on multiple keys
    pub fn multi_key(mut self) -> Self {
        self.set_flag(ValkeyCommandFlags::MultiKey);
        self
    }

    /// This command is a "write" command
    pub fn write(mut self) -> Self {
        self.set_flag(ValkeyCommandFlags::Write);
        self
    }

    /// This command performs "read" on the database
    pub fn read_only(mut self) -> Self {
        self.set_flag(ValkeyCommandFlags::Read);
        self
    }

    /// An administrator command
    pub fn admin(mut self) -> Self {
        self.set_flag(ValkeyCommandFlags::Admin);
        self
    }

    /// This command falls under the @connection category
    pub fn connection(mut self) -> Self {
        self.cmd_flags |= ValkeyCommandFlags::Connection as u64;
        self
    }

    pub fn name(&self) -> &ValkeyCommandName {
        &self.cmd_name
    }

    /// Is this command a "Write" command?
    pub fn is_write_command(&self) -> bool {
        self.has_flag(ValkeyCommandFlags::Write)
    }

    /// Is this command a "Write" command?
    pub fn is_read_only_command(&self) -> bool {
        self.has_flag(ValkeyCommandFlags::Read)
    }

    pub fn is_blocking(&self) -> bool {
        self.has_flag(ValkeyCommandFlags::Blocking)
    }

    pub fn is_notxn(&self) -> bool {
        self.has_flag(ValkeyCommandFlags::NoTxn)
    }

    pub fn is_multi(&self) -> bool {
        self.has_flag(ValkeyCommandFlags::MultiKey)
    }

    pub fn to_resp_v2(&self) -> BytesMut {
        let builder = crate::RespBuilderV2::default();
        let mut buffer = BytesMut::with_capacity(64);

        let mut flags = Vec::<&str>::new();
        if self.has_flag(ValkeyCommandFlags::Read) {
            flags.push("readonly");
        }
        if self.has_flag(ValkeyCommandFlags::Write) {
            flags.push("write");
        }
        if self.has_flag(ValkeyCommandFlags::Blocking) {
            flags.push("blocking");
        }
        if self.has_flag(ValkeyCommandFlags::Admin) {
            flags.push("admin");
        }
        if self.has_flag(ValkeyCommandFlags::Connection) {
            flags.push("connection");
        }

        if self.has_flag(ValkeyCommandFlags::NoTxn) {
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

    fn set_flag(&mut self, flag: ValkeyCommandFlags) {
        self.cmd_flags |= flag as u64
    }

    fn has_flag(&self, flag: ValkeyCommandFlags) -> bool {
        let res = self.cmd_flags & flag.clone() as u64;
        res == flag as u64
    }
}

impl Default for CommandsManager {
    fn default() -> Self {
        let cmds: HashMap<&str, CommandMetadata> = HashMap::from([
            (
                "config",
                CommandMetadata::new(ValkeyCommandName::Config)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "info",
                CommandMetadata::new(ValkeyCommandName::Info)
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
                CommandMetadata::new(ValkeyCommandName::Append)
                    .write()
                    .with_arity(3),
            ),
            (
                "decr",
                CommandMetadata::new(ValkeyCommandName::Decr)
                    .write()
                    .with_arity(2),
            ),
            (
                "decrby",
                CommandMetadata::new(ValkeyCommandName::DecrBy)
                    .write()
                    .with_arity(3),
            ),
            (
                "incr",
                CommandMetadata::new(ValkeyCommandName::Incr)
                    .write()
                    .with_arity(2),
            ),
            (
                "incrby",
                CommandMetadata::new(ValkeyCommandName::IncrBy)
                    .write()
                    .with_arity(3),
            ),
            (
                "incrbyfloat",
                CommandMetadata::new(ValkeyCommandName::IncrByFloat)
                    .write()
                    .with_arity(3),
            ),
            (
                "set",
                CommandMetadata::new(ValkeyCommandName::Set)
                    .write()
                    .with_arity(3),
            ),
            (
                "get",
                CommandMetadata::new(ValkeyCommandName::Get)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "getdel",
                CommandMetadata::new(ValkeyCommandName::GetDel)
                    .write()
                    .with_arity(2),
            ),
            (
                "getset",
                CommandMetadata::new(ValkeyCommandName::GetSet)
                    .write()
                    .with_arity(3),
            ),
            (
                "getex",
                CommandMetadata::new(ValkeyCommandName::GetEx)
                    .write()
                    .with_arity(-2),
            ),
            (
                "getrange",
                CommandMetadata::new(ValkeyCommandName::GetRange)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "lcs",
                CommandMetadata::new(ValkeyCommandName::Lcs)
                    .read_only()
                    .with_arity(-3)
                    .with_last_key(2),
            ),
            (
                "mget",
                CommandMetadata::new(ValkeyCommandName::Mget)
                    .read_only()
                    .with_arity(-2)
                    .with_last_key(-1)
                    .multi_key(),
            ),
            (
                "mset",
                CommandMetadata::new(ValkeyCommandName::Mset)
                    .write()
                    .with_arity(-3)
                    .with_last_key(-1)
                    .with_step(2)
                    .multi_key(),
            ),
            (
                "msetnx",
                CommandMetadata::new(ValkeyCommandName::Msetnx)
                    .write()
                    .with_arity(-3)
                    .with_last_key(-1)
                    .with_step(2)
                    .multi_key(),
            ),
            (
                "psetex",
                CommandMetadata::new(ValkeyCommandName::Psetex)
                    .write()
                    .with_arity(4),
            ),
            (
                "setex",
                CommandMetadata::new(ValkeyCommandName::Setex)
                    .write()
                    .with_arity(4),
            ),
            (
                "setnx",
                CommandMetadata::new(ValkeyCommandName::Setnx)
                    .write()
                    .with_arity(3),
            ),
            (
                "setrange",
                CommandMetadata::new(ValkeyCommandName::SetRange)
                    .write()
                    .with_arity(4),
            ),
            (
                "strlen",
                CommandMetadata::new(ValkeyCommandName::Strlen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "substr",
                CommandMetadata::new(ValkeyCommandName::Substr)
                    .read_only()
                    .with_arity(4),
            ),
            // list commands
            (
                "lpush",
                CommandMetadata::new(ValkeyCommandName::Lpush)
                    .write()
                    .with_arity(-3),
            ),
            (
                "lpushx",
                CommandMetadata::new(ValkeyCommandName::Lpushx)
                    .write()
                    .with_arity(-3),
            ),
            (
                "rpush",
                CommandMetadata::new(ValkeyCommandName::Rpush)
                    .write()
                    .with_arity(-3),
            ),
            (
                "rpushx",
                CommandMetadata::new(ValkeyCommandName::Rpushx)
                    .write()
                    .with_arity(-3),
            ),
            (
                "lpop",
                CommandMetadata::new(ValkeyCommandName::Lpop)
                    .write()
                    .with_arity(-2),
            ),
            (
                "rpop",
                CommandMetadata::new(ValkeyCommandName::Rpop)
                    .write()
                    .with_arity(-2),
            ),
            (
                "llen",
                CommandMetadata::new(ValkeyCommandName::Llen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "lindex",
                CommandMetadata::new(ValkeyCommandName::Lindex)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "linsert",
                CommandMetadata::new(ValkeyCommandName::Linsert)
                    .write()
                    .with_arity(5),
            ),
            (
                "lset",
                CommandMetadata::new(ValkeyCommandName::Lset)
                    .write()
                    .with_arity(4),
            ),
            (
                "lpos",
                CommandMetadata::new(ValkeyCommandName::Lpos)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "ltrim",
                CommandMetadata::new(ValkeyCommandName::Ltrim)
                    .write()
                    .with_arity(4),
            ),
            (
                "lrange",
                CommandMetadata::new(ValkeyCommandName::Lrange)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "lrem",
                CommandMetadata::new(ValkeyCommandName::Lrem)
                    .write()
                    .with_arity(4),
            ),
            (
                "lmove",
                CommandMetadata::new(ValkeyCommandName::Lmove)
                    .write()
                    .with_arity(5)
                    .with_last_key(2)
                    .multi_key(),
            ),
            (
                "rpoplpush",
                CommandMetadata::new(ValkeyCommandName::Rpoplpush)
                    .write()
                    .with_arity(3)
                    .with_last_key(2),
            ),
            (
                "lmpop",
                CommandMetadata::new(ValkeyCommandName::Lmpop)
                    .write()
                    .with_arity(-4)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "brpoplpush",
                CommandMetadata::new(ValkeyCommandName::Brpoplpush)
                    .write()
                    .blocking()
                    .with_arity(4)
                    .with_last_key(2)
                    .multi_key(),
            ),
            (
                "blpop",
                CommandMetadata::new(ValkeyCommandName::Blpop)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_last_key(-2)
                    .multi_key(),
            ),
            (
                "blmove",
                CommandMetadata::new(ValkeyCommandName::Blmove)
                    .write()
                    .blocking()
                    .with_arity(6)
                    .with_last_key(2)
                    .multi_key(),
            ),
            (
                "blmpop",
                CommandMetadata::new(ValkeyCommandName::Blmpop)
                    .write()
                    .blocking()
                    .with_arity(-5)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "brpop",
                CommandMetadata::new(ValkeyCommandName::Brpop)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_last_key(-2)
                    .multi_key(),
            ),
            // Client commands
            (
                "client",
                CommandMetadata::new(ValkeyCommandName::Client)
                    .connection()
                    .no_transaction(),
            ),
            (
                "select",
                CommandMetadata::new(ValkeyCommandName::Select)
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
                CommandMetadata::new(ValkeyCommandName::ReplicaOf)
                    .admin()
                    .with_arity(3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "slaveof",
                CommandMetadata::new(ValkeyCommandName::SlaveOf)
                    .admin()
                    .with_arity(3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "ping",
                CommandMetadata::new(ValkeyCommandName::Ping)
                    .read_only()
                    .with_arity(-1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "command",
                CommandMetadata::new(ValkeyCommandName::Command)
                    .with_arity(-1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            // generic commands
            (
                "ttl",
                CommandMetadata::new(ValkeyCommandName::Ttl)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "del",
                CommandMetadata::new(ValkeyCommandName::Del)
                    .write()
                    .with_arity(-2)
                    .with_last_key(-1)
                    .multi_key(),
            ),
            (
                "exists",
                CommandMetadata::new(ValkeyCommandName::Exists)
                    .read_only()
                    .with_arity(-2)
                    .with_last_key(-1)
                    .multi_key(),
            ),
            (
                "expire",
                CommandMetadata::new(ValkeyCommandName::Expire)
                    .write()
                    .with_arity(-3),
            ),
            (
                "keys",
                CommandMetadata::new(ValkeyCommandName::Keys)
                    .read_only()
                    .with_arity(2)
                    .with_last_key(0)
                    .with_first_key(0)
                    .with_step(0),
            ),
            // Hash commands
            (
                "hset",
                CommandMetadata::new(ValkeyCommandName::Hset)
                    .write()
                    .with_arity(-4),
            ),
            (
                "hmset",
                CommandMetadata::new(ValkeyCommandName::Hmset)
                    .write()
                    .with_arity(-4),
            ),
            (
                "hget",
                CommandMetadata::new(ValkeyCommandName::Hget)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "hdel",
                CommandMetadata::new(ValkeyCommandName::Hdel)
                    .write()
                    .with_arity(-3),
            ),
            (
                "hlen",
                CommandMetadata::new(ValkeyCommandName::Hlen)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hexists",
                CommandMetadata::new(ValkeyCommandName::Hexists)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "hgetall",
                CommandMetadata::new(ValkeyCommandName::Hgetall)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hincrbyfloat",
                CommandMetadata::new(ValkeyCommandName::Hincrbyfloat)
                    .write()
                    .with_arity(4),
            ),
            (
                "hincrby",
                CommandMetadata::new(ValkeyCommandName::Hincrby)
                    .write()
                    .with_arity(4),
            ),
            (
                "hkeys",
                CommandMetadata::new(ValkeyCommandName::Hkeys)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hvals",
                CommandMetadata::new(ValkeyCommandName::Hvals)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "hmget",
                CommandMetadata::new(ValkeyCommandName::Hmget)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "hrandfield",
                CommandMetadata::new(ValkeyCommandName::Hrandfield)
                    .read_only()
                    .with_arity(-2),
            ),
            (
                "hscan",
                CommandMetadata::new(ValkeyCommandName::Hscan)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "hsetnx",
                CommandMetadata::new(ValkeyCommandName::Hsetnx)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "hstrlen",
                CommandMetadata::new(ValkeyCommandName::Hstrlen)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "multi",
                CommandMetadata::new(ValkeyCommandName::Multi)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .no_transaction(),
            ),
            (
                "exec",
                CommandMetadata::new(ValkeyCommandName::Exec)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "discard",
                CommandMetadata::new(ValkeyCommandName::Discard)
                    .read_only()
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
            (
                "watch",
                CommandMetadata::new(ValkeyCommandName::Watch)
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .no_transaction()
                    .multi_key(),
            ),
            (
                "unwatch",
                CommandMetadata::new(ValkeyCommandName::Unwatch)
                    .with_arity(1)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_first_key(0)
                    .no_transaction(),
            ),
            (
                "zadd",
                CommandMetadata::new(ValkeyCommandName::Zadd)
                    .write()
                    .with_arity(-4),
            ),
            (
                "zcard",
                CommandMetadata::new(ValkeyCommandName::Zcard)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "zincrby",
                CommandMetadata::new(ValkeyCommandName::Zincrby)
                    .write()
                    .with_arity(4),
            ),
            (
                "zrangebyscore",
                CommandMetadata::new(ValkeyCommandName::Zrangebyscore)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrangebyscore",
                CommandMetadata::new(ValkeyCommandName::Zrevrangebyscore)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrangebylex",
                CommandMetadata::new(ValkeyCommandName::Zrangebylex)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrangebylex",
                CommandMetadata::new(ValkeyCommandName::Zrevrangebylex)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zcount",
                CommandMetadata::new(ValkeyCommandName::Zcount)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "zdiff",
                CommandMetadata::new(ValkeyCommandName::Zdiff)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "zdiffstore",
                CommandMetadata::new(ValkeyCommandName::Zdiffstore)
                    .write()
                    .with_arity(-4)
                    .with_first_key(1)
                    .with_last_key(1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "zinter",
                CommandMetadata::new(ValkeyCommandName::Zinter)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "zintercard",
                CommandMetadata::new(ValkeyCommandName::Zintercard)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "zinterstore",
                CommandMetadata::new(ValkeyCommandName::Zinterstore)
                    .write()
                    .with_arity(-4)
                    .multi_key(),
            ),
            (
                "zlexcount",
                CommandMetadata::new(ValkeyCommandName::Zlexcount)
                    .read_only()
                    .with_arity(4),
            ),
            (
                "zmpop",
                CommandMetadata::new(ValkeyCommandName::Zmpop)
                    .write()
                    .with_arity(-4)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "bzmpop",
                CommandMetadata::new(ValkeyCommandName::Bzmpop)
                    .write()
                    .blocking()
                    .with_arity(-5)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "zmscore",
                CommandMetadata::new(ValkeyCommandName::Zmscore)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zpopmax",
                CommandMetadata::new(ValkeyCommandName::Zpopmax)
                    .write()
                    .with_arity(-2),
            ),
            (
                "zpopmin",
                CommandMetadata::new(ValkeyCommandName::Zpopmin)
                    .write()
                    .with_arity(-2),
            ),
            (
                "bzpopmax",
                CommandMetadata::new(ValkeyCommandName::Bzpopmax)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-2)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "bzpopmin",
                CommandMetadata::new(ValkeyCommandName::Bzpopmin)
                    .write()
                    .blocking()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-2)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "zrandmember",
                CommandMetadata::new(ValkeyCommandName::Zrandmember)
                    .read_only()
                    .with_arity(-2),
            ),
            (
                "zrange",
                CommandMetadata::new(ValkeyCommandName::Zrange)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrangestore",
                CommandMetadata::new(ValkeyCommandName::Zrangestore)
                    .write()
                    .with_arity(-5)
                    .with_first_key(1)
                    .with_last_key(2)
                    .with_step(1),
            ),
            (
                "zrank",
                CommandMetadata::new(ValkeyCommandName::Zrank)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zrem",
                CommandMetadata::new(ValkeyCommandName::Zrem)
                    .write()
                    .with_arity(-3),
            ),
            (
                "zremrangebylex",
                CommandMetadata::new(ValkeyCommandName::Zremrangebylex)
                    .write()
                    .with_arity(4),
            ),
            (
                "zremrangebyrank",
                CommandMetadata::new(ValkeyCommandName::Zremrangebyrank)
                    .write()
                    .with_arity(4),
            ),
            (
                "zremrangebyscore",
                CommandMetadata::new(ValkeyCommandName::Zremrangebyscore)
                    .write()
                    .with_arity(4),
            ),
            (
                "zrevrange",
                CommandMetadata::new(ValkeyCommandName::Zrevrange)
                    .read_only()
                    .with_arity(-4),
            ),
            (
                "zrevrank",
                CommandMetadata::new(ValkeyCommandName::Zrevrank)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "zunion",
                CommandMetadata::new(ValkeyCommandName::Zunion)
                    .read_only()
                    .with_arity(-3)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0)
                    .multi_key(),
            ),
            (
                "zunionstore",
                CommandMetadata::new(ValkeyCommandName::Zunionstore)
                    .write()
                    .with_arity(-4)
                    .multi_key(),
            ),
            (
                "zscore",
                CommandMetadata::new(ValkeyCommandName::Zscore)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "zscan",
                CommandMetadata::new(ValkeyCommandName::Zscan)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "flushall",
                CommandMetadata::new(ValkeyCommandName::FlushAll)
                    .write()
                    .with_arity(-1)
                    .no_transaction(),
            ),
            (
                "flushdb",
                CommandMetadata::new(ValkeyCommandName::FlushDb)
                    .write()
                    .with_arity(-1)
                    .no_transaction(),
            ),
            (
                "dbsize",
                CommandMetadata::new(ValkeyCommandName::DbSize)
                    .read_only()
                    .with_arity(1),
            ),
            (
                "sadd",
                CommandMetadata::new(ValkeyCommandName::Sadd)
                    .write()
                    .with_arity(-3),
            ),
            (
                "scard",
                CommandMetadata::new(ValkeyCommandName::Scard)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "sdiff",
                CommandMetadata::new(ValkeyCommandName::Sdiff)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "sdiffstore",
                CommandMetadata::new(ValkeyCommandName::Sdiffstore)
                    .write()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "sinter",
                CommandMetadata::new(ValkeyCommandName::Sinter)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "sintercard",
                CommandMetadata::new(ValkeyCommandName::Sintercard)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "sinterstore",
                CommandMetadata::new(ValkeyCommandName::Sinterstore)
                    .write()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1),
            ),
            (
                "sismember",
                CommandMetadata::new(ValkeyCommandName::Sismember)
                    .read_only()
                    .with_arity(3),
            ),
            (
                "smismember",
                CommandMetadata::new(ValkeyCommandName::Smismember)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "smembers",
                CommandMetadata::new(ValkeyCommandName::Smembers)
                    .read_only()
                    .with_arity(2),
            ),
            (
                "smove",
                CommandMetadata::new(ValkeyCommandName::Smove)
                    .write()
                    .with_arity(4)
                    .with_first_key(1)
                    .with_last_key(2)
                    .with_step(1),
            ),
            (
                "spop",
                CommandMetadata::new(ValkeyCommandName::Spop)
                    .write()
                    .with_arity(-2),
            ),
            (
                "srandmember",
                CommandMetadata::new(ValkeyCommandName::Srandmember)
                    .read_only()
                    .with_arity(-2),
            ),
            (
                "srem",
                CommandMetadata::new(ValkeyCommandName::Srem)
                    .write()
                    .with_arity(-3),
            ),
            (
                "sscan",
                CommandMetadata::new(ValkeyCommandName::Sscan)
                    .read_only()
                    .with_arity(-3),
            ),
            (
                "sunion",
                CommandMetadata::new(ValkeyCommandName::Sunion)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "sunionstore",
                CommandMetadata::new(ValkeyCommandName::Sunionstore)
                    .write()
                    .with_arity(-3)
                    .with_first_key(1)
                    .with_last_key(-1)
                    .with_step(1)
                    .multi_key(),
            ),
            (
                "scan",
                CommandMetadata::new(ValkeyCommandName::Scan)
                    .read_only()
                    .with_arity(-2)
                    .with_first_key(0)
                    .with_last_key(0)
                    .with_step(0),
            ),
        ]);

        let cmds: HashMap<&str, Arc<CommandMetadata>> = cmds
            .iter()
            .map(|(k, v)| ((*k), Arc::new(v.clone())))
            .collect();
        CommandsManager { cmds }
    }
}
