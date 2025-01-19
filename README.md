[![ubuntu-latest-build-and-test](https://github.com/sabledb-io/sabledb/actions/workflows/rust.yml/badge.svg)](https://github.com/sabledb-io/sabledb/actions/workflows/rust.yml)
[![macOS-latest](https://github.com/sabledb-io/sabledb/actions/workflows/macos-latest.yml/badge.svg)](https://github.com/sabledb-io/sabledb/actions/workflows/macos-latest.yml)



# What is `SableDB`?

<img src="/docs/images/sabledb.svg" width="200" height="200" align="right" />

`SableDB` is a key-value NoSQL database that utilizes [`RocksDB`][3] as its storage engine and is compatible with the Valkey protocol.
It aims to reduce memory costs and increase capacity compared to Valkey. `SableDB` features include Valkey-compatible access via
any Valkey client, up to 64K databases support, asynchronous replication using transaction log tailing and TLS connectivity support.

## Building

`SableDB` is supported on all major OS: `Linux`, `macOS` and `Windows`

### Linux /macOS

```bash
git clone https://github.com/sabledb-io/sabledb.git
cd sabledb
git submodule update --init
cargo build --release
cargo test --release
```

### Windows

On Windows, we require `MSYS2` terminal for building `SableDB`.

First, ensure that you have the required toolchain (beside Rust):

```bash
pacman -Sy git                                  \
           mingw-w64-clang-x86_64-toolchain     \
           mingw-w64-clang-x86_64-python3       \
           mingw-w64-clang-x86_64-cmake         \
           mingw-w64-clang-x86_64-libffi        \
           unzip                                \
           mingw-w64-clang-x86_64-rust-bindgen  \
           mingw-w64-clang-x86_64-nasm          \
           mingw-w64-clang-x86_64-gcc-compat
```

```bash
git clone https://github.com/sabledb-io/sabledb.git
cd sabledb
git submodule update --init
cargo build --release
cargo test --release
```

## Running `SableDB`

```bash
./target/release/sabledb
```

Usage:

```bash
$target/release/sabledb [sabledb.ini]
```


## Docker

```bash
docker build -t sabledb:latest .
docker run -p 6379:6379 sabledb:latest
```

### Docker compose

If you prefer to use `docker-compose`, you can use the following command:

```bash
docker compose up --build
```

### Tail logs

To tail the logs, use the below command:

```bash
docker exec -it sabledb-sabledb-1 /bin/bash -c "tail -f /var/lib/sabledb/log/sabledb.log.*"
```

**Note**: the container name (in the above example: `sabledb-sabledb-1`) can be found using the command `docker ps`

## Supported features

- Persistent data using RocksDB - use `SableDB` as a persistent storage using `Valkey`'s API
- TLS connections
- Replication using tailing of the transaction log
- Highly configurable, but comes with sensible default values
- Use the `sb` command line utility (`target/release/sb`) for performance testing
- Transactions ( `MULTI` / `EXEC` )
- Auto-failover & recovery

## Benchmark tool - `sb`

`SableDB` uses its own benchmarking tool named `sb`. `sb` supports the following commands:

- `set`
- `get`
- `lpush`
- `lpop`
- `rpush`
- `rpop`
- `incr`
- `ping`
- `hset`
- `setget` (a mix between SET:GET commands, the ratio is controlled by the `--setget-ratio` option. The default is set to `1:4`)

Run `sb --help` to get the full help message.

Below is a simple ( `ping` ) test conducted locally using WSL2 on Windows 10 (same machine is running both `SableDB` and `sb`...):

![sabledb-benchmark progress demo](/docs/images/sabledb-benchmark-demo.gif)

`set` test, on the same set-up (local machine, WSL2 on Windows 10):

![sabledb-benchmark set progress demo](/docs/images/sabledb-benchmark-demo-set.gif)

## Supported commands


**IMPORTANT**

`SableDB` is under constant development, if you are missing a command, feel free to open an issue
and visit this page again in couple of days

---

### String commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| append   | ✓  | ✓  |
| decr  | ✓  | ✓  |
| decrby  | ✓  | ✓  |
| get  | ✓  |  ✓ |
| getdel  | ✓  | ✓  |
| getex  | ✓  |  ✓ |
| getrange  | ✓  | ✓  |
| getset  | ✓  | ✓  |
| incr  | ✓  | ✓  |
| incrby  | ✓  | ✓  |
| incrbyfloat  | ✓  | ✓  |
| lcs  | ✓  | x  | Does not support: `IDX`, `MINMATCHLEN` and `WITHMATCHLEN`  |
| mget  | ✓  | ✓  |
| mset  | ✓  | ✓  |
| msetnx  | ✓  | ✓  |
| psetex  | ✓  | ✓  |
| set  | ✓  | ✓  |
| setex  | ✓  | ✓  |
| setnx  | ✓  | ✓  |
| setrange  | ✓  | ✓  |
| strlen  | ✓  | ✓  |
| substr  | ✓  | ✓  |


### List commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| blmove   | ✓ |✓ |   |
| blmpop   | ✓ |✓|   |
| blpop   | ✓  | ✓  |   |
| brpop   | ✓  | ✓  |   |
| brpoplpush   | ✓   | ✓   |   |
| lindex   | ✓   | ✓   |   |
| linsert   |  ✓  | ✓ |   |
| llen   | ✓ | ✓|   |
| lmove   | ✓  | ✓  |   |
| lmpop   | ✓  | ✓  |   |
| lpop   | ✓   | ✓   |   |
| lpos   |  ✓  | ✓  |   |
| lpush   | ✓   | ✓   |   |
| lpushx   | ✓  | ✓   |   |
| lrange   | ✓  | ✓   |   |
| lrem   |  ✓ |  ✓ |   |
| lset   | ✓   | ✓   |   |
| ltrim   | ✓  | ✓   |   |
| rpop   | ✓   | ✓   |   |
| rpoplpush   | ✓   | ✓   |   |
| rpush   | ✓   | ✓  |   |
| rpushx   | ✓   | ✓   |   |

### Hash commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| hset | ✓ |✓ |   |
| hget | ✓ |✓ |   |
| hmget | ✓ |✓ |   |
| hmset | ✓ |✓ |   |
| hgetall | ✓ |✓ |   |
| hdel | ✓ |✓ |   |
| hlen | ✓ |✓ |   |
| hexists | ✓ |✓ |   |
| hincrby | ✓ |✓ |   |
| hincrbyfloat | ✓ |✓ |   |
| hkeys | ✓ |✓ |   |
| hvals | ✓ |✓ |   |
| hrandfield | ✓ |✓ |   |
| hscan | ✓ |✓ |   |
| hsetnx | ✓ |✓ |   |
| hstrlen | ✓ |✓ |   |

### Sorted Set (`ZSET`) commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| bzmpop | ✓ |✓ | |
| bzpopmax | ✓ |✓ | |
| bzpopmin | ✓ |✓ | |
| zadd | ✓ |✓ | |
| zcard | ✓ |✓ | |
| zincrby | ✓ |✓ | |
| zcount | ✓ |✓ | |
| zdiff | ✓ |✓ | |
| zdiffstore | ✓ |✓ | |
| zinter | ✓ |✓ | |
| zintercard | ✓ |✓ | |
| zinterstore | ✓ |✓ | |
| zlexcount | ✓ |✓ | |
| zmpop | ✓ |✓ | |
| zmscore | ✓ |✓ | |
| zpopmax | ✓ |✓ | |
| zpopmin | ✓ |✓ | |
| zrandmember | ✓ |✓ | |
| zrangebyscore | ✓ |✓ | |
| zrevrangebyscore | ✓ |✓ | |
| zrangebylex | ✓ |✓ | |
| zrevrangebylex | ✓ |✓ | |
| zrange | ✓ |✓ | |
| zrangestore | ✓ |✓ | |
| zrank | ✓ |✓ | |
| zrem | ✓ |✓ | |
| zremrangebylex | ✓ |✓ | |
| zremrangebyrank | ✓ |✓ | |
| zremrangebyscore | ✓ |✓ | |
| zrevrange | ✓ |✓ | |
| zrevrank | ✓ |✓ | |
| zunion | ✓ |✓ | |
| zunionstore | ✓ |✓ | |
| zscore | ✓ |✓ | |
| zscan | ✓ |✓ | |


### Set commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| sadd | ✓ |✓ | |
| scard | ✓ |✓ | |
| sdiff | ✓ |✓ | |
| sdiffstore | ✓ |✓ | |
| sinter | ✓ |✓ | |
| sintercard | ✓ |✓ | |
| sinterstore | ✓ |✓ | |
| sismember | ✓ |✓ | |
| smismember | ✓ |✓ | |
| smembers | ✓ |✓ | |
| smove | ✓ |✓ | |
| spop | ✓ |✓ | |
| srandmember | ✓ |✓ | |
| srem | ✓ |✓ | |
| sscan | ✓ |✓ | |
| sunion | ✓ |✓ | |
| sunionstore | ✓ |✓ | |

### Generic commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| del | ✓ |✓ |   |
| ttl | ✓ |✓ |   |
| exists | ✓ |✓ |   |
| expire | ✓ |✓ |   |
| keys | ✓ |x | Pattern uses wildcard match ( `?` and `*` ) |
| scan | ✓ |x | Pattern uses wildcard match ( `?` and `*` ) |

### Server management commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| info | ✓ |✓ |  `SableDB` has its own INFO output format |
| ping | ✓ |✓ |   |
| replicaof | ✓ |✓ |   |
| slaveof | ✓ |✓ |   |
| command | ✓ |✓ |   |
| command docs | ✓ | x |   |
| flushall | ✓ | ✓ |   |
| flushdb | ✓ | ✓ |   |
| dbsize | ✓ | ✓ | Data is accurate for the last scan performed on the storage |

### Transaction

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| multi | ✓ |✓ |   |
| exec | ✓ |✓ |   |
| discard | ✓ |✓ |   |
| watch | ✓ |✓ |   |
| unwatch | ✓ |✓ |   |

### Connection management commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| client id | ✓ |✓ |   |
| client kill | ✓ |x |  supports: `client kill ID <client-id>` |
| select | ✓ |✓ |   |
| ping | ✓ |✓ |   |


### Locking commands

`SableDB` offers locking capabilities for application that requires it. Locks in `SableDB` are ephemeral data commands,
this means, that the data is not persistent and is not replicated from primary to replica. These commands are marked as
`readonly` so they can be used with replica servers.

Note about the locks:

- Locks are non recursive - if a client attempts to lock an already lock that it owns, it will get the `DEADLOCK` error
- Lock names are using their own namespace. This is means that you can have a string (or any other type) with name "my-lock" and a lock with the same name
- The LOCK command can be a blocking command if timeout is provided
- If a client terminates while holding a lock, the lock is released automatically by `SableDB`

The syntax is:

```
LOCK <LOCK-NAME> [TIMEOUT-MS]
UNLOCK <LOCK-NAME>
```


| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| LOCK | ✓ |✓ | If timeout is provided, this is a blocking command |
| UNLOCK | ✓ |✓ | |

## Benchmarks

### Benchmark machine

```
Processor:      AMD Ryzen 9 7950X 16-Core Processor 4.50 GHz
Installed RAM:	64.0 GB (63.2 GB usable)
System type:    64-bit operating system, x64-based processor
Disk:           Crucial T700, 2TB PCIe Gen5 NVMe M.2 SSD
```

### Benchmark setup

- Used 5M unique keys with varying payload size
- 512 clients on multiple threads
- `get` tests used randomg keys in the range of `0000001` - `5000000` (key size = 7Bytes)
- Before each test, the database was removed complete
- Before each `get` & `setget` test:
    - Delete the database
    - Fill the database with 5M unique keys with the test payload size (64/128/256)
    - Run the use case

### Command `set`

Command used:

```bash
sb --threads 6 -c 512 -t set -n 5000000 -r 5000000 -d <64|128|256>
```

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64   | 781K  | 0.559ms  | 0.751ms  |  1.919ms  |
| 128  | 715K  | 0.595ms  | 0.919ms  | 2.015ms  |
| 256  | 656K  | 0.583ms  | 1.359ms  | 2.007ms  |

### Command `get`

Command used:

```bash
sb --threads 6 -c 512 -t set -n 5000000 -r 5000000 -d <64|128|256> -z
```

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64  | 1.04M  | 0.457ms  | 0.607ms  | 0.783ms  |
| 128   | 1.03M  | 0.469ms  | 0.607ms  | 0.735ms  |
| 256  | 931K  | 0.483ms  | 0.635ms  | 0.795ms  |


### Mixed load `setget` with `1:4` ratio (1 `SET` for every `4` `GET` calls)

Command used:

```bash
sb --threads 6 -c 512 -t setget -n 5000000 -r 5000000 -d <64|128|256> -z
```

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64  | 923K  | 0.483ms  | 0.635ms  | 1.239ms  |
| 128   | 911K  | 0.481ms  | 0.639ms  | 1.495ms  |
| 256  | 787K  | 0.563ms  | 0.755ms  | 1.727ms  |


### Network only (`ping` command)

| Command | rps | pipeline | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| ping | 1.6M  | 1 | 0.407  | 0.775  | 1.143  |

---

[1]: https://github.com/valkey-io/valkey
[3]: https://rocksdb.org/
[4]: https://tokio.rs/
