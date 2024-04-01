# SableDb

A modern design to persistent `Redis`, written in `Rust`.
Under the hood `SableDb` uses a shared nothing architecture (well, almost nothing) using `Rust`'s [tokio][4]'s runtime and threads (ecah thread is running its own Runtime that does not share anything with other threads). All tasks spawned in a thread, are "local threads" which require no locking in order to exchange data via channels.

For the underlying storage, `SableDb` uses [`RocksDb`][3] binding for `Rust`. `RocksDb` is a good compromise between persistency and performance.

## Building

### Linux /macOS

```bash
git clone https://github.com/sabledb-io/sabledb.git
cd sabledb
cargo build --release
cargo test --release
```

### Windows

On Windows, we require `MSYS2` terminal for building `SableDb`.

```bash
git clone https://github.com/sabledb-io/sabledb.git
cd sabledb
CFLAGS="-D_ISOC11_SOURCE" cargo build --release
cargo test --release
```

## Running `SableDb`

```bash
./target/release/sabledb
```

Usage:

```bash
$target/release/sabledb [sabledb.ini]
```

## Supported features

- Persistent data using RocksDb - use `SableDb` as a persistent storage using `Redis`'s API
- TLS connections
- Replication using tailing of the transaction log
- Highly configurable, but comes with sensible default values
- Use the `sb` command line utility (`target/release/sb`) for performance testing

## Benchmark tool - `sb`

`SableDb` uses its own benchmarking tool named `sb` ("SableDb Benchmark"). `sb` supports the following commands:

- `set`
- `get`
- `lpush` 
- `lpop`
- `rpush` 
- `rpop`
- `incr`
- `ping`

Run `sb --help` to get the full help message.

![sb progress demo](/docs/images/sb-demo.gif)

## Supported commands

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
| lcs  | ✓  | 🗙  | Does not support: `IDX`, `MINMATCHLEN` and `WITHMATCHLEN`  |
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

### Generic commands
| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| del | ✓ |✓ |   |
| ttl | ✓ |✓ |   |

## Benchmarks

### Command `set`

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64   | 448K  | 1.127  | 1.279  | 1.423  |
| 128  | 420K  | 1.199  | 1.375  | 1.551  |
| 256  | 363K  | 1.375  | 1.567  | 1.799  |


### Command `get`

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64   | 950K  | 0.495  | 0.671  | 1.087  |
| 128  | 907K  | 0.511  | 0.695  | 1.111  |
| 256  | 905K  | 0.519  | 0.711  | 1.119  |


### Command `mset`

Note: Each `mset` command is equivalent of `10` `set` commands


| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| 64   | 116K  | 3.8  | 4.6  | 6.3  |
| 128  | 72K  | 6.375  | 7.039  | 54.111  |
| 256  | 41.5K  | 0.432  | 7.279  | 226.943  |


### Command `incr`

The increment command is unique because it uses a "read-modify-update" in order to ensure the atomicity of the action which in a multithreaded environment causes a challenge

| Payload size (bytes) | rps | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|
| N/A   | 443K  | 1.127  | 1.295  | 1.383  |

### Network only (`ping` command)


| Command | rps | pipeline | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| ping_inline | 1.05M  | 1 | 0.407  | 0.775  | 1.143  |
| ping_inline | 6.65M  | 20 | 0.855  | 1.551  | 1.815  |
| ping_mbulk | 1.05M  | 1 | 0.399  | 0.727  | 1.127  |
| ping_mbulk | 7.96M  | 20 | 0.807  | 1.471  | 1.711  |

---

Command executions can be seen [here][2]

[1]: https://github.com/redis/redis
[2]: https://github.com/sabledb-io/sabledb/blob/main/BENCHMARK.md
[3]: https://rocksdb.org/
[4]: https://tokio.rs/
