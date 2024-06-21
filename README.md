[![ubuntu-latest-build-and-test](https://github.com/sabledb-io/sabledb/actions/workflows/rust.yml/badge.svg)](https://github.com/sabledb-io/sabledb/actions/workflows/rust.yml)
[![macOS-latest](https://github.com/sabledb-io/sabledb/actions/workflows/macos-latest.yml/badge.svg)](https://github.com/sabledb-io/sabledb/actions/workflows/macos-latest.yml)



# What is `SableDb`?

<img src="/docs/images/sabledb.svg" width="200" height="200" align="right" />

`SableDb` is a key-value NoSQL database that utilizes [`RocksDb`][3] as its storage engine and is compatible with the Redis protocol.
It aims to reduce memory costs and increase capacity compared to Redis. `SableDb` features include Redis-compatible access via 
any Redis client, up to 64K databases support, asynchronous replication using transaction log tailing and TLS connectivity support.

Oh, and it's written in `Rust` :) 

## Building

`SableDb` is supported on all major OS: `Linux`, `macOS` and `Windows`

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


## Docker
```
docker build -t sabledb:latest .
docker run -p 6379:6379 sabledb:latest
```

### Docker compose
1. In your docker-compose.yaml: 
```yaml
version: '3.8'

services:
  sabledb:
    image: sabledb:latest
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "6379:6379"
    volumes:
      - sabledb_data:/var/lib/sabledb/data
      - ./server.ini:/etc/sabledb/conf/server.ini
    entrypoint: ["sabledb", "/etc/sabledb/conf/server.ini"]
    restart: unless-stopped

volumes:
  sabledb_data: {}
```

2. Update the configuration lines found in `server.ini` under section `[general]` with the following changes:
```
# The IP on which new connections are accepted
listen_ip = 0.0.0.0

# Log directory
logdir = "/var/lib/sabledb/logs/"

# Path to the storage directory
db_path = "/var/lib/sabledb/data/sabledb.db"
````

3. Run the program with docker-compose:
`docker compose up --build`


4. Tail logs
Specify the docker container name (i.e. *sabledb-sabledb-1*) which can be found with `docker ps`. In our example: 

`docker exec -it sabledb-sabledb-1 sh -c 'tail -f /var/lib/sabledb/logs/sabledb.log.*'`


## Supported features

- Persistent data using RocksDb - use `SableDb` as a persistent storage using `Redis`'s API
- TLS connections
- Replication using tailing of the transaction log
- Highly configurable, but comes with sensible default values
- Use the `sb` command line utility (`target/release/sb`) for performance testing
- Transactions ( `MULTI` / `EXEC` )

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
- `hset`

Run `sb --help` to get the full help message.

Below is a simple ( `ping` ) test conducted locally using WSL2 on Windows 10 (same machine is running both `SableDb` and `sb`...):

![sb progress demo](/docs/images/sb-demo.gif)

`set` test, on the same set-up (local machine, WSL2 on Windows 10):

![sb set progress demo](/docs/images/sb-demo-set.gif)

## Supported commands


**IMPORTANT**

`SableDb` is under constant development, if you are missing a command, feel free to open an issue
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


### Generic commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| del | ✓ |✓ |   |
| ttl | ✓ |✓ |   |
| exists | ✓ |✓ |   |
| expire | ✓ |✓ |   |

### Server management commands

| Command  | Supported  | Fully supported?  | Comment  |
|---|---|---|---|
| info | ✓ |✓ |  `SableDb` has its own INFO output format |
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

The increment command is unique because it uses a "read-modify-update" in order to ensure the atomicity of the action which in a 
multi-threaded environment causes a challenge

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
