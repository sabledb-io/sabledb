# Benchmarks

---


- Server machine: `AWS`'s `c6gd.4xlarge` ( `16 vCPU + 32 GB of memory` )
- Client machine: `AWS`'s `c6gn.4xlarge` ( `16 vCPU + 32 GB of memory` )
- Both clients are placed in the same `AWS` region and `AZ`
- Benchmarking tool `redis-benchmark` compiled from sources (in release mode)


## `set` command. Payload size: `64`, `128` and `256`

  ```bash
  ./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t set -d 64

  throughput summary: 448792.75 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.125     0.328     1.127     1.279     1.423     9.511


./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t set -d 128

  Summary:
  throughput summary: 420433.06 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.207     0.312     1.199     1.375     1.551    10.191

./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t set -d 256

Summary:
  throughput summary: 363543.81 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.389     0.728     1.375     1.567     1.799     9.759
```

## `get` command. Payload size: `64`, `128` and `256`

- Stop the server
- Clear the database folder
- Fill the database with keys with the requested size

```bash
## database values are all of size 64
./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t get

Summary:
  throughput summary: 950751.12 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.508     0.192     0.495     0.671     1.087     2.647

## database values are all of size 128
./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t get

Summary:
  throughput summary: 907441.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.524     0.192     0.511     0.695     1.111     2.967

## database values are all of size 256
./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t get

Summary:
  throughput summary: 907605.75 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.532     0.192     0.519     0.711     1.119     2.847

```

## `mset` command. Payload size `64`, `128` and `256`

- Each `mset` command is equivalent of `10` `set` commands

```bash
./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t mset -d 64

Summary:
  throughput summary: 116535.18 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        4.373     1.136     3.871     4.623     6.367    99.519

./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t mset -d 128

Summary:
  throughput summary: 71874.19 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        7.108     1.616     6.375     7.039    54.111    94.271

./redis-benchmark --threads 16 -c 512 -n 10000000  -r 100000 -h 172.31.13.191 -t mset -d 128

Summary:
  throughput summary: 41552.40 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
       12.226     0.432     7.279     7.935   226.943   267.263

```

## `incr` command

The increment command is unique because it uses a "read-modify-update" in order to ensure the atomicity of the action
which in a multithreaded environment causes a challenge

```bash
./redis-benchmark --threads 16 -c 512 -n 1000000  -r 100000 -h 172.31.13.191 -t incr

Summary:
  throughput summary: 443655.72 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.126     0.544     1.127     1.295     1.383     6.071
```

### `ping_inline` command:

---

```
Summary:
  throughput summary: 1050640.88 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.450     0.072     0.407     0.775     1.143     3.455
```

With pipeline set to `20`:

```
Summary:
  throughput summary: 6653360.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.880     0.136     0.855     1.551     1.815     3.135
```

### `ping_mbulk` command:

---

```
Summary:
  throughput summary: 1050640.88 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.434     0.072     0.399     0.727     1.127     3.335
```

With pipeline set to `20`:

```
Summary:
  throughput summary: 7968127.50 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.848     0.144     0.807     1.471     1.711     2.887
```

