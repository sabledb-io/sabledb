# SableDB cluster

## About slots

Each key is converted using a function into a number between `0-16383` (inclusive). A node is responsible for range of
slots. In case a key is requested from a node that does not own the key's slot - a `MOVED` error is returned to
client with details on where to find the key.

## About shards

Each cluster consists of one or more shards. A shard is a group of `1` primary node + `N` replicas (where `N` **can** be `0`).
Ideally, in a cluster of `K` shards, each shard is managing `16384 / K` slots. In theory, a cluster can have
`16K` primaries which should be sufficient for any workload.

## Central database

`SableDB` uses a central database ("cluster database") which is essentially another instance of `SableDB` to perform
all communication between the shards and keep the cluster state. It is the responsibility of each node to register and update itself in the database
in a regular interval. Each node pulls from the central database the information about other nodes (e.g. list of slots
owned by each node, public node address etc)

## Slot migration

`SableDB` provide set of commands for transferring slot ownership from one shard to another.

For example, to send slot `1234` from `Node_1` -> `Node_2`, the following steps are required:

- Open `sabledb-cli` and connect to `Node_1` (the current slot owner)
- Run the command: `SLOT SENDTO <IP> <PORT> 1234` (IP & PORT and `Node_2` **private IP & port**)
- Once the transfer is completed, you will get `OK` response

Under the hood:

- `Node_1` locks the slots for read-only
- `Node_1` exports the slot to a file
- `Node_1` spans a thread that sends the file to `Node_2` and waits for confirmation (during this time, `Node_1` is accessible for read/write commands except for slot `1234` where it can only perform read commands)
- Once `Node_2` confirms, the slot is removed from the current node's map and the read-only lock is removed
- `Node_2` becomes the owner of slot `1234`
- `Node_2` updates its status in the cluster database so other nodes will know which node hosts slot `1234`
- `Node_1` updates its node status in the cluster database (it removes slot `1234` from its map)
