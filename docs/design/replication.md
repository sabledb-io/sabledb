## Overview

`SableDb` supports a `1` : `N` replication (single primary -> multiple replicas) configuration.

### Replication Client / Server model

On startup, `SableDb` spawns a thread (internally called `Relicator`) which is listening on the main port + `1000`.
So if, for example, the server is configured to listen on port `6379`, the replication port is set to `7379`

For every new incoming replication client, a new thread is spawned to server it.

The replication is done using the following methodology:

1. The replica is requesting from the primary a set of changes starting from a given ID (initially, it starts with `0`)
2. If the primary is able to locate the requested change ID, it builds a "change request message" and sends it over to the client (the ID accepted from the client is the starting ID for the change request)
3. Steps 1-2 are repeated indefinitely
4. If the primary is unable to locate the requested change ID, it replies with a "Negative ack" to the client
5. The client sends a "Full Sync" request to the server
6. The primary creates a checkpoint from the database and sends it over to the replica + it sends the last change committed to the checkpoint (this will be the "Next ID" to fetch)
7. From hereon, steps 1-2 are repeated

!!!Note
    Its worth mentioning that the primary server is **stateless** i.e. it does not keep track of its replicas. It is up to the 
    replica server to pull data from the primary and to keep track of the next change sequence ID to pull.

!!!Note
    In case there are no changes to send to the replica, the primary delays the response until something is available


Internally, `SableDb` utilizes `RocksDb` APIs: [`create_checkpoint`][1] and [`get_updates_since`][2]

In addition to the above APIs, `SableDb` maintains a file named `changes.seq` inside the database folder of the replica server
which holds the next transaction ID that should be pulled from the primary.

In any case of error, the replica switches to `FULLSYNC` request.

The below sequence of events describes the data flow between the replica and the primary:

![changes sync](../images/happy-flow-sync.svg)

When a `FULLSYNC` is needed, the flow changes to this:

![fullsync](../images/fullsync.svg)

### Replication client

In addition to the above, the replication instance of `SableDb` is running in `read-only` mode. i.e. it does not allow
execution of any command marked as `Write`

[1]: https://github.com/facebook/rocksdb/wiki/Checkpoints
[2]: https://github.com/facebook/rocksdb/wiki/Replication-Helpers
