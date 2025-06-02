## Overview

`SableDB` is compatible with a `1`:`N` replication setup, where one primary server replicates to multiple secondary servers.

### Replication Client/Server Model

Upon initialization, `SableDB` creates a thread known internally as the `Replicator`, which listens on the `private_address`
(by default it is set to: main port plus `1000`).

For each new incoming replication client, a dedicated thread is spawned to handle the connection.

The replication process follows this methodology:

1. The replica requests a set of changes from the primary starting from a specific ID (initially set to `0`).
2. If this is the first request from the replica to the primary, the primary responds with an error, indicating `FullSyncNotDone`.
3. The replica then sends a `FullSync` request, prompting the primary to send the entire data store.
4. Subsequently, the replica sends `GetChanges` requests and applies these changes locally. Any errors on either the replica or primary side trigger a `FullSync` request.
5. Step 4 repeats indefinitely; any error causes the shard to revert to `FullSync`.

!!! Note
    It is important to note that the primary server is stateless, meaning it does not track its replicas. The replica server is responsible for pulling data from the primary and keeping track of the next change sequence ID to pull.

!!! Note
    If there are no changes to send to the replica, the primary delays the response as specified in the configuration file.

### In-Depth Overview of `GetChanges` and `FullSync` Requests

`SableDB` internally uses `RocksDB` APIs: [`create_checkpoint`](https://github.com/facebook/rocksdb/wiki/Checkpoints) and
[`get_updates_since`](https://github.com/facebook/rocksdb/wiki/Replication-Helpers).

Additionally, `SableDB` maintains a file named `changes.seq` in the replica server's database folder, which stores the
next transaction ID to be pulled from the primary.

In case of any error, the replica switches to a `FullSync` request.

The following sequence of events describes the data flow between the replica and the primary:

![Changes Sync](../images/happy-flow-sync.svg)

When a `FullSync` is required, the flow changes to:

![Full Sync](../images/fullsync.svg)

### Replication Client

The replication instance of `SableDB` operates in `read-only` mode, meaning it does not allow the execution of any commands marked as `Write`.