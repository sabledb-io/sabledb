### Understanding the SableDB Cluster

`SableDB` organizes its data into a distributed cluster using a system of **slots** and **shards**.

#### Slots: Data Distribution

Every key in `SableDB` is assigned a unique number between 0 and 16,383, determining its **slot**. Each node in the cluster is responsible for a specific range of these slots. If a client requests a key from a node that doesn't own its corresponding slot, the node returns a `MOVED` error, directing the client to the correct location.

#### Shards: High Availability

A `SableDB` cluster is built from one or more **shards**. Each shard consists of a **primary node** and can optionally include multiple **replicas** for redundancy. Ideally, slots are evenly distributed among these shards, with each managing `16384 / K` slots in a cluster of `K` shards. This architecture can theoretically support up to 16,000 primary nodes, accommodating substantial workloads.

#### Centralized Cluster Management

`SableDB` employs a **central database** (itself a `SableDB` instance) to manage cluster communication and maintain its overall state. Each node periodically registers and updates its information (like owned slots and public address) in this central database, and also pulls information about other nodes from it.

#### Flexible Slot Migration

`SableDB` provides commands for **transferring slot ownership** between shards. For example, to move slot `1234` from `Node_1` to `Node_2`, you'd simply connect to `Node_1` via `sabledb-cli` and execute `SLOT SENDTO <NODE_ID> 1234`. The command returns `OK` upon successful completion.
