# Automatic Shard Management

## Terminology

`Shard` - a hierarchical arrangement of nodes. Within a shard, one node functions as the read/write Primary node.
While all the nodes are a read-only replicas of the primary node.

`SableDB` uses a centralised database to manage an auto-failover process and tracking nodes of the same shard.
The centralised database itself is an instance of `SableDB`.

## All Nodes

Every node in the shard, updates a record of type `HASH` every `N` seconds in the centralised database where it keeps the following hash fields:

- `node_id` this is a globally unique ID assigned to each node when it first started and it persists throughout restarts
- `node_address` the node **privata address** on which other nodes can connect to it
- `role` the node role (can be one `replica` or `primary`)
- `last_updated` the last time that this node updated its information, this field is a UNIX timestamp since Jan 1st, 1970 in microseconds. This field is also used as the "heartbeat" of node
- `last_txn_id` contains the last transaction ID applied to the local data set. In an ideal world, this number is the same across all instances of the shard. The higher the number, the more up-to-date the node is
- `primary_node_id` if the `role` field is set to `replica`, this field contains the `node_id` of the shard primary node.

The key used for this `HASH` record, is the node-id

## Primary

In addition for updating its own record, the primary node maintains an entry of type `SET` which holds the `node_id`s of all
the shard node members.

This `SET` is constantly updated whenever the primary interacts with a replica node. Only after the replica node successfully completes a FullSyc,
it can be added to this `SET`.

This `SET` entry is identified by the key `<primary_id>_replicas` where `<primary_id>` is the primary node unique id.

## Replica

Similar to the primary node, the replica updates its information in a regular intervals

## Auto-Failover

In order to detect whether the primary node is still alive, `SableDB` uses [the Raft algorithm][1] while using the centralised database
as its communication layer and the `last_txn_id` as the log entry

Each replica node regularly checks the `last_updated` field of the primary node the interval on which a replica node checks differs from node to
node - this is to minimise the risk of attempting to start multiple failover processes (but this can still happen and is solved by the [lock][4] described blow)

The failover process starts if the primary's `last_updated` was not updated after the allowed time. If the value exceeds, then
the replica node does the following:

### The replica that initiated the failover

- Marks in the centralised database that a failover is initiated for the non responsive primary
- The node that started the failover decides on the new primary. It does that by picking the one with the highest `last_txn_id` property
- Dispatches a command to the new replica instructing it to switch to Primary mode (we achieve this by using `LPUSH / BRPOP` blocking command)
- Dispatch commands to all of the remaining replicas instructing them to perform a `REPLICAOF <NEW_PRIMARY_IP> <NEW_PRIMARY_PORT>`
- Delete the old primary records from the database (if this node comes back online again later, it will re-create them)

### All other replicas

Each replica node always checks for the shard's lock record. If it exists, each replica switches to waiting mode on a dedicated queue.
This is achieved by using the below command:

```
BLPOP <NODE_ID>_queue 5
```

As mentioned above, there are 2 type of commands:

- Apply `REPLICAOF` to connect to the new primary
- Apply `REPLICAOF NO ONE` to become the new primary

### A note about locking

`SableDB` uses the command `SET <PRIMARY_ID>_FAILOVER <Unique-Value> NX EX 60` to create a unique lock.
By doing so, it ensures that only one locking record exists. If it succeeded in creating the lock record,
it becomes [the node that orchestrates the replacement][3]

If it fails (i.e. the record already exist) - it switches to read commands from the queue as described [here][2]

The only client allowed to delete the lock is the client created it, hence the `<unique_value>`. If that client crashed
we have the `EX 60` as a backup plan (the lock will be expire)

 [1]: https://raft.github.io/
 [2]: /sabledb/design/auto-failover/#all-other-replicas
 [3]: /sabledb/design/auto-failover/#the-replica-that-initiated-the-failover
 [4]: /sabledb/design/auto-failover/#a-note-about-locking