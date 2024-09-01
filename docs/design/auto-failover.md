# Automatic Shard Management

## General

`Shard` - a hierarchical arrangement of nodes, within the shard, one node functions as the read/write Primary node.
while all the other nodes in the shard read-only replicas of the primary node

`SableDB` uses a centralised database to manage an auto-failover process and tracking nodes of the same shard.
The centralised database itself is an instance of `SableDB`.

Each node in the shard, updates a record every N seconds to the centralised database where it keeps the following entries:

- `node_id` this is a globally unique ID assigned to each node when it first started and it persists throughout restarts
- `node_address` the node **privata address** on which other nodes can connect to it
- `role` the node role (can be one `replica` or `primary`)
- `last_updated` the last time that this node updated its information, this field is a UNIX timestamp since Jan 1st, 1970 in microseconds. This field is also used as the "heartbeat" of node
- `last_txn_id` contains the last transaction ID applied to the local data set. In an ideal world, this number is the same across all instances of the shard. The higher the number, the more up-to-date the node is
- `primary_node_id` if the `role` field is set to `replica`, this field contains the `node_id` of the shard primary node.

## Primary

In addition for updating its own record, the primary also maintains an entry of type `SET` which holds `node_id` of all
nodes members of the shard.

This SET is constantly updated whenever the primary interact with replica node. The first time a node is added to this SET
by the primary node is only after a successful FullSync.

The entry is identified by the key `<primary_id>_replicas` where `<primary_id>` is the primary node unique id.

## Replica:

Similar to the primary node, the replica updates its information in a regular intervals.

## Auto-Failover

In order to detect if the primary node is still alive, `SableDB` uses [the Raft algorithm][1] while using the centralised database
as its communication layer and the `last_txn_id` as the log entry

Each replica node checks regularly the `last_updated` field of the primary node. The interval on which a replica node checks
whether its primary is not responsive, differs from node to node - this is to avoid situations where the failover process is attempted
by multiple nodes (it can still happen, but with a lower chance).

The failover process starts if the primary's `last_updated` exceeds the value allowed by the checking node. If the value exceeds, then
the replica node does the following:

### The replica that initiated the failover

- Marks in the centralised database that a failover initiated for the non responsive primary. It does not by creating a unique lock record
- The node that started the failover checks which replica should take over, it does that by picking the one with the highest `last_txn_id` property
- Dispatch a command to the new replica instructing it to switch to Primary mode (we achieve this by using `LPUSH / BRPOP` blocking command)
- Dispatch a command to all of the remaining replicas instructing them to perform a `REPLICAOF <NEW_PRIMARY_IP> <NEW_PRIMARY_PORT>`
- Delete the old primary records from the database (if this node comes back online again later, it will re-create them)

### All other replicas

Each replica node always checks for the shard's lock record. If it exists, each replica switches to waiting for commands from the
replica to orchestrates the failover. It does that by calling:


```
BLPOP <NODE_ID>_queue 5
```

### A note about locking

`SableDB` uses the command `SET <PRIMARY_ID>_FAILOVER <Unique-Value> NX EX 60` to create a unique lock.
By doing so, it ensures that only one locking record exists. If it succeeded in creating the lock record,
it becomes [the node that orchestrates the replacement][3]

If it fails (i.e. the record already exist) - it switches to read commands from the queue as described [here][2]

The only client allowed to delete the lock is the client created it, hence the `<unique_value>`. If that client crashed
we have the `EX 60` as a backup plan

Commands are sent over to this queue by the orchestrator replica.

    [1]: https://raft.github.io/
    [2]: /design/auto-failover/#all-other-replicas
    [3]: /design/auto-failover/#the-replica-that-initiated-the-failover
