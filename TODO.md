Shard management:

## Primary:

- [x] Update primary info the cluster database in a constant interval (this is the equivalent of a `heartbeat` operation)
- [x] Update the replicas in the SET ( `<primary_node_id>_replicas` ) after each successful operation / timeout ( `GET_CHANGES`, `FULL_SYNC` etc)

## Replica:

- [x] Update the node info after each successful operation (mainly the fields: `last_txn_id` + `last_updated`)
- [x] Add random check interval per replica for checking whether the primary is alive or not
- [ ] When losing connection with the primary, remove itself from the `<primary_node_id>_replicas` entry
- [ ] When calling `REPLICAOF NO ONE` make sure to:
    - [x] Update the cluster database with the new role
    - [ ] Disassociate the node from the `<primary_node_id>_replicas` SET (call `SREM`)

## Auto-Failover

When a replica detects that its primary did not update its `last_updated` field in over than `N` seconds (where `N` is a random number for each replica),
it should trigger an auto-faileover process:

- [x] Mark in the DB that an "auto-failover" process is taking place `SET <primary_id>_FAILOVER <unique_value> NX EX 60`
- [ ] Decide which replica should be the next primary (the one with the highest `last_txn_id` field)
- [ ] Dispatch a message for every replica in the shard to start a failover (using `BLPUSH` / `BRPOP`) with the correct `REPLICAOF` command to execute
- [ ] Delete the `<old_primary>` HASH key from the database
- [ ] Delete the `<old_primary>_replicas` SET key from the database
- [ ] Remove the `<old_primary_id>_LOCK` (for safety we also set it with 60 seconds timeout)

**A note about locking:**
The only client allowed to delete the lock is the client created it, hence the `<unique_value>`. If that client crashed
we have the `EX 60` as a backup plan


The current information stored in the cluster DB is (HASH):

```
"aaa-bbb-ccc-ddd" => {
    "node_id": "aaa-bbb-ccc-ddd",
    "node_address": "IP:PORT",
    "role": "replica",
    "last_updated": "1234567890",
    "last_txn_id": "0987654321",
    "primary_node_id": "eee-fff-hhh-iii"
}
```

Every primary has an entry (SET) of the following format for keeping track of
replicas in its shard:

```
"eee-fff-hhh-iii_replicas" => {
    "aaa-bbb-ccc-ddd"
}
```
