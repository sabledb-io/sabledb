# Tasks

## General (ordered by priority)


## Statistics

- Provide per slot metrics

## Cluster related

SLOT migration API:

```
# Transfer slot NUMBER ownership to host at given IP:PORT
# If everything goes well, at the end of this command, the target host is the new owner
SLOT <NUMBER> SENDTO <IP:PORT>
```


### Slot transfer flow open tasks:

- Update cluster entry in the cluster database with list of shards
- Add API to `ClusterManager` for pulling cluster's primaries + their slots
- The "MOVED" error should now contain the new owner of the slot (taken from the cluster database)
- Add integration tests for the slot transfer use case
