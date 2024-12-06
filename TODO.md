# Tasks

## General (ordered by priority)

- Replication server thread should be always running, regardless of the node role
- Each node should know which slot it owns
- Fix `calculate_slot` to include HASHTAGS
- Add API to dump & load a complete slot
- Add API to dump slot from a given txn ID

## Statistics

- Provide per slot metrics

## Cluster related

SLOT migration API:

```
# Transfer slot NUMBER ownership to host at given IP:PORT
# If everything goes well, at the end of this command, the target host is the new owner
SLOT <NUMBER> SENDTO <IP:PORT>
```
