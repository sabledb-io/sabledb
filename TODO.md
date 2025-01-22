# Tasks

## General (ordered by priority)

* Add API to dump & load a complete slot
* Add API to dump slot from a given txn ID

## Statistics

- Provide per slot metrics

## Cluster related

SLOT migration API:

```
# Transfer slot NUMBER ownership to host at given IP:PORT
# If everything goes well, at the end of this command, the target host is the new owner
SLOT <NUMBER> SENDTO <IP:PORT>
```
