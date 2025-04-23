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

- Add integration tests for the slot transfer use case
