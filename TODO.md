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


Implementation note:

### Slot transfer flow:

#### Phase 1: no locking are done

- The sender takes the "current change sequence" and store it locally
- The sender creates an iterator over the prefix `<db>:<slot>`
- The sender sends over the data in chunks
- Once the last chunk is sent:

#### Phase 2: "read-only" lock on the sender size, "write" on the receiver end

- The sender locks the slot for "read-only"
- The sender sends over all the changes since the marker "current change sequence" kept earlier
- The sender notifies the receiver to accept ownership on the slot
- The receiver locks the slot for "write" `ShardLocker::lock_slots_exclusive_unconditionally(u16)`
- The receiver updates its `SlotBitmap` and updates the cluster database
- The receiver affirms that the slot ownership was accepted
- Once confirmed, the sender removes the slot from the `SlotBitmap`
- The sender deletes all records for the slot from the database
- The sender finally removes the lock
