Data eviction in **SableDB** occurs in three primary scenarios:

---

## Expired Items

**SableDB** stores data primarily on disk, a cost-effective solution. To manage expired items efficiently, **SableDB**
checks an item's expiration status only when it's accessed. If an item is found to be expired, it's deleted, and a `null`
value is returned to the caller.

---

## Overwritten Composite Items

A common issue arises when a composite item (like a **Hash**) is overwritten by a different data type. For instance, if
you have a **Hash** named `OverwatchTanks` containing multiple fields (e.g. `tank_1`, `tank_2`, and `tank_3`), and then
execute `SET OverwatchTanks "bla"`, the `OverwatchTanks` key becomes a **String**. However, as each **Hash** field is
stored as a separate record in `RocksDB`, the original `tank_1`, `tank_2`, and `tank_3` fields become "orphaned" and inaccessible.

**SableDB** addresses this with a background cron task. This task periodically compares the declared type of a composite
item with its actual stored value. If a mismatch is detected (e.g., `OverwatchTanks` is now a **String** but still has
associated **Hash** records), the cron job uses **bookkeeping records** to identify the original type and then deletes
the orphaned records from the database.

---

## User-Triggered Cleanup

When a user initiates a `FLUSHALL` or `FLUSHDB` command, **SableDB** leverages `RocksDB`'s `delete_range` method to
efficiently purge the data.
