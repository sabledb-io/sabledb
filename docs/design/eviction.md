# Data eviction

This chapter covers the data eviction as it being handled by `SableDb`.

There are 3 cases where items needs to be purged:

- Item is expired
- A composite item was overwritten by another type (e.g. user called `SET MYHASH SOMEVALUE` on an item `MYHASH` which was previously a `Hash`)
- User called `FLUSHDB` or `FLUSHALL`

## Expired items

Since the main storage used by `SableDb` is disk (which is cheap), an item is checked for expiration only when it is being accessed, if it is expired
the item is deleted and a `null` value is returned to the caller.

## Composite item has been overwritten

To explain the problem here, consider the following data is stored in `SableDb` (using `Hash` data type):

```
"OverwatchTanks" => 
    { 
        {"tank_1" => "Reinhardt"}, 
        {"tank_2" => "Orisa"}, 
        {"tank_3" => "Roadhog"}
    }
```

In the above example, we have a hash identified by the key `OverwatchTanks`. Now, imagine a user that executes the following command:

`set OverwatchTanks bla` - this effectively changes the type of the key `OverwatchTanks` and set it into a `String`.
However, as explained in [`the encoding data chapter`][1], we know that each hash field is stored in its own `RocksDb` records.
So by calling the `set` command, the `hash` fields `tank_1`, `tank_2` and `tank_3` are now "orphaned" (i.e. the user can not access them)

`SableDb` solves this problem by running an cron task that compares the type of the a composite item against its actual value.
In the above example: the type of the key `OverwatchTanks` is a `String` while it should have been `Hash`. When such a discrepancy is detected,
the cron task deletes the orphan records from the database.

The cron job knows the original type by checking the [`bookkeeping record`][2]

## User triggered clean-up (`FLUSHALL` or `FLUSHDB`)

When one of these commands is called, `SableDb` uses `RocksDb` [`delete_range`][3] method.

[1]: /design/data-encoding/#the-hash-data-type
[2]: /design/data-encoding/#bookkeeping-records
[3]: https://rocksdb.org/blog/2018/11/21/delete-range.html
