# Overview

`SableDb` uses a Key / Value database for its underlying data storage. We chose to use `RocksDb` 
as its mature, maintained and widely used in the industry by giant companies.

Because the `RocksDb` is key-value storage and Redis data structures can be more complex, an additional 
data encoding is required.

This chapter covers how `SableDb` encodes the data for the various data types (e.g. `String`, `Hash`, `Set` etc)

!!!Note
    Numbers are encoded using Big Endians to preserve lexicographic ordering

## The `String` data type

The most basic data type in `SableDb` is the `String` data type. `String`s in `SableDb` are always binary safe
Each `String` record in the `SableDb` consists of a single entry in `RocksDb`:

```
   A    B      C      D                E        F       G     H
+-----+-----+-------+----------+    +-----+------------+----+-------+
| 1u8 | DB# | Slot# | user key | => | 0u8 | Expirtaion | ID | value |
+-----+-----+-------+----------+    +-----+------------+----+-------+
```

The key for a `String` record is encoded as follows:

- `A` the first byte ( `u8` ) is always set to `1` - this indicates that this is a data entry (there are other type of keys in the database)
- `B` the database ID is encoded as `u16` (this implies that `SableDb` supports up to `64K` databases)
- `C` the slot number
- `D` the actual key value (e.g. `set mykey myvalue`  -> `mykey` is set here)

The value is encoded as follows:

- `E` the first byte is the type bit, value of `0` means that the this record is of type `String`
- `F` the record expiration info
- `G` unique ID (relevant for complex types like `Hash`), for `String` this is always `0`
- `H` the user value

Using the above encoding, we can now understand how `SableDb` reads from the database. Lets have a look a the command:

```
get mykey
```

`SableDb` encodes a key from the user key (`mykey`) by prepending the following:

- `1`u8 - to indicate that this is the data record
- The active database number (defaults to `0`)
- The slot number
- The user string key (i.e. `mykey`)

This is the key that is passed to `RocksDb` for reading
- If the key exists in the database:
    - If the type (field `E`) is `!= 0` - i.e. the entry is not a `String`, `SableDb` returns a `-WRONGTYPE` error
    - If value is expired -> `SableDb` returns `null` and deletes the record from the database
    - Otherwise, `SableDb` returns the `H` part of the value (the actual user data)
- Else (no such key) return `null`

## The `List` data type

A `List` is a composite data type. `SableDb` stores the metadata of the list using a dedicated record and each list element
is stored in a separate entry.

```
List metadata:

   A    B       C        D            
+-----+---- +--------+------------+ 
| 1u8 | DB# |  Slot# |  list name |  
+-----+---- +--------+------------+    
                             E        F        G        H      I       J
                        +-----+------------+--------- +------+------+-------+
                   =>   | 1u8 | Expirtaion | List UID | head | tail |  size |
                        +-----+------------+--------- +------+------+-------+

List item:

   K        L              M                 N      O           P
+-----+--------------+---------------+    +------+--------+------------+
| 2u8 | List ID(u64) |  Item ID(u64) | => | Left | Right  |     value  |
+-----+--------------+---------------+    +------+--------+------------+

```

Unlike `String`, a `List` is using an additional entry in the database that holds the list metadata.

- Encoded items `A` -> `D` are the same as `String`
- `E` the first byte is always set to `1` (unlike `String` which is set to `0`)
- `F` Expiration info
- `G` The list UID. Each list is assigned with a unique ID (an incremental number that never repeat itself, evern after restarts)
- `H` the UID of the list head item (`u64`)
- `I` the UID of the list tail item (`u64`)
- `J` the list length

In addition to the list metadata (`SableDb` keepts a single metadata item per list) we add a list item per new list item
using the following encoding:

- `K` the first bit which is always set to `2` ("List Item")
- `L` the parent list ID (see field `G` above)
- `M` the item UID
- `N` the UID of the previous item in the list ( `0` means that this item is the head)
- `O` the UID of the next item in the list ( `0` means that this item is the last item)
- `P` the list value

The above encoding allows `SableDb` to iterate over all list items by creating a `RocksDb` iterator and move it to 
the prefix `[ 2 | <list-id>]` (`2` indicates that only list items should be scanned, and `list-id` makes sure that only
the requested list items are visited)
