# SableDB Redis Compatibility Report

## Overall Compatibility

- **Total Commands:** 367
- **Supported:** 135 (36.78%)
- **Not Supported:** 232 (63.22%)

## Commands by Group

| Group | Total | Supported | Support % |
|-------|-------|-----------|----------|
| server | 56 | 9 | 16.1% 🟡 |
| sorted_set | 35 | 35 | 100.0% ✅ |
| cluster | 33 | 1 | 3.0% 🟡 |
| generic | 31 | 6 | 19.4% 🟡 |
| hash | 27 | 16 | 59.3% 🟡 |
| connection | 24 | 3 | 12.5% 🟡 |
| stream | 23 | 0 | 0.0% ❌ |
| list | 22 | 22 | 100.0% ✅ |
| string | 21 | 20 | 95.2% 🟡 |
| sentinel | 21 | 1 | 4.8% 🟡 |
| set | 17 | 17 | 100.0% ✅ |
| scripting | 16 | 0 | 0.0% ❌ |
| pubsub | 14 | 0 | 0.0% ❌ |
| geo | 10 | 0 | 0.0% ❌ |
| bitmap | 7 | 0 | 0.0% ❌ |
| transactions | 5 | 5 | 100.0% ✅ |
| hyperloglog | 5 | 0 | 0.0% ❌ |

## Detailed Command List

### SERVER (9/56 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| ACL | ❌ | A container for Access List Control commands. |  |
| BGREWRITEAOF | ❌ | Asynchronously rewrites the append-only file to disk. |  |
| BGSAVE | ❌ | Asynchronously saves the database(s) to disk. |  |
| CAT | ❌ | Lists the ACL categories, or the commands inside a category. |  |
| COMMAND | ✅ | Returns detailed information about all commands. |  |
| COMMANDLOG | ❌ | A container for command log commands. |  |
| COUNT | ❌ | Returns a count of commands. |  |
| DBSIZE | ✅ | Returns the number of keys in the database. | Data is accurate for the last scan performed on th |
| DELUSER | ❌ | Deletes ACL users, and terminates their connections. |  |
| DOCS | ❌ | Returns documentary information about one, multiple or all c |  |
| DOCTOR | ❌ | Outputs a memory problems report. |  |
| DRYRUN | ❌ | Simulates the execution of a command by a user, without exec |  |
| FAILOVER | ❌ | Starts a coordinated failover from a server to one of its re |  |
| FLUSHALL | ✅ | Removes all keys from all databases. |  |
| FLUSHDB | ✅ | Remove all keys from the current database. |  |
| GENPASS | ❌ | Generates a pseudorandom, secure password that can be used t |  |
| GET | ✅ | Returns the effective values of configuration parameters. |  |
| GETKEYS | ❌ | Extracts the key names from an arbitrary command. |  |
| GETKEYSANDFLAGS | ❌ | Extracts the key names and access flags for an arbitrary com |  |
| GETUSER | ❌ | Lists the ACL rules of a user. |  |
| GRAPH | ❌ | Returns a latency graph for an event. |  |
| HISTOGRAM | ❌ | Returns the cumulative distribution of latencies of a subset |  |
| HISTORY | ❌ | Returns timestamp-latency samples for an event. |  |
| INFO | ✅ | Returns information about one, multiple or all commands. | `SableDB` has its own INFO output format |
| LASTSAVE | ❌ | Returns the Unix timestamp of the last successful save to di |  |
| LATENCY | ❌ | A container for latency diagnostics commands. |  |
| LATEST | ❌ | Returns the latest latency samples for all events. |  |
| LEN | ❌ | Returns the number of entries in the slow log. |  |
| LIST | ❌ | Returns a list of command names. |  |
| LOADEX | ❌ | Loads a module using extended parameters. |  |
| LOG | ❌ | Lists recent security events generated due to ACL rules. |  |
| LOLWUT | ❌ | Displays computer art and the server version |  |
| MALLOC-STATS | ❌ | Returns the allocator statistics. |  |
| MEMORY | ❌ | A container for memory diagnostics commands. |  |
| MODULE | ❌ | A container for module commands. |  |
| PSYNC | ❌ | An internal command used in replication. |  |
| PURGE | ❌ | Asks the allocator to release memory. |  |
| REPLCONF | ❌ | An internal command for configuring the replication stream. |  |
| REPLICAOF | ✅ | Configures a server as replica of another, or promotes it to |  |
| RESETSTAT | ❌ | Resets the server's statistics. |  |
| RESTORE-ASKING | ❌ | An internal command for migrating keys in a cluster. |  |
| REWRITE | ❌ | Persists the effective configuration to file. |  |
| ROLE | ❌ | Returns the replication role. |  |
| SAVE | ❌ | Synchronously saves the database(s) to disk. |  |
| SET | ✅ | Sets configuration parameters in-flight. |  |
| SETUSER | ❌ | Creates and modifies an ACL user and its rules. |  |
| SHUTDOWN | ❌ | Synchronously saves the database(s) to disk and shuts down t |  |
| SLAVEOF | ✅ | Sets a server as a replica of another, or promotes it to bei |  |
| SLOWLOG | ❌ | A container for slow log commands. |  |
| SWAPDB | ❌ | Swaps two databases. |  |
| SYNC | ❌ | An internal command used in replication. |  |
| TIME | ❌ | Returns the server time. |  |
| UNLOAD | ❌ | Unloads a module. |  |
| USAGE | ❌ | Estimates the memory usage of a key. |  |
| USERS | ❌ | Lists all ACL users. |  |
| WHOAMI | ❌ | Returns the authenticated username of the current connection |  |

### SORTED_SET (35/35 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| BZMPOP | ✅ | Removes and returns a member by score from one or more sorte |  |
| BZPOPMAX | ✅ | Removes and returns the member with the highest score from o |  |
| BZPOPMIN | ✅ | Removes and returns the member with the lowest score from on |  |
| ZADD | ✅ | Adds one or more members to a sorted set, or updates their s |  |
| ZCARD | ✅ | Returns the number of members in a sorted set. |  |
| ZCOUNT | ✅ | Returns the count of members in a sorted set that have score |  |
| ZDIFF | ✅ | Returns the difference between multiple sorted sets. |  |
| ZDIFFSTORE | ✅ | Stores the difference of multiple sorted sets in a key. |  |
| ZINCRBY | ✅ | Increments the score of a member in a sorted set. |  |
| ZINTER | ✅ | Returns the intersect of multiple sorted sets. |  |
| ZINTERCARD | ✅ | Returns the number of members of the intersect of multiple s |  |
| ZINTERSTORE | ✅ | Stores the intersect of multiple sorted sets in a key. |  |
| ZLEXCOUNT | ✅ | Returns the number of members in a sorted set within a lexic |  |
| ZMPOP | ✅ | Returns the highest- or lowest-scoring members from one or m |  |
| ZMSCORE | ✅ | Returns the score of one or more members in a sorted set. |  |
| ZPOPMAX | ✅ | Returns the highest-scoring members from a sorted set after  |  |
| ZPOPMIN | ✅ | Returns the lowest-scoring members from a sorted set after r |  |
| ZRANDMEMBER | ✅ | Returns one or more random members from a sorted set. |  |
| ZRANGE | ✅ | Returns members in a sorted set within a range of indexes. |  |
| ZRANGEBYLEX | ✅ | Returns members in a sorted set within a lexicographical ran |  |
| ZRANGEBYSCORE | ✅ | Returns members in a sorted set within a range of scores. |  |
| ZRANGESTORE | ✅ | Stores a range of members from sorted set in a key. |  |
| ZRANK | ✅ | Returns the index of a member in a sorted set ordered by asc |  |
| ZREM | ✅ | Removes one or more members from a sorted set. Deletes the s |  |
| ZREMRANGEBYLEX | ✅ | Removes members in a sorted set within a lexicographical ran |  |
| ZREMRANGEBYRANK | ✅ | Removes members in a sorted set within a range of indexes. D |  |
| ZREMRANGEBYSCORE | ✅ | Removes members in a sorted set within a range of scores. De |  |
| ZREVRANGE | ✅ | Returns members in a sorted set within a range of indexes in |  |
| ZREVRANGEBYLEX | ✅ | Returns members in a sorted set within a lexicographical ran |  |
| ZREVRANGEBYSCORE | ✅ | Returns members in a sorted set within a range of scores in  |  |
| ZREVRANK | ✅ | Returns the index of a member in a sorted set ordered by des |  |
| ZSCAN | ✅ | Iterates over members and scores of a sorted set. |  |
| ZSCORE | ✅ | Returns the score of a member in a sorted set. |  |
| ZUNION | ✅ | Returns the union of multiple sorted sets. |  |
| ZUNIONSTORE | ✅ | Stores the union of multiple sorted sets in a key. |  |

### CLUSTER (1/33 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| ADDSLOTS | ❌ | Assigns new hash slots to a node. |  |
| ADDSLOTSRANGE | ❌ | Assigns new hash slot ranges to a node. |  |
| ASKING | ❌ | Signals that a cluster client is following an -ASK redirect. |  |
| BUMPEPOCH | ❌ | Advances the cluster config epoch. |  |
| CANCELSLOTMIGRATIONS | ❌ | Cancel all current ongoing slot migration operations. |  |
| CLUSTER | ✅ | A container for Cluster commands. |  |
| COUNT-FAILURE-REPORTS | ❌ | Returns the number of active failure reports for a node. No  |  |
| COUNTKEYSINSLOT | ❌ | Returns the number of keys in a hash slot. |  |
| DELSLOTS | ❌ | Sets hash slots as unbound for a node. |  |
| DELSLOTSRANGE | ❌ | Sets hash slot ranges as unbound for a node. |  |
| FLUSHSLOT | ❌ | Remove all keys from the target slot. |  |
| FLUSHSLOTS | ❌ | Deletes all slots information from a node. |  |
| FORGET | ❌ | Removes a node from the nodes table. |  |
| GETKEYSINSLOT | ❌ | Returns the key names in a hash slot. |  |
| GETSLOTMIGRATIONS | ❌ | Get the status of ongoing and recently finished slot import  |  |
| KEYSLOT | ❌ | Returns the hash slot for a key. |  |
| LINKS | ❌ | Returns a list of all TCP links to and from peer nodes. |  |
| MEET | ❌ | Forces a node to handshake with another node. |  |
| MIGRATESLOTS | ❌ | Migrate the given slots from this node to the specified node |  |
| MYSHARDID | ❌ | Returns the shard ID of a node. |  |
| NODES | ❌ | Returns the cluster configuration for a node. |  |
| READONLY | ❌ | Enables read-only queries for a connection to a Valkey repli |  |
| READWRITE | ❌ | Enables read-write queries for a connection to a Valkey repl |  |
| REPLICAS | ❌ | Lists the replica nodes of a primary node. |  |
| REPLICATE | ❌ | Configure a node as replica of a primary node or detach a re |  |
| RESET | ❌ | Resets a node. |  |
| SAVECONFIG | ❌ | Forces a node to save the cluster configuration to disk. |  |
| SET-CONFIG-EPOCH | ❌ | Sets the configuration epoch for a new node. |  |
| SETSLOT | ❌ | Binds a hash slot to a node. |  |
| SHARDS | ❌ | Returns the mapping of cluster slots to shards. |  |
| SLOT-STATS | ❌ | Return an array of slot usage statistics for slots assigned  |  |
| SLOTS | ❌ | Returns the mapping of cluster slots to nodes. |  |
| SYNCSLOTS | ❌ | A container for internal slot migration commands. |  |

### GENERIC (6/31 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| COPY | ❌ | Copies the value of a key to a new key. |  |
| DEL | ✅ | Deletes one or more keys. |  |
| ENCODING | ❌ | Returns the internal encoding of an object. |  |
| EXISTS | ✅ | Determines whether one or more keys exist. |  |
| EXPIRE | ✅ | Sets the expiration time of a key in seconds. |  |
| EXPIREAT | ❌ | Sets the expiration time of a key to a Unix timestamp. |  |
| EXPIRETIME | ❌ | Returns the expiration time of a key as a Unix timestamp. |  |
| FREQ | ❌ | Returns the logarithmic access frequency counter of an objec |  |
| IDLETIME | ❌ | Returns the time since the last access to an object. |  |
| KEYS | ✅ | Returns all key names that match a pattern. | Pattern uses wildcard match ( `?` and `*` ) |
| MIGRATE | ❌ | Atomically transfers a key from one instance to another. |  |
| MOVE | ❌ | Moves a key to another database. |  |
| OBJECT | ❌ | A container for object introspection commands. |  |
| PERSIST | ❌ | Removes the expiration time of a key. |  |
| PEXPIRE | ❌ | Sets the expiration time of a key in milliseconds. |  |
| PEXPIREAT | ❌ | Sets the expiration time of a key to a Unix milliseconds tim |  |
| PEXPIRETIME | ❌ | Returns the expiration time of a key as a Unix milliseconds  |  |
| PTTL | ❌ | Returns the expiration time in milliseconds of a key. |  |
| RANDOMKEY | ❌ | Returns a random key name from the database. |  |
| REFCOUNT | ❌ | Returns the reference count of a value of a key. |  |
| RENAME | ❌ | Renames a key and overwrites the destination. |  |
| RENAMENX | ❌ | Renames a key only when the target key name doesn't exist. |  |
| SCAN | ✅ | Iterates over the key names in the database. | Pattern uses wildcard match ( `?` and `*` ) |
| SORT | ❌ | Sorts the elements in a list, a set, or a sorted set, option |  |
| SORT_RO | ❌ | Returns the sorted elements of a list, a set, or a sorted se |  |
| TOUCH | ❌ | Returns the number of existing keys out of those specified a |  |
| TTL | ✅ | Returns the expiration time in seconds of a key. |  |
| TYPE | ❌ | Determines the type of value stored at a key. |  |
| UNLINK | ❌ | Asynchronously deletes one or more keys. |  |
| WAIT | ❌ | Blocks until the asynchronous replication of all preceding w |  |
| WAITAOF | ❌ | Blocks until all of the preceding write commands sent by the |  |

### HASH (16/27 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| HDEL | ✅ | Deletes one or more fields and their values from a hash. Del |  |
| HEXISTS | ✅ | Determines whether a field exists in a hash. |  |
| HEXPIRE | ❌ | Set expiry time on hash fields. |  |
| HEXPIREAT | ❌ | Set expiry time on hash fields. |  |
| HEXPIRETIME | ❌ | Returns Unix timestamps in seconds since the epoch at which  |  |
| HGET | ✅ | Returns the value of a field in a hash. |  |
| HGETALL | ✅ | Returns all fields and values in a hash. |  |
| HGETEX | ❌ | Get the value of one or more fields of a given hash key, and |  |
| HINCRBY | ✅ | Increments the integer value of a field in a hash by a numbe |  |
| HINCRBYFLOAT | ✅ | Increments the floating point value of a field by a number.  |  |
| HKEYS | ✅ | Returns all fields in a hash. |  |
| HLEN | ✅ | Returns the number of fields in a hash. |  |
| HMGET | ✅ | Returns the values of all fields in a hash. |  |
| HMSET | ✅ | Sets the values of multiple fields. |  |
| HPERSIST | ❌ | Remove the existing expiration on a hash key's field(s). |  |
| HPEXPIRE | ❌ | Set expiry time on hash object. |  |
| HPEXPIREAT | ❌ | Set expiration time on hash field. |  |
| HPEXPIRETIME | ❌ | Returns the Unix timestamp in milliseconds since Unix epoch  |  |
| HPTTL | ❌ | Returns the remaining time to live in milliseconds of a hash |  |
| HRANDFIELD | ✅ | Returns one or more random fields from a hash. |  |
| HSCAN | ✅ | Iterates over fields and values of a hash. |  |
| HSET | ✅ | Creates or modifies the value of a field in a hash. |  |
| HSETEX | ❌ | Set the value of one or more fields of a given hash key, and |  |
| HSETNX | ✅ | Sets the value of a field in a hash only when the field does |  |
| HSTRLEN | ✅ | Returns the length of the value of a field. |  |
| HTTL | ❌ | Returns the remaining time to live (in seconds) of a hash ke |  |
| HVALS | ✅ | Returns all values in a hash. |  |

### CONNECTION (3/24 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| AUTH | ❌ | Authenticates the connection. |  |
| CACHING | ❌ | Instructs the server whether to track the keys in the next r |  |
| CAPA | ❌ | A client claims its capability. |  |
| CLIENT | ✅ | A container for client connection commands. |  |
| ECHO | ❌ | Returns the given string. |  |
| GETNAME | ❌ | Returns the name of the connection. |  |
| GETREDIR | ❌ | Returns the client ID to which the connection's tracking not |  |
| HELLO | ❌ | Handshakes with the server. |  |
| ID | ❌ | Returns the unique client ID of the connection. |  |
| IMPORT-SOURCE | ❌ | Mark this client as an import source when server is in impor |  |
| KILL | ❌ | Terminates open connections. |  |
| NO-EVICT | ❌ | Sets the client eviction mode of the connection. |  |
| NO-TOUCH | ❌ | Controls whether commands sent by the client affect the LRU/ |  |
| PAUSE | ❌ | Suspends commands processing. |  |
| PING | ✅ | Returns the server's liveliness response. |  |
| QUIT | ❌ | Closes the connection. |  |
| REPLY | ❌ | Instructs the server whether to reply to commands. |  |
| SELECT | ✅ | Changes the selected database. |  |
| SETINFO | ❌ | Sets information specific to the client or connection. |  |
| SETNAME | ❌ | Sets the connection name. |  |
| TRACKING | ❌ | Controls server-assisted client-side caching for the connect |  |
| TRACKINGINFO | ❌ | Returns information about server-assisted client-side cachin |  |
| UNBLOCK | ❌ | Unblocks a client blocked by a blocking command from a diffe |  |
| UNPAUSE | ❌ | Resumes processing commands from paused clients. |  |

### STREAM (0/23 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| CONSUMERS | ❌ | Returns a list of the consumers in a consumer group. |  |
| CREATE | ❌ | Creates a consumer group. |  |
| CREATECONSUMER | ❌ | Creates a consumer in a consumer group. |  |
| DELCONSUMER | ❌ | Deletes a consumer from a consumer group. |  |
| DESTROY | ❌ | Destroys a consumer group. |  |
| GROUPS | ❌ | Returns a list of the consumer groups of a stream. |  |
| SETID | ❌ | Sets the last-delivered ID of a consumer group. |  |
| STREAM | ❌ | Returns information about a stream. |  |
| XACK | ❌ | Returns the number of messages that were successfully acknow |  |
| XADD | ❌ | Appends a new message to a stream. Creates the key if it doe |  |
| XAUTOCLAIM | ❌ | Changes, or acquires, ownership of messages in a consumer gr |  |
| XCLAIM | ❌ | Changes, or acquires, ownership of a message in a consumer g |  |
| XDEL | ❌ | Returns the number of messages after removing them from a st |  |
| XGROUP | ❌ | A container for consumer groups commands. |  |
| XINFO | ❌ | A container for stream introspection commands. |  |
| XLEN | ❌ | Return the number of messages in a stream. |  |
| XPENDING | ❌ | Returns the information and entries from a stream consumer g |  |
| XRANGE | ❌ | Returns the messages from a stream within a range of IDs. |  |
| XREAD | ❌ | Returns messages from multiple streams with IDs greater than |  |
| XREADGROUP | ❌ | Returns new or historical messages from a stream for a consu |  |
| XREVRANGE | ❌ | Returns the messages from a stream within a range of IDs in  |  |
| XSETID | ❌ | An internal command for replicating stream values. |  |
| XTRIM | ❌ | Deletes messages from the beginning of a stream. |  |

### LIST (22/22 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| BLMOVE | ✅ | Pops an element from a list, pushes it to another list and r |  |
| BLMPOP | ✅ | Pops the first element from one of multiple lists. Blocks un |  |
| BLPOP | ✅ | Removes and returns the first element in a list. Blocks unti |  |
| BRPOP | ✅ | Removes and returns the last element in a list. Blocks until |  |
| BRPOPLPUSH | ✅ | Pops an element from a list, pushes it to another list and r |  |
| LINDEX | ✅ | Returns an element from a list by its index. |  |
| LINSERT | ✅ | Inserts an element before or after another element in a list |  |
| LLEN | ✅ | Returns the length of a list. |  |
| LMOVE | ✅ | Returns an element after popping it from one list and pushin |  |
| LMPOP | ✅ | Returns multiple elements from a list after removing them. D |  |
| LPOP | ✅ | Returns and removes one or more elements from the beginning  |  |
| LPOS | ✅ | Returns the index of matching elements in a list. |  |
| LPUSH | ✅ | Prepends one or more elements to a list. Creates the key if  |  |
| LPUSHX | ✅ | Prepends one or more elements to a list only when the list e |  |
| LRANGE | ✅ | Returns a range of elements from a list. |  |
| LREM | ✅ | Removes elements from a list. Deletes the list if the last e |  |
| LSET | ✅ | Sets the value of an element in a list by its index. |  |
| LTRIM | ✅ | Removes elements from both ends a list. Deletes the list if  |  |
| RPOP | ✅ | Returns and removes one or more elements from the end of a l |  |
| RPOPLPUSH | ✅ | Returns the last element of a list after removing and pushin |  |
| RPUSH | ✅ | Appends one or more elements to a list. Creates the key if i |  |
| RPUSHX | ✅ | Appends one or more elements to a list only when the list ex |  |

### STRING (20/21 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| APPEND | ✅ | Appends a string to the value of a key. Creates the key if i |  |
| DECR | ✅ | Decrements the integer value of a key by one. Uses 0 as init |  |
| DECRBY | ✅ | Decrements a number from the integer value of a key. Uses 0  |  |
| DELIFEQ | ❌ | Delete key if value matches string. |  |
| GETDEL | ✅ | Returns the string value of a key after deleting the key. |  |
| GETEX | ✅ | Returns the string value of a key after setting its expirati |  |
| GETRANGE | ✅ | Returns a substring of the string stored at a key. |  |
| GETSET | ✅ | Returns the previous string value of a key after setting it  |  |
| INCR | ✅ | Increments the integer value of a key by one. Uses 0 as init |  |
| INCRBY | ✅ | Increments the integer value of a key by a number. Uses 0 as |  |
| INCRBYFLOAT | ✅ | Increment the floating point value of a key by a number. Use |  |
| LCS | ✅ | Finds the longest common substring. | Does not support: `IDX`, `MINMATCHLEN` and `WITHMA |
| MGET | ✅ | Atomically returns the string values of one or more keys. |  |
| MSET | ✅ | Atomically creates or modifies the string values of one or m |  |
| MSETNX | ✅ | Atomically modifies the string values of one or more keys on |  |
| PSETEX | ✅ | Sets both string value and expiration time in milliseconds o |  |
| SETEX | ✅ | Sets the string value and expiration time of a key. Creates  |  |
| SETNX | ✅ | Set the string value of a key only when the key doesn't exis |  |
| SETRANGE | ✅ | Overwrites a part of a string value with another by an offse |  |
| STRLEN | ✅ | Returns the length of a string value. |  |
| SUBSTR | ✅ | Returns a substring from a string value. |  |

### SENTINEL (1/21 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| CKQUORUM | ❌ | Checks for a Sentinel quorum. |  |
| CONFIG | ✅ | Configures Sentinel. |  |
| FLUSHCONFIG | ❌ | Rewrites the Sentinel configuration file. |  |
| GET-MASTER-ADDR-BY-NAME | ❌ | Returns the port and address of a primary instance. |  |
| GET-PRIMARY-ADDR-BY-NAME | ❌ | Returns the port and address of a primary instance. |  |
| HELP | ❌ | Returns helpful text about the different subcommands. |  |
| INFO-CACHE | ❌ | Returns the cached `INFO` replies from the deployment's inst |  |
| IS-MASTER-DOWN-BY-ADDR | ❌ | Determines whether a primary instance is down. |  |
| IS-PRIMARY-DOWN-BY-ADDR | ❌ | Determines whether a primary instance is down. |  |
| MASTER | ❌ | Returns the state of a primary instance. |  |
| MASTERS | ❌ | Returns a list of monitored primaries. |  |
| MONITOR | ❌ | Starts monitoring. |  |
| MYID | ❌ | Returns the Sentinel instance ID. |  |
| PENDING-SCRIPTS | ❌ | Returns information about pending scripts for Sentinel. |  |
| PRIMARIES | ❌ | Returns a list of monitored primaries. |  |
| PRIMARY | ❌ | Returns the state of a primary instance. |  |
| REMOVE | ❌ | Stops monitoring. |  |
| SENTINEL | ❌ | A container for Sentinel commands. |  |
| SENTINELS | ❌ | Returns a list of Sentinel instances. |  |
| SIMULATE-FAILURE | ❌ | Simulates failover scenarios. |  |
| SLAVES | ❌ | Returns a list of the monitored replicas. |  |

### SET (17/17 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| SADD | ✅ | Adds one or more members to a set. Creates the key if it doe |  |
| SCARD | ✅ | Returns the number of members in a set. |  |
| SDIFF | ✅ | Returns the difference of multiple sets. |  |
| SDIFFSTORE | ✅ | Stores the difference of multiple sets in a key. |  |
| SINTER | ✅ | Returns the intersect of multiple sets. |  |
| SINTERCARD | ✅ | Returns the number of members of the intersect of multiple s |  |
| SINTERSTORE | ✅ | Stores the intersect of multiple sets in a key. |  |
| SISMEMBER | ✅ | Determines whether a member belongs to a set. |  |
| SMEMBERS | ✅ | Returns all members of a set. |  |
| SMISMEMBER | ✅ | Determines whether multiple members belong to a set. |  |
| SMOVE | ✅ | Moves a member from one set to another. |  |
| SPOP | ✅ | Returns one or more random members from a set after removing |  |
| SRANDMEMBER | ✅ | Get one or multiple random members from a set |  |
| SREM | ✅ | Removes one or more members from a set. Deletes the set if t |  |
| SSCAN | ✅ | Iterates over members of a set. |  |
| SUNION | ✅ | Returns the union of multiple sets. |  |
| SUNIONSTORE | ✅ | Stores the union of multiple sets in a key. |  |

### SCRIPTING (0/16 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| DEBUG | ❌ | Sets the debug mode of server-side Lua scripts. |  |
| DELETE | ❌ | Deletes a library and its functions. |  |
| DUMP | ❌ | Dumps all libraries into a serialized binary payload. |  |
| EVAL | ❌ | Executes a server-side Lua script. |  |
| EVAL_RO | ❌ | Executes a read-only server-side Lua script. |  |
| EVALSHA | ❌ | Executes a server-side Lua script by SHA1 digest. |  |
| EVALSHA_RO | ❌ | Executes a read-only server-side Lua script by SHA1 digest. |  |
| FCALL | ❌ | Invokes a function. |  |
| FCALL_RO | ❌ | Invokes a read-only function. |  |
| FLUSH | ❌ | Removes all server-side Lua scripts from the script cache. |  |
| FUNCTION | ❌ | A container for function commands. |  |
| LOAD | ❌ | Loads a server-side Lua script to the script cache. |  |
| RESTORE | ❌ | Restores all libraries from a payload. |  |
| SCRIPT | ❌ | A container for Lua scripts management commands. |  |
| SHOW | ❌ | Show server-side Lua script in the script cache. |  |
| STATS | ❌ | Returns information about a function during execution. |  |

### PUBSUB (0/14 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| CHANNELS | ❌ | Returns the active channels. |  |
| NUMPAT | ❌ | Returns a count of unique pattern subscriptions. |  |
| NUMSUB | ❌ | Returns a count of subscribers to channels. |  |
| PSUBSCRIBE | ❌ | Listens for messages published to channels that match one or |  |
| PUBLISH | ❌ | Posts a message to a channel. |  |
| PUBSUB | ❌ | A container for Pub/Sub commands. |  |
| PUNSUBSCRIBE | ❌ | Stops listening to messages published to channels that match |  |
| SHARDCHANNELS | ❌ | Returns the active shard channels. |  |
| SHARDNUMSUB | ❌ | Returns the count of subscribers of shard channels. |  |
| SPUBLISH | ❌ | Post a message to a shard channel |  |
| SSUBSCRIBE | ❌ | Listens for messages published to shard channels. |  |
| SUBSCRIBE | ❌ | Listens for messages published to channels. |  |
| SUNSUBSCRIBE | ❌ | Stops listening to messages posted to shard channels. |  |
| UNSUBSCRIBE | ❌ | Stops listening to messages posted to channels. |  |

### GEO (0/10 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| GEOADD | ❌ | Adds one or more members to a geospatial index. The key is c |  |
| GEODIST | ❌ | Returns the distance between two members of a geospatial ind |  |
| GEOHASH | ❌ | Returns members from a geospatial index as geohash strings. |  |
| GEOPOS | ❌ | Returns the longitude and latitude of members from a geospat |  |
| GEORADIUS | ❌ | Queries a geospatial index for members within a distance fro |  |
| GEORADIUS_RO | ❌ | Returns members from a geospatial index that are within a di |  |
| GEORADIUSBYMEMBER | ❌ | Queries a geospatial index for members within a distance fro |  |
| GEORADIUSBYMEMBER_RO | ❌ | Returns members from a geospatial index that are within a di |  |
| GEOSEARCH | ❌ | Queries a geospatial index for members inside an area of a b |  |
| GEOSEARCHSTORE | ❌ | Queries a geospatial index for members inside an area of a b |  |

### BITMAP (0/7 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| BITCOUNT | ❌ | Counts the number of set bits (population counting) in a str |  |
| BITFIELD | ❌ | Performs arbitrary bitfield integer operations on strings. |  |
| BITFIELD_RO | ❌ | Performs arbitrary read-only bitfield integer operations on  |  |
| BITOP | ❌ | Performs bitwise operations on multiple strings, and stores  |  |
| BITPOS | ❌ | Finds the first set (1) or clear (0) bit in a string. |  |
| GETBIT | ❌ | Returns a bit value by offset. |  |
| SETBIT | ❌ | Sets or clears the bit at offset of the string value. Create |  |

### TRANSACTIONS (5/5 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| DISCARD | ✅ | Discards a transaction. |  |
| EXEC | ✅ | Executes all commands in a transaction. |  |
| MULTI | ✅ | Starts a transaction. |  |
| UNWATCH | ✅ | Forgets about watched keys of a transaction. |  |
| WATCH | ✅ | Monitors changes to keys to determine the execution of a tra |  |

### HYPERLOGLOG (0/5 supported)

| Command | Supported | Summary | Notes |
|---------|-----------|---------|-------|
| PFADD | ❌ | Adds elements to a HyperLogLog key. Creates the key if it do |  |
| PFCOUNT | ❌ | Returns the approximated cardinality of the set(s) observed  |  |
| PFDEBUG | ❌ | Internal commands for debugging HyperLogLog values. |  |
| PFMERGE | ❌ | Merges one or more HyperLogLog values into a single key. |  |
| PFSELFTEST | ❌ | An internal command for testing HyperLogLog values. |  |

