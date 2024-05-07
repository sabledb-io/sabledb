## Overview

This chapter covers the overall design choices made for building `SableDb`.

The networking layer of SableDb uses a lock free design. i.e. once a connection is assigned to a worker thread
it does not interact with any other threads or shared data structures.

Having said that, there is one obvious "point" that requires locking: the storage. 
The current implementation of `SableDb` uses `RocksDb` as its storage engine 
(but it can, in principal, work with other storage engines like [`Sled`][1]), even though
the the storage itself is thread-safe, `SableDb` still needs to provide atomicity for multiple database access (consider the `ValKey`'s
`getset` command which requires to perform both `get` and `set` in a single operation) - `SableDb` achieves this by using a shard locking (more details on this later).

By default, `SableDb` listens on port `6379` for incoming connections. A newly arrived connection is then assigned
to a worker thread (using simple round-robin method). The worker thread spawns a [local task][2] (A task, is tokio's implementation for [green threads][3])
which performs the TLS handshake (if dictated by the configuration) and then splits the connection stream into two: 

- Reader end
- Writer end

Each end of the stream is then passed into a dedicated [local task][2] for handling

### The reader task

The reader task is responsible for:

- Reading bytes from the stream 
- Parsing the incoming message and forming a `RedisCommand` structure
- Once a full command is read from the socket, it is moved to the writer task

### The writer task

The writer task input are the commands read and constructed by the reader task.

Once a command is received, the writer task invokes the proper handler for that command (if the command it not supported
an error message is sent back to the client). 

As an output, the handler of the command can either build a response buffer to send over to the client or send the response directly.

The decision whether to reply directly or propagate the response to the caller task is done on per command basis.

For example, the `hgetall` command might generate a huge output (depends on the number of fields in the hash and their size)
so it is probably better to write the response directly to the socket (using a controlled fixed chunks) rather than building 
a complete response in memory (which can take Gigabytes of RAM) and only then write it to the client.

[1]: https://sled.rs/
[2]: https://tokio.rs/tokio/tutorial/spawning#tasks
[3]: https://en.wikipedia.org/wiki/Green_thread
