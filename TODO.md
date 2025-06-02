# Tasks

## General (ordered by priority)

- Write a complete example no how to setup a cluster using `SableDB`
- Support multiple concurrent slot migration: "SLOT SENDTO <NODE_ID> [SLOT1, SLOT2...]"
- Add more "cluster" commands that make sense
- Add "vacuum" command
- Add "dir" directive to the server to determine the working directory of the process
- Provide `sabledb-admin` with commands to examine the cluster-database
- Convert the configuration into a singleton

## Statistics

- Provide per slot metrics

### Slot transfer flow open tasks:

- Add integration tests for the slot transfer use case

## Keys command:

- Use the `async` ( `RespWriter` ) write API instead of building the response in memory
