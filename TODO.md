# Tasks

## General (ordered by priority)


## Statistics

- Provide per slot metrics

### Slot transfer flow open tasks:

- once a slot has moved to its new home:
    - the old slot owner should update its cluster database entry immediately (and do not rely on the cron thread)
    - the old slot owner should purge the slot from the database
- Add integration tests for the slot transfer use case
