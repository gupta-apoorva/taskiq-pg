# Upgrade Notes: Enhanced PostgreSQL Features

This document describes the breaking changes and new features added to taskiq-pg inspired by SAQ's PostgreSQL implementation.

## Breaking Changes

### Database Schema Changes

The broker now uses an enhanced database schema with additional columns:

- `status`: Tracks message state (queued, active, completed)
- `scheduled_at`: Controls when messages become available for processing
- `expire_at`: Automatic cleanup timestamp
- `group_key`: For coordinating related messages
- `retry_count`: Counts deliveries. Bumped by the claim, so a crashed attempt and a failed one draw on the same budget
- `ordered`: Opt-in FIFO within a `group_key` (defaults to false, i.e. mutex only)

**Migration Required**: If you have existing messages in your database, you'll need to either:
1. Drop and recreate the messages table (losing existing messages)
2. Manually add the new columns with appropriate defaults

### `id` widened to bigint

Every insert takes one `id`, and cleanup deletes completed rows without giving their ids
back, so the sequence counts lifetime inserts. `SERIAL` capped that at 2 147 483 647,
which is about 25 days at 1000 messages per second. `nextval` then fails and the queue
stops accepting work.

`id` is now `BIGSERIAL`, and startup widens an existing `integer` column together with
its sequence. Both statements below are needed: `ALTER TABLE ... ALTER COLUMN id TYPE
BIGINT` on its own leaves the sequence capped at the int4 maximum.

The column change rewrites the table and rebuilds the primary key while it holds ACCESS
EXCLUSIVE, and startup runs it inside the DDL transaction. Every producer and every
worker waits for it. If your table carries a backlog, run it yourself before you deploy;
startup then finds `bigint` and skips the block:

```sql
ALTER TABLE taskiq_messages ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE taskiq_messages_id_seq AS BIGINT;
```

### `lock_key` dropped

The column held the key for the row-level advisory lock that the claim used before
`FOR UPDATE SKIP LOCKED`. No released version reads it, so startup drops it, and the drop
removes the sequence it owned. Inserts no longer call `nextval` on it. Roll back to an
older version and its own DDL adds the column again at startup, which costs a table
rewrite, because a `SERIAL` default is volatile.

### Attempt Counting

`retry_count` is incremented when a message is claimed, not when it fails or when the
sweeper reclaims it. One counter covers every reason an attempt ended, and
`max_retry_attempts` bounds attempts rather than retries — a limit of 5 allows 5
executions, and a message that succeeds first time ends at 1.

### Retry Middleware

`OrderedRetryMiddleware` replaces taskiq's `SmartRetryMiddleware`: it subclasses it and
overrides `on_error` to requeue the existing row, so both hooking the same event would
retry twice. Requires `taskiq>=0.11.20`. The sweeper now reads the per-message
`max_retries` label, falling back to `max_retry_attempts`; `-1` means retry forever.

Migrating an existing `SmartRetryMiddleware` setup is not a drop-in swap. The cap comes
from the `max_retries` label or the broker's `max_retry_attempts`, never from
`default_retry_count` — the constructor still accepts it, but it does not control the
budget, because the sweeper has to cap a crashed attempt without any middleware in the
loop. Move `default_retry_count=N` to `max_retry_attempts=N` on the broker, or set
`max_retries` per task. `delay`, jitter and the exponent options carry over unchanged.

### Injected Labels

Delivered messages carry `_tpg_row_id` and `_tpg_attempts`, stamped at claim time. They
are added to the delivered copy, not to the stored body, and are reserved for the broker.

### Retired Index

`idx_<table>_status_scheduled` is dropped at startup. `idx_<table>_scheduled_id`
supersedes it — `status` is constant inside a partial index on `status`, and the
dequeue orders by `(scheduled_at, id)`.

### New Database Object

The broker creates a `<table_name>_claim(BIGINT)` plpgsql function at startup and
dequeues through it. The connecting role needs `CREATE` on the schema. It returns
`BIGINT`, not the table's row type, so it is not a dependency of the table and
`DROP TABLE` still works; startup recreates it with `CREATE OR REPLACE`.

Because there is no dependency, `DROP TABLE` leaves the function behind. Drop it
explicitly with `DROP FUNCTION IF EXISTS <table_name>_claim(BIGINT)` if you create
tables dynamically, or the catalog accumulates one function per table.

### API Changes

The `AsyncpgBroker` constructor now accepts additional parameters:
- `job_lock_keyspace`: Seed for the group-mutex advisory lock (default: 1). Required for correctness: all workers on the same table must share the same stable signed 64-bit integer; use a unique value per broker/table to avoid advisory-lock collisions.
- `message_ttl`: Time to live for completed messages in seconds (default: 86400)
- `stuck_message_timeout`: Time before message is considered stuck (default: 300)
- `enable_sweeping`: Enable automatic cleanup (default: True)
- `sweep_interval`: Interval between sweep operations (default: 60)

## New Features

### 1. Atomic Message Claiming
- Prevents duplicate message processing using `SELECT ... FOR UPDATE SKIP LOCKED`
- Each message is claimed by exactly one worker; a heartbeat lease guards it during processing
- A dead worker's stale lease is reclaimed by the sweeper and re-queued

### 2. Message States
- `queued`: Message waiting to be processed
- `active`: Message currently being processed
- `completed`: Message has been acknowledged

### 3. Scheduled Messages
- Messages with a `delay` label are scheduled for future processing
- The broker efficiently handles delayed messages without blocking

### 4. Group Coordination
- Messages with the same `group_key` won't be processed concurrently
- Useful for ensuring sequential processing of related tasks

### 5. Message TTL
- Completed messages are automatically cleaned up after TTL expires
- Configure per-message with the `ttl` label or globally via `message_ttl`

### 6. Automatic Sweeping
- Stuck messages (no active lock) are automatically returned to queue
- Expired messages are cleaned up periodically
- Configurable sweep interval and timeout

### 7. Connection Resilience
- Dedicated dequeue connection with health checks
- Automatic reconnection on connection failures
- Better connection pool management

## Usage Examples

### Group Coordination
```python
# These tasks won't run concurrently
await my_task.kicker().with_labels(group_key="user_123").kiq()
await another_task.kicker().with_labels(group_key="user_123").kiq()
```

### Message TTL
```python
# This message will be cleaned up after 1 hour
await my_task.kicker().with_labels(ttl=3600).kiq()
```

### Delayed Messages
```python
# This message will be processed after 5 minutes
await my_task.kicker().with_labels(delay="300").kiq()
```

## Performance Improvements

- `FOR UPDATE SKIP LOCKED` for efficient concurrent dequeuing
- Optimized indexes for message queries
- Batch operations for cleanup tasks
- Connection pooling best practices

## Monitoring

The broker logs important events:
- Swept messages returned to queue
- Connection health issues and reconnections
- Expired message cleanup

Monitor these logs to ensure your system is operating correctly.