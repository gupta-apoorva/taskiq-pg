# Upgrade Notes

What changed since 0.1.4, and what you have to do about it. The queue design follows
SAQ's.

## Breaking Changes

### Database Schema Changes

The broker's table carries these columns:

- `status`: message state, one of queued, active, completed, dead
- `scheduled_at`: when the message becomes available to claim
- `expire_at`: when cleanup deletes a completed message
- `group_key`: groups messages that must not run at the same time
- `retry_count`: counts deliveries. The claim bumps it, so a crashed attempt and a failed one draw on the same budget
- `ordered`: opt-in FIFO within a `group_key`, false by default, which leaves the group mutex on its own

Startup adds whichever of these an older table lacks. Their defaults are constants, so on
PostgreSQL 11 and up each `ADD COLUMN` changes metadata and does not rewrite the table.
The two exceptions are below.

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

### `lock_key` stays int4 and cycles

The column holds the key for the row-level advisory lock that the claim used before
`FOR UPDATE SKIP LOCKED`. Current code ignores it, but older clients still insert with
`RETURNING id, lock_key` and then call `pg_try_advisory_lock(keyspace, lock_key)`. That
is the two-int4 form. Widen the column and those clients get
`function pg_try_advisory_lock(integer, bigint) does not exist` as soon as a value passes
2 147 483 647, so the column keeps its `integer` type. They also reset a row to queued
when the lock fails, so the values have to stay distinct between rows in flight, which
rules out a constant default.

Its sequence therefore cycles instead: `nextval` wraps from 2 147 483 647 back to 1
rather than failing, and two rows collide only if both are in flight 2.1 billion inserts
apart. Startup sets this, and you can set it ahead of time. It changes metadata only:

```sql
ALTER SEQUENCE taskiq_messages_lock_key_seq CYCLE;
```

Drop the column once no client reads it. Until then an insert pays one `nextval` for it.

### Attempt Counting

`retry_count` is incremented when a message is claimed, not when it fails or when the
sweeper reclaims it. One counter covers every reason an attempt ended, and
`max_retry_attempts` bounds attempts rather than retries: a limit of 5 allows 5
executions, and a message that succeeds first time ends at 1.

### Retry Middleware

`OrderedRetryMiddleware` replaces taskiq's `SmartRetryMiddleware`: it subclasses it and
overrides `on_error` to requeue the existing row, so both hooking the same event would
retry twice. Requires `taskiq>=0.11.20`. The sweeper now reads the per-message
`max_retries` label, falling back to `max_retry_attempts`; `-1` means retry forever.

Migrating an existing `SmartRetryMiddleware` setup is not a drop-in swap. The cap comes
from the `max_retries` label or the broker's `max_retry_attempts`, never from
`default_retry_count`. The constructor still accepts that argument, but it does not
control the budget, because the sweeper has to cap a crashed attempt without any
middleware in the loop. Move `default_retry_count=N` to `max_retry_attempts=N` on the broker, or set
`max_retries` per task. `delay`, jitter and the exponent options carry over unchanged.

### Injected Labels

Delivered messages carry `_tpg_row_id` and `_tpg_attempts`, stamped at claim time. They
are added to the delivered copy, not to the stored body, and are reserved for the broker.

### Retired Index

`idx_<table>_status_scheduled` is dropped at startup, because
`idx_<table>_scheduled_id` supersedes it: `status` is constant inside a partial index on
`status`, and the dequeue orders by `(scheduled_at, id)`.

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

## Behaviour

### Claiming

`SELECT ... FOR UPDATE SKIP LOCKED` inside the claim function gives a message to exactly
one worker. The row then holds a heartbeat lease, and the sweeper requeues the lease of a
worker that died.

### Message states

`queued` waits to be claimed, `active` is running, `completed` is acknowledged and
waiting for cleanup, and `dead` has spent its attempts and is never claimed again.

### Scheduled messages

A `delay` label sets `scheduled_at`, and the claim passes over the row until that time.
Delayed messages sit in the same table and block nothing.

### Groups

Two messages that share a `group_key` never run at the same time. Add `ordered=True` and
the group runs in `id` order as well.

### Message TTL

Cleanup deletes a completed message once its `expire_at` passes. Set the window per
message with the `ttl` label, or for every message with `message_ttl`.

### Sweeping

Every `sweep_interval` seconds the sweeper requeues messages whose lease went stale and
deletes expired completed rows. `enable_sweeping=False` stops both.

### Connections

The broker keeps one connection for dequeuing, separate from the pool, checks its health
before each claim, and reconnects when it drops.

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

## Logging

The broker logs the messages a sweep returns to the queue, connection failures and the
reconnections that follow, and the count of expired messages it deleted.