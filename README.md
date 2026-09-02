# TaskIQ - asyncpg

TaskIQ-pg is a plugin for taskiq that adds a new result backend and a new broker based on PostgreSQL and [asyncpg](https://github.com/MagicStack/asyncpg).

The broker makes use of Postgres' built in `LISTEN/NOTIFY` functionality.

This is a fork of [taskiq-psqlpy](https://github.com/taskiq-python/taskiq-psqlpy) that adds a broker (because PSQLPy does not currently support `LISTEN/NOTIFY`).

## Installation

This project needs the core taskiq library:

```bash
pip install taskiq
```

This project can be installed using pip:

```bash
pip install taskiq-pg
```

Or using poetry:

```
poetry add taskiq-pg
```

## Usage

An example with the broker and result backend:

```python
# example.py
import asyncio

from taskiq.serializers.json_serializer import JSONSerializer
from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

asyncpg_result_backend = AsyncpgResultBackend(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
    serializer=JSONSerializer(),
)

broker = AsyncpgBroker(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
).with_result_backend(asyncpg_result_backend)


@broker.task()
async def best_task_ever() -> str:
    """Solve all problems in the world."""
    await asyncio.sleep(1.0)
    return "All problems are solved!"


async def main() -> None:
    """Main."""
    await broker.startup()
    task = await best_task_ever.kiq()
    result = await task.wait_result(timeout=2)
    print(result)
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Run example

**shell 1: start a worker**

```sh
$ taskiq worker example:broker
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Pid of a main process: 80434
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Starting 2 worker processes.
[2025-01-06 11:48:14,175][taskiq.process-manager][INFO   ][MainProcess] Started process worker-0 with pid 80436
[2025-01-06 11:48:14,176][taskiq.process-manager][INFO   ][MainProcess] Started process worker-1 with pid 80437
```

**shell 2: run the example script**

```sh
$ python example.py
is_err=False log=None return_value='All problems are solved!' execution_time=1.0 labels={} error=None

```

### Details

The result backend stores the data as raw bytes by default, you can decode them in SQL:

```sql
select convert_from(result, 'UTF8') from taskiq_results;
-- Example results:
-- - success:
--   {
--     "is_err": false,
--     "log": null,
--     "return_value": "All problems are solved!",
--     "execution_time": 1.0,
--     "labels": {},
--     "error": null
--   }
-- - failure:
--   {
--     "is_err": true,
--     "log": null,
--     "return_value": null,
--     "execution_time": 10.0,
--     "labels": {},
--     "error": {
--       "exc_type": "ValueError",
--       "exc_message": ["Borked"],
--       "exc_module": "builtins",
--       "exc_cause": null,
--       "exc_context": null,
--       "exc_suppress_context": false
--     }
--   }
```

## AsyncpgResultBackend configuration

- `dsn`: connection string to PostgreSQL.
- `keep_results`: keep a result in the table after it is read instead of deleting it.
- `table_name`: name of the table in PostgreSQL to store TaskIQ results.
- `field_for_task_id`: type of a field for `task_id`, you may need it if you want to have length of task_id more than 255 symbols.
- `**connect_kwargs`: additional connection parameters, you can read more about it in [asyncpg](https://github.com/MagicStack/asyncpg) repository.

## AsyncpgBroker configuration

- `dsn`: Connection string to PostgreSQL.
- `result_backend`: Custom result backend.
- `task_id_generator`: Custom task_id generator.
- `channel_name`: Name of the channel to listen on.
- `table_name`: Name of the table to store messages.
- `max_retry_attempts`: Maximum number of message processing attempts.
- `connection_kwargs`: Additional arguments for asyncpg connection.
- `pool_kwargs`: Additional arguments for asyncpg pool creation.
- `job_lock_keyspace`: Seed for the group-mutex advisory lock. Required for correctness of same-group serialization across processes: all workers on the same table must pass the same stable, signed 64-bit integer, and each distinct broker/table should use a unique value to avoid advisory-lock collisions.
- `message_ttl`: Time to live for completed messages in seconds (default: 86400).
- `stuck_message_timeout`: Time before message is considered stuck in seconds (default: 300).
- `enable_sweeping`: Enable automatic cleanup of stuck messages (default: True).
- `sweep_interval`: Interval between sweep operations in seconds (default: 60).

## Features

### Atomic Message Claiming
The broker claims each message with `SELECT ... FOR UPDATE SKIP LOCKED`, so two workers never take the same row. An in-flight message holds a heartbeat lease, and when a worker dies the sweeper requeues the message its lease was on.

### Message States
A message is `queued` while it waits, `active` while it runs, `completed` once it is acknowledged, and `dead` once it has spent its attempts.

### Group-based Coordination
Set a `group_key` label to stop related tasks from running at the same time:

```python
await my_task.kicker().with_labels(group_key="user_123").kiq()
```

### Ordered Groups
Add `ordered=True` to run a group in insertion order as well. An ordered message is claimed only once every older message in its group has finished, so a message that is delayed or retrying is not overtaken:

```python
await my_task.kicker().with_labels(group_key="user_123", ordered=True).kiq()
```

Order follows the broker's `id`, not `scheduled_at`, so a `delay` label cannot move a message ahead of its group. The label requires a `group_key` and must be a bool. It is opt-in per message, and an unlabelled message keeps the mutex-only behaviour above. Two cautions: a blocked group looks exactly like a stalled queue, and a worker on an older version of this broker ignores the column, so roll the broker out everywhere before you set the label.

A dead-lettered message halts its group: nothing behind it is claimed until it is resolved. Skipping it would drop a message out of the ordered stream with no signal. Unordered groups are unaffected and continue past dead rows as before.

Each group must be enqueued by a single producer. `id` comes from a sequence and is assigned before the transaction commits, so two producers writing the same group at once can commit ids out of order: the higher id becomes visible first and is claimed, and the lower id then arrives with nothing older left to wait for.

### Retries in Place

`OrderedRetryMiddleware` retries by requeueing the existing row instead of kicking a new
message, which would get a higher `id` and land behind its own group:

```python
from taskiq_pg import AsyncpgBroker, OrderedRetryMiddleware

broker = AsyncpgBroker(dsn).with_middlewares(OrderedRetryMiddleware(default_delay=5))

@broker.task(retry_on_error=True, max_retries=-1, group_key="user_123", ordered=True)
async def relay() -> None: ...
```

It subclasses `SmartRetryMiddleware` and takes the same labels and delay options, but
**it is not a drop-in replacement**. Read the migration note in `UPGRADE_NOTES.md` before
you swap. `default_retry_count` is inherited and does not set the cap. The budget is the
`max_retries` label, or the broker's `max_retry_attempts` without one, because the sweeper
has to cap a crashed attempt with no middleware in the loop. Both read the same two
numbers, so a message ends the same way whether it failed or crashed.

Use it instead of, not alongside, `SmartRetryMiddleware`. It needs `AsyncpgBroker` and
raises on any other. `max_retries` must be an int and is rejected at `kick` otherwise;
`-1` retries forever. Attempts come from the row, so a message that runs out is marked
dead.

Keep the worker's ack type at `when_saved` (the default) or `when_executed`. Retrying
needs the row still held; `when_received` acks it before the task runs, and retries stop
working.

### Message TTL
Control how long completed messages are retained:

```python
# Keep completed message for 1 hour
await my_task.kicker().with_labels(ttl=3600).kiq()
```

### Automatic Cleanup
A background sweep requeues messages whose heartbeat lease went stale, deletes completed
messages past their TTL, and reconnects the dequeue connection when it drops.

## Benchmarking

`bench/throughput.py` is a standalone end-to-end throughput test. It spawns a real
`taskiq worker`, enqueues N tasks, and times the drain. The task is a 50/50 mix of
CPU busy-loop and `asyncio.sleep`, so the worker is the bottleneck.

```sh
bin/pg-up
export POSTGRESQL_URL="postgresql://postgres:postgres@localhost:25432/postgres"

# 5000 tasks, 1 worker, up to 100 concurrent async tasks
uv run python -m bench.throughput -n 5000 -w 1 -m 100
# tune per-task cost (seconds): uv run python -m bench.throughput --busy-seconds 0.002 --sleep-seconds 0.01

bin/pg-down
```

```text
end-to-end : 15.363s (325.5 tasks/s)
```

Note: the broker has no per-message lock, so with `-w > 1` every worker runs every task.
Use `-w 1` for a clean number. See `--help` and the module docstring for more.

## Acknowledgements

Builds on work from [pgmq](https://github.com/oliverlambson/pgmq).
