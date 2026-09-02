# ERRATA

Known defects, deferred out of the heartbeat-lease / `FOR UPDATE SKIP LOCKED` PR.
Severity order.

1. **Health check on every dequeue.** `_dequeue_message` runs `SELECT 1` before every
   claim. That is an extra round-trip per message on the serialized `dequeue_conn`, on a
   branch whose point is throughput.

2. **`_queue` grows unbounded under sustained backlog.** The queue is now only a
   wakeup signal, but every NOTIFY still `put_nowait`s while `listen()` calls
   `get()` only when a dequeue returns None. Under sustained load `get()` never
   runs and kicks accumulate permanently.
