import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncGenerator
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
)

import asyncpg
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage
from typing_extensions import override

from taskiq_pg import schema
from taskiq_pg.broker_queries import (
    CLAIM_MESSAGE_QUERY,
    CLEANUP_EXPIRED_QUERY,
    COMPLETE_MESSAGE_QUERY,
    HEARTBEAT_MESSAGES_QUERY,
    INSERT_MESSAGE_QUERY,
    MARK_DEAD_QUERY,
    RETRY_IN_PLACE_QUERY,
    SELECT_MESSAGE_QUERY,
    SWEEP_MESSAGES_QUERY,
)
from taskiq_pg.labels import ATTEMPTS_LABEL, ROW_ID_LABEL
from taskiq_pg.status import MessageStatus

_T = TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: Union[
            str, Callable[[], str]
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        connection_kwargs: Optional[dict[str, Any]] = None,
        pool_kwargs: Optional[dict[str, Any]] = None,
        job_lock_keyspace: int = 1,
        message_ttl: int = 86400,  # 24 hours default
        stuck_message_timeout: int = 300,  # heartbeat-lease TTL; 10x beat interval
        enable_sweeping: bool = True,
        sweep_interval: int = 60,  # 1 minute default
        heartbeat_interval: int = 30,  # liveness refresh period, seconds
    ) -> None:
        """
        Construct a new broker.

        :param dsn: connection string to PostgreSQL, or callable returning one.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        :param pool_kwargs: Additional arguments for asyncpg pool creation.
        :param job_lock_keyspace: Seed for the group-mutex advisory lock. Required for correctness: all workers processing the same table MUST pass the same stable, signed 64-bit integer, and each distinct broker/table should use a unique value so its group locks don't collide with other advisory-lock users on the database.
        :param message_ttl: Time to live for completed messages in seconds.
        :param stuck_message_timeout: Lease staleness before a message is reclaimed.
        :param enable_sweeping: Enable automatic reclamation of stuck messages.
        :param sweep_interval: Interval between sweep operations in seconds.
        :param heartbeat_interval: Interval between liveness refreshes in seconds.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: Union[str, Callable[[], str]] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        # Bound to a SQL param (hashtextextended int8 seed); validate at construction
        # so a bad value fails loudly instead of being swallowed by _dequeue_message.
        if not isinstance(job_lock_keyspace, int):
            raise TypeError("job_lock_keyspace must be an int")
        if job_lock_keyspace.bit_length() > 63:
            raise ValueError("job_lock_keyspace must fit a signed 64-bit integer")
        self.job_lock_keyspace: int = job_lock_keyspace  # group-mutex advisory seed
        self.message_ttl: int = message_ttl
        self.stuck_message_timeout: int = stuck_message_timeout
        self.enable_sweeping: bool = enable_sweeping
        self.sweep_interval: int = sweep_interval
        self.heartbeat_interval: int = heartbeat_interval
        self.claim_fn: str = schema.claim_fn_name(table_name)

        self.read_conn: Optional["asyncpg.Connection[asyncpg.Record]"] = None
        self.dequeue_conn: Optional["asyncpg.Connection[asyncpg.Record]"] = None
        self.write_pool: Optional["asyncpg.pool.Pool[asyncpg.Record]"] = None
        self._queue: Optional[asyncio.Queue[str]] = None
        self._sweep_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._inflight_ids: set[int] = set()  # ids this process must keep alive
        self._dequeue_lock: asyncio.Lock = asyncio.Lock()
        self._connection_lock: asyncio.Lock = asyncio.Lock()

    @property
    def dsn(self) -> str:
        """Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    @staticmethod
    def _resolve_ttl(labels: Any) -> int:
        """Per-message TTL: positive `ttl` label wins, else caller's default."""
        if isinstance(labels, str):
            with contextlib.suppress(json.JSONDecodeError):
                labels = json.loads(labels)
        if isinstance(labels, dict):
            ttl = labels.get("ttl")
            if isinstance(ttl, (int, float)) and ttl > 0:
                return int(ttl)
        return -1

    @staticmethod
    def _resolve_ordered(labels: dict[str, Any]) -> bool:
        """Opt-in FIFO. Strict: a typo must not silently unorder a queue."""
        value = labels.get("ordered")
        if value is None or isinstance(value, bool):
            return bool(value)
        if isinstance(value, str) and (text := value.strip().lower()) in {
            "true",
            "false",
        }:
            return text == "true"
        raise ValueError(f"`ordered` label must be a bool, got {value!r}")

    @override
    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        self.read_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.dequeue_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.pool_kwargs)

        if self.read_conn is None:
            msg = "read_conn not initialized"
            raise RuntimeError(msg)
        if self.dequeue_conn is None:
            msg = "dequeue_conn not initialized"
            raise RuntimeError(msg)
        if self.write_pool is None:
            msg = "write_pool not initialized"
            raise RuntimeError(msg)

        async with self.write_pool.acquire() as conn:
            await schema.ensure(conn, self.table_name, self.job_lock_keyspace)

        await self.read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self.enable_sweeping:
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    @override
    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        for task in (self._sweep_task, self._heartbeat_task):
            if task is not None:
                _ = task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        if self.read_conn is not None:
            await self.read_conn.close()
        if self.dequeue_conn is not None:
            await self.dequeue_conn.close()
        if self.write_pool is not None:
            await self.write_pool.close()

    def _notification_handler(
        self,
        _con_ref: Union[
            "asyncpg.Connection[asyncpg.Record]",
            "asyncpg.pool.PoolConnectionProxy[asyncpg.Record]",
        ],
        _pid: int,
        channel: str,
        payload: object,
        /,
    ) -> None:
        """Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **con_ref**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        logger.debug(f"Received notification on channel {channel}: {payload}")
        if self._queue is not None:
            self._queue.put_nowait(str(payload))

    @override
    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY. TTL is applied
        at completion time (see ack), not at insert, so expire_at starts NULL.

        :param message: Message to send.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before kicking.")

        async with self.write_pool.acquire() as conn:
            group_key = message.labels.get("group_key")
            ordered = self._resolve_ordered(message.labels)
            if ordered and group_key is None:
                raise ValueError("`ordered` label requires a `group_key`")
            delay_value = message.labels.get("delay")

            if delay_value is not None:
                delay_seconds = int(delay_value)
                scheduled_at_query = f"NOW() + INTERVAL '{delay_seconds} seconds'"
            else:
                scheduled_at_query = "NOW()"

            result = await conn.fetchrow(
                INSERT_MESSAGE_QUERY.format(
                    table_name=self.table_name,
                    scheduled_at=scheduled_at_query,
                ),
                message.task_id,
                message.task_name,
                message.message.decode(),
                json.dumps(message.labels),
                group_key,
                ordered,
            )

            if result is None:
                raise RuntimeError("Failed to insert message")

            message_inserted_id = result["id"]

            _ = await conn.execute(
                f"NOTIFY {self.channel_name}, '{message_inserted_id}'"
            )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self.write_pool is None:
            return
        async with self.write_pool.acquire() as conn:
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    @override
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:  # noqa: C901
        """
        Listen to the channel.

        Yields messages as they are received using proper dequeuing with locking.

        :yields: AckableMessage instances.
        """
        if self.dequeue_conn is None:
            raise ValueError("Call startup before starting listening.")
        if self._queue is None:
            raise ValueError("Startup did not initialize the queue.")

        while True:
            try:
                message_row = await self._dequeue_message()

                if message_row is None:
                    try:
                        _ = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                        message_row = await self._dequeue_message()
                    except asyncio.TimeoutError:
                        message_row = await self._dequeue_message()

                if message_row is None:
                    continue

                message_id = message_row["id"]
                attempts = message_row["retry_count"]
                # Per-message ttl label overrides broker default at completion.
                resolved_ttl = self._resolve_ttl(message_row["labels"])
                if resolved_ttl < 0:
                    resolved_ttl = self.message_ttl

                if message_row.get("message") is None:
                    msg = "Message row does not have 'message' column"
                    raise ValueError(msg)
                message_str = message_row["message"]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise ValueError(msg)
                message_data = self._inject_delivery_meta(
                    message_str, message_id, attempts
                )

                self._inflight_ids.add(message_id)

                async def ack(
                    *,
                    _message_id: int = message_id,
                    _ttl: int = resolved_ttl,
                    _attempts: int = attempts,
                ) -> None:
                    if self.write_pool is None:
                        raise ValueError("Call startup before starting listening.")

                    # Keep the lease refreshed until completion lands; discarding
                    # early lets the sweeper reclaim a mid-ack row -> dup work.
                    async with self.write_pool.acquire() as conn:
                        _ = await conn.execute(
                            COMPLETE_MESSAGE_QUERY.format(table_name=self.table_name),
                            _ttl,
                            _message_id,
                            _attempts,
                        )
                    self._inflight_ids.discard(_message_id)

                yield AckableMessage(data=message_data, ack=ack)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                continue

    def _inject_delivery_meta(self, raw: str, row_id: int, attempts: int) -> bytes:
        """Stamp row id and attempt count onto the message about to be delivered.

        Retry-in-place redelivers the stored body untouched, so the count cannot live
        in it.
        """
        message = self.formatter.loads(raw.encode())
        message.labels[ROW_ID_LABEL] = row_id
        message.labels[ATTEMPTS_LABEL] = attempts
        return self.formatter.dumps(message).message

    async def retry_in_place(self, row_id: int, attempts: int, delay: float) -> bool:
        """Hand a delivery back to the queue, keeping its id and its place in a group.

        `attempts` is what the caller was given at claim; a mismatch means the row was
        reclaimed and handed to someone else, and nothing is updated. Returns whether
        the requeue landed.
        """
        return await self._release(RETRY_IN_PLACE_QUERY, row_id, attempts, delay)

    async def mark_dead(self, row_id: int, attempts: int) -> bool:
        """Park a delivery in `dead`. Fenced like retry_in_place."""
        return await self._release(MARK_DEAD_QUERY, row_id, attempts)

    async def _release(self, query: str, row_id: int, *args: Any) -> bool:
        if self.write_pool is None:
            raise ValueError("Call startup before releasing a message.")
        # Discard first: the heartbeat must not refresh a lease we are dropping.
        self._inflight_ids.discard(row_id)
        status = await self.write_pool.execute(
            query.format(table_name=self.table_name), row_id, *args
        )
        return status.endswith(" 1")

    async def _claim_on(
        self, conn: "asyncpg.Connection[asyncpg.Record]"
    ) -> Optional[asyncpg.Record]:
        """Claim one message on `conn`, in the caller's txn. Two statements."""
        claimed_id = await conn.fetchval(
            CLAIM_MESSAGE_QUERY.format(claim_fn=self.claim_fn), self.job_lock_keyspace
        )
        if claimed_id is None:
            return None
        return await conn.fetchrow(
            SELECT_MESSAGE_QUERY.format(table_name=self.table_name), claimed_id
        )

    async def _dequeue_message(self) -> Optional[asyncpg.Record]:
        """
        Claim one message on the dedicated dequeue connection.

        Returns the message row if one is available, None otherwise.
        """
        if self.dequeue_conn is None:
            return None

        async with self._dequeue_lock:
            try:
                await self._ensure_connection_healthy()
                async with self.dequeue_conn.transaction():
                    return await self._claim_on(self.dequeue_conn)
            except Exception as e:
                logger.error(f"Error dequeuing message: {e}")
                return None

    async def _ensure_connection_healthy(self) -> None:
        """Ensure the dequeue connection is healthy, reconnect if needed."""
        if self.dequeue_conn is None:
            return

        async with self._connection_lock:
            try:
                await self.dequeue_conn.fetchval("SELECT 1")
            except Exception as e:
                logger.warning(f"Dequeue connection unhealthy, reconnecting: {e}")
                with contextlib.suppress(Exception):
                    await self.dequeue_conn.close()

                self.dequeue_conn = await asyncpg.connect(
                    self.dsn, **self.connection_kwargs
                )

    async def _heartbeat_loop(self) -> None:
        """Refresh the lease on in-flight messages so the sweep leaves them alone."""
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                if not self._inflight_ids or self.write_pool is None:
                    continue
                ids = list(self._inflight_ids)
                async with self.write_pool.acquire() as conn:
                    _ = await conn.execute(
                        HEARTBEAT_MESSAGES_QUERY.format(table_name=self.table_name), ids
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")

    async def _sweep_loop(self) -> None:
        """Background task to sweep stuck messages and clean up expired ones."""
        while True:
            try:
                await asyncio.sleep(self.sweep_interval)
                await self._sweep_stuck_messages()
                await self._cleanup_expired_messages()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in sweep loop: {e}")

    async def _sweep_stuck_messages(self) -> None:
        """Reclaim messages whose heartbeat lease went stale."""
        if self.write_pool is None:
            return

        try:
            async with self.write_pool.acquire() as conn:
                swept = await conn.fetch(
                    SWEEP_MESSAGES_QUERY.format(table_name=self.table_name),
                    self.stuck_message_timeout,
                    self.max_retry_attempts,
                )

                if swept:
                    dead = sum(
                        1 for r in swept if r["status"] == MessageStatus.DEAD.value
                    )
                    requeued = len(swept) - dead
                    logger.info(
                        f"Swept {len(swept)} stuck messages: "
                        f"{requeued} requeued, {dead} dead-lettered"
                    )

        except Exception as e:
            logger.error(f"Error sweeping stuck messages: {e}")

    async def _cleanup_expired_messages(self) -> None:
        """Clean up messages that have expired."""
        if self.write_pool is None:
            return

        try:
            async with self.write_pool.acquire() as conn:
                result = await conn.execute(
                    CLEANUP_EXPIRED_QUERY.format(table_name=self.table_name)
                )
                deleted_count = int(result.split()[-1]) if result else 0

                if deleted_count > 0:
                    logger.debug(f"Cleaned up {deleted_count} expired messages")

        except Exception as e:
            logger.error(f"Error cleaning up expired messages: {e}")
