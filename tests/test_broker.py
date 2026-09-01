import asyncio
import json
import uuid
from typing import Any, Optional

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage, TaskiqMessage
from taskiq.utils import maybe_awaitable

from taskiq_pg import AsyncpgBroker
from taskiq_pg.broker_queries import (
    COMPLETE_MESSAGE_QUERY,
    HEARTBEAT_MESSAGES_QUERY,
)
from taskiq_pg.labels import ATTEMPTS_LABEL, ROW_ID_LABEL


def make_message(
    broker: AsyncpgBroker,
    task_name: str = "test_task",
    labels: Optional[dict[str, Any]] = None,
) -> BrokerMessage:
    """Build a kickable message with a real taskiq body."""
    return broker.formatter.dumps(
        TaskiqMessage(
            task_id=uuid.uuid4().hex,
            task_name=task_name,
            labels=labels or {},
            args=[],
            kwargs={},
        )
    )


async def get_first_task(asyncpg_broker: AsyncpgBroker) -> AckableMessage:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of AsyncpgBroker.
    :return: The first AckableMessage received.
    """
    async for message in asyncpg_broker.listen():
        return message
    msg = "Unreachable"
    raise RuntimeError(msg)


@pytest.mark.anyio
async def test_kick_success(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test that messages are published and read correctly.

    We kick the message, listen to the queue, and check that
    the received message matches what was sent.
    """
    sent = make_message(asyncpg_broker, labels={"label1": "val1"})
    await asyncpg_broker.kick(sent)

    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    # Delivery stamps its own labels on, so compare the payload, not the bytes.
    received = asyncpg_broker.formatter.loads(message.data)
    assert received.task_id == sent.task_id
    assert received.task_name == sent.task_name
    assert received.labels["label1"] == "val1"

    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_startup(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test the startup process of the broker.

    We drop the messages table, restart the broker, and ensure
    that the table is recreated.
    """
    # Drop the messages table
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    await conn.execute(f"DROP TABLE IF EXISTS {asyncpg_broker.table_name}")
    await conn.close()

    # Shutdown and restart the broker
    await asyncpg_broker.shutdown()
    await asyncpg_broker.startup()

    # Verify that the table exists
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = $1
        )
        """,
        asyncpg_broker.table_name,
    )
    await conn.close()
    assert table_exists


@pytest.mark.anyio
async def test_listen(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test listen.

    Test that the broker can listen to messages inserted directly into the database
    and notified via the channel.
    """
    sent = make_message(asyncpg_broker, labels={"label1": "label_val"})

    # Insert a message directly into the database
    conn = await asyncpg.connect(dsn=asyncpg_broker.dsn)
    # For test, insert directly with NOW() for scheduled_at
    result = await conn.fetchrow(
        f"""
        INSERT INTO {asyncpg_broker.table_name}
        (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING id, lock_key
        """,
        sent.task_id,
        sent.task_name,
        sent.message.decode(),
        json.dumps(sent.labels),
        None,  # group_key
        None,  # expire_at
    )
    assert result is not None
    message_id = result["id"]
    # Send a NOTIFY with the message ID
    await conn.execute(f"NOTIFY {asyncpg_broker.channel_name}, '{message_id}'")
    await conn.close()

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    assert asyncpg_broker.formatter.loads(message.data).task_id == sent.task_id

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_unparseable_message_is_not_delivered(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A body the formatter cannot parse is never handed out."""
    conn = await asyncpg.connect(dsn=asyncpg_broker.dsn)
    result = await conn.fetchrow(
        f"""
        INSERT INTO {asyncpg_broker.table_name}
        (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING id
        """,
        "",  # Missing task_id
        "",  # Missing task_name
        "wrong",  # Message content
        json.dumps({}),  # Empty labels
        None,  # group_key
        None,  # expire_at
    )
    assert result is not None
    await conn.execute(f"NOTIFY {asyncpg_broker.channel_name}, '{result['id']}'")

    with pytest.raises(asyncio.TimeoutError):
        _ = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    status = await conn.fetchval(
        f"SELECT status FROM {asyncpg_broker.table_name} WHERE id = $1",  # noqa: S608
        result["id"],
    )
    await conn.close()
    assert status == "active"  # claimed, undeliverable, left for the sweeper


@pytest.mark.anyio
async def test_delayed_message(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that delayed messages are delivered correctly after the specified delay."""
    # Send a message with a delay
    sent = make_message(asyncpg_broker, labels={"delay": "2"})
    await asyncpg_broker.kick(sent)

    # The message will be inserted immediately but notification will be delayed
    # So we should be able to see it's queued but not get notified
    start_time = asyncio.get_event_loop().time()

    # Wait for the notification (should take ~2 seconds)
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=3.0)
    elapsed = asyncio.get_event_loop().time() - start_time

    # Check that it took at least 1.5 seconds (allowing some margin)
    assert elapsed >= 1.5, f"Message arrived too quickly: {elapsed}s"
    assert asyncpg_broker.formatter.loads(message.data).task_id == sent.task_id

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_group_key_coordination(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that messages with the same group_key are not processed concurrently."""
    # Send two messages with the same group_key
    group_key = "test_group_123"

    sent1 = make_message(asyncpg_broker, "test_task_1", {"group_key": group_key})
    sent2 = make_message(asyncpg_broker, "test_task_2", {"group_key": group_key})

    await asyncpg_broker.kick(sent1)
    await asyncpg_broker.kick(sent2)

    # Create two listeners to simulate concurrent workers
    async def get_message_with_timeout(timeout: float) -> Optional[AckableMessage]:
        try:
            return await asyncio.wait_for(
                get_first_task(asyncpg_broker), timeout=timeout
            )
        except asyncio.TimeoutError:
            return None

    # Start two concurrent tasks to get messages
    task1 = asyncio.create_task(get_message_with_timeout(1.0))
    await asyncio.sleep(0.1)  # Small delay to ensure first task starts
    task2 = asyncio.create_task(get_message_with_timeout(0.5))

    # Wait for both tasks
    msg1, msg2 = await asyncio.gather(task1, task2)

    # One should get a message, the other should timeout
    assert msg1 is not None
    assert msg2 is None

    # Acknowledge the first message
    await maybe_awaitable(msg1.ack())

    # Now the second message should be available
    message2 = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    await maybe_awaitable(message2.ack())


@pytest.mark.anyio
async def test_message_ttl(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that messages respect TTL settings."""
    # Send a message with a short TTL
    sent = make_message(asyncpg_broker, labels={"ttl": 2})
    await asyncpg_broker.kick(sent)

    # Receive and acknowledge the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    await maybe_awaitable(message.ack())

    # Check that the message is marked as completed with expire_at set
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    row = await conn.fetchrow(
        f"SELECT status, expire_at FROM {asyncpg_broker.table_name} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    await conn.close()

    assert row is not None
    assert row["status"] == "completed"
    assert row["expire_at"] is not None


@pytest.mark.anyio
async def test_dequeue_stamps_heartbeat(asyncpg_broker: AsyncpgBroker) -> None:
    """A claim sets status=active, stamps heartbeat_at and counts the attempt."""
    tbl = asyncpg_broker.table_name
    sent = make_message(asyncpg_broker)
    await asyncpg_broker.kick(sent)
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    assert asyncpg_broker.write_pool is not None
    row = await asyncpg_broker.write_pool.fetchrow(
        f"SELECT status, heartbeat_at, retry_count FROM {tbl} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    assert row is not None
    assert row["status"] == "active"
    assert row["heartbeat_at"] is not None
    assert row["retry_count"] == 1

    await maybe_awaitable(message.ack())
    after = await asyncpg_broker.write_pool.fetchval(
        f"SELECT retry_count FROM {tbl} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    assert after == 1  # acking is not an attempt


@pytest.mark.anyio
async def test_delivery_carries_row_id_and_attempts(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """The middleware learns which row it holds, and how many attempts it has had."""
    await asyncpg_broker.kick(make_message(asyncpg_broker))
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    labels = asyncpg_broker.formatter.loads(message.data).labels
    assert asyncpg_broker.write_pool is not None
    row_id = await asyncpg_broker.write_pool.fetchval(
        f"SELECT id FROM {asyncpg_broker.table_name}"  # noqa: S608
    )
    assert labels[ROW_ID_LABEL] == row_id
    assert labels[ATTEMPTS_LABEL] == 1

    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_retry_in_place_keeps_the_row(asyncpg_broker: AsyncpgBroker) -> None:
    """A requeued delivery keeps its id and comes back due after the delay."""
    tbl = asyncpg_broker.table_name
    await asyncpg_broker.kick(make_message(asyncpg_broker))
    claimed = await asyncpg_broker._dequeue_message()
    assert claimed is not None

    assert await asyncpg_broker.retry_in_place(int(claimed["id"]), 1, 30.0)

    assert asyncpg_broker.write_pool is not None
    row = await asyncpg_broker.write_pool.fetchrow(
        f"SELECT id, status, retry_count, heartbeat_at, scheduled_at > NOW() AS later "  # noqa: S608
        f"FROM {tbl}"
    )
    assert row is not None
    assert row["id"] == claimed["id"]
    assert row["status"] == "queued"
    assert row["retry_count"] == 1  # the next claim counts the next attempt
    assert row["heartbeat_at"] is None
    assert row["later"] is True
    assert int(claimed["id"]) not in asyncpg_broker._inflight_ids


@pytest.mark.anyio
async def test_release_is_fenced_on_attempts(asyncpg_broker: AsyncpgBroker) -> None:
    """A worker whose row was handed to someone else can neither requeue nor kill it."""
    await asyncpg_broker.kick(make_message(asyncpg_broker))
    claimed = await asyncpg_broker._dequeue_message()
    assert claimed is not None
    row_id = int(claimed["id"])
    stale = int(claimed["retry_count"]) - 1

    assert not await asyncpg_broker.retry_in_place(row_id, stale, 0.0)
    assert not await asyncpg_broker.mark_dead(row_id, stale)

    assert asyncpg_broker.write_pool is not None
    status = await asyncpg_broker.write_pool.fetchval(
        f"SELECT status FROM {asyncpg_broker.table_name} WHERE id = $1",  # noqa: S608
        row_id,
    )
    assert status == "active"


async def _insert_active(
    conn: "asyncpg.Connection[asyncpg.Record]",
    tbl: str,
    name: str,
    heartbeat_sql: str,
) -> int:
    """Insert an active row with explicit heartbeat_at; return its id."""
    return int(
        await conn.fetchval(
            f"INSERT INTO {tbl} "  # noqa: S608
            "(task_id, task_name, message, labels, status, heartbeat_at) "
            f"VALUES ($1, $2, 'x', '{{}}'::jsonb, 'active', {heartbeat_sql}) "
            "RETURNING id",
            uuid.uuid4().hex,
            name,
        )
    )


@pytest.mark.anyio
async def test_sweep_reclaims_stale_lease(asyncpg_broker: AsyncpgBroker) -> None:
    """Sweep requeues active rows with a stale heartbeat, leaving the count alone."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    stale_id = await _insert_active(conn, tbl, "stale", "NOW() - INTERVAL '10 seconds'")
    fresh_id = await _insert_active(conn, tbl, "fresh", "NOW()")

    await asyncpg_broker._sweep_stuck_messages()

    stale = await conn.fetchrow(
        f"SELECT status, retry_count FROM {tbl} WHERE id = $1",  # noqa: S608
        stale_id,
    )
    fresh = await conn.fetchrow(
        f"SELECT status FROM {tbl} WHERE id = $1",  # noqa: S608
        fresh_id,
    )
    await conn.close()
    assert stale is not None and stale["status"] == "queued"
    assert stale["retry_count"] == 0  # the next claim counts it, not the sweep
    assert fresh is not None and fresh["status"] == "active"


@pytest.mark.anyio
async def test_heartbeat_refreshes_inflight(asyncpg_broker: AsyncpgBroker) -> None:
    """Heartbeat query advances heartbeat_at only for active in-flight ids."""
    tbl = asyncpg_broker.table_name
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    msg_id = await _insert_active(conn, tbl, "t", "NOW() - INTERVAL '10 seconds'")

    assert asyncpg_broker.write_pool is not None
    await asyncpg_broker.write_pool.execute(
        HEARTBEAT_MESSAGES_QUERY.format(table_name=asyncpg_broker.table_name),
        [msg_id],
    )
    fresh = await conn.fetchval(
        f"SELECT heartbeat_at > NOW() - INTERVAL '2 seconds' "  # noqa: S608
        f"FROM {tbl} WHERE id = $1",
        msg_id,
    )
    await conn.close()
    assert fresh is True


@pytest.mark.anyio
async def test_kick_leaves_expire_at_null_until_ack(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """TTL is applied at completion, not at insert: expire_at is NULL while queued."""
    tbl = asyncpg_broker.table_name
    sent = make_message(asyncpg_broker, "t", {"ttl": 5})
    await asyncpg_broker.kick(sent)

    conn = await asyncpg.connect(asyncpg_broker.dsn)
    before = await conn.fetchrow(
        f"SELECT status, expire_at FROM {tbl} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    assert before is not None
    assert before["status"] == "queued"
    assert before["expire_at"] is None  # not stamped at insert

    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    await maybe_awaitable(message.ack())

    after = await conn.fetchrow(
        f"SELECT status, expire_at FROM {tbl} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    await conn.close()
    assert after is not None
    assert after["status"] == "completed"
    assert after["expire_at"] is not None  # stamped at completion


@pytest.mark.anyio
async def test_null_heartbeat_active_row_not_swept(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Mixed-rollout safety: legacy active rows (NULL heartbeat) survive sweep."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    row_id = await _insert_active(conn, tbl, "legacy", "NULL")

    await asyncpg_broker._sweep_stuck_messages()

    row = await conn.fetchrow(
        f"SELECT status FROM {tbl} WHERE id = $1",  # noqa: S608
        row_id,
    )
    await conn.close()
    assert row is not None
    assert row["status"] == "active"  # NULL < threshold is false -> left alone


@pytest.mark.anyio
async def test_attempts_count_deliveries_whatever_ended_them(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A crashed attempt and a requeued one draw on the same budget."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    assert asyncpg_broker.write_pool is not None
    await asyncpg_broker.kick(
        BrokerMessage(task_id=uuid.uuid4().hex, task_name="t", message=b"x", labels={})
    )

    first = await asyncpg_broker._dequeue_message()
    assert first is not None
    row_id = int(first["id"])

    # ended by a crash: lease goes stale, sweeper requeues it
    _ = await asyncpg_broker.write_pool.execute(
        f"UPDATE {tbl} SET heartbeat_at = NOW() - INTERVAL '10 seconds' "  # noqa: S608
        "WHERE id = $1",
        row_id,
    )
    await asyncpg_broker._sweep_stuck_messages()
    assert await asyncpg_broker._dequeue_message() is not None

    # ended by a failure the middleware requeues in place
    _ = await asyncpg_broker.write_pool.execute(
        f"UPDATE {tbl} SET status = 'queued', heartbeat_at = NULL "  # noqa: S608
        "WHERE id = $1",
        row_id,
    )
    assert await asyncpg_broker._dequeue_message() is not None

    attempts = await asyncpg_broker.write_pool.fetchval(
        f"SELECT retry_count FROM {tbl} WHERE id = $1",  # noqa: S608
        row_id,
    )
    assert attempts == 3  # one per delivery, regardless of what ended it


@pytest.mark.anyio
async def test_sweep_dead_letters_at_max_retry_attempts(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A row that burns max_retry_attempts is parked in 'dead', not requeued."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    asyncpg_broker.max_retry_attempts = 2
    assert asyncpg_broker.write_pool is not None
    await asyncpg_broker.kick(
        BrokerMessage(task_id=uuid.uuid4().hex, task_name="t", message=b"x", labels={})
    )

    async def claim_then_die() -> None:
        assert asyncpg_broker.write_pool is not None
        claimed = await asyncpg_broker._dequeue_message()
        assert claimed is not None
        _ = await asyncpg_broker.write_pool.execute(
            f"UPDATE {tbl} SET heartbeat_at = NOW() - INTERVAL '10 seconds' "  # noqa: S608
            "WHERE id = $1",
            claimed["id"],
        )
        await asyncpg_broker._sweep_stuck_messages()

    await claim_then_die()  # attempt 1 of 2 -> requeued
    row = await asyncpg_broker.write_pool.fetchrow(
        f"SELECT status, retry_count FROM {tbl}"  # noqa: S608
    )
    assert row is not None and row["status"] == "queued"
    assert row["retry_count"] == 1

    await claim_then_die()  # attempt 2 of 2 -> dead
    row = await asyncpg_broker.write_pool.fetchrow(
        f"SELECT status, retry_count FROM {tbl}"  # noqa: S608
    )
    assert row is not None and row["status"] == "dead"
    assert row["retry_count"] == 2


async def _sweep_with_cap(
    asyncpg_broker: AsyncpgBroker, cap: Any, attempts: int
) -> str:
    """Strand a row on `attempts` deliveries under a `max_retries` label; sweep it."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    assert asyncpg_broker.write_pool is not None
    await asyncpg_broker.kick(make_message(asyncpg_broker, labels={"max_retries": cap}))
    _ = await asyncpg_broker.write_pool.execute(
        f"UPDATE {tbl} SET status = 'active', retry_count = $1, "  # noqa: S608
        "heartbeat_at = NOW() - INTERVAL '10 seconds'",
        attempts,
    )

    await asyncpg_broker._sweep_stuck_messages()

    return str(await asyncpg_broker.write_pool.fetchval(f"SELECT status FROM {tbl}"))  # noqa: S608


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("cap", "default", "attempts", "expected"),
    [
        (2, 10, 2, "dead"),  # the label overrules a laxer default
        (-1, 1, 99, "queued"),  # forever: no attempt count exhausts it
        (None, 5, 5, "dead"),  # absent: the broker's cap applies
    ],
)
async def test_sweep_caps_attempts_by_label(
    asyncpg_broker: AsyncpgBroker, cap: Any, default: int, attempts: int, expected: str
) -> None:
    """`max_retries` wins over the broker default when the label is set."""
    asyncpg_broker.max_retry_attempts = default
    assert await _sweep_with_cap(asyncpg_broker, cap, attempts) == expected


@pytest.mark.anyio
@pytest.mark.parametrize("cap", ["many", True, 1.5, "9" * 10, -(2**31) - 1])
async def test_max_retries_label_validation(
    asyncpg_broker: AsyncpgBroker, cap: Any
) -> None:
    """The sweeper casts the label to INTEGER, so a bad one fails at enqueue."""
    with pytest.raises(ValueError):
        await asyncpg_broker.kick(
            make_message(asyncpg_broker, labels={"max_retries": cap})
        )


@pytest.mark.anyio
async def test_dead_row_not_dequeued(asyncpg_broker: AsyncpgBroker) -> None:
    """Dead-lettered rows are terminal: dequeue must skip them."""
    tbl = asyncpg_broker.table_name
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    dead_id = await conn.fetchval(
        f"INSERT INTO {tbl} "  # noqa: S608
        "(task_id, task_name, message, labels, status) "
        "VALUES ($1, 'dead', 'x', '{}'::jsonb, 'dead') RETURNING id",
        uuid.uuid4().hex,
    )
    await conn.close()

    claimed = await asyncpg_broker._dequeue_message()
    assert claimed is None or claimed["id"] != dead_id


@pytest.mark.anyio
async def test_heartbeat_prevents_reclaim_of_live_claim(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Fresh-heartbeat claim survives sweep; a stale one is reclaimed.

    Mirrors the bench liveness scenario without a real worker process.
    """
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    await asyncpg_broker.kick(
        BrokerMessage(task_id=uuid.uuid4().hex, task_name="t", message=b"x", labels={})
    )
    claimed = await asyncpg_broker._dequeue_message()  # sets active + heartbeat=NOW()
    assert claimed is not None
    mid = claimed["id"]

    # Fresh lease -> not reclaimed even with a 1s timeout.
    await asyncpg_broker._sweep_stuck_messages()
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    still = await conn.fetchval(
        f"SELECT status FROM {tbl} WHERE id = $1",  # noqa: S608
        mid,
    )
    assert still == "active"

    # Age the lease past the timeout -> reclaimed.
    await conn.execute(
        f"UPDATE {tbl} SET heartbeat_at = NOW() - INTERVAL '10 seconds' "  # noqa: S608
        "WHERE id = $1",
        mid,
    )
    await asyncpg_broker._sweep_stuck_messages()
    reclaimed = await conn.fetchrow(
        f"SELECT status, retry_count FROM {tbl} WHERE id = $1",  # noqa: S608
        mid,
    )
    await conn.close()
    assert reclaimed is not None
    assert reclaimed["status"] == "queued"
    assert reclaimed["retry_count"] == 1


@pytest.mark.anyio
async def test_dead_worker_reclaim_and_reprocess(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """End-to-end reclaim: claim -> die -> sweep requeues -> re-claim -> complete."""
    tbl = asyncpg_broker.table_name
    asyncpg_broker.stuck_message_timeout = 1
    await asyncpg_broker.kick(
        BrokerMessage(task_id=uuid.uuid4().hex, task_name="t", message=b"x", labels={})
    )

    first = await asyncpg_broker._dequeue_message()  # worker A claims
    assert first is not None
    mid = first["id"]

    # Worker A dies mid-flight: freeze/age its heartbeat, never acks.
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    await conn.execute(
        f"UPDATE {tbl} SET heartbeat_at = NOW() - INTERVAL '10 seconds' "  # noqa: S608
        "WHERE id = $1",
        mid,
    )

    await asyncpg_broker._sweep_stuck_messages()  # sweeper requeues it
    requeued = await conn.fetchrow(
        f"SELECT status, retry_count FROM {tbl} WHERE id = $1",  # noqa: S608
        mid,
    )
    assert requeued is not None
    assert requeued["status"] == "queued"
    assert requeued["retry_count"] == 1

    second = await asyncpg_broker._dequeue_message()  # worker B re-claims same id
    assert second is not None
    assert second["id"] == mid

    assert asyncpg_broker.write_pool is not None
    _ = await asyncpg_broker.write_pool.execute(
        COMPLETE_MESSAGE_QUERY.format(table_name=tbl),
        asyncpg_broker.message_ttl,
        mid,
        second["retry_count"],
    )
    final = await conn.fetchval(
        f"SELECT status FROM {tbl} WHERE id = $1",  # noqa: S608
        mid,
    )
    await conn.close()
    assert final == "completed"


@pytest.mark.anyio
async def test_concurrent_claim_exclusivity(asyncpg_broker: AsyncpgBroker) -> None:
    """Two competing consumers claim a shared backlog with no double-claim, no loss.

    Mirrors the multi-worker bench: FOR UPDATE SKIP LOCKED must hand each message
    to exactly one consumer, and each delivery must be counted exactly once --
    a candidate the claim loop rejects or loses a race for must not be charged.
    """
    backlog = 60
    for _ in range(backlog):
        await asyncpg_broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex, task_name="t", message=b"x", labels={}
            )
        )

    async def drain(claimed: list[int]) -> None:
        conn = await asyncpg.connect(asyncpg_broker.dsn)
        try:
            while True:
                async with conn.transaction():
                    row = await asyncpg_broker._claim_on(conn)
                if row is None:
                    return
                claimed.append(int(row["id"]))
                await asyncio.sleep(0)  # yield so the two drainers interleave
        finally:
            await conn.close()

    a: list[int] = []
    b: list[int] = []
    await asyncio.gather(drain(a), drain(b))

    all_ids = a + b
    assert len(all_ids) == backlog  # nothing lost
    assert len(set(all_ids)) == backlog  # nothing claimed twice
    assert a and b  # both consumers actually did work

    assert asyncpg_broker.write_pool is not None
    counts = await asyncpg_broker.write_pool.fetch(
        f"SELECT retry_count FROM {asyncpg_broker.table_name}"  # noqa: S608
    )
    assert [row["retry_count"] for row in counts] == [1] * backlog


@pytest.mark.anyio
async def test_group_mutex_across_connections(asyncpg_broker: AsyncpgBroker) -> None:
    """Cross-process group mutex, deterministic interleaving.

    Two asyncpg connections = two backends, i.e. two workers as far as Postgres and
    the advisory lock are concerned. We drive the race window by hand (hold A's txn
    open, run B inside it) instead of gather() so the failure is reproducible rather
    than timing-dependent.

    Without the advisory lock both connections' READ COMMITTED snapshots see the
    other's row as 'queued' during the check-and-set window and both promote. With it,
    B's pg_try_advisory_xact_lock fails while A's txn holds it, so B claims nothing.
    """
    tbl = asyncpg_broker.table_name
    group_key = "grp_race"

    for _ in range(2):
        await asyncpg_broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex,
                task_name="t",
                message=b"x",
                labels={"group_key": group_key},
            )
        )

    conn_a = await asyncpg.connect(asyncpg_broker.dsn)
    conn_b = await asyncpg.connect(asyncpg_broker.dsn)
    try:
        tx_a = conn_a.transaction()
        tx_b = conn_b.transaction()
        await tx_a.start()
        await tx_b.start()

        # A claims a row of the group and holds the advisory lock (txn still open).
        row_a = await asyncpg_broker._claim_on(conn_a)
        assert row_a is not None

        # B runs while A is uncommitted: A's row still looks 'queued' to B, but the
        # group's advisory lock is held -> B must claim nothing.
        row_b = await asyncpg_broker._claim_on(conn_b)
        assert row_b is None

        await tx_a.commit()
        await tx_b.commit()

        # Group now has an active row -> the NOT IN(active) guard keeps the second
        # queued row parked even after A's lock is released.
        row_b2 = await asyncpg_broker._claim_on(conn_b)
        assert row_b2 is None

        # Complete A's row; the group frees and the second row becomes claimable.
        assert asyncpg_broker.write_pool is not None
        _ = await asyncpg_broker.write_pool.execute(
            COMPLETE_MESSAGE_QUERY.format(table_name=tbl),
            asyncpg_broker.message_ttl,
            row_a["id"],
            row_a["retry_count"],
        )
        async with conn_b.transaction():
            row_b3 = await asyncpg_broker._claim_on(conn_b)
        assert row_b3 is not None
        assert row_b3["id"] != row_a["id"]
    finally:
        await conn_a.close()
        await conn_b.close()


@pytest.mark.anyio
async def test_group_mutex_concurrent_workers(asyncpg_broker: AsyncpgBroker) -> None:
    """Actual contention: N workers on N connections hammer one group at once.

    All rows share a group_key, so the mutex allows at most one active at a time.
    Each worker: dequeue -> (hold the claim briefly) -> complete, in a loop until the
    backlog drains. The invariant checked live is that no two workers ever hold a
    claim of the group simultaneously; the totals confirm nothing is lost or
    double-claimed.
    """
    tbl = asyncpg_broker.table_name
    group_key = "grp_concurrent"
    backlog = 30
    workers = 8

    for _ in range(backlog):
        await asyncpg_broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex,
                task_name="t",
                message=b"x",
                labels={"group_key": group_key},
            )
        )

    complete_sql = COMPLETE_MESSAGE_QUERY.format(table_name=tbl)
    ttl = asyncpg_broker.message_ttl

    active_now = 0
    max_concurrent = 0
    claimed: list[int] = []

    async def worker() -> None:
        nonlocal active_now, max_concurrent
        conn = await asyncpg.connect(asyncpg_broker.dsn)
        try:
            while True:
                async with conn.transaction():
                    row = await asyncpg_broker._claim_on(conn)
                if row is None:
                    # Either drained, or the single active slot is taken. Distinguish:
                    # if any row is still queued, back off and retry; else stop.
                    remaining = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {tbl} "  # noqa: S608
                        "WHERE status = 'queued' AND group_key = $1",
                        group_key,
                    )
                    if remaining == 0:
                        return
                    await asyncio.sleep(0.01)
                    continue

                active_now += 1
                max_concurrent = max(max_concurrent, active_now)
                claimed.append(int(row["id"]))
                await asyncio.sleep(0)  # force a scheduling point while "active"
                active_now -= 1
                await conn.execute(complete_sql, ttl, row["id"], row["retry_count"])
        finally:
            await conn.close()

    await asyncio.gather(*(worker() for _ in range(workers)))

    assert max_concurrent == 1  # group mutex held: never two active at once
    assert len(claimed) == backlog  # nothing lost
    assert len(set(claimed)) == backlog  # nothing claimed twice


async def _kick_group(
    broker: AsyncpgBroker,
    group_key: str,
    name: str,
    *,
    ordered: bool,
    delay: Optional[int] = None,
) -> None:
    """Kick one grouped message, optionally due in the past/future."""
    labels: dict[str, object] = {"group_key": group_key, "ordered": ordered}
    if delay is not None:
        labels["delay"] = delay
    await broker.kick(
        BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name=name,
            message=name.encode(),
            labels=labels,
        )
    )


async def _kick(broker: AsyncpgBroker, name: str, group_key: Optional[str]) -> None:
    labels = {} if group_key is None else {"group_key": group_key}
    await broker.kick(
        BrokerMessage(
            task_id=uuid.uuid4().hex, task_name=name, message=b"x", labels=labels
        )
    )


async def _complete(broker: AsyncpgBroker, row_id: int, attempts: int = 1) -> None:
    """Ack a claimed row so its group frees up."""
    assert broker.write_pool is not None
    _ = await broker.write_pool.execute(
        COMPLETE_MESSAGE_QUERY.format(table_name=broker.table_name),
        broker.message_ttl,
        row_id,
        attempts,
    )


async def _scramble_timestamps(broker: AsyncpgBroker, ids: list[int]) -> None:
    """Push every row into the past, timestamps disagreeing with id order.

    Everything is due, so only the dequeue predicates can block a claim.
    """
    assert broker.write_pool is not None
    offsets = [3, 41, 7, 100, 20]
    assert len(offsets) == len(ids)
    for row_id, offset in zip(ids, offsets):
        _ = await broker.write_pool.execute(
            f"UPDATE {broker.table_name} "  # noqa: S608
            f"SET scheduled_at = NOW() - ($1 * INTERVAL '1 second'), "
            f"created_at = NOW() - ($2 * INTERVAL '1 second') WHERE id = $3",
            offset,
            100 - offset,
            row_id,
        )


@pytest.mark.anyio
async def test_ordered_group_blocks_behind_undue_head(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A younger sibling must not overtake an older row that isn't due yet.

    Nothing is active, so the group mutex alone would hand over 'second'.
    """
    group_key = "ordered_undue_head"
    await _kick_group(asyncpg_broker, group_key, "first", ordered=True, delay=30)
    await _kick_group(asyncpg_broker, group_key, "second", ordered=True)
    await _kick_group(asyncpg_broker, group_key, "third", ordered=True)

    assert await asyncpg_broker._dequeue_message() is None


@pytest.mark.anyio
async def test_ordered_group_drains_by_id(asyncpg_broker: AsyncpgBroker) -> None:
    """Claim order follows id even when both timestamps say otherwise."""
    group_key = "ordered_drain"
    for i in range(5):
        await _kick_group(asyncpg_broker, group_key, f"m{i}", ordered=True)

    assert asyncpg_broker.write_pool is not None
    ids = [
        int(r["id"])
        for r in await asyncpg_broker.write_pool.fetch(
            f"SELECT id FROM {asyncpg_broker.table_name} ORDER BY id"  # noqa: S608
        )
    ]
    await _scramble_timestamps(asyncpg_broker, ids)

    for expected_id in ids:
        row = await asyncpg_broker._dequeue_message()
        assert row is not None
        assert int(row["id"]) == expected_id
        assert await asyncpg_broker._dequeue_message() is None  # mutex still holds
        await _complete(asyncpg_broker, expected_id)


@pytest.mark.anyio
async def test_unordered_group_drains_by_schedule(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Opt-in is inert: without `ordered`, claim order is by scheduled_at as before."""
    group_key = "unordered_drain"
    for i in range(5):
        await _kick_group(asyncpg_broker, group_key, f"m{i}", ordered=False)

    assert asyncpg_broker.write_pool is not None
    ids = [
        int(r["id"])
        for r in await asyncpg_broker.write_pool.fetch(
            f"SELECT id FROM {asyncpg_broker.table_name} ORDER BY id"  # noqa: S608
        )
    ]
    await _scramble_timestamps(asyncpg_broker, ids)

    # offsets [3, 41, 7, 100, 20] seconds ago -> oldest scheduled_at first.
    expected = [ids[3], ids[1], ids[4], ids[2], ids[0]]
    for expected_id in expected:
        row = await asyncpg_broker._dequeue_message()
        assert row is not None
        assert int(row["id"]) == expected_id
        await _complete(asyncpg_broker, expected_id)


@pytest.mark.anyio
async def test_ordered_group_does_not_block_other_groups(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Head-of-line blocking is per group, not global."""
    await _kick_group(asyncpg_broker, "blocked", "held", ordered=True, delay=30)
    await _kick_group(asyncpg_broker, "blocked", "waiting", ordered=True)
    await _kick_group(asyncpg_broker, "free", "runnable", ordered=True)
    await asyncpg_broker.kick(
        BrokerMessage(
            task_id=uuid.uuid4().hex, task_name="ungrouped", message=b"x", labels={}
        )
    )

    claimed = set()
    while (row := await asyncpg_broker._dequeue_message()) is not None:
        claimed.add(str(row["task_name"]))
        await _complete(asyncpg_broker, int(row["id"]), int(row["retry_count"]))

    assert claimed == {"runnable", "ungrouped"}


@pytest.mark.anyio
async def _kill(broker: AsyncpgBroker, row_id: int) -> None:
    """Dead-letter a row, as the sweeper does at max_retry_attempts."""
    assert broker.write_pool is not None
    _ = await broker.write_pool.execute(
        f"UPDATE {broker.table_name} SET status = 'dead' WHERE id = $1",  # noqa: S608
        row_id,
    )


@pytest.mark.anyio
async def test_swept_ordered_row_keeps_its_slot(asyncpg_broker: AsyncpgBroker) -> None:
    """The sweeper moves rows back to 'queued' behind the dequeue's back."""
    group_key = "swept"
    await _kick_group(asyncpg_broker, group_key, "first", ordered=True)
    # 'second' sorts first by scheduled_at, so only the ordering clause holds it back
    await _kick_group(asyncpg_broker, group_key, "second", ordered=True, delay=-5)

    first = await asyncpg_broker._dequeue_message()
    assert first is not None
    assert first["task_name"] == "first"

    assert asyncpg_broker.write_pool is not None
    _ = await asyncpg_broker.write_pool.execute(
        f"UPDATE {asyncpg_broker.table_name} "  # noqa: S608
        "SET heartbeat_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
        first["id"],
    )
    await asyncpg_broker._sweep_stuck_messages()

    row = await asyncpg_broker._dequeue_message()
    assert row is not None
    assert row["task_name"] == "first"  # reclaimed head, not overtaken by 'second'


@pytest.mark.anyio
async def test_dead_row_halts_its_ordered_group(asyncpg_broker: AsyncpgBroker) -> None:
    """A dead row must not be skipped: the group stops rather than losing order."""
    group_key = "halted"
    await _kick_group(asyncpg_broker, group_key, "first", ordered=True)
    await _kick_group(asyncpg_broker, group_key, "second", ordered=True)

    first = await asyncpg_broker._dequeue_message()
    assert first is not None
    await _kill(asyncpg_broker, int(first["id"]))

    assert await asyncpg_broker._dequeue_message() is None


@pytest.mark.anyio
async def test_dead_row_does_not_halt_an_unordered_group(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Halting is opt-in with `ordered`; mutex-only groups keep skipping dead rows."""
    group_key = "not_halted"
    await _kick_group(asyncpg_broker, group_key, "first", ordered=False)
    await _kick_group(asyncpg_broker, group_key, "second", ordered=False)

    first = await asyncpg_broker._dequeue_message()
    assert first is not None
    await _kill(asyncpg_broker, int(first["id"]))

    second = await asyncpg_broker._dequeue_message()
    assert second is not None
    assert second["task_name"] == "second"


@pytest.mark.anyio
async def test_dead_row_halts_only_its_own_group(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A halted group must not stop the rest of the queue."""
    await _kick_group(asyncpg_broker, "halted", "doomed", ordered=True)
    await _kick_group(asyncpg_broker, "halted", "stuck", ordered=True)
    await _kick_group(asyncpg_broker, "healthy", "runnable", ordered=True)

    doomed = await asyncpg_broker._dequeue_message()
    assert doomed is not None
    await _kill(asyncpg_broker, int(doomed["id"]))

    claimed = set()
    while (row := await asyncpg_broker._dequeue_message()) is not None:
        claimed.add(str(row["task_name"]))
        await _complete(asyncpg_broker, int(row["id"]), int(row["retry_count"]))
    assert claimed == {"runnable"}


@pytest.mark.anyio
async def test_ordered_label_validation(asyncpg_broker: AsyncpgBroker) -> None:
    """`ordered` is strict: it needs a group, and it will not guess at a value."""
    with pytest.raises(ValueError, match="requires a `group_key`"):
        await asyncpg_broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex,
                task_name="t",
                message=b"x",
                labels={"ordered": True},
            )
        )
    with pytest.raises(ValueError, match="must be a bool"):
        await asyncpg_broker.kick(
            BrokerMessage(
                task_id=uuid.uuid4().hex,
                task_name="t",
                message=b"x",
                labels={"group_key": "g", "ordered": "yes"},
            )
        )
    assert AsyncpgBroker._resolve_ordered({"ordered": "True"}) is True
    assert AsyncpgBroker._resolve_ordered({"ordered": "false"}) is False
    assert AsyncpgBroker._resolve_ordered({}) is False


@pytest.mark.anyio
async def test_busy_group_does_not_block_ungrouped(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Ungrouped rows skip the mutex and the advisory lock."""
    await _kick(asyncpg_broker, "grouped", "g")
    await _kick(asyncpg_broker, "ungrouped", None)

    first = await asyncpg_broker._dequeue_message()
    assert first is not None
    assert first["group_key"] == "g"

    second = await asyncpg_broker._dequeue_message()
    assert second is not None
    assert second["group_key"] is None


@pytest.mark.anyio
async def test_claim_returns_none_when_every_group_is_busy(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Rejected candidates must be skipped, not re-picked: a stuck cursor spins."""
    for group_key in ("a", "b"):
        for _ in range(2):
            await _kick(asyncpg_broker, "t", group_key)

    claimed = [await asyncpg_broker._dequeue_message() for _ in range(2)]
    assert {row["group_key"] for row in claimed if row is not None} == {"a", "b"}

    # wait_for turns a spin inside the claim loop into a failure instead of a hang
    assert await asyncio.wait_for(asyncpg_broker._dequeue_message(), timeout=10) is None


@pytest.mark.anyio
async def test_group_with_held_advisory_lock_is_skipped(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """A group whose advisory lock is held elsewhere is passed over, not waited on."""
    await _kick(asyncpg_broker, "locked", "locked_group")
    await _kick(asyncpg_broker, "free", "free_group")

    holder = await asyncpg.connect(asyncpg_broker.dsn)
    try:
        await holder.execute(
            "SELECT pg_advisory_lock(hashtextextended($1, $2))",
            "locked_group",
            asyncpg_broker.job_lock_keyspace,
        )
        row = await asyncpg_broker._dequeue_message()
        assert row is not None
        assert row["group_key"] == "free_group"
        assert await asyncpg_broker._dequeue_message() is None
    finally:
        await holder.execute("SELECT pg_advisory_unlock_all()")
        await holder.close()


@pytest.mark.anyio
async def test_job_lock_keyspace_validation(postgresql_dsn: str) -> None:
    """Bad job_lock_keyspace fails at construction, not silently at dequeue."""
    with pytest.raises(TypeError):
        AsyncpgBroker(dsn=postgresql_dsn, job_lock_keyspace="1")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="signed 64-bit"):
        AsyncpgBroker(dsn=postgresql_dsn, job_lock_keyspace=2**63)
