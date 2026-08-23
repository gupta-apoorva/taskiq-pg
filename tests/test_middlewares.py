import asyncio
import logging
import uuid

import pytest
from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq.exceptions import NoResultError
from taskiq.utils import maybe_awaitable

from taskiq_pg import AsyncpgBroker
from taskiq_pg.labels import ATTEMPTS_LABEL, ROW_ID_LABEL
from taskiq_pg.middlewares import OrderedRetryMiddleware


def _delivered(broker: AsyncpgBroker, data: bytes) -> TaskiqMessage:
    message = broker.formatter.loads(data)
    message.parse_labels()
    return message


async def _kick(broker: AsyncpgBroker, labels: dict[str, object]) -> None:
    await broker.kick(
        broker.formatter.dumps(
            TaskiqMessage(
                task_id=uuid.uuid4().hex,
                task_name="t",
                labels=labels,
                args=[],
                kwargs={},
            )
        )
    )


async def _claim(broker: AsyncpgBroker) -> "TaskiqMessage | None":
    """Claim the next message and return it as the receiver would see it."""
    row = await broker._dequeue_message()
    if row is None:
        return None
    return _delivered(
        broker,
        broker._inject_delivery_meta(row["message"], row["id"], row["retry_count"]),
    )


async def _deliver(broker: AsyncpgBroker, labels: dict[str, object]) -> TaskiqMessage:
    await _kick(broker, labels)
    message = await _claim(broker)
    assert message is not None
    return message


def _failure() -> "TaskiqResult[None]":
    return TaskiqResult(is_err=True, return_value=None, execution_time=0.0, labels={})


async def _status(broker: AsyncpgBroker, row_id: int) -> str:
    assert broker.write_pool is not None
    return str(
        await broker.write_pool.fetchval(
            f"SELECT status FROM {broker.table_name} WHERE id = $1",  # noqa: S608
            row_id,
        )
    )


@pytest.mark.anyio
async def test_failure_requeues_the_same_row(asyncpg_broker: AsyncpgBroker) -> None:
    """The row goes back to queued; nothing new is enqueued."""
    middleware = OrderedRetryMiddleware(default_retry_count=3, default_delay=0)
    middleware.set_broker(asyncpg_broker)
    message = await _deliver(asyncpg_broker, {"retry_on_error": True})
    result = _failure()

    await middleware.on_error(message, result, ValueError("boom"))

    row_id = int(message.labels[ROW_ID_LABEL])
    assert await _status(asyncpg_broker, row_id) == "queued"
    assert isinstance(result.error, NoResultError)  # result suppressed while retrying

    assert asyncpg_broker.write_pool is not None
    rows = await asyncpg_broker.write_pool.fetchval(
        f"SELECT count(*) FROM {asyncpg_broker.table_name}"  # noqa: S608
    )
    assert rows == 1  # requeued in place, not re-kicked


@pytest.mark.anyio
@pytest.mark.parametrize("cap", [{"max_retries": 1}, {}])
async def test_spent_attempts_dead_letter(
    asyncpg_broker: AsyncpgBroker,
    cap: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The budget is the label where there is one, the broker's cap otherwise."""
    asyncpg_broker.max_retry_attempts = 1  # the sweeper's cap, shared by both paths
    middleware = OrderedRetryMiddleware(default_retry_count=99, default_delay=0)
    middleware.set_broker(asyncpg_broker)
    message = await _deliver(asyncpg_broker, {"retry_on_error": True, **cap})
    result = _failure()

    with caplog.at_level(logging.WARNING):
        await middleware.on_error(message, result, ValueError("boom"))

    assert message.labels[ATTEMPTS_LABEL] == 1
    assert await _status(asyncpg_broker, int(message.labels[ROW_ID_LABEL])) == "dead"
    # The failure is terminal: it belongs in the result backend and in the log.
    assert not isinstance(result.error, NoResultError)
    assert "dead-lettered" in caplog.text


@pytest.mark.anyio
async def test_negative_max_retries_never_dead_letters(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """`max_retries=-1` means forever, however many attempts have been spent."""
    middleware = OrderedRetryMiddleware(default_delay=0)
    middleware.set_broker(asyncpg_broker)
    message = await _deliver(
        asyncpg_broker, {"retry_on_error": True, "max_retries": -1}
    )

    # Spend far more attempts than any finite budget would allow.
    assert asyncpg_broker.write_pool is not None
    _ = await asyncpg_broker.write_pool.execute(
        f"UPDATE {asyncpg_broker.table_name} SET retry_count = 99 WHERE id = $1",  # noqa: S608
        message.labels[ROW_ID_LABEL],
    )
    message.labels[ATTEMPTS_LABEL] = 99

    await middleware.on_error(message, _failure(), ValueError("boom"))

    assert await _status(asyncpg_broker, int(message.labels[ROW_ID_LABEL])) == "queued"


def test_a_foreign_broker_is_rejected_at_wiring_time() -> None:
    """There is no row to requeue: fail here, not at the first task failure."""
    with pytest.raises(TypeError, match="AsyncpgBroker"):
        OrderedRetryMiddleware().set_broker(InMemoryBroker())


@pytest.mark.anyio
async def test_retry_holds_its_slot_in_an_ordered_group(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """The point of the middleware: a retry is not overtaken by its own group."""
    middleware = OrderedRetryMiddleware(default_delay=0)
    middleware.set_broker(asyncpg_broker)
    labels = {"retry_on_error": True, "group_key": "g", "ordered": True}
    await _kick(asyncpg_broker, labels)
    await _kick(asyncpg_broker, labels)
    head = await _claim(asyncpg_broker)
    assert head is not None

    await middleware.on_error(head, _failure(), ValueError("boom"))
    again = await _claim(asyncpg_broker)

    assert again is not None
    assert again.labels[ROW_ID_LABEL] == head.labels[ROW_ID_LABEL]
    assert again.labels[ATTEMPTS_LABEL] == head.labels[ATTEMPTS_LABEL] + 1


@pytest.mark.anyio
async def test_the_ack_after_a_retry_spares_the_next_attempt(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """The receiver acks a delivery we already requeued; it must not complete it.

    Redelivery goes through the same listener, so the ack is stale against an attempt
    this very process is holding.
    """
    middleware = OrderedRetryMiddleware(default_delay=0)
    middleware.set_broker(asyncpg_broker)
    await _kick(asyncpg_broker, {"retry_on_error": True})

    listener = asyncpg_broker.listen()
    delivered = await asyncio.wait_for(listener.__anext__(), timeout=5)
    message = _delivered(asyncpg_broker, delivered.data)
    row_id = int(message.labels[ROW_ID_LABEL])

    await middleware.on_error(message, _failure(), ValueError("boom"))
    redelivered = await asyncio.wait_for(listener.__anext__(), timeout=5)
    assert _delivered(asyncpg_broker, redelivered.data).labels[ATTEMPTS_LABEL] == 2

    await maybe_awaitable(delivered.ack())  # attempt 1, long past owning the row
    assert await _status(asyncpg_broker, row_id) == "active"  # still attempt 2's

    await maybe_awaitable(redelivered.ack())  # the owner's ack lands
    assert await _status(asyncpg_broker, row_id) == "completed"
    await listener.aclose()


@pytest.mark.anyio
async def test_backoff_releases_an_unordered_group(
    asyncpg_broker: AsyncpgBroker,
) -> None:
    """Mutex only: a sibling runs while the retry waits out its backoff."""
    middleware = OrderedRetryMiddleware(default_delay=60)
    middleware.set_broker(asyncpg_broker)
    labels = {"retry_on_error": True, "group_key": "g"}
    await _kick(asyncpg_broker, labels)
    await _kick(asyncpg_broker, labels)
    head = await _claim(asyncpg_broker)
    assert head is not None
    assert await _claim(asyncpg_broker) is None  # mutex held while head runs

    await middleware.on_error(head, _failure(), ValueError("boom"))
    sibling = await _claim(asyncpg_broker)

    assert asyncpg_broker.write_pool is not None
    undue = await asyncpg_broker.write_pool.fetchval(
        f"SELECT scheduled_at > NOW() FROM {asyncpg_broker.table_name} "  # noqa: S608
        "WHERE id = $1",
        head.labels[ROW_ID_LABEL],
    )
    assert undue is True  # the head is serving its backoff, not merely released
    assert sibling is not None
    assert sibling.labels[ROW_ID_LABEL] != head.labels[ROW_ID_LABEL]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("labels", "types_of_exceptions", "exception"),
    [
        ({}, None, ValueError("boom")),  # not opted in
        ({"retry_on_error": True}, [KeyError], ValueError("boom")),  # wrong type
        ({"retry_on_error": True}, None, NoResultError()),  # opted out of a result
    ],
)
async def test_failures_that_are_not_ours_are_left_alone(
    asyncpg_broker: AsyncpgBroker,
    labels: dict[str, object],
    types_of_exceptions: "list[type[BaseException]] | None",
    exception: BaseException,
) -> None:
    """Each guard leaves the row to the receiver, untouched and unsuppressed."""
    middleware = OrderedRetryMiddleware(
        default_delay=0, types_of_exceptions=types_of_exceptions
    )
    middleware.set_broker(asyncpg_broker)
    message = await _deliver(asyncpg_broker, labels)
    result = _failure()

    await middleware.on_error(message, result, exception)

    assert await _status(asyncpg_broker, int(message.labels[ROW_ID_LABEL])) == "active"
    assert not isinstance(result.error, NoResultError)
