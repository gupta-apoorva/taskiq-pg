"""Retry middleware that keeps a failed message in its place in the queue."""

import logging
from typing import Any, cast

from taskiq.abc.broker import AsyncBroker
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.result import TaskiqResult
from typing_extensions import override

from taskiq_pg.broker import AsyncpgBroker
from taskiq_pg.labels import ATTEMPTS_LABEL, ROW_ID_LABEL

logger = logging.getLogger("taskiq.asyncpg_broker")


class OrderedRetryMiddleware(SmartRetryMiddleware):
    """Retries by requeueing the same row, so `ordered` groups keep their order.

    A re-kick would land at the back of the queue with a fresh id, behind the
    message's own group. Replaces SmartRetryMiddleware rather than accompanying it:
    both hook on_error. Inherits delay policy and label parsing; `on_send` is unused.
    """

    @override
    def set_broker(self, broker: AsyncBroker) -> None:
        """Reject a broker without a row to requeue, at wiring time."""
        if not isinstance(broker, AsyncpgBroker):
            msg = (
                f"{type(self).__name__} requires AsyncpgBroker, "
                f"got {type(broker).__name__}"
            )
            raise TypeError(msg)
        super().set_broker(broker)

    @override
    async def on_error(
        self,
        message: TaskiqMessage,
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """Requeue the delivery, or dead-letter it once its attempts are spent."""
        if isinstance(exception, NoResultError):
            return
        if self.types_of_exceptions is not None and not isinstance(
            exception, tuple(self.types_of_exceptions)
        ):
            return
        if not self.is_retry_on_error(message):
            return

        broker = cast(AsyncpgBroker, self.broker)
        row_id = int(message.labels[ROW_ID_LABEL])
        attempts = int(message.labels[ATTEMPTS_LABEL])
        # Broker's cap, not default_retry_count: the sweeper reads the same one.
        max_retries = int(message.labels.get("max_retries", broker.max_retry_attempts))

        if max_retries < 0 or attempts < max_retries:
            _ = await broker.retry_in_place(
                row_id, attempts, self.make_delay(message, attempts)
            )
            # Only a retry withholds the result; a spent message reports its failure.
            if self.no_result_on_retry:
                result.error = NoResultError()
        elif await broker.mark_dead(row_id, attempts):
            logger.warning(
                f"Task '{message.task_name}' spent {attempts} attempts, "
                f"dead-lettered row {row_id}"
            )
