"""Guards the taskiq internals OrderedRetryMiddleware is built on.

These are not our API. If taskiq moves them, this file fails here instead of
silently in production, where a skipped on_error means a message never retries.
"""

import inspect

from taskiq.message import TaskiqMessage
from taskiq.middlewares import SmartRetryMiddleware

from taskiq_pg.middlewares import OrderedRetryMiddleware


def _message(labels: dict[str, object]) -> TaskiqMessage:
    return TaskiqMessage(task_id="1", task_name="t", labels=labels, args=[], kwargs={})


def test_smart_retry_still_exposes_what_we_override() -> None:
    for attribute in ("on_error", "is_retry_on_error", "make_delay"):
        assert callable(getattr(SmartRetryMiddleware, attribute))
    parameters = inspect.signature(SmartRetryMiddleware.__init__).parameters
    for option in ("default_retry_count", "no_result_on_retry", "types_of_exceptions"):
        assert option in parameters


def test_on_error_signature_is_unchanged() -> None:
    """We override on_error positionally; a reorder would misroute the arguments."""
    assert list(inspect.signature(SmartRetryMiddleware.on_error).parameters) == [
        "self",
        "message",
        "result",
        "exception",
    ]
    assert list(inspect.signature(OrderedRetryMiddleware.on_error).parameters) == list(
        inspect.signature(SmartRetryMiddleware.on_error).parameters
    )


def test_retry_labels_are_read_the_way_we_write_them() -> None:
    """`retry_on_error` opts in, `max_retries` caps -- both plain taskiq labels."""
    middleware = OrderedRetryMiddleware()
    assert middleware.is_retry_on_error(_message({"retry_on_error": True}))
    assert not middleware.is_retry_on_error(_message({}))
    assert "max_retries" in inspect.getsource(SmartRetryMiddleware)
