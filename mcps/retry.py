"""Retry decorator and HTTP-status classifier.

Adapter methods convert provider exceptions to ``TransientError`` /
``NonTransientError`` at the boundary; the ``retry_transient`` decorator then
consumes those signals, applies bounded exponential backoff, honors the
``Retry-After`` header (when present), and on exhaustion re-raises a
``RetriesExhausted`` from ``mcps.errors``.

The design (Property 11) requires:

  * A bounded number of attempts (``max_retries``) with exponential backoff
    starting at ``initial_backoff_ms`` and doubling each attempt, capped at
    ``max_backoff_ms`` (Requirement 12.1, 12.4).
  * On HTTP 429 with ``Retry-After`` honoring, the actual wait is
    ``max(computed_backoff, retry_after)`` capped at ``max_backoff_ms``
    (Requirement 12.3).
  * Non-transient errors are never retried (Requirement 12.2).
  * The clocks (``sleep`` and ``now``) are injectable so property tests
    can drive the loop deterministically (Requirement 12.1-12.6).

``classify_http`` exposes the HTTP-status taxonomy used by every
``SourceAdapter`` to decide whether to wrap a ClientError in
``TransientError`` or ``NonTransientError`` before raising.
"""

from __future__ import annotations

import functools
import time
from typing import Any, Callable, Optional, TypeVar

from mcps.errors import NonTransientError, RetriesExhausted

__all__ = [
    "TransientError",
    "NonTransientError",
    "RetriesExhausted",
    "classify_http",
    "retry_transient",
    "TRANSIENT_HTTP",
    "NON_TRANSIENT_HTTP",
]


# HTTP status codes the design classifies as transient (Requirement 12.1).
TRANSIENT_HTTP: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

# HTTP status codes the design classifies as non-transient (Requirement 12.2).
# 404 is here too but ``classify_http`` re-classifies it as ``"absent"`` when
# the caller passes ``expect_404_as_absent=True``.
NON_TRANSIENT_HTTP: frozenset[int] = frozenset({400, 401, 403, 404})


class TransientError(Exception):
    """Control-flow signal raised by an adapter to indicate a retryable error.

    This is *not* a ``McpsError`` — it never propagates past the
    ``retry_transient`` decorator. The decorator either returns the wrapped
    function's eventual ``ok`` value or raises ``RetriesExhausted`` (a
    ``McpsError``) once attempts are spent.

    Attributes:
        status: The HTTP status code the adapter saw (or ``None`` for
            connection-timeout errors that never produced a response).
        retry_after_seconds: Parsed ``Retry-After`` header value in seconds,
            or ``None`` if the response did not include one.
        message: Optional descriptive message for diagnostics.
    """

    def __init__(
        self,
        status: Optional[int] = None,
        retry_after_seconds: Optional[float] = None,
        message: str = "",
    ) -> None:
        self.status = status
        self.retry_after_seconds = retry_after_seconds
        self.message = message
        super().__init__(
            f"TransientError(status={status!r}, "
            f"retry_after_seconds={retry_after_seconds!r}, message={message!r})"
        )


def classify_http(status: int, *, expect_404_as_absent: bool = False) -> str:
    """Classify an HTTP status code into one of four control-flow categories.

    Args:
        status: HTTP status code returned by the provider.
        expect_404_as_absent: When ``True`` and ``status == 404``, return
            ``"absent"`` so the caller can treat the response as a successful
            "object does not exist" probe rather than as an error.

    Returns:
        One of ``"transient"``, ``"non_transient"``, ``"absent"``, ``"ok"``.
    """
    if status in TRANSIENT_HTTP:
        return "transient"
    if status == 404 and expect_404_as_absent:
        return "absent"
    if status in NON_TRANSIENT_HTTP:
        return "non_transient"
    if 200 <= status < 300:
        return "ok"
    # Defensive default: anything else is treated as a hard failure.
    return "non_transient"


F = TypeVar("F", bound=Callable[..., Any])


def retry_transient(
    *,
    max_retries: int,
    initial_backoff_ms: int,
    max_backoff_ms: int,
    request_timeout_ms: int,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> Callable[[F], F]:
    """Build a decorator that retries ``TransientError`` with bounded backoff.

    The decorated function is expected to raise ``TransientError`` for
    retryable failures, ``NonTransientError`` (or any other ``Exception``)
    for non-retryable failures, or to return normally on success.

    Args:
        max_retries: Maximum number of retry attempts before raising
            ``RetriesExhausted``. The initial attempt is *not* counted, so
            ``max_retries=N`` means the function may be called up to ``N+1``
            times. (Range 1..10 per Requirement 12.1.)
        initial_backoff_ms: Backoff for the first retry, in milliseconds
            (range 100..10000 per Requirement 12.1).
        max_backoff_ms: Hard ceiling on any single backoff, in milliseconds
            (range 1000..300000 per Requirement 12.4).
        request_timeout_ms: Per-request timeout, in milliseconds. The
            decorator does not enforce this directly — it is exposed so
            adapter wrappers can read it at the call site — but it is part
            of the documented decorator contract.
        sleep: Injectable sleep function (seconds). Defaults to
            ``time.sleep``.
        now: Injectable monotonic clock. Currently used for diagnostics
            and to keep the signature open for future jitter; defaults to
            ``time.monotonic``.

    Returns:
        A decorator suitable for wrapping an adapter method.
    """

    # Validate the ranges once, at decorator-construction time, so misuse
    # surfaces at import or class definition time rather than mid-run.
    if not (1 <= max_retries <= 10):
        raise ValueError(
            f"max_retries must be in [1, 10], got {max_retries!r}"
        )
    if not (100 <= initial_backoff_ms <= 10000):
        raise ValueError(
            f"initial_backoff_ms must be in [100, 10000], got {initial_backoff_ms!r}"
        )
    if not (1000 <= max_backoff_ms <= 300000):
        raise ValueError(
            f"max_backoff_ms must be in [1000, 300000], got {max_backoff_ms!r}"
        )
    if not (1000 <= request_timeout_ms <= 120000):
        raise ValueError(
            f"request_timeout_ms must be in [1000, 120000], got {request_timeout_ms!r}"
        )
    # Touch ``now`` so static-analysis / linters do not flag it unused; the
    # parameter is part of the documented contract for future use.
    _ = now

    def deco(fn: F) -> F:
        operation = getattr(fn, "__qualname__", getattr(fn, "__name__", "operation"))

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            backoff_ms = initial_backoff_ms
            while True:
                try:
                    return fn(*args, **kwargs)
                except NonTransientError:
                    # Non-transient errors short-circuit immediately
                    # (Requirement 12.2).
                    raise
                except TransientError as e:
                    attempt += 1
                    if attempt > max_retries:
                        # Requirement 12.5: surface a structured
                        # RetriesExhausted carrying the last observed
                        # error and the total attempt count.
                        raise RetriesExhausted(
                            operation=operation,
                            last=e,
                            attempts=attempt,
                        ) from e

                    # Compute the wait. Honor Retry-After by selecting the
                    # max of the computed backoff and the server-provided
                    # value, then cap at max_backoff_ms (Requirement 12.3,
                    # 12.4).
                    retry_after_ms = 0
                    if e.retry_after_seconds is not None and e.retry_after_seconds > 0:
                        retry_after_ms = int(e.retry_after_seconds * 1000)
                    wait_ms = max(backoff_ms, retry_after_ms)
                    wait_ms = min(wait_ms, max_backoff_ms)
                    sleep(wait_ms / 1000.0)

                    # Double the backoff for the next attempt, capped.
                    backoff_ms = min(backoff_ms * 2, max_backoff_ms)
                    continue

        return wrapper  # type: ignore[return-value]

    return deco
