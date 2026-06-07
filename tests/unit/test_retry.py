# Feature: multicloud-photo-sync, Property 11: Retry bounds and Retry-After honoring
"""Tests for the ``retry_transient`` decorator and ``classify_http``.

Hypothesis property + example-based tests for ``mcps.retry``.

The property under test (design.md, "Correctness Properties — Property 11:
Retry bounds and Retry-After honoring") is:

  For any sequence of n TransientError outcomes followed by an ``ok``
  outcome, with retry parameters ``(max_retries, initial_backoff_ms,
  max_backoff_ms)`` and for any ``retry_after_seconds`` value ``r >= 0``
  accompanying any of those errors, the ``retry_transient`` decorator
  either succeeds (when ``n <= max_retries``) or raises
  ``RetriesExhausted`` (when ``n > max_retries``), and every observed
  sleep ``s`` satisfies
      max(computed_backoff, r) <= s <= max_backoff_ms / 1000
  and ``s >= 0``. Non-transient errors are never retried.

Validates: Requirements 2.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6.
"""

from __future__ import annotations

from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.errors import NonTransientError, RetriesExhausted
from mcps.retry import (
    NON_TRANSIENT_HTTP,
    TRANSIENT_HTTP,
    TransientError,
    classify_http,
    retry_transient,
)


# ---------------------------------------------------------------------------
# Helpers: sleep recorder + bounded-clock fake + fail-then-succeed function
# ---------------------------------------------------------------------------


class SleepRecorder:
    """Captures every value passed to ``sleep`` so the property can assert
    bounds on each individual call."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def _make_fail_then_ok(
    *,
    n_failures: int,
    retry_after_seconds_per_attempt: list[Optional[float]],
):
    """Return a callable that raises ``TransientError`` on the first
    ``n_failures`` invocations and returns ``"ok"`` on the next one.

    The k-th raised ``TransientError`` (1-indexed) carries
    ``retry_after_seconds = retry_after_seconds_per_attempt[k - 1]`` if
    that index is defined; otherwise ``None``.
    """
    state = {"attempts": 0}

    def fn() -> str:
        state["attempts"] += 1
        k = state["attempts"]
        if k <= n_failures:
            ra: Optional[float] = None
            if k - 1 < len(retry_after_seconds_per_attempt):
                ra = retry_after_seconds_per_attempt[k - 1]
            raise TransientError(status=503, retry_after_seconds=ra)
        return "ok"

    fn.calls = state  # type: ignore[attr-defined]
    return fn


def _expected_computed_backoff_ms(
    attempt: int,
    *,
    initial_backoff_ms: int,
    max_backoff_ms: int,
) -> int:
    """The exponential backoff value used for the k-th sleep (1-indexed).

    Mirrors the decorator's internal logic: ``backoff_ms`` starts at
    ``initial_backoff_ms`` and doubles after each sleep, capped at
    ``max_backoff_ms``.
    """
    backoff = initial_backoff_ms
    for _ in range(attempt - 1):
        backoff = min(backoff * 2, max_backoff_ms)
    return min(backoff, max_backoff_ms)


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


# Per design.md and Requirement 12: retry parameters live in fixed ranges.
_MAX_RETRIES = st.integers(min_value=1, max_value=10)
_INITIAL_BACKOFF_MS = st.integers(min_value=100, max_value=10_000)
_MAX_BACKOFF_MS = st.integers(min_value=1_000, max_value=300_000)
_REQUEST_TIMEOUT_MS = st.integers(min_value=1_000, max_value=120_000)


# ``retry_after_seconds`` may be ``None`` (no header) or a non-negative
# float in [0, 60].
_RETRY_AFTER = st.one_of(
    st.none(),
    st.floats(
        min_value=0.0,
        max_value=60.0,
        allow_nan=False,
        allow_infinity=False,
    ),
)


@st.composite
def _retry_scenarios(draw):
    """Compose a full retry scenario.

    Yields a tuple ``(n_failures, retry_after_per_attempt, max_retries,
    initial_backoff_ms, max_backoff_ms, request_timeout_ms)`` where
    ``n_failures`` is in ``[0, max_retries + 3]`` so the property
    exercises both the ``n <= max_retries`` (success) and the
    ``n > max_retries`` (RetriesExhausted) regimes.
    """
    max_retries = draw(_MAX_RETRIES)
    initial_backoff_ms = draw(_INITIAL_BACKOFF_MS)
    # Ensure max_backoff_ms >= initial_backoff_ms so the cap behaves sanely.
    max_backoff_ms = draw(
        st.integers(min_value=max(1_000, initial_backoff_ms), max_value=300_000)
    )
    request_timeout_ms = draw(_REQUEST_TIMEOUT_MS)
    n_failures = draw(st.integers(min_value=0, max_value=max_retries + 3))
    retry_after_per_attempt = draw(
        st.lists(_RETRY_AFTER, min_size=n_failures, max_size=n_failures)
    )
    return (
        n_failures,
        retry_after_per_attempt,
        max_retries,
        initial_backoff_ms,
        max_backoff_ms,
        request_timeout_ms,
    )


# ---------------------------------------------------------------------------
# Property test: Property 11
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(scenario=_retry_scenarios())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_retry_bounds_and_retry_after_honoring(scenario) -> None:
    """Property 11: Retry bounds and Retry-After honoring.

    Validates: Requirements 2.6, 12.1, 12.2, 12.3, 12.4, 12.5.
    """
    (
        n_failures,
        retry_after_per_attempt,
        max_retries,
        initial_backoff_ms,
        max_backoff_ms,
        request_timeout_ms,
    ) = scenario

    sleep_recorder = SleepRecorder()
    inner = _make_fail_then_ok(
        n_failures=n_failures,
        retry_after_seconds_per_attempt=retry_after_per_attempt,
    )
    decorated = retry_transient(
        max_retries=max_retries,
        initial_backoff_ms=initial_backoff_ms,
        max_backoff_ms=max_backoff_ms,
        request_timeout_ms=request_timeout_ms,
        sleep=sleep_recorder,
    )(inner)

    if n_failures <= max_retries:
        # Success branch: the function must eventually return "ok".
        assert decorated() == "ok"
        # Exactly ``n_failures`` sleeps happened (one per failed attempt
        # before the eventual successful attempt).
        assert len(sleep_recorder.calls) == n_failures
    else:
        # Failure branch: the decorator must raise RetriesExhausted on the
        # ``max_retries + 1``-th failure.
        with pytest.raises(RetriesExhausted) as excinfo:
            decorated()
        # ``attempts`` reflects the count at the moment the limit was
        # exceeded (i.e. max_retries + 1).
        assert excinfo.value.attempts == max_retries + 1
        assert isinstance(excinfo.value.last, TransientError)
        # We sleep on every retry except the final one that triggers the
        # exhaustion — that is, exactly ``max_retries`` sleeps.
        assert len(sleep_recorder.calls) == max_retries

    # Bound assertions on every observed sleep: each ``s`` satisfies
    # ``max(computed_backoff, retry_after) <= s <= max_backoff_ms / 1000``
    # and ``s >= 0``.
    max_backoff_seconds = max_backoff_ms / 1000.0
    for k, observed_s in enumerate(sleep_recorder.calls, start=1):
        computed_backoff_ms = _expected_computed_backoff_ms(
            k,
            initial_backoff_ms=initial_backoff_ms,
            max_backoff_ms=max_backoff_ms,
        )
        ra_for_k = retry_after_per_attempt[k - 1]
        retry_after_ms = (
            int(ra_for_k * 1000)
            if (ra_for_k is not None and ra_for_k > 0)
            else 0
        )
        expected_min_ms = min(
            max(computed_backoff_ms, retry_after_ms),
            max_backoff_ms,
        )
        expected_min_s = expected_min_ms / 1000.0

        assert observed_s >= 0.0, (
            f"sleep #{k} = {observed_s} must be non-negative"
        )
        assert observed_s <= max_backoff_seconds, (
            f"sleep #{k} = {observed_s} exceeds max_backoff "
            f"{max_backoff_seconds}"
        )
        assert observed_s >= expected_min_s, (
            f"sleep #{k} = {observed_s} below expected lower bound "
            f"{expected_min_s} (computed_backoff_ms={computed_backoff_ms}, "
            f"retry_after_ms={retry_after_ms}, max_backoff_ms={max_backoff_ms})"
        )


# ---------------------------------------------------------------------------
# Example-based tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", sorted(TRANSIENT_HTTP))
def test_classify_http_transient_codes(status: int) -> None:
    """Validates: Requirement 12.1."""
    assert classify_http(status) == "transient"


@pytest.mark.parametrize("status", [400, 401, 403, 405, 410, 451])
def test_classify_http_non_transient_codes(status: int) -> None:
    """Validates: Requirement 12.2."""
    assert classify_http(status) == "non_transient"


def test_classify_http_404_default_is_non_transient() -> None:
    """Validates: Requirement 12.2."""
    assert classify_http(404) == "non_transient"


def test_classify_http_404_with_expect_404_as_absent_returns_absent() -> None:
    """Validates: Requirement 12.2."""
    assert classify_http(404, expect_404_as_absent=True) == "absent"


@pytest.mark.parametrize("status", [200, 201, 204, 206, 299])
def test_classify_http_2xx_is_ok(status: int) -> None:
    """Validates: Requirement 12.1."""
    assert classify_http(status) == "ok"


@pytest.mark.parametrize("status", [100, 101, 300, 301, 302, 599])
def test_classify_http_unknown_codes_default_to_non_transient(status: int) -> None:
    """Defensive default for codes outside the documented buckets."""
    assert classify_http(status) == "non_transient"


def test_transient_http_and_non_transient_http_are_disjoint() -> None:
    assert TRANSIENT_HTTP.isdisjoint(NON_TRANSIENT_HTTP)


def test_transient_error_stores_status_and_retry_after() -> None:
    """Validates: Requirement 12.3."""
    e = TransientError(status=429, retry_after_seconds=5.0, message="rate-limited")
    assert e.status == 429
    assert e.retry_after_seconds == 5.0
    assert e.message == "rate-limited"


def test_transient_error_defaults() -> None:
    e = TransientError()
    assert e.status is None
    assert e.retry_after_seconds is None
    assert e.message == ""


def test_function_that_succeeds_immediately_returns_without_sleeping() -> None:
    sleep_recorder = SleepRecorder()

    @retry_transient(
        max_retries=5,
        initial_backoff_ms=500,
        max_backoff_ms=30_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> str:
        return "ok"

    assert fn() == "ok"
    assert sleep_recorder.calls == []


def test_non_transient_error_is_never_retried() -> None:
    """Validates: Requirement 12.2."""
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    @retry_transient(
        max_retries=5,
        initial_backoff_ms=500,
        max_backoff_ms=30_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> None:
        state["calls"] += 1
        raise NonTransientError(status=403, body="forbidden")

    with pytest.raises(NonTransientError) as excinfo:
        fn()

    assert excinfo.value.status == 403
    assert state["calls"] == 1
    assert sleep_recorder.calls == []


def test_unrelated_exception_propagates_unchanged() -> None:
    """Any non-Transient/NonTransient exception bubbles up without retry."""
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    class CustomBoom(RuntimeError):
        pass

    @retry_transient(
        max_retries=5,
        initial_backoff_ms=500,
        max_backoff_ms=30_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> None:
        state["calls"] += 1
        raise CustomBoom("kaboom")

    with pytest.raises(CustomBoom):
        fn()

    assert state["calls"] == 1
    assert sleep_recorder.calls == []


def test_retry_after_honored_when_larger_than_computed_backoff() -> None:
    """Validates: Requirement 12.3.

    A single TransientError with retry_after_seconds=5.0 and
    initial_backoff_ms=500 must produce a recorded sleep of
    max(0.5, 5.0) = 5.0 seconds (well below max_backoff_ms cap).
    """
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    @retry_transient(
        max_retries=3,
        initial_backoff_ms=500,
        max_backoff_ms=30_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> str:
        state["calls"] += 1
        if state["calls"] == 1:
            raise TransientError(status=429, retry_after_seconds=5.0)
        return "ok"

    assert fn() == "ok"
    assert sleep_recorder.calls == [5.0]


def test_retry_after_capped_at_max_backoff_ms() -> None:
    """Validates: Requirement 12.4.

    A Retry-After larger than max_backoff_ms must be capped at
    max_backoff_ms.
    """
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    @retry_transient(
        max_retries=3,
        initial_backoff_ms=500,
        max_backoff_ms=2_000,  # cap = 2.0 seconds
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> str:
        state["calls"] += 1
        if state["calls"] == 1:
            raise TransientError(status=429, retry_after_seconds=60.0)
        return "ok"

    assert fn() == "ok"
    assert sleep_recorder.calls == [2.0]


def test_exponential_backoff_doubles_each_attempt_capped() -> None:
    """Validates: Requirements 12.1, 12.4."""
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    @retry_transient(
        max_retries=4,
        initial_backoff_ms=500,
        max_backoff_ms=2_000,  # 0.5 -> 1.0 -> 2.0 -> 2.0 (capped)
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> str:
        state["calls"] += 1
        if state["calls"] <= 4:
            raise TransientError(status=503)
        return "ok"

    assert fn() == "ok"
    assert sleep_recorder.calls == [0.5, 1.0, 2.0, 2.0]


def test_retries_exhausted_carries_last_error_and_attempt_count() -> None:
    """Validates: Requirement 12.5."""
    sleep_recorder = SleepRecorder()
    last_seen = TransientError(status=503, retry_after_seconds=None)
    state = {"calls": 0}

    @retry_transient(
        max_retries=2,
        initial_backoff_ms=100,
        max_backoff_ms=1_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn() -> None:
        state["calls"] += 1
        raise last_seen

    with pytest.raises(RetriesExhausted) as excinfo:
        fn()
    assert excinfo.value.attempts == 3  # 2 retries + initial = 3 total
    # ``last`` references the same TransientError raised on the final attempt.
    assert isinstance(excinfo.value.last, TransientError)
    assert excinfo.value.last.status == 503
    # Two sleeps happened (one per retry); the final attempt did not sleep.
    assert len(sleep_recorder.calls) == 2


@pytest.mark.parametrize(
    "param,bad_value",
    [
        ("max_retries", 0),
        ("max_retries", 11),
        ("initial_backoff_ms", 99),
        ("initial_backoff_ms", 10_001),
        ("max_backoff_ms", 999),
        ("max_backoff_ms", 300_001),
        ("request_timeout_ms", 999),
        ("request_timeout_ms", 120_001),
    ],
)
def test_retry_transient_rejects_out_of_range_parameters(
    param: str, bad_value: int
) -> None:
    """Validates: Requirement 12.6."""
    kwargs = {
        "max_retries": 5,
        "initial_backoff_ms": 500,
        "max_backoff_ms": 30_000,
        "request_timeout_ms": 30_000,
    }
    kwargs[param] = bad_value
    with pytest.raises(ValueError) as excinfo:
        retry_transient(**kwargs)
    assert param in str(excinfo.value)


def test_retry_transient_preserves_function_metadata() -> None:
    """``functools.wraps`` keeps __name__/__doc__ intact for diagnostics."""

    @retry_transient(
        max_retries=1,
        initial_backoff_ms=100,
        max_backoff_ms=1_000,
        request_timeout_ms=1_000,
        sleep=lambda _s: None,
    )
    def _named_function() -> str:
        """My docstring."""
        return "ok"

    assert _named_function.__name__ == "_named_function"
    assert _named_function.__doc__ == "My docstring."


def test_retry_transient_passes_args_and_kwargs_through() -> None:
    sleep_recorder = SleepRecorder()
    state = {"calls": 0}

    @retry_transient(
        max_retries=3,
        initial_backoff_ms=100,
        max_backoff_ms=1_000,
        request_timeout_ms=30_000,
        sleep=sleep_recorder,
    )
    def fn(a: int, *, b: str) -> str:
        state["calls"] += 1
        if state["calls"] == 1:
            raise TransientError(status=503)
        return f"{a}:{b}"

    assert fn(7, b="hello") == "7:hello"
