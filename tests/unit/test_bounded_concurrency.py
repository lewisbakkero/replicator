# Feature: multicloud-photo-sync, Property 14: Bounded concurrency
"""Bounded-concurrency property test for `mcps.concurrency.make_executor`.

Property under test (design.md, "Correctness Properties — Property 14:
Bounded concurrency"):

  For any workload submitted to the bounded executor with
  ``max_concurrent_transfers = N``, the maximum observed in-flight
  count of `read_bytes` / `write_bytes` / `set_tag` / `delete` calls
  (measured by an instrumented adapter that increments a counter on
  entry and decrements on exit) is ≤ ``N`` for every observed instant.

The test:

1. Generates a workload of size 0..30 with a per-task hold time
   between 0..2 ms so the example runs quickly while still creating
   enough overlap to expose any pool-bypass bug.
2. Builds an instrumented adapter whose `read_bytes` / `write_bytes`
   / `set_tag` / `delete` methods bump a shared counter under a lock,
   capture the running max, sleep for the per-task hold, and then
   decrement.
3. Submits one of the four operations per task to a
   :func:`mcps.concurrency.make_executor` pool sized to
   ``max_concurrent_transfers``.
4. Asserts the captured peak counter is ≤ ``max_concurrent_transfers``.

Validates: Requirements 16.1, 16.2.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import as_completed
from typing import Iterator, Mapping, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.concurrency import make_executor
from mcps.sources.base import ObjectMeta


# Bound the per-task hold time tightly so an example with the maximum
# generated workload (30 tasks) finishes well inside Hypothesis's default
# deadline once we set ``deadline=None``. Held time is in seconds.
_MIN_HOLD_S = 0.000
_MAX_HOLD_S = 0.002

# Operations the property exercises. These are the four
# transfer-related SourceAdapter methods Property 14 names explicitly.
_OPS = ("read_bytes", "write_bytes", "set_tag", "delete")


class _InstrumentedAdapter:
    """Counts simultaneous in-flight operations across the four methods.

    Not a full ``SourceAdapter`` — Property 14 is about pool bounds, not
    adapter semantics, so we only need a hook that the executor can
    schedule. The shared ``_in_flight`` counter is incremented on entry
    and decremented on exit under a single lock; ``_peak`` records the
    running maximum.
    """

    def __init__(self, hold_s: float) -> None:
        self._hold_s = hold_s
        self._lock = threading.Lock()
        self._in_flight = 0
        self._peak = 0
        # Per-method invocation counts so the test can assert every
        # task ran (i.e. nothing was silently swallowed by the pool).
        self.calls: dict[str, int] = {op: 0 for op in _OPS}

    @property
    def peak(self) -> int:
        return self._peak

    def _enter(self, op: str) -> None:
        with self._lock:
            self._in_flight += 1
            if self._in_flight > self._peak:
                self._peak = self._in_flight
            self.calls[op] += 1

    def _exit(self) -> None:
        with self._lock:
            self._in_flight -= 1

    def _do(self, op: str) -> None:
        self._enter(op)
        try:
            # Brief I/O surrogate. The hold ensures overlap among
            # concurrently-running workers; without it the OS scheduler
            # could serialize tasks below the pool ceiling and mask a
            # pool-bypass bug.
            if self._hold_s > 0:
                time.sleep(self._hold_s)
        finally:
            self._exit()

    # The four ops Property 14 names. They all funnel through the same
    # counter; the dispatch is so test failures can pinpoint which
    # method's call site was uneven.
    def read_bytes(self, key: str) -> Iterator[bytes]:
        self._do("read_bytes")
        return iter(())

    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        self._do("write_bytes")

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        self._do("set_tag")

    def delete(self, key: str) -> None:
        self._do("delete")


def _dispatch(adapter: _InstrumentedAdapter, op: str, idx: int) -> None:
    """Invoke ``op`` on ``adapter`` with shape-correct dummy arguments."""
    if op == "read_bytes":
        # Force iterator consumption so the per-call counter actually
        # bumps; ``read_bytes`` is the only op that returns an iterator.
        for _ in adapter.read_bytes(f"k{idx}"):
            pass
    elif op == "write_bytes":
        adapter.write_bytes(
            key=f"k{idx}",
            chunks=iter(()),
            size_bytes=0,
            content_type=None,
            user_metadata={},
        )
    elif op == "set_tag":
        adapter.set_tag(f"k{idx}", "mcps-quarantined-at", "2024-01-01T00:00:00Z")
    elif op == "delete":
        adapter.delete(f"k{idx}")
    else:  # pragma: no cover - guarded by strategy
        raise AssertionError(f"unknown op {op!r}")


@pytest.mark.property
@given(
    max_concurrent_transfers=st.integers(min_value=1, max_value=8),
    workload=st.lists(
        st.tuples(
            st.sampled_from(_OPS),
            st.floats(
                min_value=_MIN_HOLD_S,
                max_value=_MAX_HOLD_S,
                allow_nan=False,
                allow_infinity=False,
            ),
        ),
        min_size=0,
        max_size=30,
    ),
)
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_bounded_concurrency_peak_in_flight_le_max(
    max_concurrent_transfers: int,
    workload: list[tuple[str, float]],
) -> None:
    """The simultaneous-in-flight count never exceeds ``max_concurrent_transfers``.

    Validates: Requirements 16.1, 16.2.
    """
    # Smoke check: an empty workload trivially satisfies the property
    # (peak stays at 0). We still run the executor through to confirm
    # ``make_executor`` constructs / shuts down cleanly on edge inputs.
    if not workload:
        with make_executor(max_concurrent_transfers) as executor:
            assert executor._max_workers == max_concurrent_transfers
        return

    # Pick a uniform hold time per example. Using one shared hold (the
    # max generated value) keeps the in-flight overlap window stable
    # across tasks so the property has a fair chance to observe the
    # peak even on a heavily-loaded CI worker.
    hold_s = max(h for _, h in workload)
    adapter = _InstrumentedAdapter(hold_s=hold_s)

    with make_executor(max_concurrent_transfers) as executor:
        futures = [
            executor.submit(_dispatch, adapter, op, idx)
            for idx, (op, _hold) in enumerate(workload)
        ]
        # Surface any worker exception immediately; otherwise a swallowed
        # error in the worker would let the property silently pass.
        for fut in as_completed(futures):
            fut.result()

    # The core property.
    assert adapter.peak <= max_concurrent_transfers, (
        f"peak in-flight {adapter.peak} exceeded "
        f"max_concurrent_transfers {max_concurrent_transfers}"
    )

    # Sanity: every submitted task ran (no pool-side dropping).
    assert sum(adapter.calls.values()) == len(workload)

    # Sanity: the executor was actually used; if hold_s>0 and there are
    # at least two tasks of the same op type, we expect the peak to be
    # ≥ 2 *most of the time*. We do NOT assert on a lower bound because
    # CI scheduling can serialize tasks below the ceiling — the upper
    # bound is the only guarantee Property 14 makes.
