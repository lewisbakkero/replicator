"""Example-based tests for `mcps.concurrency`.

Covers the deterministic edge cases that complement the Property 14
property test in ``test_bounded_concurrency.py``:

- ``make_executor`` constructs a :class:`ThreadPoolExecutor` with the
  configured ``max_workers`` and the ``mcps-xfer`` thread-name prefix.
- ``writer_lock`` acquires successfully on a fresh path, yields a
  writable fd, writes a ``{pid, run_id, started_at}`` JSON line that
  fsyncs to disk, and unlinks the lock file on exit.
- ``writer_lock`` raises :class:`mcps.errors.LockConflict` (exit code
  73) within ``timeout_s`` when a planted lock file holds the current
  process's PID — simulating a still-alive holder.
- ``writer_lock`` reclaims a planted lock file whose recorded PID is
  not running (we monkey-patch ``_pid_alive`` to simulate the dead
  holder, which is more reliable across Linux/macOS than spelunking
  for an actually-dead PID).

Validates: Requirements 16.1, 16.2, 16.3, 16.4, 16.5, 16.6.
"""

from __future__ import annotations

import errno
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from mcps import concurrency
from mcps.concurrency import make_executor, writer_lock
from mcps.errors import ExitCode, LockConflict


# ---------------------------------------------------------------------------
# make_executor
# ---------------------------------------------------------------------------


def test_make_executor_returns_bounded_thread_pool() -> None:
    """``make_executor`` returns a ThreadPoolExecutor with the configured size."""
    pool = make_executor(4)
    try:
        assert isinstance(pool, ThreadPoolExecutor)
        assert pool._max_workers == 4
        # Executor's thread name prefix is exposed via the private
        # ``_thread_name_prefix`` attribute on CPython. The contract is
        # the prefix string itself, so we assert on the value the tests
        # observe rather than on stdlib internals.
        assert pool._thread_name_prefix == "mcps-xfer"
    finally:
        pool.shutdown(wait=True)


def test_make_executor_uses_mcps_xfer_thread_name_prefix() -> None:
    """Worker threads carry the ``mcps-xfer`` name prefix."""
    pool = make_executor(2)
    try:
        observed: list[str] = []

        def _capture() -> None:
            observed.append(threading.current_thread().name)

        for _ in range(4):
            pool.submit(_capture).result()

        for name in observed:
            assert name.startswith("mcps-xfer"), (
                f"expected thread name to start with 'mcps-xfer', got {name!r}"
            )
    finally:
        pool.shutdown(wait=True)


def test_make_executor_rejects_zero_or_negative() -> None:
    """``make_executor`` rejects ``max_concurrent_transfers < 1``."""
    with pytest.raises(ValueError):
        make_executor(0)
    with pytest.raises(ValueError):
        make_executor(-1)


# ---------------------------------------------------------------------------
# writer_lock — happy path
# ---------------------------------------------------------------------------


def test_writer_lock_acquires_writes_holder_record_and_releases(tmp_path) -> None:
    """A fresh path is locked, holder JSON is fsynced, and the file is unlinked on exit."""
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")

    captured_holder: dict | None = None

    with writer_lock(lock_path, run_id="run-abc", timeout_s=1.0) as fd:
        # The yielded fd corresponds to the locked file.
        assert isinstance(fd, int)
        assert os.path.exists(lock_path)
        # Read the holder record back from disk to confirm fsync wrote
        # it (and to confirm the JSON shape).
        with open(lock_path, "r", encoding="utf-8") as fh:
            line = fh.readline().strip()
        captured_holder = json.loads(line)

    # Post-exit invariants.
    assert captured_holder is not None
    assert captured_holder["pid"] == os.getpid()
    assert captured_holder["run_id"] == "run-abc"
    # ISO-8601 UTC second precision with trailing Z (req 16.4).
    assert isinstance(captured_holder["started_at"], str)
    assert captured_holder["started_at"].endswith("Z")
    # The lock file is unlinked when the context manager exits.
    assert not os.path.exists(lock_path)


def test_writer_lock_releases_on_exception(tmp_path) -> None:
    """An exception inside the with-block still triggers unlock and unlink."""
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")

    class _BoomError(Exception):
        pass

    with pytest.raises(_BoomError):
        with writer_lock(lock_path, run_id="run-x", timeout_s=1.0):
            raise _BoomError("inside protected region")

    assert not os.path.exists(lock_path)


def test_writer_lock_back_to_back_acquisitions(tmp_path) -> None:
    """A clean release lets the next acquirer proceed without waiting."""
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")

    with writer_lock(lock_path, run_id="run-1", timeout_s=1.0):
        pass
    # A second acquisition must succeed quickly (no timeout) because
    # the first one released cleanly.
    start = time.monotonic()
    with writer_lock(lock_path, run_id="run-2", timeout_s=1.0):
        elapsed = time.monotonic() - start
        assert elapsed < 0.5


# ---------------------------------------------------------------------------
# writer_lock — conflict against a live holder
# ---------------------------------------------------------------------------


def test_writer_lock_raises_lock_conflict_when_current_pid_holds(tmp_path) -> None:
    """A planted lock file claiming the current PID raises LockConflict.

    The current process is necessarily alive, so the stale-PID reclaim
    branch is skipped and the deadline expires. The raised
    :class:`LockConflict` carries the holder PID and maps to exit code
    73 (``LOCK_CONFLICT``) per req 16.5.
    """
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")

    # Acquire and *retain* the lock from a background context manager,
    # then attempt a second acquisition from the same process. The
    # second attempt must observe a live holder (us) and time out.
    held_event = threading.Event()
    release_event = threading.Event()
    holder_pid_seen: list[int] = []

    def _hold_lock() -> None:
        with writer_lock(lock_path, run_id="holder", timeout_s=2.0):
            held_event.set()
            release_event.wait(timeout=5.0)

    holder_thread = threading.Thread(target=_hold_lock)
    holder_thread.start()
    try:
        assert held_event.wait(timeout=2.0), "holder thread never acquired the lock"

        # The first attempt should fail with LockConflict, since the
        # holder is the current process and ``_pid_alive`` returns
        # True for us. We use a short timeout to keep the test fast.
        start = time.monotonic()
        with pytest.raises(LockConflict) as exc_info:
            with writer_lock(lock_path, run_id="contender", timeout_s=0.3):
                pass  # pragma: no cover - should not reach
        elapsed = time.monotonic() - start

        # Req 16.5: exit within 5 seconds. We use a much tighter
        # timeout but the contract is the upper bound.
        assert elapsed < 5.0, f"writer_lock took {elapsed:.3f}s to time out"
        assert exc_info.value.holder_pid == os.getpid()
        assert exc_info.value.exit_code == ExitCode.LOCK_CONFLICT
        holder_pid_seen.append(exc_info.value.holder_pid)
    finally:
        release_event.set()
        holder_thread.join(timeout=5.0)


# ---------------------------------------------------------------------------
# writer_lock — stale-PID reclaim
# ---------------------------------------------------------------------------


def _plant_stale_lock_file(path: str, pid: int, run_id: str = "stale") -> None:
    """Write a ``{pid, run_id, started_at}`` line to ``path``.

    The file is written but **not** flock'd — that's the point of the
    test. A stale PID file with no actual flock holder is the exact
    on-disk shape left behind by a process that was SIGKILL'd between
    writing the holder line and releasing the flock.
    """
    payload = {
        "pid": pid,
        "run_id": run_id,
        "started_at": "2024-01-01T00:00:00Z",
    }
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def test_writer_lock_reclaims_stale_lock_when_holder_pid_is_dead(
    tmp_path, monkeypatch
) -> None:
    """A planted lock claiming a dead PID is reclaimed and the run proceeds.

    We monkey-patch ``mcps.concurrency._pid_alive`` to return False for
    the planted PID rather than relying on a known-dead PID, which is
    not portable across Linux/macOS sandboxes. This still exercises the
    real reclaim path (unlink + reopen + flock retry) — only the
    is-it-alive check is faked.

    Validates: Requirement 16.6.
    """
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")
    fake_dead_pid = 999_999  # arbitrary; matches what the planted file claims

    _plant_stale_lock_file(lock_path, pid=fake_dead_pid)

    # However: planting the file does not actually hold the kernel's
    # flock, so the very first ``fcntl.flock(LOCK_EX | LOCK_NB)`` will
    # succeed and ``_pid_alive`` will not be consulted on the happy
    # path. The test still proves the *outcome* required by req 16.6
    # (the run proceeds against a planted-stale lock file). To exercise
    # the explicit reclaim branch — where flock raises BlockingIOError
    # because some other process holds the kernel lock against a dead
    # PID record — we additionally simulate that branch in
    # ``test_writer_lock_reclaim_branch_on_blocking_io`` below.

    # Force the reclaim branch to be taken even on the happy path by
    # making ``_pid_alive`` return False unconditionally; combined with
    # the planted file this ensures we exercise the unlink-and-reopen
    # logic when applicable.
    monkeypatch.setattr(concurrency, "_pid_alive", lambda pid: False)

    with writer_lock(lock_path, run_id="reclaim", timeout_s=1.0) as fd:
        # We hold the lock now; the file has been re-written with our
        # PID and run_id, replacing the planted stale record.
        assert isinstance(fd, int)
        with open(lock_path, "r", encoding="utf-8") as fh:
            holder = json.loads(fh.readline())
        assert holder["pid"] == os.getpid()
        assert holder["run_id"] == "reclaim"

    assert not os.path.exists(lock_path)


def test_writer_lock_reclaim_branch_on_blocking_io(tmp_path, monkeypatch) -> None:
    """Force ``BlockingIOError`` once and confirm the reclaim retry succeeds.

    Patches ``fcntl.flock`` to raise ``BlockingIOError`` on the first
    call only, then delegates to the real ``flock`` thereafter. With
    ``_pid_alive`` patched to return False, the reclaim path takes the
    "stale → unlink → reopen → retry" branch and the second flock
    succeeds. This is the precise reclaim sequence req 16.6 requires.
    """
    import fcntl as real_fcntl

    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")
    fake_dead_pid = 999_998
    _plant_stale_lock_file(lock_path, pid=fake_dead_pid)

    # Patch _pid_alive: planted PID is "not alive".
    monkeypatch.setattr(concurrency, "_pid_alive", lambda pid: pid != os.getpid())

    real_flock = real_fcntl.flock
    state = {"first": True}

    def _flaky_flock(fd, op):
        if state["first"] and (op & real_fcntl.LOCK_EX):
            state["first"] = False
            raise BlockingIOError(errno.EWOULDBLOCK, "simulated contention")
        return real_flock(fd, op)

    monkeypatch.setattr(concurrency.fcntl, "flock", _flaky_flock)

    with writer_lock(lock_path, run_id="reclaim2", timeout_s=2.0) as fd:
        with open(lock_path, "r", encoding="utf-8") as fh:
            holder = json.loads(fh.readline())
        assert holder["pid"] == os.getpid()
        assert holder["run_id"] == "reclaim2"

    assert not os.path.exists(lock_path)


# ---------------------------------------------------------------------------
# _pid_alive helper
# ---------------------------------------------------------------------------


def test_pid_alive_returns_true_for_current_process() -> None:
    """``_pid_alive(os.getpid())`` is True — we're obviously running."""
    assert concurrency._pid_alive(os.getpid()) is True


def test_pid_alive_returns_false_for_zero_or_negative() -> None:
    """Non-positive PIDs are treated as not-alive (defensive)."""
    assert concurrency._pid_alive(0) is False
    assert concurrency._pid_alive(-1) is False


def test_pid_alive_returns_false_when_kill_raises_process_lookup(monkeypatch) -> None:
    """A genuinely-absent PID surfaces ProcessLookupError → False.

    We patch ``os.kill`` to raise ProcessLookupError so the test does
    not depend on finding an actually-dead PID on the host (PID 1 is
    always alive, and any other "free" PID could be reallocated).
    """
    def _fake_kill(pid: int, sig: int) -> None:
        raise ProcessLookupError(errno.ESRCH, "no such process")

    monkeypatch.setattr(concurrency.os, "kill", _fake_kill)
    assert concurrency._pid_alive(424242) is False


def test_pid_alive_returns_true_on_permission_error(monkeypatch) -> None:
    """A PID in a different uid surfaces PermissionError → True (assume alive)."""
    def _fake_kill(pid: int, sig: int) -> None:
        raise PermissionError(errno.EPERM, "operation not permitted")

    monkeypatch.setattr(concurrency.os, "kill", _fake_kill)
    assert concurrency._pid_alive(1) is True
