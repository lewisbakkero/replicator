"""Bounded transfer executor and single-writer file lock.

This module implements two concurrency primitives used by the
MultiCloud_Photo_Sync runtime:

1. :func:`make_executor` — a :class:`concurrent.futures.ThreadPoolExecutor`
   with a bounded ``max_workers`` and a stable thread-name prefix
   (``mcps-xfer``). Replicator and Drive_Importer submit one future per
   per-object operation (read+verify+write); the pool is the single
   chokepoint, so the simultaneous-in-flight count never exceeds the
   configured ``max_concurrent_transfers``.

2. :func:`writer_lock` — a context manager that acquires
   ``fcntl.flock(LOCK_EX | LOCK_NB)`` on a sibling lock file, writes a
   ``{pid, run_id, started_at}`` JSON line to record the holder, fsyncs,
   registers an ``atexit`` release, and on conflict either reclaims a
   stale lock (the holder PID is no longer running) or raises
   :class:`mcps.errors.LockConflict` with the live holder's PID.

POSIX-only by design: the implementation uses ``fcntl.flock`` and
``os.kill(pid, 0)``. The tool ships for cron / systemd, so Linux/macOS
only is acceptable. Windows would need a different implementation
(``msvcrt.locking``).

Validates: Requirements 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7.
"""

from __future__ import annotations

import atexit
import contextlib
import errno
import fcntl
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Iterator, Optional

from mcps.errors import LockConflict


__all__ = [
    "make_executor",
    "writer_lock",
    "LockConflict",
]


# Thread-name prefix for the bounded transfer pool. The prefix is part of
# the contract so log analysis can distinguish transfer threads from any
# stdlib helper threads (e.g. the executor's bookkeeping thread).
_THREAD_NAME_PREFIX = "mcps-xfer"

# Poll interval used when the lock is held by a *live* process and the
# deadline has not yet expired. Kept small so the worst-case wait is
# close to ``timeout_s`` without busy-looping.
_LOCK_POLL_INTERVAL_S = 0.1


def make_executor(max_concurrent_transfers: int) -> ThreadPoolExecutor:
    """Return a bounded :class:`ThreadPoolExecutor` for transfer tasks.

    The pool's ``max_workers`` equals ``max_concurrent_transfers`` so
    that the simultaneous-in-flight count of read/write/tag/delete calls
    issued through the pool can never exceed the configured bound
    (req 16.2). Threads are appropriate here because all three SDKs
    (boto3, google-cloud-storage, google-api-python-client) release the
    GIL during socket I/O.

    Validates: Requirements 16.1, 16.2.
    """
    if max_concurrent_transfers < 1:
        # The Config_Parser already enforces 1..64 (req 16.1) at startup;
        # this guard is defence-in-depth so a programmatic caller that
        # bypasses the config layer fails loudly rather than constructing
        # a useless 0-worker pool.
        raise ValueError(
            "max_concurrent_transfers must be >= 1, "
            f"got {max_concurrent_transfers!r}"
        )
    return ThreadPoolExecutor(
        max_workers=max_concurrent_transfers,
        thread_name_prefix=_THREAD_NAME_PREFIX,
    )


# ---------------------------------------------------------------------------
# Writer lock
# ---------------------------------------------------------------------------


def _now_iso_seconds() -> str:
    """Return the current UTC time as ``YYYY-MM-DDTHH:MM:SSZ``.

    Mirrors the formatting used elsewhere in the codebase
    (``replication._now_iso_seconds`` etc.). Kept module-private so the
    lock-file contents stay human-readable and round-trip-comparable in
    tests without coupling to the Replicator's ``now`` clock.
    """
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _pid_alive(pid: int) -> bool:
    """Return True if signal 0 to ``pid`` succeeds.

    Uses ``os.kill(pid, 0)`` which delivers no signal but performs the
    permission/existence check the kernel would do for a real signal.

    - ``ProcessLookupError`` (ESRCH): the PID is not running → False.
    - ``PermissionError`` (EPERM): the PID is running but owned by
      another user → True (assume alive; we cannot reclaim).
    - Any other ``OSError``: treat as alive (defence in depth — refuse
      to reclaim a lock when we cannot positively confirm staleness).

    Module-level so unit tests can monkey-patch it to simulate a dead
    holder without spawning a real child process.

    Validates: Requirement 16.6.
    """
    if pid <= 0:
        # PID 0 / negative PIDs: ``os.kill`` would target a process group
        # or fail; either way they cannot be a real holder. Treat as
        # not-alive so the caller reclaims the lock.
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:  # pragma: no cover - rare kernel errnos
        # ESRCH should have been caught above; any other errno here
        # means we genuinely don't know. Be conservative.
        if exc.errno == errno.ESRCH:
            return False
        return True
    return True


def _read_holder_pid(path: str) -> Optional[int]:
    """Return the PID recorded in ``path``'s lock file, or ``None``.

    The lock file format is one JSON object per line with at least a
    ``pid`` integer. We only look at the first line — subsequent lines
    are ignored. Returns ``None`` if the file is missing, empty, not
    valid JSON, or lacks a ``pid`` field. Callers treat ``None`` the
    same as a stale holder: the lock contents are unparseable, so the
    only safe action is to reclaim.
    """
    try:
        with open(path, "rb") as fh:
            first_line = fh.readline()
    except FileNotFoundError:
        return None
    except OSError:
        return None

    if not first_line:
        return None

    try:
        decoded = first_line.decode("utf-8").strip()
        payload = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None
    pid = payload.get("pid")
    if not isinstance(pid, int):
        return None
    return pid


def _write_holder_record(fd: int, run_id: str) -> None:
    """Truncate ``fd`` and write a fresh ``{pid, run_id, started_at}`` line.

    The fsync after writing makes the holder PID durable on disk before
    we yield control to the protected region. If the process is killed
    (e.g. SIGKILL) after this point, the next invocation's stale-PID
    reclaim path can read the PID and verify it is no longer alive.

    Validates: Requirement 16.4.
    """
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    payload = {
        "pid": os.getpid(),
        "run_id": run_id,
        "started_at": _now_iso_seconds(),
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    os.write(fd, encoded + b"\n")
    os.fsync(fd)


def _release_lock(fd: int, path: str) -> None:
    """Idempotently release the lock at ``fd`` and unlink ``path``.

    Suppresses errors so the release path is safe to call from both the
    context-manager ``finally`` block and the ``atexit`` handler. The
    sequence is: unlock → close → unlink. If ``fd`` is already closed
    (e.g. the context manager already cleaned up), the OSError is
    swallowed; the unlink is similarly best-effort.
    """
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(fd)
    except OSError:
        pass
    with contextlib.suppress(FileNotFoundError, OSError):
        os.unlink(path)


def _inode_matches(fd: int, path: str) -> bool:
    """Return True if ``fd`` and ``path`` resolve to the same inode.

    After ``fcntl.flock`` succeeds we re-stat ``path`` and compare
    ``(st_dev, st_ino)`` to ``fstat(fd)``. If they differ, our fd has
    been "orphaned" — typically because another reclaimer unlinked the
    file and recreated it after we opened ours. In that case the lock
    we hold is on a different inode than any subsequent acquirer would
    target, so we must release and retry on the new inode.
    """
    try:
        st_fd = os.fstat(fd)
        st_path = os.stat(path)
    except FileNotFoundError:
        return False
    return (st_fd.st_dev, st_fd.st_ino) == (st_path.st_dev, st_path.st_ino)


@contextlib.contextmanager
def writer_lock(
    path: str,
    run_id: str,
    timeout_s: float = 5.0,
) -> Iterator[int]:
    """Acquire an exclusive single-writer lock at ``path``.

    Behaviour:

    - Opens (or creates) the lock file with mode ``0o644`` and tries
      ``fcntl.flock(fd, LOCK_EX | LOCK_NB)``. On success, truncates the
      file and writes a single JSON line ``{pid, run_id, started_at}``
      followed by ``fsync``, then yields the file descriptor.
    - Registers an ``atexit`` handler that releases the lock and
      unlinks the file, so a hard exit between ``yield`` and the
      ``finally`` block does not leak the lock.
    - On ``BlockingIOError`` (lock already held), reads the recorded
      PID from the file. If the PID is no longer running
      (``os.kill(pid, 0)`` raises ``ProcessLookupError``), unlinks the
      file and retries on a fresh inode. If the PID is alive, sleeps
      briefly and retries until ``timeout_s`` elapses, then raises
      :class:`LockConflict(holder_pid=...)`.
    - Performs an inode-stability check after a successful flock to
      defend against the unlink-then-recreate race: if our fd's inode
      no longer matches the path, we release and retry on the new
      inode rather than holding a lock on an orphan.

    Yields the writable file descriptor so callers that want to
    inspect or update the lock contents can do so. The descriptor is
    closed automatically on context exit.

    Validates: Requirements 16.3, 16.4, 16.5, 16.6.
    """
    deadline = time.monotonic() + timeout_s
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)

    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # The lock is held. Inspect the on-disk holder record.
            holder_pid = _read_holder_pid(path)
            if holder_pid is not None and not _pid_alive(holder_pid):
                # Stale: the recorded PID is gone. Drop the dead
                # holder's file (best-effort unlink) and reopen so
                # subsequent acquirers and we agree on the same inode.
                with contextlib.suppress(FileNotFoundError, OSError):
                    os.unlink(path)
                try:
                    os.close(fd)
                except OSError:
                    pass
                fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
                continue
            if time.monotonic() >= deadline:
                # Live holder, deadline exceeded → unrecoverable.
                # Close our fd before raising so we don't leak it.
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise LockConflict(
                    holder_pid=holder_pid if holder_pid is not None else -1
                )
            time.sleep(_LOCK_POLL_INTERVAL_S)
            continue

        # flock succeeded. Verify our fd still corresponds to the file
        # at ``path``; another reclaimer may have unlinked-and-recreated
        # the lock file between our open and our flock.
        if not _inode_matches(fd, path):
            # Orphan: our flock is on a stale inode. Release, reopen,
            # and retry. This is the pathological "unlink race" path;
            # in normal operation flock fires once and we proceed.
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(fd)
            except OSError:
                pass
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
            if time.monotonic() >= deadline:
                # Out of time during the recovery dance; report the
                # currently-recorded holder if any.
                holder_pid = _read_holder_pid(path)
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise LockConflict(
                    holder_pid=holder_pid if holder_pid is not None else -1
                )
            continue

        # Lock acquired and inode confirmed. Record the holder and arm
        # the atexit release.
        _write_holder_record(fd, run_id)
        released = [False]

        def _atexit_release(fd: int = fd, path: str = path) -> None:
            # Captured fd / path so the handler is independent of any
            # surrounding scope rebinding on retry. ``released`` short-
            # circuits the normal-exit case so we don't double-close.
            if released[0]:
                return
            released[0] = True
            _release_lock(fd, path)

        atexit.register(_atexit_release)

        try:
            yield fd
        finally:
            released[0] = True
            _release_lock(fd, path)
            with contextlib.suppress(Exception):
                # ``atexit.unregister`` is a no-op if not registered on
                # current versions; the suppress guards against any
                # implementation-specific quirk on older interpreters.
                atexit.unregister(_atexit_release)
        return
