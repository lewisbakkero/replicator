"""Integration test: lock-conflict against a live PID and stale-PID reclaim.

Validates: Requirements 16.5, 16.6.

This test exercises ``mcps.cli.run``'s end-to-end interaction with
``mcps.concurrency.writer_lock`` against a pre-planted lock file. It
mirrors the adapter-injection pattern used by
``tests/integration/test_full_run_dry.py`` and
``tests/integration/test_full_run_apply.py``: the run is driven through
the real CLI with stub credential resolution and a closure that returns
empty :class:`FakeSourceAdapter` instances, so the only state under
test is the lock-acquisition path.

Two cases:

1. **Live-PID lock-held** (req 16.5): the test plants a lock file
   recording ``os.getpid()`` AND holds the kernel-level
   ``fcntl.flock`` on it via a separate file descriptor. The CLI's
   subsequent ``writer_lock`` attempt observes ``BlockingIOError``,
   reads the recorded PID, finds it alive (we are the recorded
   process), and ultimately raises :class:`LockConflict`. The test
   asserts the exception's exit code is ``ExitCode.LOCK_CONFLICT``
   (73) and that the wall-clock elapsed time is under five seconds —
   the upper bound documented by req 16.5.

2. **Stale-PID reclaim** (req 16.6): the test plants a lock file
   recording a PID that is *not* running (verified via
   ``os.kill(pid, 0)`` raising ``ProcessLookupError``) WITHOUT
   holding the kernel-level lock. The CLI's ``writer_lock`` either
   acquires the kernel lock immediately (no holder process) or, on
   the off chance that a stray fd is still on the file, falls into
   the stale-PID reclaim branch. Either way the run proceeds, writes
   a SUMMARY Manifest entry, and exits with ``ExitCode.OK`` (0).
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Iterable, List, Tuple

import pytest
import yaml

from mcps import cli
from mcps.credentials import Credential_Manager, ResolvedCredentials
from mcps.errors import ExitCode, LockConflict
from mcps.manifest.model import Action, Result
from mcps.manifest.parser import parse_manifest_file
from mcps.sources.base import SourceAdapter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


# Two replicated Sources are sufficient for a minimal Sync_Run; we
# omit the optional Drive Pull_Only_Source so the test does not need
# to mint a Drive adapter just to satisfy the photos section.
_S3_NAME = "s3-bucket"
_GCS_NAME = "gcs-bucket"

_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# Wall-clock guard for the live-PID case. Req 16.5 mandates that the
# CLI exits with ``LOCK_CONFLICT`` within five seconds; ``writer_lock``
# uses a five-second default deadline, so we allow a small margin for
# scheduling jitter and the outer Python frames the call passes
# through.
_LOCK_WALL_CLOCK_BUDGET_S = 6.0


# ---------------------------------------------------------------------------
# Stub credential manager — bypasses real provider chains
# ---------------------------------------------------------------------------


class _StubCredentialManager(Credential_Manager):
    """Credential manager that returns empty resolutions without I/O."""

    def __init__(self) -> None:
        # Skip the parent ``__init__``; we do not need any SDK lookup
        # closures for the integration test.
        pass

    def resolve_aws(self) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="aws", source="stub")

    def resolve_gcp(self, scopes=None) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="gcp", source="stub")

    def resolve_drive(self) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="drive", source="stub")


# ---------------------------------------------------------------------------
# Helpers: config, adapters, lock files
# ---------------------------------------------------------------------------


def _write_minimal_config(tmp_path, *, lock_path: str) -> str:
    """Plant a minimal YAML config wiring two empty replicated Sources.

    The lock path is pinned explicitly so the test controls the file
    that ``writer_lock`` operates on.
    """
    config = {
        "sources": [
            {"name": _S3_NAME, "kind": "s3", "bucket": "test-s3"},
            {"name": _GCS_NAME, "kind": "gcs", "bucket": "test-gcs"},
        ],
        "replication": {
            "pairs": [[_S3_NAME, _GCS_NAME], [_GCS_NAME, _S3_NAME]],
            "on_key_conflict": "skip",
            "fail_on_conflict": False,
            "delete_propagation": "none",
            "tombstone_retention_days": 30,
            "fail_on_inconsistency": False,
        },
        "duplicates": {
            "canonical_source_priority": [_S3_NAME, _GCS_NAME],
            "quarantine_retention_days": 30,
        },
        # photos section is required by the parser even when no
        # Drive source is configured; both fields are optional and
        # left unset so the Drive_Importer step is a no-op.
        "photos": {},
        "retries": {
            "max_retries": 1,
            "initial_backoff_ms": 100,
            "max_backoff_ms": 1000,
            "request_timeout_ms": 1000,
        },
        "runtime": {
            "catalog_path": str(tmp_path / "catalog.jsonl"),
            "manifest_dir": str(tmp_path / "manifests"),
            "max_concurrent_transfers": 1,
            "lock_path": lock_path,
        },
    }
    config_path = str(tmp_path / "mcps.config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return config_path


def _empty_adapter_factory():
    """Return an ``adapter_factory`` that builds empty fake adapters.

    The fakes record every call into ``call_log`` but the populations
    are empty, so a Sync_Run lists zero objects, performs zero
    replication writes, and emits only the SUMMARY entry. That keeps
    the test focused on the lock-acquisition path.
    """
    s3 = FakeSourceAdapter(
        name=_S3_NAME, kind="s3", supports_writes=True, records={}
    )
    gcs = FakeSourceAdapter(
        name=_GCS_NAME, kind="gcs", supports_writes=True, records={}
    )

    def _factory(src) -> SourceAdapter:
        if src.name == _S3_NAME:
            return s3
        if src.name == _GCS_NAME:
            return gcs
        raise AssertionError(f"unexpected source name: {src.name!r}")

    return _factory


def _build_args(*, config_path: str) -> argparse.Namespace:
    """Build the argparse Namespace for a minimal ``--dry-run`` invocation."""
    return argparse.Namespace(
        config=config_path,
        dry_run=True,
        apply=False,
        auto_approve=False,
        first_pass_confirmed=False,
        log_level="ERROR",  # quiet stderr
        run_id="lockstaletest01",
        catalog=None,
        manifest_dir=None,
        lock_path=None,
    )


def _plant_lock_record(path: str, *, pid: int, run_id: str) -> None:
    """Write a single ``{pid, run_id, started_at}`` line to ``path``.

    The encoding mirrors :func:`mcps.concurrency._write_holder_record`
    exactly (compact JSON, trailing newline) so the CLI's
    ``_read_holder_pid`` parses it correctly.
    """
    started_at = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    payload = {"pid": pid, "run_id": run_id, "started_at": started_at}
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
    # Truncate / create the file with mode 0o644 to match the CLI.
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, encoded)
        os.fsync(fd)
    finally:
        os.close(fd)


def _find_dead_pid() -> int:
    """Return a PID that is currently not in use by any running process.

    Probes high-numbered PIDs with ``os.kill(pid, 0)``; the first one
    that raises :class:`ProcessLookupError` wins. We start from a
    high value to minimise the chance of colliding with a real
    process and walk downward in case the kernel has assigned a very
    high PID. If no candidate is found in the search window the test
    is skipped — this should effectively never happen on any
    development machine.
    """
    for candidate in (999_999, 998_877, 987_654, 654_321, 123_456):
        try:
            os.kill(candidate, 0)
        except ProcessLookupError:
            return candidate
        except PermissionError:
            # Owned by another user → in use; keep looking.
            continue
        except OSError as exc:
            # Defensive: any unexpected errno → skip this candidate.
            if exc.errno == errno.ESRCH:
                return candidate
            continue
    pytest.skip("could not locate a known-unused PID for the stale-PID test")
    raise AssertionError("unreachable")  # pragma: no cover


# ---------------------------------------------------------------------------
# Test 1 — live-PID lock-held case (req 16.5)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_lock_held_by_live_current_pid_raises_lock_conflict_within_5s(
    tmp_path,
) -> None:
    """The CLI exits with LOCK_CONFLICT (73) within 5s when the lock is held.

    The test acquires the kernel-level ``fcntl.flock`` on a planted
    lock file in this very process (a separate file descriptor than
    the one the CLI will open). The lock file's first line records
    ``os.getpid()``. When ``cli.run`` calls ``writer_lock``:

    * ``os.open`` succeeds (the file exists).
    * ``fcntl.flock(fd, LOCK_EX | LOCK_NB)`` raises
      ``BlockingIOError`` because we hold the lock on a sibling fd.
    * ``_read_holder_pid`` returns our own PID.
    * ``_pid_alive`` returns True (we are obviously running), so the
      reclaim branch is skipped.
    * The five-second deadline elapses and :class:`LockConflict` is
      raised with ``holder_pid=os.getpid()``.

    We catch the exception, translate it to its exit code via
    ``to_exit_code()``, and assert (a) the value is 73 and (b) the
    wall-clock elapsed time is below the documented upper bound.
    """
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")
    config_path = _write_minimal_config(tmp_path, lock_path=lock_path)

    # Plant the holder record AND hold the kernel flock from this
    # process. Two separate fds: one we hold for the test's lifetime,
    # one closed immediately after writing the record.
    _plant_lock_record(lock_path, pid=os.getpid(), run_id="planted-holder")

    held_fd = os.open(lock_path, os.O_RDWR)
    fcntl.flock(held_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    try:
        args = _build_args(config_path=config_path)
        stdout = io.StringIO()
        stderr = io.StringIO()

        start = time.monotonic()
        with pytest.raises(LockConflict) as exc_info:
            cli.run(
                args,
                env=os.environ,
                stdout=stdout,
                stderr=stderr,
                cwd=str(tmp_path),
                adapter_factory=_empty_adapter_factory(),
                credential_manager=_StubCredentialManager(),
                now=lambda: _FIXED_NOW,
            )
        elapsed = time.monotonic() - start
    finally:
        # Always release the kernel flock, even on assertion failure.
        try:
            fcntl.flock(held_fd, fcntl.LOCK_UN)
        finally:
            os.close(held_fd)

    # Req 16.5: the run exits within 5 seconds with exit code 73.
    assert elapsed < _LOCK_WALL_CLOCK_BUDGET_S, (
        f"writer_lock took {elapsed:.3f}s to time out; "
        f"req 16.5 mandates ≤ 5s"
    )

    err = exc_info.value
    assert err.holder_pid == os.getpid(), (
        f"expected holder_pid={os.getpid()}, got {err.holder_pid}"
    )
    assert err.to_exit_code() == ExitCode.LOCK_CONFLICT
    assert int(err.to_exit_code()) == 73


# ---------------------------------------------------------------------------
# Test 2 — stale-PID reclaim case (req 16.6)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_lock_with_dead_pid_is_reclaimed_and_run_proceeds(tmp_path) -> None:
    """A planted lock file recording a dead PID is reclaimed; the run exits 0.

    No process holds the kernel ``flock`` on the planted file (the
    putative holder is gone). The CLI's ``writer_lock`` will:

    * ``os.open`` the file.
    * ``fcntl.flock(LOCK_EX | LOCK_NB)`` succeeds (no kernel holder).
    * ``_inode_matches`` confirms our fd targets the planted file.
    * ``_write_holder_record`` overwrites the planted record with a
      fresh one identifying *this* process as the holder.

    The Sync_Run then proceeds against empty fake adapters and writes
    one SUMMARY Manifest entry. ``cli.run`` returns ``ExitCode.OK``
    (0) and the on-disk Manifest contains the SUMMARY record.
    """
    lock_path = str(tmp_path / "mcps.catalog.jsonl.lock")
    config_path = _write_minimal_config(tmp_path, lock_path=lock_path)

    dead_pid = _find_dead_pid()
    # Re-confirm the candidate is dead immediately before planting,
    # in case the OS recycled the PID since ``_find_dead_pid``.
    with pytest.raises(ProcessLookupError):
        os.kill(dead_pid, 0)

    _plant_lock_record(lock_path, pid=dead_pid, run_id="dead-holder")

    args = _build_args(config_path=config_path)
    stdout = io.StringIO()
    stderr = io.StringIO()

    exit_code = cli.run(
        args,
        env=os.environ,
        stdout=stdout,
        stderr=stderr,
        cwd=str(tmp_path),
        adapter_factory=_empty_adapter_factory(),
        credential_manager=_StubCredentialManager(),
        now=lambda: _FIXED_NOW,
    )

    # Req 16.6: the run reclaims the lock and proceeds. Exit code 0.
    assert exit_code == int(ExitCode.OK), (
        f"expected exit code OK ({int(ExitCode.OK)}), got {exit_code}; "
        f"stderr={stderr.getvalue()!r}"
    )

    # The lock file must have been released and unlinked by
    # ``writer_lock`` on context exit. Its absence is the visible
    # post-condition that the run completed normally.
    assert not os.path.exists(lock_path), (
        f"writer_lock did not unlink {lock_path!r} on release"
    )

    # The Manifest must contain a SUMMARY record — the proof that the
    # Sync_Run executed past the lock-acquisition step.
    manifest_dir = str(tmp_path / "manifests")
    manifest_files = sorted(
        f for f in os.listdir(manifest_dir) if f.endswith(".jsonl")
    )
    assert len(manifest_files) == 1, (
        f"expected exactly one manifest file under {manifest_dir!r}, "
        f"got {manifest_files!r}"
    )
    records, errors = parse_manifest_file(
        os.path.join(manifest_dir, manifest_files[0])
    )
    assert errors == [], f"manifest parse errors: {errors!r}"

    summary_records = [r for r in records if r.action == Action.SUMMARY]
    assert len(summary_records) == 1, (
        f"expected exactly one SUMMARY record, got {len(summary_records)}: "
        f"{[r.action.value for r in records]!r}"
    )
    summary = summary_records[0]
    assert summary.result == Result.SUCCESS
    assert summary.run_id == args.run_id
