# Feature: multicloud-photo-sync, Property 16: First-pass safety
"""First-pass safety property test.

Property under test (design.md, "Correctness Properties — Property 16:
First-pass safety"):

  For any Cold_Start ``Sync_Run`` invoked with ``--apply`` but without
  ``--first-pass-confirmed``, for any multi-source workload R (including
  workloads with same-source duplicate groups, cross-source duplicate
  groups, and expired-quarantine records), and for any configuration
  (``on_key_conflict``, ``delete_propagation``,
  ``quarantine_retention_days``, ``tombstone_retention_days``):

  * the count of ``set_tag(key, "mcps-quarantined-at", ...)`` calls
    observed on every adapter is exactly ``0``;
  * the count of ``delete(key)`` calls observed on every adapter is
    exactly ``0``;
  * the count of ``write_bytes(key, ...)`` calls observed against any
    destination key whose pre-existing destination Content_Hash differs
    from the incoming Content_Hash (the ``overwrite`` arm of
    ``on_key_conflict``) is exactly ``0``;
  * the count of ``write_bytes(key, ...)`` calls against destinations
    where the incoming Content_Hash is **absent** from the destination
    is unconstrained (req 18.3 explicitly permits non-destructive
    replication writes);
  * the count of Drive_Importer ``write_bytes(...)`` calls to the
    configured ``drive_destination`` for files whose Content_Hash is
    absent from every Replicated_Source is unconstrained (req 18.3
    explicitly permits Drive_Importer-to-absent uploads);
  * the run's exit code equals ``FIRST_PASS_REVIEW_REQUIRED`` (76).

The strategy generates a Cold_Start workload with arbitrary
``{key: bytes}`` mappings on each Source, drives a CLI invocation
through ``cli.run(args)`` with monkeypatched adapter and credential
factories, and asserts against each adapter's ``call_log``.

Validates: Requirements 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 18.3,
18.4, 18.7.
"""

from __future__ import annotations

import argparse
import io
import os
from datetime import datetime, timezone
from typing import Mapping

import pytest
import yaml
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.cli import run as cli_run
from mcps.credentials import Credential_Manager, ResolvedCredentials
from mcps.drive_import import (
    MCPS_CONTENT_SHA256_KEY as DRIVE_HASH_KEY,
)
from mcps.errors import ExitCode
from mcps.replication import (
    MCPS_CONTENT_SHA256_KEY,
    MCPS_SOURCE_KEY,
)
from mcps.sources.base import SourceAdapter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


# Two Replicated_Sources + one Pull_Only_Source, mirroring design.md's
# canonical three-Source topology. Names are stable so the property
# test's adapter factory can match by name.
_S3_NAME = "s3-bucket"
_GCS_NAME = "gcs-bucket"
_DRIVE_NAME = "drive-folder"


# Small key pool so cross-source overlap and same-source duplicates fire
# regularly. Keys are valid Drive paths (the Drive adapter records the
# file id in ``key`` and the relative path under the configured root in
# ``user_metadata['drive_path']`` — for the fake we keep them aligned).
_KEY_POOL: tuple[str, ...] = ("a.jpg", "b.jpg", "c.png")


# Small content pool so duplicates across Sources are likely. Each
# element is the literal file content; SHA-256 over these bytes
# determines the Content_Hash both at listing time (the fake adapter
# computes it via ``stream_sha256``) and after any replication writes.
_CONTENT_POOL: tuple[bytes, ...] = (
    b"alpha-content",
    b"beta-content",
    b"gamma-content",
)


_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


@st.composite
def _source_population(draw) -> Mapping[str, bytes]:
    """Draw an arbitrary ``{key: bytes}`` mapping for one Source.

    Keys are drawn from `_KEY_POOL` without replacement; each key gets
    one bytes payload from `_CONTENT_POOL`. The resulting dict has
    cardinality 0..|key_pool|, which keeps the state space tractable
    for 200 examples while still exercising every duplicate /
    cross-source / collision branch.
    """
    n = draw(st.integers(min_value=0, max_value=len(_KEY_POOL)))
    keys = draw(
        st.lists(
            st.sampled_from(_KEY_POOL),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    population: dict[str, bytes] = {}
    for key in keys:
        population[key] = draw(st.sampled_from(_CONTENT_POOL))
    return population


@st.composite
def _cold_start_workload(draw) -> dict[str, Mapping[str, bytes]]:
    """Draw a Cold_Start workload across the three Sources."""
    return {
        _S3_NAME: draw(_source_population()),
        _GCS_NAME: draw(_source_population()),
        _DRIVE_NAME: draw(_source_population()),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_drive_adapter(population: Mapping[str, bytes]) -> FakeSourceAdapter:
    """Build a read-only Drive adapter with mime-type + createdTime metadata.

    Drive's :class:`GoogleDriveSourceAdapter` exposes ``content_type`` =
    the file's mimeType and stuffs ``createdTime`` and ``drive_path``
    into ``user_metadata``. The Drive_Importer's filter requires the
    mimeType to start with ``image/`` or ``video/``, and the
    destination-key builder requires ``createdTime`` for the year /
    month components. We supply both so the importer treats the files
    as eligible.
    """
    metadata: dict[str, dict[str, str]] = {}
    last_modified: dict[str, str] = {}
    # FakeSourceAdapter._guess_content_type uses the file extension —
    # `.jpg` / `.png` map to image/jpeg and image/png respectively, so
    # the fake's content_type already passes the importer's filter
    # without us setting it explicitly.
    for key in population:
        metadata[key] = {
            # No mcps-content-sha256: Cold_Start mandates streaming.
            "createdTime": "2024-01-15T10:30:00Z",
            "drive_path": key,
            "drive_file_id": key,
        }
        last_modified[key] = "2024-01-01T00:00:00Z"

    return FakeSourceAdapter(
        name=_DRIVE_NAME,
        kind="google_drive",
        supports_writes=False,
        records=dict(population),
        metadata=metadata,
        last_modified=last_modified,
    )


def _make_replicated_adapter(
    name: str, kind: str, population: Mapping[str, bytes]
) -> FakeSourceAdapter:
    """Build a writable S3 / GCS adapter with no mcps-* metadata.

    The Cold_Start preconditions explicitly state that BOTH the S3
    bucket(s) and the Drive folder are pre-populated with objects that
    carry NO ``mcps-*`` metadata. We mirror that here: the records
    have content but no ``mcps-content-sha256``, so the listing path
    is forced through the streaming SHA-256 fallback (req 7.2).
    """
    last_modified: dict[str, str] = {key: "2024-01-01T00:00:00Z" for key in population}
    return FakeSourceAdapter(
        name=name,
        kind=kind,
        supports_writes=True,
        records=dict(population),
        metadata={},  # cold-start: no mcps-* metadata
        last_modified=last_modified,
    )


def _write_config(tmp_path) -> str:
    """Plant a configuration file referencing all three sources."""
    config = {
        "sources": [
            {"name": _S3_NAME, "kind": "s3", "bucket": "test-s3"},
            {"name": _GCS_NAME, "kind": "gcs", "bucket": "test-gcs"},
            {
                "name": _DRIVE_NAME,
                "kind": "google_drive",
                "drive_root_folder_id": "test-folder-id",
            },
        ],
        "replication": {
            "pairs": [[_S3_NAME, _GCS_NAME], [_GCS_NAME, _S3_NAME]],
            "on_key_conflict": "overwrite",  # property must hold even here
            "fail_on_conflict": False,
            "delete_propagation": "none",
            "tombstone_retention_days": 30,
            "fail_on_inconsistency": False,
        },
        "duplicates": {
            "canonical_source_priority": [_S3_NAME, _GCS_NAME],
            "quarantine_retention_days": 1,  # so any expired records would be eligible
        },
        "photos": {
            "drive_source": _DRIVE_NAME,
            "drive_destination": _S3_NAME,
        },
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
        },
    }
    config_path = str(tmp_path / "mcps.config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return config_path


class _StubCredentialManager(Credential_Manager):
    """Credential_Manager that always returns empty ResolvedCredentials.

    The property test never hits a real provider SDK, so we sidestep
    real credential resolution by returning empty objects.
    """

    def __init__(self) -> None:
        # Skip the parent init's SDK lookup; we only need the resolver
        # methods to return non-raising values.
        pass

    def resolve_aws(self) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="aws", source="stub")

    def resolve_gcp(self, scopes=None) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="gcp", source="stub")

    def resolve_drive(self) -> ResolvedCredentials:  # type: ignore[override]
        return ResolvedCredentials(provider="drive", source="stub")


def _count_overwriting_writes(
    adapter: FakeSourceAdapter,
    pre_existing_keys: frozenset[str],
) -> int:
    """Count write_bytes calls that target a pre-existing key (overwrite).

    A write is "overwriting" iff its ``key`` was already present in the
    adapter at run start. The set ``pre_existing_keys`` is captured
    before ``cli_run`` so subsequent writes that *create* a new key
    (the non-destructive replicate-to-absent path, req 18.3 permits
    these) are not counted.
    """
    count = 0
    for method, kwargs in adapter.call_log:
        if method != "write_bytes":
            continue
        if kwargs.get("key") in pre_existing_keys:
            count += 1
    return count


def _count_calls(adapter: FakeSourceAdapter, method_name: str) -> int:
    return sum(1 for method, _ in adapter.call_log if method == method_name)


def _count_set_tag_quarantined(adapter: FakeSourceAdapter) -> int:
    """Count ``set_tag(key, "mcps-quarantined-at", ...)`` calls."""
    count = 0
    for method, kwargs in adapter.call_log:
        if method != "set_tag":
            continue
        if kwargs.get("tag_key") == "mcps-quarantined-at":
            count += 1
    return count


# ---------------------------------------------------------------------------
# The Property 16 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(workload=_cold_start_workload())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_first_pass_safety(
    workload: dict[str, Mapping[str, bytes]],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Cold_Start --apply without --first-pass-confirmed is non-destructive.

    Validates: Requirements 13.1-13.6, 18.3, 18.4, 18.7.
    """
    tmp_path = tmp_path_factory.mktemp("first_pass_safety")

    # Build adapters seeded with the generated workloads.
    s3_adapter = _make_replicated_adapter(_S3_NAME, "s3", workload[_S3_NAME])
    gcs_adapter = _make_replicated_adapter(_GCS_NAME, "gcs", workload[_GCS_NAME])
    drive_adapter = _make_drive_adapter(workload[_DRIVE_NAME])

    adapters: dict[str, SourceAdapter] = {
        _S3_NAME: s3_adapter,
        _GCS_NAME: gcs_adapter,
        _DRIVE_NAME: drive_adapter,
    }

    # Capture the pre-existing key sets so we can distinguish
    # "destructive overwrite" writes from "non-destructive create" writes.
    s3_pre = frozenset(s3_adapter.records)
    gcs_pre = frozenset(gcs_adapter.records)

    config_path = _write_config(tmp_path)

    # Build the args namespace directly so we can run with --apply
    # without exec'ing argparse (which is exercised by test_cli_smoke).
    args = argparse.Namespace(
        config=config_path,
        dry_run=False,
        apply=True,
        auto_approve=True,
        first_pass_confirmed=False,
        log_level="ERROR",  # quieten the structured logger during the test
        run_id="property16",
        catalog=None,
        manifest_dir=None,
        lock_path=None,
    )

    stdout = io.StringIO()
    stderr = io.StringIO()
    cwd = str(tmp_path)
    # Plant a sentinel non-legacy config.ini so detect_legacy_config is a no-op.
    # (No file is required; the helper returns None when the file is absent.)

    exit_code = cli_run(
        args,
        env=os.environ,
        stdout=stdout,
        stderr=stderr,
        cwd=cwd,
        adapter_factory=lambda src: adapters[src.name],
        credential_manager=_StubCredentialManager(),
        now=lambda: _FIXED_NOW,
    )

    # 1. Exit code is FIRST_PASS_REVIEW_REQUIRED (76).
    assert exit_code == int(ExitCode.FIRST_PASS_REVIEW_REQUIRED), (
        f"expected exit code {int(ExitCode.FIRST_PASS_REVIEW_REQUIRED)} "
        f"({ExitCode.FIRST_PASS_REVIEW_REQUIRED.name}), got {exit_code}"
    )

    # 2. Zero set_tag(mcps-quarantined-at, …) calls on every adapter.
    for adapter in (s3_adapter, gcs_adapter, drive_adapter):
        assert _count_set_tag_quarantined(adapter) == 0, (
            f"adapter {adapter.name!r} received "
            f"{_count_set_tag_quarantined(adapter)} mcps-quarantined-at "
            f"set_tag calls"
        )

    # 3. Zero delete calls on every adapter.
    for adapter in (s3_adapter, gcs_adapter, drive_adapter):
        assert _count_calls(adapter, "delete") == 0, (
            f"adapter {adapter.name!r} received "
            f"{_count_calls(adapter, 'delete')} delete calls"
        )

    # 4. Zero overwriting write_bytes calls on either Replicated_Source.
    s3_overwrites = _count_overwriting_writes(s3_adapter, s3_pre)
    gcs_overwrites = _count_overwriting_writes(gcs_adapter, gcs_pre)
    assert s3_overwrites == 0, (
        f"adapter {_S3_NAME!r} received {s3_overwrites} overwriting "
        f"write_bytes calls (pre-existing keys: {sorted(s3_pre)})"
    )
    assert gcs_overwrites == 0, (
        f"adapter {_GCS_NAME!r} received {gcs_overwrites} overwriting "
        f"write_bytes calls (pre-existing keys: {sorted(gcs_pre)})"
    )

    # 5. Drive (read-only) received zero write_bytes calls regardless
    #    of branch — both the design's Drive read-only contract
    #    (req 10.8) and the property's "no overwrites" rule converge here.
    assert _count_calls(drive_adapter, "write_bytes") == 0, (
        f"Drive adapter received {_count_calls(drive_adapter, 'write_bytes')} "
        f"write_bytes calls — Drive is read-only"
    )
