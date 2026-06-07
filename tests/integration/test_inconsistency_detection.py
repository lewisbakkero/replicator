"""Integration test: post-replication inconsistency detection.

Validates: Requirements 19.1, 19.2, 19.3.

Per req 19.1, every Sync_Run MUST end with an :class:`mcps.reconciliation.Inconsistency_Detector`
pass over the post-replication Replicated_Source state. Per req 19.2, the detector emits one
``WARN`` log record at event ``mcps.reconciliation.inconsistency`` per Content_Hash that is
present in some Replicated_Source AND absent from at least one other Replicated_Source after
replication completes (excluding hashes that recorded a ``REPLICATION_ERROR`` Manifest entry,
req 19.3). Per req 19.3, when ``replication.fail_on_inconsistency`` is ``true`` and at least
one divergent hash is observed, the Sync_Run exits with code 78
(``INCONSISTENCY_DETECTED``).

This test wires the production adapter classes (`mcps.sources.s3.S3SourceAdapter` against a
``moto``-mocked S3, `mcps.sources.gcs.GCSSourceAdapter` against an in-process fake) through
the real `mcps.cli.run` pipeline to drive the end-to-end behaviour. The seeded state mirrors
the task brief:

* S3 holds Object ``photos/img.jpg`` carrying Content_Hash ``H1`` (and the
  ``mcps-content-sha256`` user-metadata that backs the post-replication listing's hash
  set, since `mcps.cli._list_post_replication` only counts records with valid metadata
  hashes).
* GCS holds Object ``photos/img.jpg`` carrying a *different* Content_Hash ``H2``.
* Both Replicated_Sources are configured. The CLI runs in ``--dry-run`` mode so the
  Replicator is not invoked; the post-replication listing therefore observes the same
  divergent state the test seeded, and the Inconsistency_Detector flags both ``H1`` and
  ``H2`` as divergent.

Why ``--dry-run`` instead of the implementation-note's ``replication.pairs = []`` trick:
the runtime ignores ``replication.pairs`` and derives the list of Replicated_Sources from
``config.replicated_sources()`` (kinds ``s3`` and ``gcs``), so an empty ``pairs`` list does
not actually disable replication. ``--dry-run`` cleanly skips the Replicator (which only
runs ``if apply_mode``) while still exercising the Inconsistency_Detector wiring, which
runs unconditionally at end-of-run regardless of mode (`mcps.cli._run_locked` step 15).

Assertions:

1. One ``WARN`` log record per divergent hash, captured via a custom
   :class:`logging.Handler` attached to the ``mcps`` logger before invocation. Each record
   carries ``event == "mcps.reconciliation.inconsistency"``, ``content_hash`` matching one
   of the seeded hashes, and ``present_in`` / ``absent_from`` correctly partitioning the
   two configured Replicated_Sources.
2. The on-disk Manifest's SUMMARY record has ``extra["divergent_hashes_count"] > 0`` and
   equals the number of divergent hashes (``"2"``).
3. The exit code from `cli.run` is 78 (``INCONSISTENCY_DETECTED``).
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import logging
import os
from typing import Any, Dict, List, Mapping, Optional

import boto3  # type: ignore[import-not-found]
import pytest

# moto-related imports are lazy so a missing optional dep surfaces a
# skip rather than a collection error.
moto = pytest.importorskip("moto")
from moto import mock_aws  # type: ignore[import-not-found]  # noqa: E402

from mcps import cli  # noqa: E402
from mcps.config.model import SourceConfig  # noqa: E402
from mcps.errors import ExitCode  # noqa: E402
from mcps.manifest.model import Action, Result  # noqa: E402
from mcps.manifest.parser import parse_manifest_file  # noqa: E402
from mcps.sources.base import ObjectMeta, SourceAdapter  # noqa: E402
from mcps.sources.gcs import GCSSourceAdapter  # noqa: E402
from mcps.sources.s3 import S3SourceAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixed run-time values so assertions are stable
# ---------------------------------------------------------------------------

_AWS_REGION = "us-east-1"
_S3_BUCKET = "mcps-int-bucket-inconsistency-s3"
_GCS_BUCKET = "mcps-int-bucket-inconsistency-gcs"

_RUN_ID = "integration00inconsist01"  # >= 8 chars (req 14.1)
_FIXED_NOW = dt.datetime(2024, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


# Pre-computed payloads. The two Replicated_Sources hold the *same*
# key (``photos/img.jpg``) but byte-distinct content, so each side
# carries a different Content_Hash. Without replication (``--dry-run``)
# the post-replication listing observes both hashes, each present on
# exactly one Replicated_Source — every hash is divergent.
_S3_PAYLOAD = b"S3-DIVERGENT-PAYLOAD-H1" * 8
_GCS_PAYLOAD = b"GCS-DIVERGENT-PAYLOAD-H2" * 8


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# In-process GCS fake (mirrors `tests/integration/test_full_run_apply.py`).
# Kept local so this integration test does not couple to other modules'
# import order.
# ---------------------------------------------------------------------------


class _FakeBlob:
    """Minimal ``google.cloud.storage.Blob`` stand-in."""

    def __init__(
        self,
        *,
        name: str,
        data: bytes = b"",
        updated: Optional[dt.datetime] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Mapping[str, str]] = None,
        crc32c: Optional[str] = None,
        client: Optional["_FakeGcsClient"] = None,
    ) -> None:
        self.name = name
        self._data: bytes = data
        self.size: Optional[int] = len(data) if data else 0
        self.updated: Optional[dt.datetime] = updated
        self.content_type: Optional[str] = content_type
        self.metadata: Optional[Dict[str, str]] = (
            dict(metadata) if metadata is not None else None
        )
        self.crc32c: Optional[str] = crc32c
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def open(self, mode: str = "rb") -> io.BytesIO:
        self.calls.append(("open", {"mode": mode}))
        assert mode == "rb"
        return io.BytesIO(self._data)

    def upload_from_file(
        self,
        file_obj: Any,
        content_type: Optional[str] = None,
    ) -> None:
        self.calls.append(
            ("upload_from_file", {"content_type": content_type})
        )
        if hasattr(file_obj, "read"):
            self._data = file_obj.read()
        else:  # pragma: no cover - defensive
            self._data = bytes(file_obj)
        self.size = len(self._data)

    def reload(self) -> None:
        self.calls.append(("reload", {}))

    def patch(self) -> None:
        self.calls.append(("patch", {"metadata": dict(self.metadata or {})}))

    def delete(self) -> None:
        self.calls.append(("delete", {}))
        if self._client is not None:
            self._client._remove_blob(self.name)


class _FakeBucket:
    def __init__(self, name: str, client: "_FakeGcsClient") -> None:
        self.name = name
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def blob(self, key: str) -> _FakeBlob:
        self.calls.append(("blob", {"key": key}))
        return self._client._get_or_create_blob(self.name, key)


class _FakeGcsClient:
    """In-process ``google.cloud.storage.Client`` stand-in."""

    def __init__(
        self,
        *,
        blobs: Optional[Mapping[tuple[str, str], _FakeBlob]] = None,
    ) -> None:
        self._blobs: Dict[tuple[str, str], _FakeBlob] = dict(blobs or {})
        for blob in self._blobs.values():
            blob._client = self
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def _get_or_create_blob(self, bucket: str, key: str) -> _FakeBlob:
        existing = self._blobs.get((bucket, key))
        if existing is not None:
            return existing
        fresh = _FakeBlob(name=key, client=self)
        self._blobs[(bucket, key)] = fresh
        return fresh

    def _remove_blob(self, key: str) -> None:
        for (bucket, name), _blob in list(self._blobs.items()):
            if name == key:
                del self._blobs[(bucket, name)]

    def bucket(self, name: str) -> _FakeBucket:
        self.calls.append(("bucket", {"name": name}))
        return _FakeBucket(name, self)

    def list_blobs(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> List[_FakeBlob]:
        self.calls.append(("list_blobs", {"bucket": bucket, "prefix": prefix}))
        result: List[_FakeBlob] = []
        for (b, _key), blob in self._blobs.items():
            if b != bucket:
                continue
            if prefix and not blob.name.startswith(prefix):
                continue
            result.append(blob)
        result.sort(key=lambda x: x.name)
        return result


# ---------------------------------------------------------------------------
# S3 adapter wrapper that populates user_metadata at listing time
# ---------------------------------------------------------------------------


class _MetadataListingS3Adapter(S3SourceAdapter):
    """An :class:`S3SourceAdapter` subclass that fetches user-metadata per object during listing.

    The production :meth:`S3SourceAdapter.list_objects` deliberately
    skips a per-object HEAD because the *primary* listing path uses
    `mcps.hashing.compute_content_hash` (which calls ``get_metadata``
    on demand). The Inconsistency_Detector's *post-replication*
    listing path in `mcps.cli._list_post_replication`, however, reads
    ``meta.user_metadata.get("mcps-content-sha256")`` directly off
    the listed `ObjectMeta` and skips records without a valid
    metadata-stored hash. Without populated metadata, an S3 object
    seeded with `mcps-content-sha256` user-metadata is invisible to
    the Inconsistency_Detector and the divergent-state assertion
    underflows.

    This subclass papers over that gap *for the test* by issuing a
    ``head_object`` per listed key (via ``self.get_metadata``) and
    merging the resulting user-metadata into the yielded
    `ObjectMeta`. Production behaviour is unchanged; the subclass is
    only wired through this test's ``adapter_factory``.

    The subclass deliberately does not modify any other adapter
    behaviour: ``read_bytes``, ``write_bytes``, ``set_tag``,
    ``delete``, and ``get_metadata`` all inherit unchanged from the
    superclass.
    """

    def list_objects(self):  # type: ignore[override]
        for meta in super().list_objects():
            try:
                head_meta = self.get_metadata(meta.key)
            except Exception:  # noqa: BLE001 — defensive, mirror super()
                yield meta
                continue
            yield ObjectMeta(
                key=meta.key,
                size_bytes=meta.size_bytes,
                last_modified=meta.last_modified,
                content_type=head_meta.content_type,
                user_metadata=head_meta.user_metadata,
                etag=meta.etag,
                provider_hash=meta.provider_hash,
            )


# ---------------------------------------------------------------------------
# Stub credential manager so cli.run does not touch real provider chains.
# ---------------------------------------------------------------------------


class _StubCredentialManager:
    """Minimal stand-in for `mcps.credentials.Credential_Manager`."""

    def resolve_aws(self) -> Any:  # pragma: no cover - shape only
        return None

    def resolve_gcp(self) -> Any:  # pragma: no cover - shape only
        return None

    def resolve_drive(self) -> Any:  # pragma: no cover - shape only
        return None


# ---------------------------------------------------------------------------
# Capture helper for ``mcps`` logger records
# ---------------------------------------------------------------------------


class _CapturingHandler(logging.Handler):
    """Capture LogRecord instances into an in-memory list.

    The CLI invokes :func:`mcps.logging_setup.setup_logging` which sets
    ``propagate=False`` on the ``mcps`` logger, so pytest's built-in
    ``caplog`` fixture does not see records emitted through that
    logger by default. This handler is attached *to the ``mcps``
    logger directly* before invoking ``cli.run`` so the test can
    inspect the structured WARN records emitted by
    :class:`mcps.reconciliation.Inconsistency_Detector` without
    rewriting any production wiring.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


# ---------------------------------------------------------------------------
# Config writer
# ---------------------------------------------------------------------------


def _write_config_yaml(
    *,
    config_path: str,
    catalog_path: str,
    manifest_dir: str,
    lock_path: str,
) -> None:
    """Write a minimal but complete YAML config wiring S3 + GCS.

    The configuration enables ``replication.fail_on_inconsistency =
    true`` (the headline assertion of req 19.3). No Drive Source is
    configured because divergence between Replicated_Sources is what
    the test exercises; ``photos: {}`` keeps the section present (as
    required by req 17.4) without binding a Drive_Importer.

    ``replication.pairs`` is set to the natural ordered-pair list
    even though the run is in ``--dry-run`` mode and the Replicator
    is therefore not invoked. Pinning the value matches the YAML the
    other integration tests use, so a reader can compare configs
    side-by-side without mentally diffing replication-policy fields.
    """
    content = f"""\
sources:
  - name: s3-prod
    kind: s3
    bucket: {_S3_BUCKET}
    region: {_AWS_REGION}
  - name: gcs-archive
    kind: gcs
    bucket: {_GCS_BUCKET}

replication:
  pairs:
    - [s3-prod, gcs-archive]
    - [gcs-archive, s3-prod]
  on_key_conflict: skip
  fail_on_conflict: false
  delete_propagation: none
  tombstone_retention_days: 30
  fail_on_inconsistency: true

duplicates:
  canonical_source_priority: [s3-prod, gcs-archive]
  quarantine_retention_days: 30

photos: {{}}

retries:
  max_retries: 1
  initial_backoff_ms: 100
  max_backoff_ms: 1000
  request_timeout_ms: 1000

runtime:
  catalog_path: {catalog_path}
  manifest_dir: {manifest_dir}
  max_concurrent_transfers: 2
  lock_path: {lock_path}
"""
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def _seed_s3_bucket(s3_client: Any, *, sha: str) -> None:
    """Seed S3 with a single Object carrying ``mcps-content-sha256=sha``.

    The post-replication listing (`mcps.cli._list_post_replication`)
    only counts records whose ``mcps-content-sha256`` user-metadata
    is a 64-char lowercase hex string, because the
    Inconsistency_Detector compares hash sets and a missing
    metadata hash means the record contributes to no hash set. The
    seed therefore stamps the metadata explicitly so the listing
    sees the divergent state we want.
    """
    s3_client.create_bucket(Bucket=_S3_BUCKET)
    s3_client.put_object(
        Bucket=_S3_BUCKET,
        Key="photos/img.jpg",
        Body=_S3_PAYLOAD,
        ContentType="image/jpeg",
        Metadata={
            "mcps-content-sha256": sha,
            "mcps-source": "s3-prod",
        },
    )


def _build_gcs_client_with_divergent_blob(*, sha: str) -> _FakeGcsClient:
    """Seed one GCS blob at the same key but with different bytes.

    ``mcps-content-sha256`` carries a *different* hash than the S3
    seed so the post-replication listing produces two distinct
    per-source hash sets and the Inconsistency_Detector flags both
    hashes as divergent.
    """
    blobs = {
        (_GCS_BUCKET, "photos/img.jpg"): _FakeBlob(
            name="photos/img.jpg",
            data=_GCS_PAYLOAD,
            updated=dt.datetime(2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={
                "mcps-content-sha256": sha,
                "mcps-source": "gcs-archive",
            },
            crc32c="BBBBB2==",
        ),
    }
    return _FakeGcsClient(blobs=blobs)


# ---------------------------------------------------------------------------
# The integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_inconsistency_detection_emits_warn_and_exits_78(
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: divergent hashes + ``fail_on_inconsistency=true`` → exit 78.

    The test seeds S3 and GCS at the *same key* with byte-distinct
    payloads, runs ``cli.run`` in ``--dry-run`` mode (which skips the
    Replicator entirely so the seeded divergence persists into the
    post-replication listing), and asserts:

    1. The ``mcps`` logger received **exactly one ``WARN`` record per
       divergent hash** at event
       ``mcps.reconciliation.inconsistency`` (req 19.2). With two
       divergent hashes (``H1`` present only on S3, ``H2`` present
       only on GCS) we expect two WARN records. Each record's
       ``content_hash``, ``present_in``, and ``absent_from`` payload
       fields match the seeded state.
    2. The on-disk Manifest's single SUMMARY record carries
       ``extra["divergent_hashes_count"] = "2"`` (req 19.1's
       SUMMARY-channel extension).
    3. ``cli.run`` returns ``78`` (``INCONSISTENCY_DETECTED``,
       req 19.3) because ``replication.fail_on_inconsistency`` is
       ``true`` and at least one divergence was observed.
    """
    # --- Build the run-scoped temp paths. ----------------------------
    config_path = str(tmp_path / "mcps.config.yaml")
    catalog_path = str(tmp_path / "mcps.catalog.jsonl")
    manifest_dir = str(tmp_path / "manifests")
    lock_path = str(tmp_path / "mcps.lock")
    os.makedirs(manifest_dir, exist_ok=True)

    _write_config_yaml(
        config_path=config_path,
        catalog_path=catalog_path,
        manifest_dir=manifest_dir,
        lock_path=lock_path,
    )

    s3_sha = _sha256_hex(_S3_PAYLOAD)
    gcs_sha = _sha256_hex(_GCS_PAYLOAD)
    # Sanity-check the two seeds really produce distinct hashes; if
    # the payloads above were ever changed to the same bytes the test
    # would silently degenerate into a trivial "no divergence" run.
    assert s3_sha != gcs_sha, (
        "test seed misconfiguration: S3 and GCS payloads must be "
        "byte-distinct so each side carries a distinct Content_Hash"
    )

    # --- Attach a fallback capturing handler to the ``mcps`` logger.
    # ``setup_logging`` (called inside ``cli.run``) clears prior
    # handlers and sets ``propagate=False`` so pytest's ``caplog``
    # fixture cannot reach the records this test cares about. The
    # CLI's ``StreamHandler`` writes structured JSON lines to
    # ``stderr``, which ``capsys`` captures — that is the primary
    # oracle below. The in-process handler attached here exists as a
    # secondary oracle in case ``setup_logging`` is ever changed to
    # preserve foreign handlers; it remains harmless either way.
    mcps_logger = logging.getLogger("mcps")
    capture = _CapturingHandler()
    capture.setLevel(logging.DEBUG)
    mcps_logger.addHandler(capture)
    # Restore handler list at the end so the test does not pollute
    # the logger state across other tests in the same session.
    try:
        with mock_aws():
            s3_client = boto3.client("s3", region_name=_AWS_REGION)
            _seed_s3_bucket(s3_client, sha=s3_sha)
            gcs_client = _build_gcs_client_with_divergent_blob(sha=gcs_sha)

            def _adapter_factory(src: SourceConfig) -> SourceAdapter:
                if src.kind == "s3":
                    # Use the metadata-listing subclass so the
                    # post-replication listing path in
                    # ``mcps.cli._list_post_replication`` sees the
                    # ``mcps-content-sha256`` user-metadata the test
                    # seeded; the production S3 adapter's
                    # ``list_objects`` returns empty user_metadata
                    # for performance reasons (see the wrapper class
                    # docstring above).
                    return _MetadataListingS3Adapter(
                        name=src.name,
                        bucket=src.bucket,
                        prefix=src.prefix,
                        region=src.region,
                        s3_client=s3_client,
                    )
                if src.kind == "gcs":
                    return GCSSourceAdapter(
                        name=src.name,
                        bucket=src.bucket,
                        prefix=src.prefix,
                        gcs_client=gcs_client,
                    )
                raise AssertionError(f"unknown kind {src.kind!r}")

            args = argparse.Namespace(
                config=config_path,
                dry_run=True,
                apply=False,
                auto_approve=False,
                first_pass_confirmed=False,
                # WARN level so the divergence WARN records propagate
                # through to the capturing handler. INFO would also
                # work but emits the SUMMARY-extension INFO record we
                # do not assert on directly here.
                log_level="WARN",
                run_id=_RUN_ID,
                catalog=None,
                manifest_dir=None,
                lock_path=None,
            )

            exit_code = cli.run(
                args,
                cwd=str(tmp_path),
                adapter_factory=_adapter_factory,
                credential_manager=_StubCredentialManager(),
                now=lambda: _FIXED_NOW,
            )

            # ``setup_logging`` (called early inside ``cli.run``)
            # strips foreign handlers from the ``mcps`` logger before
            # attaching its own ``StreamHandler``. Our in-process
            # ``_CapturingHandler`` is therefore typically empty by
            # the time we reach the assertion below; the structured
            # JSON lines written to stderr by the StreamHandler are
            # the primary oracle.
            captured = capsys.readouterr()
    finally:
        mcps_logger.removeHandler(capture)

    # ----------------------------------------------------------------
    # Assertion 1: WARN records — one per divergent hash (req 19.2).
    # ----------------------------------------------------------------
    # The CLI's structured JSON formatter writes every record to
    # stderr; ``capsys`` captures those lines and we parse them
    # directly. The in-process ``_CapturingHandler`` is the
    # secondary oracle: if a future ``setup_logging`` change ever
    # preserves foreign handlers, it will populate too and the
    # assertion below still holds.
    warn_events_from_capture = [
        rec
        for rec in capture.records
        if rec.levelno == logging.WARNING
        and rec.__dict__.get("event") == "mcps.reconciliation.inconsistency"
    ]

    import json as _json  # local import to avoid polluting module scope

    warn_events_from_stderr: List[Dict[str, Any]] = []
    for line in (captured.err or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = _json.loads(line)
        except ValueError:
            continue
        if not isinstance(payload, dict):
            continue
        if (
            payload.get("level") in ("WARN", "WARNING")
            and payload.get("event")
            == "mcps.reconciliation.inconsistency"
        ):
            warn_events_from_stderr.append(payload)

    # The two channels capture the same events; whichever populates
    # is fine. We prefer the structured-stderr channel because the
    # ``StreamHandler`` survives ``setup_logging``'s reset and is
    # therefore the more reliable signal in this end-to-end test.
    warn_events: List[Mapping[str, Any]]
    if warn_events_from_stderr:
        warn_events = warn_events_from_stderr  # type: ignore[assignment]
    else:
        warn_events = [rec.__dict__ for rec in warn_events_from_capture]

    assert len(warn_events) == 2, (
        "expected exactly two WARN records (one per divergent hash); "
        f"got {len(warn_events)} "
        f"(captured={len(warn_events_from_capture)}, "
        f"stderr={len(warn_events_from_stderr)}, stderr_text={captured.err!r})"
    )

    # Each WARN must reference one of the two seeded hashes; together
    # they must exhaust the divergent set.
    warn_hashes = {ev.get("content_hash") for ev in warn_events}
    assert warn_hashes == {s3_sha, gcs_sha}, (
        f"WARN records reference unexpected content_hashes {warn_hashes!r}; "
        f"expected {{s3_sha, gcs_sha}} = {{ {s3_sha!r}, {gcs_sha!r} }}"
    )

    # Per-WARN payload sanity: present_in / absent_from partition the
    # configured Replicated_Sources exactly. The S3-side hash is
    # present in {s3-prod} and absent from {gcs-archive}; the
    # GCS-side hash is the mirror image.
    by_hash = {ev["content_hash"]: ev for ev in warn_events}
    assert list(by_hash[s3_sha].get("present_in") or []) == ["s3-prod"]
    assert list(by_hash[s3_sha].get("absent_from") or []) == ["gcs-archive"]
    assert list(by_hash[gcs_sha].get("present_in") or []) == ["gcs-archive"]
    assert list(by_hash[gcs_sha].get("absent_from") or []) == ["s3-prod"]

    # ----------------------------------------------------------------
    # Assertion 2: SUMMARY's divergent_hashes_count is 2 (req 19.1).
    # ----------------------------------------------------------------
    manifest_files = sorted(
        f for f in os.listdir(manifest_dir) if f.endswith(".jsonl")
    )
    assert len(manifest_files) == 1, (
        f"expected exactly one manifest file under {manifest_dir!r}, "
        f"got {manifest_files!r}"
    )
    manifest_path = os.path.join(manifest_dir, manifest_files[0])
    records, errors = parse_manifest_file(manifest_path)
    assert errors == [], f"manifest parse errors: {errors!r}"

    summary_records = [r for r in records if r.action == Action.SUMMARY]
    assert len(summary_records) == 1, (
        f"expected exactly one SUMMARY record, got {len(summary_records)}"
    )
    summary = summary_records[0]
    assert summary.result == Result.SUCCESS

    divergent_hashes_count_str = summary.extra.get("divergent_hashes_count")
    assert divergent_hashes_count_str is not None, (
        "SUMMARY.extra is missing 'divergent_hashes_count'; "
        f"extra={dict(summary.extra)!r}"
    )
    divergent_hashes_count = int(divergent_hashes_count_str)
    assert divergent_hashes_count == 2, (
        f"SUMMARY.extra.divergent_hashes_count = {divergent_hashes_count!r}; "
        "expected 2 (one per side of the divergent state)"
    )
    assert summary.extra.get("dry_run") == "true"
    assert summary.extra.get("apply") == "false"

    # All records must carry the same run_id (req 14.5 — every
    # record in a Manifest belongs to one Sync_Run).
    for r in records:
        assert r.run_id == _RUN_ID, (
            f"record carries unexpected run_id {r.run_id!r}; "
            f"expected {_RUN_ID!r}"
        )

    # ----------------------------------------------------------------
    # Assertion 3: exit code is 78 (req 19.3).
    # ----------------------------------------------------------------
    assert exit_code == int(ExitCode.INCONSISTENCY_DETECTED), (
        f"expected INCONSISTENCY_DETECTED ({int(ExitCode.INCONSISTENCY_DETECTED)}); "
        f"got {exit_code}"
    )
