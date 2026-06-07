"""Integration test: a Cold_Start Sync_Run aborts when a Source listing fails.

Validates: Requirement 18.6.

Per req 18.6, when a Source's listing fails after the configured retry
budget on a Cold_Start Sync_Run, MultiCloud_Photo_Sync MUST refuse to
produce the Reconciliation_Report and exit with a code that is distinct
from ``FIRST_PASS_REVIEW_REQUIRED`` (76) and ``LOCK_CONFLICT`` (73). The
design pins this code to ``COLD_START_LISTING_FAILED = 77`` and the CLI
maps :class:`mcps.errors.ColdStartListingFailed` to that value.

This test seeds an empty on-disk Catalog (Cold_Start by definition),
wires a real :class:`GCSSourceAdapter` against an in-process fake
``Client`` whose ``list_blobs(...)`` always raises a
:class:`mcps.retry.TransientError`. The retry decorator exhausts its
single retry attempt (``retries.max_retries = 1`` in the YAML config)
and re-raises ``RetriesExhausted`` from inside the adapter; the CLI's
listing loop catches it and — because the run is Cold_Start — wraps it
in :class:`ColdStartListingFailed`.

The test is permissive about whether ``cli.run`` returns the exit code
directly or lets the exception propagate to ``cli.main``: it accepts
either shape per the task brief, and asserts that
``ColdStartListingFailed.to_exit_code()`` resolves to 77 in the
exception-propagation case.

Adapter-injection mirrors the pattern used in
``tests/integration/test_full_run_apply.py`` and
``tests/integration/test_cold_start_two_step.py``: a
:class:`_StubCredentialManager` bypasses real provider credential
chains, ``adapter_factory`` builds real S3/GCS adapters wired to a
moto-mocked S3 (used for the *successful* S3 listing that precedes the
GCS failure) and the in-process failing GCS client. No Google Drive
Source is configured because the listing failure on GCS suffices to
abort the report; keeping the Drive surface out of the wiring keeps
this test focused on req 18.6.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Any, List, Optional

import boto3  # type: ignore[import-not-found]
import pytest

# moto-related imports are lazy so a missing optional dep surfaces a
# skip rather than a collection error.
moto = pytest.importorskip("moto")
from moto import mock_aws  # type: ignore[import-not-found]  # noqa: E402

from mcps import cli  # noqa: E402
from mcps.config.model import SourceConfig  # noqa: E402
from mcps.errors import ColdStartListingFailed, ExitCode  # noqa: E402
from mcps.retry import TransientError  # noqa: E402
from mcps.sources.base import SourceAdapter  # noqa: E402
from mcps.sources.gcs import GCSSourceAdapter  # noqa: E402
from mcps.sources.s3 import S3SourceAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixed run-time values so assertions are stable
# ---------------------------------------------------------------------------

_AWS_REGION = "us-east-1"
_S3_BUCKET = "mcps-int-bucket-listing-failure"
_GCS_BUCKET = "mcps-int-bucket-listing-failure-gcs"

_RUN_ID = "coldstart00listingfail01"  # >= 8 chars (req 14.1)
_FIXED_NOW = dt.datetime(2024, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# Failing GCS client: ``list_blobs(...)`` always raises TransientError so
# the retry decorator exhausts retries and re-raises RetriesExhausted.
# ---------------------------------------------------------------------------


class _FailingGcsClient:
    """Minimal ``google.cloud.storage.Client`` stand-in whose listing fails.

    Only ``list_blobs`` is exercised in this test — the GCS listing is
    the first call the CLI makes against the GCS adapter on a Cold_Start
    run, and it raises before any other GCS-side method gets invoked.
    The other ``Client`` methods are stubbed defensively so a regression
    that suddenly calls (for example) ``bucket(...)`` before listing
    surfaces an explicit ``AssertionError`` rather than an obscure
    ``AttributeError``.
    """

    def __init__(self) -> None:
        self.list_calls: List[dict[str, Any]] = []

    def list_blobs(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> Any:
        self.list_calls.append({"bucket": bucket, "prefix": prefix})
        # TransientError is the in-band signal the retry decorator
        # consumes; raising it here forces the decorator to retry up to
        # ``retries.max_retries`` times and then re-raise
        # :class:`mcps.errors.RetriesExhausted`. The CLI's Cold_Start
        # listing loop wraps that into ``ColdStartListingFailed`` per
        # req 18.6.
        raise TransientError(
            status=503,
            retry_after_seconds=None,
            message="injected: GCS list_blobs unavailable",
        )

    def bucket(self, name: str) -> Any:  # pragma: no cover - defensive
        raise AssertionError(
            f"_FailingGcsClient.bucket({name!r}) called unexpectedly; "
            "the listing failure must abort the run before any "
            "blob-level operation runs"
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
# Config writer
# ---------------------------------------------------------------------------


def _write_config_yaml(
    *,
    config_path: str,
    catalog_path: str,
    manifest_dir: str,
    lock_path: str,
) -> None:
    """Write a minimal but complete YAML config wiring S3 + GCS Sources.

    ``retries.max_retries = 1`` and ``initial_backoff_ms = 100`` keep
    the retry decorator's total wall-clock cost below ~0.1 second so
    the integration test stays fast. The GCS adapter is the second
    Source listed; the CLI's listing loop iterates ``config.sources``
    in declaration order, so the S3 listing succeeds first and then
    the GCS listing raises.
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
  fail_on_inconsistency: false

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


def _seed_s3_bucket(s3_client: Any) -> None:
    """Seed S3 with a single benign Object so the S3 listing succeeds.

    The CLI iterates ``config.sources`` in order; S3 is first, GCS
    second. Seeding S3 with at least one Object is not required for
    req 18.6 — the listing failure on GCS aborts the run regardless —
    but it exercises the realistic Cold_Start ordering and confirms
    that the abort happens *after* an unrelated Source has already
    listed cleanly.
    """
    s3_client.create_bucket(Bucket=_S3_BUCKET)
    s3_client.put_object(
        Bucket=_S3_BUCKET,
        Key="photos/img.jpg",
        Body=b"S3-OBJECT-PAYLOAD" * 16,
        ContentType="image/jpeg",
    )


# ---------------------------------------------------------------------------
# The integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_cold_start_listing_failure_aborts_report(
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: Cold_Start GCS-listing failure → exit code 77, no report.

    Sequence:

    1. Fresh tmp workspace, no on-disk Catalog → Cold_Start.
    2. ``cli.run`` is invoked with ``--apply --first-pass-confirmed
       --auto-approve`` so the full Cold_Start flow runs end-to-end;
       the listing failure aborts before any Reconciliation_Report,
       Replicator, Drive_Importer, or Inconsistency_Detector step
       fires.
    3. The S3 adapter (real ``S3SourceAdapter`` against a moto-mocked
       S3 client) lists ``photos/img.jpg`` cleanly.
    4. The GCS adapter (real ``GCSSourceAdapter`` against a
       :class:`_FailingGcsClient`) raises a ``TransientError`` on
       every ``list_blobs`` call; the retry decorator exhausts
       ``retries.max_retries = 1`` retries and re-raises
       :class:`mcps.errors.RetriesExhausted`.
    5. The CLI's Cold_Start listing loop catches that, wraps it in
       :class:`ColdStartListingFailed(source_name="gcs-archive",
       source_kind="gcs", cause=...)`, and raises.

    Assertions (per task brief):

    * Either ``cli.run(...)`` returns ``77``
      (``COLD_START_LISTING_FAILED``) or it raises
      :class:`ColdStartListingFailed` whose ``to_exit_code()`` returns
      ``77``. Both shapes are accepted because the CLI's
      :class:`McpsError` → exit-code translation lives in
      ``cli.main`` and not in ``cli.run``; in the current
      implementation the exception propagates out of ``run`` and is
      mapped to the exit code by ``main``. We honour both by catching
      :class:`ColdStartListingFailed` explicitly.
    * No ``reconciliation-*.txt`` file exists under ``manifest_dir``:
      the abort happens *before* :func:`mcps.cli._emit_cold_start_report`
      is called (req 18.6 — "refuse to produce a Reconciliation_Report").
    * The exit code value is distinct from
      ``FIRST_PASS_REVIEW_REQUIRED`` (76) and ``LOCK_CONFLICT`` (73)
      per req 18.6's distinctness clause.
    * The injected GCS failing client recorded at least one
      ``list_blobs`` call — if zero, the test has wired the wrong
      Source and would silently pass via a different code path.
    """
    # --- Build the run-scoped temp paths.
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

    with mock_aws():
        s3_client = boto3.client("s3", region_name=_AWS_REGION)
        _seed_s3_bucket(s3_client)

        failing_gcs_client = _FailingGcsClient()

        def _adapter_factory(src: SourceConfig) -> SourceAdapter:
            if src.kind == "s3":
                return S3SourceAdapter(
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
                    gcs_client=failing_gcs_client,
                )
            raise AssertionError(f"unknown kind {src.kind!r}")

        args = argparse.Namespace(
            config=config_path,
            dry_run=False,
            apply=True,
            auto_approve=True,
            first_pass_confirmed=True,
            log_level="ERROR",  # quiet stderr
            run_id=_RUN_ID,
            catalog=None,
            manifest_dir=None,
            lock_path=None,
        )

        # ------------------------------------------------------------
        # Invoke the run, accepting either shape (return-77 OR raise
        # ColdStartListingFailed).
        # ------------------------------------------------------------
        observed_exit_code: Optional[int] = None
        observed_exception: Optional[ColdStartListingFailed] = None
        try:
            observed_exit_code = cli.run(
                args,
                cwd=str(tmp_path),
                adapter_factory=_adapter_factory,
                credential_manager=_StubCredentialManager(),
                now=lambda: _FIXED_NOW,
            )
        except ColdStartListingFailed as exc:
            observed_exception = exc

        captured = capsys.readouterr()

        # ------------------------------------------------------------
        # Assertion 1: exactly one of the two shapes occurred.
        # ------------------------------------------------------------
        assert (observed_exit_code is not None) ^ (
            observed_exception is not None
        ), (
            "expected cli.run to either return an int OR raise "
            "ColdStartListingFailed, observed neither/both: "
            f"exit_code={observed_exit_code!r}, "
            f"exception={observed_exception!r}; "
            f"stderr={captured.err!r}"
        )

        # ------------------------------------------------------------
        # Assertion 2: the resolved exit code is 77
        # (COLD_START_LISTING_FAILED, req 18.6).
        # ------------------------------------------------------------
        if observed_exit_code is not None:
            resolved_exit_code = observed_exit_code
        else:
            assert observed_exception is not None  # for type checkers
            # The exception's source identification is part of the
            # error contract (errors.py: ColdStartListingFailed
            # carries source_name + source_kind). Verify it pinned
            # the GCS Source so the test would surface a regression
            # that wraps the wrong Source.
            assert observed_exception.source_name == "gcs-archive", (
                f"ColdStartListingFailed.source_name = "
                f"{observed_exception.source_name!r}; expected "
                "'gcs-archive'"
            )
            assert observed_exception.source_kind == "gcs", (
                f"ColdStartListingFailed.source_kind = "
                f"{observed_exception.source_kind!r}; expected 'gcs'"
            )
            resolved_exit_code = int(observed_exception.to_exit_code())

        assert resolved_exit_code == int(ExitCode.COLD_START_LISTING_FAILED), (
            f"expected exit code "
            f"{int(ExitCode.COLD_START_LISTING_FAILED)} "
            f"(COLD_START_LISTING_FAILED), got {resolved_exit_code}; "
            f"stderr={captured.err!r}"
        )

        # ------------------------------------------------------------
        # Assertion 3: the exit code is distinct from
        # FIRST_PASS_REVIEW_REQUIRED (76) and LOCK_CONFLICT (73) per
        # req 18.6's distinctness clause.
        # ------------------------------------------------------------
        assert resolved_exit_code != int(ExitCode.FIRST_PASS_REVIEW_REQUIRED), (
            "Cold_Start listing failure must surface a code distinct "
            "from FIRST_PASS_REVIEW_REQUIRED (req 18.6)"
        )
        assert resolved_exit_code != int(ExitCode.LOCK_CONFLICT), (
            "Cold_Start listing failure must surface a code distinct "
            "from LOCK_CONFLICT (req 18.6)"
        )

        # ------------------------------------------------------------
        # Assertion 4: no reconciliation-*.txt file exists under
        # ``manifest_dir`` (req 18.6 — "refuse to produce a
        # Reconciliation_Report").
        # ------------------------------------------------------------
        recon_files = sorted(
            f
            for f in os.listdir(manifest_dir)
            if f.startswith("reconciliation-") and f.endswith(".txt")
        )
        assert recon_files == [], (
            f"expected no reconciliation-*.txt files under "
            f"{manifest_dir!r}; got {recon_files!r}. "
            "The Cold_Start listing failure must abort the run before "
            "the Reconciliation_Report is emitted (req 18.6)."
        )

        # ------------------------------------------------------------
        # Assertion 5: the failing GCS client was actually exercised.
        # If zero list_blobs calls were observed, the test has wired
        # the wrong adapter and a regression that bypasses GCS entirely
        # would falsely pass.
        # ------------------------------------------------------------
        assert failing_gcs_client.list_calls, (
            "expected the failing GCS client's list_blobs to have been "
            "called at least once; the test has not actually exercised "
            "the GCS listing failure path"
        )
        # The retry decorator runs the call at least twice: the
        # initial attempt plus ``max_retries=1`` retries → 2 total
        # invocations before RetriesExhausted is raised.
        assert len(failing_gcs_client.list_calls) >= 2, (
            f"expected >=2 list_blobs invocations (initial attempt + "
            f"1 retry per ``retries.max_retries=1``), got "
            f"{len(failing_gcs_client.list_calls)}: "
            f"{failing_gcs_client.list_calls!r}"
        )
