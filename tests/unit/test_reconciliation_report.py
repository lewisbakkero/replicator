# Feature: multicloud-photo-sync, Property 15: Reconciliation_Report completeness and determinism
"""Reconciliation_Report completeness and determinism — Property 15.

Property under test (design.md, "Correctness Properties — Property 15:
Reconciliation_Report completeness and determinism"):

  For any Cold_Start input — that is, for any multi-source
  ``ObjectRecord`` population ``R`` (spread across Sources of kinds
  ``s3``, ``gcs``, and ``google_drive``), for any set of duplicate
  groups ``G = Duplicate_Detector(R)``, and for any listing-order
  permutation ``pi`` over ``R`` — the ``ReconciliationReport`` produced
  by ``Reconciliation_Reporter.build(catalog_at_start=empty,
  records=pi(R), duplicate_groups=G, drive_would_import_count=k, ...)``
  is equal under field-by-field equality to the report produced under
  any other listing-order permutation. Furthermore, every field of the
  report equals its brute-force closed-form value over ``R``.

This test exercises:

* permutation-invariance over the listing order of `ObjectRecord`
  values (the Reporter promises Property 4-style determinism for
  Property 15);
* per-Source counts (`object_count`, `total_bytes`,
  `distinct_content_hashes`);
* cross-Source diff partitioning over the three Source-kinds (`s3`,
  `gcs`, `google_drive`);
* same-source / cross-source duplicate-group tallies;
* the ``drive_would_import`` pass-through;
* the ``estimated_bytes_to_hash`` accumulator over a Hypothesis-
  selected ``bytes_to_hash_estimator`` predicate.

The strategy generates a `(records, source_kinds, hash_predicate)`
triple. The Hypothesis predicate is materialised as a ``set[int]`` of
record indices to flag True; the test wraps it in a callable so the
Reporter sees a ``Callable[[ObjectRecord], bool]`` matching the
production signature.

Validates: Requirements 18.1, 18.2, 18.5.
"""

from __future__ import annotations

import io
import os
import random
import re
import tempfile
from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import detect_duplicates
from mcps.reconciliation import (
    CrossSourceDiff,
    PerSourceCounts,
    ReconciliationReport,
    Reconciliation_Reporter,
)


# ---------------------------------------------------------------------------
# Strategy pools
# ---------------------------------------------------------------------------


# Source pool covering all three configured kinds. The mapping from name
# to kind is fixed so that two runs over the same record set always agree
# on which kind a record's source is in (matching the way the CLI builds
# `source_kinds` from the loaded `Config` once and threads it through
# every Reporter call in the same Sync_Run).
_SOURCE_KINDS: dict[str, str] = {
    "s3-prod": "s3",
    "s3-archive": "s3",
    "gcs-mirror": "gcs",
    "gcs-cold": "gcs",
    "drive-folder": "google_drive",
}

_SOURCE_POOL: tuple[str, ...] = tuple(_SOURCE_KINDS.keys())

# Small hash pool so Hypothesis frequently produces real cross-source
# duplicate groups within the 0..40 record range. Using only a handful of
# distinct hashes lifts the per-example signal-to-noise ratio for the
# duplicate-group leg of the property.
_HASH_POOL: tuple[str, ...] = tuple(
    chr(ord("a") + i) * 64 for i in range(6)
) + ("0" * 64, "f" * 64)

# Sizes paired with hashes so that two records with the same hash *and*
# size become duplicates (req 4.2). Different sizes for the same hash
# split the bucket.
_SIZE_POOL: tuple[int, ...] = (0, 1024, 1_048_576, 4_194_304)

_CONTENT_TYPE_POOL: tuple[Optional[str], ...] = (
    None,
    "image/jpeg",
    "image/png",
    "video/mp4",
)

# ISO-8601 timestamp window matched to the rest of the test suite for
# consistent shrinkage behaviour.
_EPOCH_MIN = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
_EPOCH_MAX = int(
    datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()
)


@st.composite
def _iso_timestamps(draw) -> str:
    epoch = draw(st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX))
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


_KEY_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters=("\x00", "\n", "\r"),
    ),
    min_size=1,
    max_size=20,
)


@st.composite
def _object_records(draw) -> ObjectRecord:
    return ObjectRecord(
        source=draw(st.sampled_from(_SOURCE_POOL)),
        key=draw(_KEY_TEXT),
        content_hash=draw(st.sampled_from(_HASH_POOL)),
        size_bytes=draw(st.sampled_from(_SIZE_POOL)),
        last_seen_at=draw(_iso_timestamps()),
        last_modified=draw(_iso_timestamps()),
        content_type=draw(st.sampled_from(_CONTENT_TYPE_POOL)),
        quarantined_at=None,
        tombstoned_at=None,
        mcps_source_meta=None,
    )


@st.composite
def _record_lists(draw) -> list[ObjectRecord]:
    """Draw a unique-by-(source, key) list of 0..40 records.

    Deduplicating by ``(source, key)`` keeps the test's permutation leg
    honest: the Reporter is permutation-invariant over the *set* of
    records it sees, so we feed it sets (rendered as lists in two
    permutations).
    """
    n = draw(st.integers(min_value=0, max_value=40))
    raw = draw(st.lists(_object_records(), min_size=n, max_size=n))

    seen: set[tuple[str, str]] = set()
    deduped: list[ObjectRecord] = []
    for rec in raw:
        ident = (rec.source, rec.key)
        if ident in seen:
            continue
        seen.add(ident)
        deduped.append(rec)
    return deduped


# ---------------------------------------------------------------------------
# Closed-form helpers (the right-hand side of the property)
# ---------------------------------------------------------------------------


def _expected_per_source(
    records: list[ObjectRecord],
) -> dict[str, PerSourceCounts]:
    """Brute-force per-Source counts.

    Mirrors the closed-form definition in design.md / Property 15:

    * ``object_count`` = ``|{r : r.source == name}|``
    * ``total_bytes`` = ``sum(r.size_bytes : r.source == name, size >= 0)``
    * ``distinct_content_hashes`` = ``|{r.content_hash : r.source == name}|``
    """
    by_source: dict[str, list[ObjectRecord]] = {}
    for r in records:
        by_source.setdefault(r.source, []).append(r)
    out: dict[str, PerSourceCounts] = {}
    for name, members in by_source.items():
        total = sum(
            r.size_bytes
            for r in members
            if isinstance(r.size_bytes, int)
            and not isinstance(r.size_bytes, bool)
            and r.size_bytes >= 0
        )
        out[name] = PerSourceCounts(
            object_count=len(members),
            total_bytes=total,
            distinct_content_hashes=len({r.content_hash for r in members}),
        )
    return out


def _expected_cross_source_diff(
    records: list[ObjectRecord],
) -> CrossSourceDiff:
    """Brute-force cross-Source-kind partitioning.

    Closed form: collapse Sources to kinds, build three hash sets, walk
    the union and count by presence-bitmask.
    """
    s3 = {r.content_hash for r in records if _SOURCE_KINDS.get(r.source) == "s3"}
    gcs = {
        r.content_hash for r in records if _SOURCE_KINDS.get(r.source) == "gcs"
    }
    drive = {
        r.content_hash
        for r in records
        if _SOURCE_KINDS.get(r.source) == "google_drive"
    }
    s3_only = gcs_only = drive_only = exactly_two = all_three = 0
    for h in s3 | gcs | drive:
        in_s3 = h in s3
        in_gcs = h in gcs
        in_drive = h in drive
        count = sum((in_s3, in_gcs, in_drive))
        if count == 1:
            if in_s3:
                s3_only += 1
            elif in_gcs:
                gcs_only += 1
            else:
                drive_only += 1
        elif count == 2:
            exactly_two += 1
        elif count == 3:
            all_three += 1
    return CrossSourceDiff(
        s3_only=s3_only,
        gcs_only=gcs_only,
        drive_only=drive_only,
        exactly_two=exactly_two,
        all_three=all_three,
    )


def _expected_same_source_groups(catalog: Catalog) -> dict[str, int]:
    """Brute-force same-source duplicate-group counts.

    Builds a ``(content_hash, size, source) -> count`` map; any cell of
    size ≥ 2 within a single source contributes one same-source group
    keyed by that source.
    """
    by_key: dict[tuple[str, int, str], int] = {}
    for r in catalog.all_records():
        # Match the detector's validity gates (req 4.2/4.6) so the
        # closed-form here lines up with the detector's view.
        if not isinstance(r.size_bytes, int) or isinstance(r.size_bytes, bool):
            continue
        if r.size_bytes < 0:
            continue
        if not isinstance(r.content_hash, str) or len(r.content_hash) != 64:
            continue
        cell = (r.content_hash, r.size_bytes, r.source)
        by_key[cell] = by_key.get(cell, 0) + 1

    # A same-source group is a (hash, size) cell with members in exactly
    # one source, count >= 2. Detect by grouping on (hash, size) and
    # checking how many distinct sources the cell spans.
    by_hash_size: dict[tuple[str, int], dict[str, int]] = {}
    for (h, s, src), count in by_key.items():
        by_hash_size.setdefault((h, s), {})[src] = count

    same_source: dict[str, int] = {}
    for (_h, _s), src_counts in by_hash_size.items():
        # Each source's members count >= 2 alone makes it a same-source
        # group iff the (hash, size) cell is single-source overall (i.e.
        # there is exactly one source key with at least one record AND
        # that source's count is >= 2). The detector groups by
        # (hash, size) and labels by distinct sources, so two records of
        # the same hash+size in source A and one in source B is a single
        # cross-source group, not a same-source group plus anything
        # else.
        if len(src_counts) == 1:
            (src, c), = src_counts.items()
            if c >= 2:
                same_source[src] = same_source.get(src, 0) + 1
    return same_source


def _expected_cross_source_groups(catalog: Catalog) -> int:
    """Brute-force cross-source duplicate-group count.

    A (hash, size) cell with members in ≥ 2 distinct sources is one
    cross-source duplicate group regardless of how many records each
    source contributes (req 4.3).
    """
    by_key: dict[tuple[str, int], set[str]] = {}
    for r in catalog.all_records():
        if not isinstance(r.size_bytes, int) or isinstance(r.size_bytes, bool):
            continue
        if r.size_bytes < 0:
            continue
        if not isinstance(r.content_hash, str) or len(r.content_hash) != 64:
            continue
        by_key.setdefault((r.content_hash, r.size_bytes), set()).add(r.source)
    return sum(1 for sources in by_key.values() if len(sources) >= 2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_catalog(records: list[ObjectRecord]) -> Catalog:
    cat = Catalog()
    for rec in records:
        cat = cat.upsert(rec)
    return cat


def _make_predicate(flagged_indices: set[int], records: list[ObjectRecord]):
    """Return a stable ``ObjectRecord -> bool`` predicate.

    The predicate is keyed on ``(source, key, content_hash)`` rather
    than identity so it survives across two permutations of the same
    records (Hypothesis re-creates the records by value when shrinking).
    """
    flagged_keys = {
        (records[i].source, records[i].key, records[i].content_hash)
        for i in flagged_indices
        if 0 <= i < len(records)
    }

    def predicate(r: ObjectRecord) -> bool:
        return (r.source, r.key, r.content_hash) in flagged_keys

    return predicate


def _permute(records: list[ObjectRecord], seed: int) -> list[ObjectRecord]:
    rng = random.Random(seed)
    out = records[:]
    rng.shuffle(out)
    return out


# ---------------------------------------------------------------------------
# Property 15
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(
    records=_record_lists(),
    seed1=st.integers(min_value=0, max_value=2**31 - 1),
    seed2=st.integers(min_value=0, max_value=2**31 - 1),
    flagged=st.frozensets(st.integers(min_value=0, max_value=39), max_size=40),
    drive_would_import=st.integers(min_value=0, max_value=10_000),
    started_at_epoch=st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX),
    run_id=st.text(
        alphabet=st.characters(
            whitelist_categories=("Lu", "Ll", "Nd"),
        ),
        min_size=8,
        max_size=12,
    ),
)
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_reconciliation_report_completeness_and_determinism(
    records: list[ObjectRecord],
    seed1: int,
    seed2: int,
    flagged: frozenset[int],
    drive_would_import: int,
    started_at_epoch: int,
    run_id: str,
) -> None:
    """Permutation-invariance plus closed-form equality.

    The body computes:

    1. The expected `ReconciliationReport` field-by-field over ``records``.
    2. The actual `ReconciliationReport` under permutation ``pi1``.
    3. The actual `ReconciliationReport` under permutation ``pi2``.

    Asserts ``pi1 == pi2`` (determinism) and ``pi1 == expected``
    (completeness).

    Validates: Requirements 18.1, 18.5.
    """
    permutation_one = _permute(records, seed1)
    permutation_two = _permute(records, seed2)

    # Catalog + duplicate groups are permutation-invariant by Property 4
    # (test_order_independence.py). We rely on that here so this test
    # narrows in on the Reporter's own determinism rather than re-proving
    # the detector's.
    catalog = _build_catalog(records)
    detection = detect_duplicates(catalog)
    duplicate_groups = detection.groups

    predicate = _make_predicate(set(flagged), records)
    started_at = datetime.fromtimestamp(
        started_at_epoch, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    reporter = Reconciliation_Reporter()

    report_one = reporter.build(
        catalog_at_start=Catalog(),
        object_records=permutation_one,
        duplicate_groups=duplicate_groups,
        drive_would_import_count=drive_would_import,
        bytes_to_hash_estimator=predicate,
        run_id=run_id,
        started_at=started_at,
        source_kinds=_SOURCE_KINDS,
    )
    report_two = reporter.build(
        catalog_at_start=Catalog(),
        object_records=permutation_two,
        duplicate_groups=duplicate_groups,
        drive_would_import_count=drive_would_import,
        bytes_to_hash_estimator=predicate,
        run_id=run_id,
        started_at=started_at,
        source_kinds=_SOURCE_KINDS,
    )

    # Determinism leg of Property 15: two permutations -> same report.
    assert report_one == report_two

    # Closed-form leg of Property 15: every field matches its brute-force
    # value over ``records``.

    # per_source
    expected_per_source = _expected_per_source(records)
    assert report_one.per_source == expected_per_source

    # cross_source_diff
    expected_diff = _expected_cross_source_diff(records)
    assert report_one.cross_source_diff == expected_diff

    # same_source_dup_groups + cross_source_dup_groups
    expected_same_source = _expected_same_source_groups(catalog)
    assert report_one.same_source_dup_groups == expected_same_source
    expected_cross_source = _expected_cross_source_groups(catalog)
    assert report_one.cross_source_dup_groups == expected_cross_source

    # drive_would_import is a pass-through.
    assert report_one.drive_would_import == drive_would_import

    # estimated_bytes_to_hash equals the sum of size_bytes over the
    # records the predicate flagged True (and that have non-negative
    # size_bytes — see Reporter.size handling).
    expected_bytes = sum(
        r.size_bytes
        for r in records
        if predicate(r)
        and isinstance(r.size_bytes, int)
        and not isinstance(r.size_bytes, bool)
        and r.size_bytes >= 0
    )
    assert report_one.estimated_bytes_to_hash == expected_bytes

    # Identity fields.
    assert report_one.run_id == run_id
    assert report_one.started_at == started_at


# ---------------------------------------------------------------------------
# Example-based emit() coverage
# ---------------------------------------------------------------------------


def _make_example_report() -> ReconciliationReport:
    """Construct a small fully-populated report for emit() tests.

    Used by the example tests below to exercise the on-disk path without
    pulling in Hypothesis machinery.
    """
    return ReconciliationReport(
        run_id="abc12345",
        started_at="2024-06-01T12:00:00Z",
        per_source={
            "s3-prod": PerSourceCounts(
                object_count=3,
                total_bytes=300,
                distinct_content_hashes=2,
            ),
            "drive-folder": PerSourceCounts(
                object_count=1,
                total_bytes=50,
                distinct_content_hashes=1,
            ),
        },
        cross_source_diff=CrossSourceDiff(
            s3_only=1, gcs_only=0, drive_only=1, exactly_two=1, all_three=0
        ),
        same_source_dup_groups={"s3-prod": 1},
        cross_source_dup_groups=2,
        drive_would_import=4,
        estimated_bytes_to_hash=350,
    )


def test_emit_writes_to_stdout_and_disk(tmp_path) -> None:
    """``emit()`` writes the same text to stdout and to the on-disk file.

    Filename shape per req 18.2:
    ``reconciliation-<YYYYMMDDTHHMMSSZ>-<run-id>.txt``.
    """
    report = _make_example_report()
    stdout = io.StringIO()
    reporter = Reconciliation_Reporter()

    reporter.emit(report, stdout=stdout, manifest_dir=str(tmp_path))

    expected_path = tmp_path / "reconciliation-20240601T120000Z-abc12345.txt"
    assert expected_path.exists(), (
        f"emit() must create {expected_path.name}; "
        f"got {[p.name for p in tmp_path.iterdir()]}"
    )

    on_disk = expected_path.read_text(encoding="utf-8")
    assert on_disk == stdout.getvalue()

    # Sanity: the rendered text mentions every section the operator
    # cares about during Cold_Start review.
    for needle in (
        "Per-Source counts",
        "Cross-Source diff",
        "Duplicate groups",
        "Drive_Importer plan",
        "Estimated cost",
        "abc12345",
        "2024-06-01T12:00:00Z",
    ):
        assert needle in on_disk


def test_emit_creates_manifest_dir_if_missing(tmp_path) -> None:
    """``emit()`` is responsible for ``os.makedirs`` of ``manifest_dir``.

    The CLI's startup checks already validate that the directory is
    writable on Sync_Run start (req 14.7); allowing emit() to create
    the directory if absent makes the unit test below straightforward
    and matches what the CLI does on the first Sync_Run after
    ``runtime.manifest_dir`` is changed.
    """
    nested = tmp_path / "deeper" / "manifests"
    assert not nested.exists()

    report = _make_example_report()
    stdout = io.StringIO()
    Reconciliation_Reporter().emit(
        report, stdout=stdout, manifest_dir=str(nested)
    )
    assert nested.is_dir()
    out_files = list(nested.glob("reconciliation-*.txt"))
    assert len(out_files) == 1


def test_emit_is_byte_deterministic(tmp_path) -> None:
    """Two ``emit()`` calls on equal reports produce byte-identical files.

    Determinism is a pre-req for ``diff``-based operator review of two
    consecutive Cold_Start reconciliations; it also acts as a regression
    guard against accidental introduction of non-stable iteration order.
    """
    report = _make_example_report()
    a = io.StringIO()
    b = io.StringIO()
    reporter = Reconciliation_Reporter()
    reporter.emit(report, stdout=a, manifest_dir=str(tmp_path / "a"))
    reporter.emit(report, stdout=b, manifest_dir=str(tmp_path / "b"))
    assert a.getvalue() == b.getvalue()
    assert (
        (tmp_path / "a" / "reconciliation-20240601T120000Z-abc12345.txt")
        .read_bytes()
        == (
            tmp_path / "b" / "reconciliation-20240601T120000Z-abc12345.txt"
        ).read_bytes()
    )
