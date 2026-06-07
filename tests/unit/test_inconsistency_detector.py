# Feature: multicloud-photo-sync, Property 17: Inconsistency_Detector soundness
"""Inconsistency_Detector soundness — Property 17.

Property under test (design.md, "Correctness Properties — Property 17:
Inconsistency_Detector soundness"):

  For any post-replication state — that is, for any ``Catalog``
  snapshot at run start ``C0``, for any ``ObjectRecord`` set
  ``R_observed`` listed during the run, for any set of replicated-
  source names ``RS``, and for any set of replication-error
  Content_Hashes ``E`` recorded in the Manifest — the WARN log
  entries emitted by `Inconsistency_Detector.emit(...)` are in
  one-to-one correspondence with the set:

    { h : ∃ s1, s2 ∈ RS,
            ∃ r1 ∈ R_observed where r1.source == s1
                                ∧ r1.content_hash == h
            ∧ ∄ r2 ∈ R_observed where r2.source == s2
                                ∧ r2.content_hash == h
            ∧ h ∉ E }

  Furthermore, for any ``C0`` and ``R_observed``, the SUMMARY-channel
  fields ``new_records_per_source[name]`` and
  ``removed_records_per_source[name]`` equal:

    new[name]     = |{ (r.source, r.key) : r ∈ R_observed,
                         r.source == name } \\
                     { (rec.source, rec.key) : rec ∈ C0.records_for_source(name) }|
    removed[name] = |{ (rec.source, rec.key) : rec ∈ C0.records_for_source(name) } \\
                     { (r.source, r.key) : r ∈ R_observed,
                         r.source == name }|

The strategy generates any ``(C0, R_observed, RS, E)`` quadruple. The
test feeds the detector and asserts both legs of Property 17:

* the WARN entries from ``emit()`` are in one-to-one correspondence
  with the divergent set computed by an independent closed-form helper;
* ``per_source_new[name]`` and ``per_source_removed[name]`` equal the
  symmetric-difference cardinalities of the ``(source, key)`` pair
  sets.

Validates: Requirements 19.1, 19.2, 19.3.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.reconciliation import (
    DivergentHash,
    InconsistencyReport,
    Inconsistency_Detector,
)


# ---------------------------------------------------------------------------
# Strategy pools
# ---------------------------------------------------------------------------


# A small Source pool with two Replicated_Sources (s3, gcs) and one
# Pull_Only_Source (drive). The Pull_Only_Source is included so the test
# also exercises the Drive-exclusion arm of req 19.2 — divergences only
# count among Replicated_Sources.
_REPLICATED_SOURCE_POOL: tuple[str, ...] = ("s3-prod", "gcs-archive", "s3-cold")
_PULL_ONLY_SOURCE_POOL: tuple[str, ...] = ("drive-folder",)
_SOURCE_POOL: tuple[str, ...] = _REPLICATED_SOURCE_POOL + _PULL_ONLY_SOURCE_POOL


# Small hash pool so Hypothesis frequently produces real divergences and
# real cross-source matches inside the bounded record range.
_HASH_POOL: tuple[str, ...] = tuple(
    chr(ord("a") + i) * 64 for i in range(6)
) + ("0" * 64, "f" * 64)

_SIZE_POOL: tuple[int, ...] = (0, 1024, 1_048_576, 4_194_304)
_CONTENT_TYPE_POOL: tuple[Optional[str], ...] = (
    None,
    "image/jpeg",
    "video/mp4",
)


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
def _object_records(draw, *, source_pool: tuple[str, ...]) -> ObjectRecord:
    return ObjectRecord(
        source=draw(st.sampled_from(source_pool)),
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
def _record_lists(draw, *, source_pool: tuple[str, ...]) -> list[ObjectRecord]:
    """Draw a unique-by-(source, key) list of 0..30 records.

    Deduplicating by ``(source, key)`` matches the Catalog invariant
    (req 11.5) and the listing-side guarantee that there is at most one
    Object_Record per ``(source, key)`` pair (req 2.5).
    """
    n = draw(st.integers(min_value=0, max_value=30))
    raw = draw(
        st.lists(
            _object_records(source_pool=source_pool),
            min_size=n,
            max_size=n,
        )
    )

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
# Closed-form helpers (the right-hand side of Property 17)
# ---------------------------------------------------------------------------


def _expected_per_source_new(
    catalog_at_start: Catalog,
    observed: list[ObjectRecord],
) -> dict[str, int]:
    """Brute-force per-Source new counts.

    Closed form: for every source name in either set, count the
    ``(source, key)`` pairs in ``observed`` for that source minus the
    pairs in ``catalog_at_start`` for that source. Sources with zero
    new pairs are omitted.
    """
    observed_pairs: dict[str, set[tuple[str, str]]] = {}
    for r in observed:
        observed_pairs.setdefault(r.source, set()).add((r.source, r.key))
    start_pairs: dict[str, set[tuple[str, str]]] = {}
    for r in catalog_at_start.all_records():
        start_pairs.setdefault(r.source, set()).add((r.source, r.key))

    out: dict[str, int] = {}
    for name in set(observed_pairs) | set(start_pairs):
        diff = observed_pairs.get(name, set()) - start_pairs.get(name, set())
        if diff:
            out[name] = len(diff)
    return out


def _expected_per_source_removed(
    catalog_at_start: Catalog,
    observed: list[ObjectRecord],
) -> dict[str, int]:
    """Brute-force per-Source removed counts.

    Mirror image of `_expected_per_source_new` — pairs in the start
    Catalog minus the observed pairs.
    """
    observed_pairs: dict[str, set[tuple[str, str]]] = {}
    for r in observed:
        observed_pairs.setdefault(r.source, set()).add((r.source, r.key))
    start_pairs: dict[str, set[tuple[str, str]]] = {}
    for r in catalog_at_start.all_records():
        start_pairs.setdefault(r.source, set()).add((r.source, r.key))

    out: dict[str, int] = {}
    for name in set(observed_pairs) | set(start_pairs):
        diff = start_pairs.get(name, set()) - observed_pairs.get(name, set())
        if diff:
            out[name] = len(diff)
    return out


def _expected_divergent_hashes(
    observed: list[ObjectRecord],
    replicated_source_names: frozenset[str],
    replication_error_hashes: frozenset[str],
) -> set[str]:
    """Brute-force divergent-hash set.

    A Content_Hash is divergent iff it is present in some
    Replicated_Source's observed records and absent from at least one
    other Replicated_Source's observed records, and the hash is not
    in ``replication_error_hashes``. Pull_Only_Sources are excluded
    by restricting the per-Source hash sets to
    ``replicated_source_names``.
    """
    by_source: dict[str, set[str]] = {}
    for r in observed:
        if r.source not in replicated_source_names:
            continue
        by_source.setdefault(r.source, set()).add(r.content_hash)

    if len(replicated_source_names) < 2:
        # With < 2 replicated sources there is nothing to diverge
        # between.
        return set()

    union: set[str] = set()
    for hashes in by_source.values():
        union.update(hashes)

    divergent: set[str] = set()
    for h in union:
        if h in replication_error_hashes:
            continue
        present_count = sum(
            1 for name in replicated_source_names if h in by_source.get(name, set())
        )
        absent_count = len(replicated_source_names) - present_count
        if present_count >= 1 and absent_count >= 1:
            divergent.add(h)
    return divergent


# ---------------------------------------------------------------------------
# Capture helper
# ---------------------------------------------------------------------------


class _CapturingHandler(logging.Handler):
    """Capture LogRecord instances into an in-memory list.

    Tests inspect ``records`` to assert on the WARN-per-divergent-hash
    leg of Property 17 without reaching for the real JsonFormatter or
    stderr stream.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _build_catalog(records: list[ObjectRecord]) -> Catalog:
    cat = Catalog()
    for rec in records:
        cat = cat.upsert(rec)
    return cat


# ---------------------------------------------------------------------------
# Property 17
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(
    catalog_records=_record_lists(source_pool=_SOURCE_POOL),
    observed_records=_record_lists(source_pool=_SOURCE_POOL),
    replicated_subset=st.frozensets(
        st.sampled_from(_REPLICATED_SOURCE_POOL),
        max_size=len(_REPLICATED_SOURCE_POOL),
    ),
    error_hashes=st.frozensets(st.sampled_from(_HASH_POOL), max_size=4),
    fail_on_inconsistency=st.booleans(),
)
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_inconsistency_detector_soundness(
    catalog_records: list[ObjectRecord],
    observed_records: list[ObjectRecord],
    replicated_subset: frozenset[str],
    error_hashes: frozenset[str],
    fail_on_inconsistency: bool,
) -> None:
    """One-to-one correspondence + symmetric-difference cardinalities.

    Walks the four-way generated quadruple ``(C0, R_observed, RS, E)``
    through ``Inconsistency_Detector.analyse()`` then ``emit()``, and
    asserts:

    1. ``analyse(...).divergent_hashes`` is in one-to-one correspondence
       with the closed-form divergent set.
    2. WARN log records from ``emit(...)`` are also in one-to-one
       correspondence with the divergent set, and each WARN carries
       the matching ``content_hash``, ``present_in``, ``absent_from``,
       and ``canonical_key`` (the latter via the test's
       ``canonical_for`` callable, which returns the record's own key).
    3. ``per_source_new[name]`` and ``per_source_removed[name]`` equal
       the closed-form symmetric-difference cardinalities of the
       ``(source, key)`` pair sets restricted to that source.
    4. The integer return value of ``emit(...)`` is 1 iff
       ``fail_on_inconsistency`` is True AND at least one divergence
       was observed; 0 otherwise (req 19.2 / 19.3 exit-code rule).

    Validates: Requirements 19.1, 19.2, 19.3.
    """
    catalog_at_start = _build_catalog(catalog_records)
    detector = Inconsistency_Detector()

    # canonical_for: for the test we return the record's own key. The
    # detector picks a deterministic record per hash, so the key is a
    # stable function of (R_observed, RS).
    def canonical_for(record: ObjectRecord) -> str:
        return record.key

    report = detector.analyse(
        catalog_at_start=catalog_at_start,
        object_records_observed=observed_records,
        replicated_source_names=replicated_subset,
        replication_error_hashes=error_hashes,
        canonical_for=canonical_for,
    )

    # --- Leg 1: divergent_hashes correspond to the closed form ----
    expected_divergent = _expected_divergent_hashes(
        observed_records, replicated_subset, error_hashes
    )
    assert {d.content_hash for d in report.divergent_hashes} == expected_divergent

    # The detector orders divergent_hashes by content_hash ascending so
    # the on-disk WARN sequence is byte-deterministic — assert that
    # ordering invariant.
    sorted_hashes = sorted(d.content_hash for d in report.divergent_hashes)
    assert [d.content_hash for d in report.divergent_hashes] == sorted_hashes

    # Every DivergentHash must satisfy the membership invariant: it is
    # present in at least one Replicated_Source AND absent from at
    # least one other Replicated_Source.
    observed_hashes_per_source: dict[str, set[str]] = {}
    for r in observed_records:
        if r.source in replicated_subset:
            observed_hashes_per_source.setdefault(r.source, set()).add(
                r.content_hash
            )
    for div in report.divergent_hashes:
        present_set = set(div.present_in)
        absent_set = set(div.absent_from)
        # No source name appears in both lists.
        assert not (present_set & absent_set)
        # Together they cover exactly the configured Replicated_Sources.
        assert present_set | absent_set == set(replicated_subset)
        # The detector's classification matches the observed records.
        for name in present_set:
            assert div.content_hash in observed_hashes_per_source.get(name, set())
        for name in absent_set:
            assert div.content_hash not in observed_hashes_per_source.get(
                name, set()
            )

    # --- Leg 2: WARN entries one-to-one with divergent_hashes ------
    handler = _CapturingHandler()
    test_logger = logging.getLogger(
        "mcps.test.inconsistency_detector_property"
    )
    # Reset handlers/levels so each Hypothesis example sees a clean
    # logger; otherwise records accumulate across examples and the
    # one-to-one assertion below would over-count.
    test_logger.handlers = [handler]
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    exit_code = detector.emit(
        report,
        manifest_writer=None,
        logger=test_logger,
        fail_on_inconsistency=fail_on_inconsistency,
    )

    warn_records = [r for r in handler.records if r.levelno == logging.WARNING]
    assert len(warn_records) == len(report.divergent_hashes)

    # Pair WARN records to DivergentHash by content_hash and assert
    # field-by-field equality.
    by_hash_warn = {r.__dict__["content_hash"]: r for r in warn_records}
    by_hash_div = {d.content_hash: d for d in report.divergent_hashes}
    assert set(by_hash_warn) == set(by_hash_div)
    for h, div in by_hash_div.items():
        warn = by_hash_warn[h]
        assert warn.__dict__["present_in"] == list(div.present_in)
        assert warn.__dict__["absent_from"] == list(div.absent_from)
        assert warn.__dict__["canonical_key"] == div.canonical_key
        assert warn.__dict__["event"] == "mcps.reconciliation.inconsistency"

    # The detector should also have emitted exactly one INFO summary
    # record carrying the per-Source counts and divergence count.
    info_records = [r for r in handler.records if r.levelno == logging.INFO]
    assert len(info_records) == 1
    summary = info_records[0]
    assert summary.__dict__["event"] == "mcps.reconciliation.summary"
    assert summary.__dict__["per_source_new"] == dict(
        sorted(report.per_source_new.items())
    )
    assert summary.__dict__["per_source_removed"] == dict(
        sorted(report.per_source_removed.items())
    )
    assert summary.__dict__["divergent_hashes_count"] == len(
        report.divergent_hashes
    )

    # --- Leg 3: per_source counts equal the symmetric-difference -----
    expected_new = _expected_per_source_new(catalog_at_start, observed_records)
    expected_removed = _expected_per_source_removed(
        catalog_at_start, observed_records
    )
    assert report.per_source_new == expected_new
    assert report.per_source_removed == expected_removed

    # --- Leg 4: exit-code rule (req 19.2 / 19.3) --------------------
    if fail_on_inconsistency and report.divergent_hashes:
        assert exit_code == 1
    else:
        assert exit_code == 0
