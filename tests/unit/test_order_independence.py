# Feature: multicloud-photo-sync, Property 4: Order-independence of run decisions
"""Order-independence of run decisions.

Property under test (design.md, "Correctness Properties — Property 4:
Order-independence of run decisions"):

  For any set of `ObjectRecord` values `R` and for any permutation `pi`
  of the order in which Sources are listed, the resulting Catalog, the
  duplicate-group set emitted by `Duplicate_Detector`, and the
  replication plan emitted by `Replicator` are equal under set/dict
  equality regardless of `pi`.

The `Replicator` (task 23) is not yet implemented, so the "replication
plan" leg of the property is exercised against a small inline helper,
``derive_replication_plan(catalog, replicated_sources)``, which is the
closed-form definition of the plan from the Catalog's perspective:

  for each ordered pair (src, dst) of Replicated_Sources, the set of
  Content_Hashes present in `src` but absent from `dst`.

This is exactly what the Replicator's per-pair diff step is required to
compute (req 6.1) before the conflict-resolution and loop-prevention
filters run. When the real Replicator lands, the property test in task
23 will extend the assertion to the full plan, and this test will
continue to gate the Catalog/Detector layer.

The property is exercised over two random permutations of the same
multi-source `ObjectRecord` list. Because `Catalog.upsert` collapses
duplicates by `(source, key)` (req 11.5), the *set* of records in the
final Catalog is what matters, not the insertion order — this test
proves it.

Validates: Requirements 4.4, 11.3.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Iterable, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import detect_duplicates


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Small pool of source names so the generated record set is dense enough
# to surface real cross-source duplicate groups during the run.
_SOURCE_POOL: tuple[str, ...] = (
    "s3-prod",
    "gcs-archive",
    "drive-folder",
)

# Pool of sources eligible to participate in replication. Drive is read-only
# so it is intentionally excluded from the (src, dst) plan pairs (req 10.8),
# matching what the real `Config.replicated_sources()` helper returns.
_REPLICATED_SOURCES: tuple[str, ...] = (
    "s3-prod",
    "gcs-archive",
)

# Small pool of 64-char lowercase hex hashes. A small alphabet of hashes
# guarantees a fair chance of collisions across sources so the duplicate
# detector and the replication-plan diff both see meaningful work.
_HASH_POOL: tuple[str, ...] = tuple(
    chr(ord("a") + i) * 64 for i in range(6)
) + ("0" * 64, "f" * 64)

# Pool of size_bytes values. Two same-hash records with different sizes
# are NOT duplicates per req 4.2, so we use a small pool to make sure the
# (hash, size) bucket logic is exercised in both branches.
_SIZE_POOL: tuple[int, ...] = (0, 1024, 1_048_576, 4_194_304)

_CONTENT_TYPE_POOL: tuple[Optional[str], ...] = (
    None,
    "image/jpeg",
    "image/png",
    "video/mp4",
)

# Bounded epoch window matching test_catalog_roundtrip.py so generated
# timestamps round-trip through ISO-8601 without padding surprises.
_EPOCH_MIN = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
_EPOCH_MAX = int(datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())


@st.composite
def _iso_timestamps(draw) -> str:
    epoch = draw(st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX))
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Keys are short, printable, no NULs / newlines.
_KEY_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters=("\x00", "\n", "\r"),
    ),
    min_size=1,
    max_size=40,
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
        mcps_source_meta=draw(
            st.one_of(st.none(), st.sampled_from(_SOURCE_POOL)),
        ),
    )


@st.composite
def _record_lists(draw) -> list[ObjectRecord]:
    """A list of 0..60 ObjectRecords drawn from the pools above.

    The list is permitted to contain ``(source, key)`` collisions; the
    Catalog's `upsert` invariant (req 11.5) means a later record with the
    same `(source, key)` replaces the earlier one, which itself is order-
    sensitive at the *list* level — but the *set* of records reachable
    from the final Catalog must still be identical for two permutations
    of the same list because permutation does not introduce or remove
    elements. The property captured below is the stronger one: equal
    multisets of records produce equal Catalogs.

    To make that argument hold, we materialise records from a list and
    then deduplicate by `(source, key)` keeping the first occurrence so
    the final inputs to `Catalog.upsert` are an ordered sequence with no
    `(source, key)` collisions. Permuting that sequence cannot change
    the resulting Catalog (req 11.3).
    """
    n = draw(st.integers(min_value=0, max_value=60))
    raw = draw(st.lists(_object_records(), min_size=n, max_size=n))

    # Deduplicate by (source, key) keeping first occurrence.
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
# Inline helper standing in for the Replicator's per-pair diff
# ---------------------------------------------------------------------------


def derive_replication_plan(
    catalog: Catalog,
    replicated_sources: Iterable[str],
) -> dict[tuple[str, str], frozenset[str]]:
    """Return the per-pair "missing-from-target" Content_Hash sets.

    For each ordered pair ``(src, dst)`` over ``replicated_sources`` with
    ``src != dst``, the value is the set of `content_hash` strings
    present in ``src`` but absent from ``dst`` according to the Catalog.

    This mirrors the Replicator's first step (req 6.1) — "identify the
    set of Content_Hashes present in one Source and absent from the
    other" — without any of the conflict-resolution, loop-prevention, or
    write logic that lives downstream of it. It is the exact closed-form
    function over the Catalog that the Replicator's per-pair diff is
    required to compute, so verifying it is permutation-invariant
    captures the "replication plan" leg of Property 4.
    """
    pair_hashes: dict[str, frozenset[str]] = {}
    rs_list = list(replicated_sources)
    for source_name in rs_list:
        hashes = frozenset(
            r.content_hash for r in catalog.records_for_source(source_name)
        )
        pair_hashes[source_name] = hashes

    plan: dict[tuple[str, str], frozenset[str]] = {}
    for src in rs_list:
        for dst in rs_list:
            if src == dst:
                continue
            plan[(src, dst)] = pair_hashes[src] - pair_hashes[dst]
    return plan


def _build_catalog(records: Iterable[ObjectRecord]) -> Catalog:
    """Build a Catalog by upserting ``records`` in the given order."""
    cat = Catalog()
    for rec in records:
        cat = cat.upsert(rec)
    return cat


def _permute(records: list[ObjectRecord], seed: int) -> list[ObjectRecord]:
    """Deterministic permutation of ``records`` parameterised by ``seed``.

    Hypothesis already shrinks the input list. Drawing the permutation
    from a seeded ``random.Random`` keeps the property test's two
    permutations independent of each other while remaining reproducible
    inside a single example.
    """
    rng = random.Random(seed)
    permuted = records[:]
    rng.shuffle(permuted)
    return permuted


# ---------------------------------------------------------------------------
# The Property 4 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(
    records=_record_lists(),
    seed1=st.integers(min_value=0, max_value=2**31 - 1),
    seed2=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_run_decisions_are_order_independent(
    records: list[ObjectRecord],
    seed1: int,
    seed2: int,
) -> None:
    """Catalog, duplicate groups, and replication plan are permutation-
    invariant.

    For two independently-seeded permutations ``pi1`` and ``pi2`` of the
    same record list ``R``:

    * The two Catalogs compare equal (req 11.3).
    * The two `DetectionResult.groups` tuples, viewed as `frozenset`,
      are equal — i.e. the duplicate-group set is the same regardless
      of insertion order (req 4.4).
    * The two `DetectionResult.skipped_records` tuples, viewed as
      `frozenset`, are equal.
    * The two derived replication plans (closed-form over the Catalog,
      keyed by ordered pair of Replicated_Source names, each value a
      frozenset of Content_Hashes) are equal under dict equality.

    Validates: Requirements 4.4, 11.3.
    """
    permutation_one = _permute(records, seed1)
    permutation_two = _permute(records, seed2)

    catalog_one = _build_catalog(permutation_one)
    catalog_two = _build_catalog(permutation_two)

    # Catalog equality (req 11.3).
    assert catalog_one == catalog_two

    # Duplicate-group set equality (req 4.4). The detector already sorts
    # its output, so a list-vs-list comparison would also pass; we use
    # frozenset to make the order-independence explicit and to match the
    # property statement in design.md.
    detection_one = detect_duplicates(catalog_one)
    detection_two = detect_duplicates(catalog_two)
    assert frozenset(detection_one.groups) == frozenset(detection_two.groups)
    assert frozenset(detection_one.skipped_records) == frozenset(
        detection_two.skipped_records
    )

    # Replication plan equality (closed-form derivation; the real
    # Replicator's per-pair diff is required to compute exactly this,
    # req 6.1).
    plan_one = derive_replication_plan(catalog_one, _REPLICATED_SOURCES)
    plan_two = derive_replication_plan(catalog_two, _REPLICATED_SOURCES)
    assert plan_one == plan_two
