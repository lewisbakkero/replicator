"""Unit tests for `mcps.duplicates.detector`.

Covers the example-based behaviour of `detect_duplicates`: empty Catalog,
single-member buckets, same-source vs cross-source labelling, the
size-must-match rule from req 4.2, the skipped-records divert path
(req 4.6), and the deterministic group ordering (req 4.4).

The order-independence Hypothesis property is implemented in task 21
(`tests/unit/test_order_independence.py`).

Validates: Requirements 4.1, 4.2, 4.3, 4.5, 4.6.
"""

from __future__ import annotations

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import (
    DetectionResult,
    DuplicateGroup,
    detect_duplicates,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def make_record(
    *,
    source: str = "s3-prod",
    key: str = "photos/IMG_0001.jpg",
    content_hash: str = HASH_A,
    size_bytes: int = 1024,
    last_seen_at: str = "2024-01-01T00:00:00Z",
    last_modified: str = "2023-12-31T23:59:59Z",
    content_type: str | None = "image/jpeg",
    quarantined_at: str | None = None,
    tombstoned_at: str | None = None,
    mcps_source_meta: str | None = None,
) -> ObjectRecord:
    return ObjectRecord(
        source=source,
        key=key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        last_seen_at=last_seen_at,
        last_modified=last_modified,
        content_type=content_type,
        quarantined_at=quarantined_at,
        tombstoned_at=tombstoned_at,
        mcps_source_meta=mcps_source_meta,
    )


def build_catalog(*records: ObjectRecord) -> Catalog:
    cat = Catalog()
    for r in records:
        cat = cat.upsert(r)
    return cat


# ---------------------------------------------------------------------------
# Empty / no-duplicate cases (req 4.5)
# ---------------------------------------------------------------------------


def test_empty_catalog_returns_empty_groups_and_no_skipped():
    result = detect_duplicates(Catalog())
    assert result == DetectionResult(groups=(), skipped_records=())


def test_single_record_emits_no_group():
    """Req 4.1 requires ``count >= 2``; lone records are not duplicates."""
    rec = make_record()
    result = detect_duplicates(build_catalog(rec))
    assert result.groups == ()
    assert result.skipped_records == ()


def test_distinct_hashes_emit_no_groups():
    r1 = make_record(key="a", content_hash=HASH_A)
    r2 = make_record(source="gcs-archive", key="b", content_hash=HASH_B)
    result = detect_duplicates(build_catalog(r1, r2))
    assert result.groups == ()


# ---------------------------------------------------------------------------
# Same-source vs cross-source labelling (req 4.3)
# ---------------------------------------------------------------------------


def test_two_records_same_hash_same_source_yields_same_source_group():
    r1 = make_record(source="s3-prod", key="a")
    r2 = make_record(source="s3-prod", key="b")
    result = detect_duplicates(build_catalog(r1, r2))

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.content_hash == HASH_A
    assert group.label == "same-source"
    assert group.members == (r1, r2)
    assert group.total_size_bytes == r1.size_bytes + r2.size_bytes


def test_two_records_same_hash_two_sources_yields_cross_source_group():
    r_s3 = make_record(source="s3-prod", key="a")
    r_gcs = make_record(source="gcs-archive", key="b")
    result = detect_duplicates(build_catalog(r_s3, r_gcs))

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.label == "cross-source"
    # Members sorted by (source, key); "gcs-archive" < "s3-prod" lex.
    assert group.members == (r_gcs, r_s3)


def test_three_records_two_sources_is_cross_source():
    r1 = make_record(source="s3-prod", key="a")
    r2 = make_record(source="s3-prod", key="b")
    r3 = make_record(source="gcs-archive", key="c")
    result = detect_duplicates(build_catalog(r1, r2, r3))

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.label == "cross-source"
    assert set(group.members) == {r1, r2, r3}


def test_three_records_three_sources_is_cross_source():
    r1 = make_record(source="s3-prod", key="a")
    r2 = make_record(source="gcs-archive", key="b")
    r3 = make_record(source="drive-folder", key="c")
    result = detect_duplicates(build_catalog(r1, r2, r3))

    assert len(result.groups) == 1
    assert result.groups[0].label == "cross-source"
    assert result.groups[0].total_size_bytes == 3 * 1024


# ---------------------------------------------------------------------------
# Size-must-match rule (req 4.2)
# ---------------------------------------------------------------------------


def test_records_with_same_hash_but_different_size_are_not_grouped():
    """Req 4.2: duplicates require both content_hash and size_bytes equal.

    With size mismatch, each record sits alone in its own bucket, so no
    group of size >= 2 ever forms.
    """
    r_small = make_record(source="s3-prod", key="a", size_bytes=1024)
    r_large = make_record(source="s3-prod", key="b", size_bytes=2048)
    result = detect_duplicates(build_catalog(r_small, r_large))

    assert result.groups == ()
    assert result.skipped_records == ()


def test_size_match_subset_still_groups_correctly():
    """Three records with the same hash, two with size 1024 and one with
    size 2048: the two same-size records group, the lone large record does
    not."""
    r1 = make_record(source="s3-prod", key="a", size_bytes=1024)
    r2 = make_record(source="gcs-archive", key="b", size_bytes=1024)
    r_outlier = make_record(source="s3-prod", key="c", size_bytes=2048)
    result = detect_duplicates(build_catalog(r1, r2, r_outlier))

    assert len(result.groups) == 1
    group = result.groups[0]
    assert set(group.members) == {r1, r2}
    assert group.total_size_bytes == 2 * 1024
    assert r_outlier not in result.skipped_records  # valid hash + size


# ---------------------------------------------------------------------------
# Skipped records (req 4.6)
# ---------------------------------------------------------------------------


def test_record_with_empty_content_hash_is_skipped():
    bad = make_record(key="bad", content_hash="")
    good_a = make_record(source="s3-prod", key="a")
    good_b = make_record(source="gcs-archive", key="b")
    result = detect_duplicates(build_catalog(bad, good_a, good_b))

    assert bad in result.skipped_records
    # The good pair still forms a group.
    assert len(result.groups) == 1


def test_record_with_short_content_hash_is_skipped():
    bad = make_record(key="bad", content_hash="abcdef")  # not 64 chars
    result = detect_duplicates(build_catalog(bad))
    assert result.groups == ()
    assert result.skipped_records == (bad,)


def test_record_with_uppercase_hex_hash_is_skipped():
    """Content_Hash is defined as lowercase hex (req 6.1)."""
    bad = make_record(key="bad", content_hash="A" * 64)
    result = detect_duplicates(build_catalog(bad))
    assert result.skipped_records == (bad,)


def test_record_with_non_hex_hash_is_skipped():
    bad = make_record(key="bad", content_hash="z" * 64)
    result = detect_duplicates(build_catalog(bad))
    assert result.skipped_records == (bad,)


def test_record_with_negative_size_is_skipped():
    bad = make_record(key="bad", size_bytes=-1)
    result = detect_duplicates(build_catalog(bad))
    assert result.groups == ()
    assert result.skipped_records == (bad,)


def test_skipped_records_do_not_appear_in_any_group():
    bad = make_record(source="s3-prod", key="bad", content_hash="")
    good_a = make_record(source="s3-prod", key="a")
    good_b = make_record(source="gcs-archive", key="b")
    result = detect_duplicates(build_catalog(bad, good_a, good_b))

    for group in result.groups:
        assert bad not in group.members


def test_skipped_records_are_sorted_for_determinism():
    bad1 = make_record(source="s3-prod", key="z", content_hash="")
    bad2 = make_record(source="gcs-archive", key="a", content_hash="")
    bad3 = make_record(source="s3-prod", key="a", content_hash="")
    result = detect_duplicates(build_catalog(bad1, bad2, bad3))
    assert result.skipped_records == (bad2, bad3, bad1)


# ---------------------------------------------------------------------------
# Determinism (req 4.4 — full Hypothesis property is in task 21)
# ---------------------------------------------------------------------------


def test_groups_sorted_by_content_hash_regardless_of_insertion_order():
    """Groups must be emitted in `content_hash` ascending order, no matter
    which order records were inserted into the Catalog."""
    pair_c1 = make_record(source="s3-prod", key="a", content_hash=HASH_C)
    pair_c2 = make_record(source="gcs-archive", key="a", content_hash=HASH_C)
    pair_a1 = make_record(source="s3-prod", key="b", content_hash=HASH_A)
    pair_a2 = make_record(source="gcs-archive", key="b", content_hash=HASH_A)
    pair_b1 = make_record(source="s3-prod", key="c", content_hash=HASH_B)
    pair_b2 = make_record(source="gcs-archive", key="c", content_hash=HASH_B)

    cat_forward = build_catalog(pair_a1, pair_a2, pair_b1, pair_b2, pair_c1, pair_c2)
    cat_reverse = build_catalog(pair_c2, pair_c1, pair_b2, pair_b1, pair_a2, pair_a1)

    result_forward = detect_duplicates(cat_forward)
    result_reverse = detect_duplicates(cat_reverse)

    assert [g.content_hash for g in result_forward.groups] == [HASH_A, HASH_B, HASH_C]
    # Group lists must be equal between the two insertion orders.
    assert result_forward.groups == result_reverse.groups


def test_total_size_bytes_equals_sum_of_member_sizes():
    r1 = make_record(source="s3-prod", key="a", size_bytes=512)
    r2 = make_record(source="gcs-archive", key="a", size_bytes=512)
    r3 = make_record(source="drive-folder", key="a", size_bytes=512)
    result = detect_duplicates(build_catalog(r1, r2, r3))

    assert len(result.groups) == 1
    group = result.groups[0]
    assert group.total_size_bytes == sum(m.size_bytes for m in group.members)
    assert group.total_size_bytes == 512 * 3


def test_members_sorted_by_source_then_key():
    """Member tuples are deterministic for the Manifest writer."""
    r_s3_z = make_record(source="s3-prod", key="z")
    r_s3_a = make_record(source="s3-prod", key="a")
    r_gcs = make_record(source="gcs-archive", key="m")
    result = detect_duplicates(build_catalog(r_s3_z, r_s3_a, r_gcs))

    assert len(result.groups) == 1
    # gcs-archive < s3-prod, and within s3-prod "a" < "z".
    assert result.groups[0].members == (r_gcs, r_s3_a, r_s3_z)


def test_disjoint_groups_each_get_their_own_label():
    same = (
        make_record(source="s3-prod", key="a", content_hash=HASH_A),
        make_record(source="s3-prod", key="b", content_hash=HASH_A),
    )
    cross = (
        make_record(source="s3-prod", key="c", content_hash=HASH_B),
        make_record(source="gcs-archive", key="d", content_hash=HASH_B),
    )
    result = detect_duplicates(build_catalog(*same, *cross))

    assert len(result.groups) == 2
    by_hash = {g.content_hash: g for g in result.groups}
    assert by_hash[HASH_A].label == "same-source"
    assert by_hash[HASH_B].label == "cross-source"


def test_result_is_frozen_dataclass():
    """`DuplicateGroup` and `DetectionResult` must be frozen so callers can
    place them in sets/dicts."""
    r1 = make_record(source="s3-prod", key="a")
    r2 = make_record(source="gcs-archive", key="b")
    result = detect_duplicates(build_catalog(r1, r2))

    # Hashing must succeed for a frozen dataclass with hashable fields.
    assert hash(result.groups[0]) == hash(result.groups[0])
