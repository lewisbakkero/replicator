"""Unit tests for `mcps.catalog.model`.

Covers `ObjectRecord` value semantics (equality, hashability, ordering, default
fields) and the `Catalog` upsert/remove/lookup invariants documented in
design.md.

Validates: Requirements 3.2, 11.3, 11.5.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields

import pytest

from mcps.catalog.model import Catalog, ObjectRecord


# ---------------------------------------------------------------------------
# Helpers — produce ObjectRecords with terse positional kwargs.
# ---------------------------------------------------------------------------


def make_record(
    *,
    source: str = "s3-prod",
    key: str = "photos/IMG_0001.jpg",
    content_hash: str = "a" * 64,
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


# ---------------------------------------------------------------------------
# ObjectRecord field shape
# ---------------------------------------------------------------------------


def test_object_record_has_exact_field_set_from_design():
    """The ten field names must match design.md byte for byte."""
    expected = [
        "source",
        "key",
        "content_hash",
        "size_bytes",
        "last_seen_at",
        "last_modified",
        "content_type",
        "quarantined_at",
        "tombstoned_at",
        "mcps_source_meta",
    ]
    assert [f.name for f in fields(ObjectRecord)] == expected


def test_object_record_optional_fields_default_to_none():
    rec = ObjectRecord(
        source="s3-prod",
        key="photos/IMG_0001.jpg",
        content_hash="a" * 64,
        size_bytes=1024,
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2023-12-31T23:59:59Z",
        content_type=None,
    )
    assert rec.quarantined_at is None
    assert rec.tombstoned_at is None
    assert rec.mcps_source_meta is None
    # content_type is Optional[str] and may itself be None.
    assert rec.content_type is None


def test_object_record_is_frozen():
    rec = make_record()
    with pytest.raises(FrozenInstanceError):
        rec.size_bytes = 2048  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ObjectRecord equality / hashability / ordering
# ---------------------------------------------------------------------------


def test_object_record_equality_is_field_wise():
    a = make_record()
    b = make_record()
    assert a == b
    assert not (a != b)


def test_object_record_inequality_on_any_field():
    base = make_record()
    assert base != make_record(source="s3-archive")
    assert base != make_record(key="photos/IMG_0002.jpg")
    assert base != make_record(content_hash="b" * 64)
    assert base != make_record(size_bytes=2048)
    assert base != make_record(last_seen_at="2024-02-02T00:00:00Z")
    assert base != make_record(last_modified="2024-02-02T00:00:00Z")
    assert base != make_record(content_type="image/png")
    assert base != make_record(quarantined_at="2024-03-03T00:00:00Z")
    assert base != make_record(tombstoned_at="2024-03-03T00:00:00Z")
    assert base != make_record(mcps_source_meta="s3-prod")


def test_object_record_is_hashable_and_frozenset_member():
    a = make_record()
    b = make_record()  # equal value, distinct identity
    s: frozenset[ObjectRecord] = frozenset({a, b})
    assert len(s) == 1
    assert a in s


def test_object_record_orders_lexicographically_by_field_tuple():
    """`@dataclass(order=True)` orders by the tuple of all fields in order.

    Source is the first field, so source dominates ordering.
    """
    older = make_record(source="a-source")
    newer = make_record(source="z-source")
    assert older < newer
    assert sorted([newer, older]) == [older, newer]


def test_object_record_ordering_falls_through_to_key_when_source_equal():
    a = make_record(key="photos/AAAA.jpg")
    b = make_record(key="photos/ZZZZ.jpg")
    assert a < b


# ---------------------------------------------------------------------------
# Catalog construction / read helpers
# ---------------------------------------------------------------------------


def test_empty_catalog_has_zero_records():
    cat = Catalog()
    assert list(cat.all_records()) == []
    assert cat.by_hash == {}
    assert cat.records_for_source("s3-prod") == frozenset()


def test_records_for_source_filters_by_source_name():
    r_s3 = make_record(source="s3-prod", key="a", content_hash="a" * 64)
    r_gcs = make_record(source="gcs-archive", key="b", content_hash="b" * 64)
    cat = Catalog().upsert(r_s3).upsert(r_gcs)

    assert cat.records_for_source("s3-prod") == frozenset({r_s3})
    assert cat.records_for_source("gcs-archive") == frozenset({r_gcs})
    assert cat.records_for_source("unknown") == frozenset()


def test_all_records_yields_every_record_regardless_of_bucket():
    r1 = make_record(source="s3-prod", key="a", content_hash="a" * 64)
    r2 = make_record(source="s3-prod", key="b", content_hash="b" * 64)
    r3 = make_record(source="gcs-archive", key="c", content_hash="a" * 64)
    cat = Catalog().upsert(r1).upsert(r2).upsert(r3)

    assert set(cat.all_records()) == {r1, r2, r3}


# ---------------------------------------------------------------------------
# Catalog.upsert invariants
# ---------------------------------------------------------------------------


def test_upsert_returns_new_catalog_and_leaves_original_unchanged():
    cat = Catalog()
    rec = make_record()
    cat2 = cat.upsert(rec)

    assert cat2 is not cat
    assert cat.by_hash == {}        # original still empty
    assert cat2.by_hash == {rec.content_hash: frozenset({rec})}


def test_upsert_adds_record_under_new_content_hash():
    rec = make_record(content_hash="a" * 64)
    cat = Catalog().upsert(rec)

    assert "a" * 64 in cat.by_hash
    assert cat.by_hash["a" * 64] == frozenset({rec})


def test_upsert_with_same_hash_but_different_source_key_extends_bucket():
    h = "a" * 64
    r1 = make_record(source="s3-prod", key="x", content_hash=h)
    r2 = make_record(source="gcs-archive", key="y", content_hash=h)

    cat = Catalog().upsert(r1).upsert(r2)

    assert set(cat.by_hash.keys()) == {h}
    assert cat.by_hash[h] == frozenset({r1, r2})


def test_upsert_replaces_record_with_same_source_key_under_new_hash():
    """Updating a record's content_hash must move it between buckets."""
    old_hash = "a" * 64
    new_hash = "b" * 64
    r_old = make_record(source="s3-prod", key="x", content_hash=old_hash)
    r_new = make_record(source="s3-prod", key="x", content_hash=new_hash)

    cat = Catalog().upsert(r_old).upsert(r_new)

    # The record should live under the new hash only.
    assert old_hash not in cat.by_hash
    assert cat.by_hash[new_hash] == frozenset({r_new})


def test_upsert_replaces_within_same_hash_when_metadata_changes():
    """Same (source, key) and same content_hash but a different mtime field
    should still leave only one record under that bucket."""
    h = "a" * 64
    r_old = make_record(source="s3-prod", key="x", content_hash=h, size_bytes=1024)
    r_new = make_record(source="s3-prod", key="x", content_hash=h, size_bytes=2048)

    cat = Catalog().upsert(r_old).upsert(r_new)

    assert cat.by_hash[h] == frozenset({r_new})


def test_upsert_idempotent_on_identical_record():
    rec = make_record()
    cat1 = Catalog().upsert(rec)
    cat2 = cat1.upsert(rec)

    assert cat2.by_hash == cat1.by_hash


# ---------------------------------------------------------------------------
# Catalog.remove invariants
# ---------------------------------------------------------------------------


def test_remove_returns_new_catalog_and_leaves_original_unchanged():
    rec = make_record()
    cat = Catalog().upsert(rec)
    cat2 = cat.remove(rec.source, rec.key)

    assert cat2 is not cat
    # Original retains the record.
    assert rec in cat.by_hash[rec.content_hash]
    # New catalog has the record dropped.
    assert cat2.by_hash == {}


def test_remove_drops_record_and_prunes_empty_bucket():
    rec = make_record()
    cat = Catalog().upsert(rec).remove(rec.source, rec.key)

    assert rec.content_hash not in cat.by_hash
    assert cat.by_hash == {}


def test_remove_preserves_other_records_in_same_bucket():
    h = "a" * 64
    r1 = make_record(source="s3-prod", key="x", content_hash=h)
    r2 = make_record(source="gcs-archive", key="y", content_hash=h)
    cat = Catalog().upsert(r1).upsert(r2).remove("s3-prod", "x")

    assert cat.by_hash == {h: frozenset({r2})}


def test_remove_no_match_is_a_noop():
    rec = make_record()
    cat = Catalog().upsert(rec)
    cat2 = cat.remove("nonexistent-source", "nonexistent-key")

    assert cat2 == cat


# ---------------------------------------------------------------------------
# Catalog equality and hashability
# ---------------------------------------------------------------------------


def test_catalog_equality_is_by_hash_element_wise():
    r1 = make_record(source="s3-prod", key="a", content_hash="a" * 64)
    r2 = make_record(source="gcs-archive", key="b", content_hash="b" * 64)

    # Two different upsert orderings of the same records compare equal.
    cat1 = Catalog().upsert(r1).upsert(r2)
    cat2 = Catalog().upsert(r2).upsert(r1)
    assert cat1 == cat2


def test_catalog_inequality_when_records_differ():
    r1 = make_record(source="s3-prod", key="a", content_hash="a" * 64)
    r2 = make_record(source="s3-prod", key="b", content_hash="a" * 64)
    cat1 = Catalog().upsert(r1)
    cat2 = Catalog().upsert(r2)
    assert cat1 != cat2


def test_catalog_equality_against_non_catalog_returns_notimplemented():
    cat = Catalog()
    assert cat != object()
    assert cat != {"a": frozenset()}


def test_catalog_is_hashable_when_buckets_are_frozenset():
    rec = make_record()
    cat = Catalog().upsert(rec)
    # Hashing must succeed; equal Catalogs must hash equal.
    assert hash(cat) == hash(Catalog().upsert(rec))
