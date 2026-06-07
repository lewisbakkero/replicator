"""Unit tests for `Catalog.cache_lookup` (task 5).

Exercises the cache-hit predicate that lets the listing path skip re-hashing
an Object whose provider-reported `(size, last_modified)` matches the
catalogued values (req 3.7), and the staleness branch that forces re-hash
when either field has changed (req 3.8).

Validates: Requirements 3.7, 3.8.
"""

from __future__ import annotations

from mcps.catalog.model import Catalog, ObjectRecord


def _record(
    *,
    source: str = "s3-prod",
    key: str = "photos/2024/img.jpg",
    content_hash: str = "a" * 64,
    size_bytes: int = 1024,
    last_modified: str = "2024-01-01T00:00:00Z",
) -> ObjectRecord:
    """Helper: build an `ObjectRecord` with sensible defaults so each test
    can override only the fields under examination."""
    return ObjectRecord(
        source=source,
        key=key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified=last_modified,
        content_type="image/jpeg",
    )


def test_empty_catalog_returns_none() -> None:
    cat = Catalog()
    assert (
        cat.cache_lookup(
            source="s3-prod",
            key="photos/2024/img.jpg",
            size=1024,
            last_modified="2024-01-01T00:00:00Z",
        )
        is None
    )


def test_exact_match_returns_cached_hash() -> None:
    rec = _record(content_hash="b" * 64)
    cat = Catalog().upsert(rec)

    result = cat.cache_lookup(
        source=rec.source,
        key=rec.key,
        size=rec.size_bytes,
        last_modified=rec.last_modified,
    )

    assert result == "b" * 64


def test_size_changed_returns_none() -> None:
    """Provider-reported size differs from catalog -> hash is stale (req 3.8)."""
    rec = _record(size_bytes=1024)
    cat = Catalog().upsert(rec)

    assert (
        cat.cache_lookup(
            source=rec.source,
            key=rec.key,
            size=2048,  # differs
            last_modified=rec.last_modified,
        )
        is None
    )


def test_last_modified_changed_returns_none() -> None:
    """Provider-reported mtime differs from catalog -> hash is stale (req 3.8)."""
    rec = _record(last_modified="2024-01-01T00:00:00Z")
    cat = Catalog().upsert(rec)

    assert (
        cat.cache_lookup(
            source=rec.source,
            key=rec.key,
            size=rec.size_bytes,
            last_modified="2024-06-15T12:34:56Z",  # differs
        )
        is None
    )


def test_different_source_returns_none() -> None:
    rec = _record(source="s3-prod")
    cat = Catalog().upsert(rec)

    assert (
        cat.cache_lookup(
            source="gcs-archive",  # differs
            key=rec.key,
            size=rec.size_bytes,
            last_modified=rec.last_modified,
        )
        is None
    )


def test_different_key_returns_none() -> None:
    rec = _record(key="photos/2024/img.jpg")
    cat = Catalog().upsert(rec)

    assert (
        cat.cache_lookup(
            source=rec.source,
            key="photos/2024/other.jpg",  # differs
            size=rec.size_bytes,
            last_modified=rec.last_modified,
        )
        is None
    )


def test_multiple_records_one_matches_returns_matching_hash() -> None:
    """Only the record whose `(source, key, size, last_modified)` matches all
    four predicates contributes its `content_hash`; the others are ignored."""
    target_hash = "c" * 64
    other_hash_1 = "d" * 64
    other_hash_2 = "e" * 64

    cat = (
        Catalog()
        .upsert(_record(source="s3-prod", key="a.jpg", content_hash=other_hash_1))
        .upsert(
            _record(
                source="gcs-archive",
                key="photos/target.jpg",
                content_hash=target_hash,
                size_bytes=4096,
                last_modified="2024-03-15T10:00:00Z",
            )
        )
        .upsert(_record(source="s3-prod", key="z.jpg", content_hash=other_hash_2))
    )

    result = cat.cache_lookup(
        source="gcs-archive",
        key="photos/target.jpg",
        size=4096,
        last_modified="2024-03-15T10:00:00Z",
    )

    assert result == target_hash


def test_same_source_key_size_and_mtime_both_differ_returns_none() -> None:
    """Defence against accidental partial-match leakage: when both size and
    mtime differ, the hash is still stale."""
    rec = _record(size_bytes=1024, last_modified="2024-01-01T00:00:00Z")
    cat = Catalog().upsert(rec)

    assert (
        cat.cache_lookup(
            source=rec.source,
            key=rec.key,
            size=2048,
            last_modified="2025-01-01T00:00:00Z",
        )
        is None
    )
