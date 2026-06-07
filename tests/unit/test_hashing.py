"""Unit tests for `mcps.hashing` (task 15).

Covers the three primitives in this module:

* `stream_sha256` — chunked SHA-256 over an iterable of bytes.
* `s3_etag_is_singlepart` — heuristic that decides whether an S3 ETag is the
  MD5 of a single-part upload.
* `is_valid_content_hash` — shape validation for the `mcps-content-sha256`
  user-metadata shortcut (req 7.1) and any other 64-char lowercase-hex
  predicate.
* `compute_content_hash` — the three-step priority chain that prefers
  metadata, then the Catalog cache, then a streamed hash.

Validates: Requirements 2.2, 2.3, 2.4, 3.7, 7.1, 7.2.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Iterator, List, Mapping

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.hashing import (
    CHUNK_SIZE,
    compute_content_hash,
    is_valid_content_hash,
    s3_etag_is_singlepart,
    stream_sha256,
)


# ---------------------------------------------------------------------------
# Test fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeMeta:
    """Minimal stand-in for `mcps.sources.base.ObjectMeta`.

    Carries only the four fields `compute_content_hash` reads. The real
    `ObjectMeta` arrives in task 16; this fake satisfies the structural
    `_ObjectMetaLike` Protocol.
    """

    key: str
    size_bytes: int
    last_modified: str
    user_metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass
class _FakeAdapter:
    """In-memory `SourceAdapter` stand-in.

    Records every call to `read_bytes` so tests can assert that the network
    fallback was (or was not) taken. The real `SourceAdapter` ABC arrives
    in task 16; this fake satisfies the structural `_SourceAdapterLike`
    Protocol.
    """

    name: str
    blobs: dict[str, bytes] = field(default_factory=dict)
    read_calls: List[str] = field(default_factory=list)

    def read_bytes(self, key: str) -> Iterator[bytes]:
        self.read_calls.append(key)
        data = self.blobs[key]
        # Deterministic chunking that exercises the multi-chunk path even
        # for small inputs: emit 7-byte chunks so a 20-byte payload yields
        # three iterations.
        chunk = 7
        for i in range(0, len(data), chunk):
            yield data[i:i + chunk]


# ---------------------------------------------------------------------------
# stream_sha256
# ---------------------------------------------------------------------------


EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_stream_sha256_empty_iterable_returns_known_empty_digest() -> None:
    assert stream_sha256(iter(())) == EMPTY_SHA256
    assert stream_sha256([]) == EMPTY_SHA256


def test_stream_sha256_single_chunk_matches_hashlib() -> None:
    payload = b"hello, multicloud-photo-sync"
    expected = hashlib.sha256(payload).hexdigest()

    assert stream_sha256([payload]) == expected


def test_stream_sha256_multiple_chunks_match_concatenated_digest() -> None:
    parts = [b"the ", b"quick ", b"brown ", b"fox"]
    expected = hashlib.sha256(b"".join(parts)).hexdigest()

    assert stream_sha256(iter(parts)) == expected


def test_stream_sha256_handles_empty_chunks_interspersed() -> None:
    """Empty chunks must not change the digest — `update(b"")` is a no-op."""
    parts = [b"", b"abc", b"", b"def", b""]
    expected = hashlib.sha256(b"abcdef").hexdigest()

    assert stream_sha256(parts) == expected


def test_stream_sha256_large_input_under_chunk_size() -> None:
    """Sanity: hashing a multi-MiB payload yields the same digest whether
    fed as one chunk or as `CHUNK_SIZE`-byte chunks."""
    payload = (b"x" * 13) * 200_000  # ~2.5 MiB, not aligned to CHUNK_SIZE
    expected = hashlib.sha256(payload).hexdigest()

    chunked = (
        payload[i:i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)
    )

    assert stream_sha256(chunked) == expected


# ---------------------------------------------------------------------------
# s3_etag_is_singlepart
# ---------------------------------------------------------------------------


SINGLEPART_ETAG = "d41d8cd98f00b204e9800998ecf8427e"  # 32 lowercase hex chars
MULTIPART_ETAG = "d41d8cd98f00b204e9800998ecf8427e-3"


def test_s3_etag_singlepart_plain_hex_returns_true() -> None:
    assert s3_etag_is_singlepart(SINGLEPART_ETAG) is True


def test_s3_etag_singlepart_quoted_returns_true() -> None:
    """S3 returns ETags surrounded by literal double quotes; the helper
    must strip them before judging the body."""
    assert s3_etag_is_singlepart(f'"{SINGLEPART_ETAG}"') is True


def test_s3_etag_multipart_returns_false() -> None:
    assert s3_etag_is_singlepart(MULTIPART_ETAG) is False


def test_s3_etag_quoted_multipart_returns_false() -> None:
    assert s3_etag_is_singlepart(f'"{MULTIPART_ETAG}"') is False


def test_s3_etag_none_returns_false() -> None:
    assert s3_etag_is_singlepart(None) is False


def test_s3_etag_empty_string_returns_false() -> None:
    assert s3_etag_is_singlepart("") is False
    assert s3_etag_is_singlepart('""') is False


def test_s3_etag_too_short_returns_false() -> None:
    assert s3_etag_is_singlepart("a" * 31) is False


def test_s3_etag_too_long_returns_false() -> None:
    assert s3_etag_is_singlepart("a" * 33) is False


def test_s3_etag_non_hex_returns_false() -> None:
    assert s3_etag_is_singlepart("g" * 32) is False  # 'g' is not hex
    assert s3_etag_is_singlepart("z" + "0" * 31) is False


def test_s3_etag_uppercase_hex_returns_false() -> None:
    """Uppercase hex is rejected: Catalog_Printer emits lowercase, so any
    cross-talk with uppercase hex is treated as malformed input."""
    assert s3_etag_is_singlepart(SINGLEPART_ETAG.upper()) is False


# ---------------------------------------------------------------------------
# is_valid_content_hash
# ---------------------------------------------------------------------------


VALID_HASH = "a" * 64  # 64 lowercase hex chars


def test_is_valid_content_hash_accepts_64_char_lowercase_hex() -> None:
    assert is_valid_content_hash(VALID_HASH) is True
    assert is_valid_content_hash("0123456789abcdef" * 4) is True


def test_is_valid_content_hash_rejects_uppercase_hex() -> None:
    assert is_valid_content_hash("A" * 64) is False


def test_is_valid_content_hash_rejects_too_short() -> None:
    assert is_valid_content_hash("a" * 63) is False


def test_is_valid_content_hash_rejects_too_long() -> None:
    assert is_valid_content_hash("a" * 65) is False


def test_is_valid_content_hash_rejects_non_hex_chars() -> None:
    bad = "a" * 63 + "z"
    assert is_valid_content_hash(bad) is False


def test_is_valid_content_hash_rejects_none_and_empty() -> None:
    assert is_valid_content_hash(None) is False
    assert is_valid_content_hash("") is False


# ---------------------------------------------------------------------------
# compute_content_hash — three-step priority chain
# ---------------------------------------------------------------------------


def _seed_catalog_with(rec: ObjectRecord) -> Catalog:
    return Catalog().upsert(rec)


def test_compute_uses_metadata_when_valid_and_skips_read() -> None:
    """Step 1 (req 7.1): valid `mcps-content-sha256` short-circuits the
    chain. Neither the Catalog nor `adapter.read_bytes` is consulted."""
    valid = "b" * 64
    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=1024,
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={"mcps-content-sha256": valid},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"photos/img.jpg": b"unused"})
    catalog = Catalog()  # empty -> would force step 3 if reached

    result = compute_content_hash(adapter, meta, catalog)

    assert result == valid
    assert adapter.read_calls == []


def test_compute_falls_through_when_metadata_uppercase() -> None:
    """Uppercase hex is invalid (req 7.1 demands 64-char lowercase hex);
    the chain falls through to step 2 / step 3."""
    payload = b"file content"
    expected = hashlib.sha256(payload).hexdigest()

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=len(payload),
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={"mcps-content-sha256": "A" * 64},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"photos/img.jpg": payload})
    catalog = Catalog()  # empty -> step 2 misses, step 3 streams

    result = compute_content_hash(adapter, meta, catalog)

    assert result == expected
    assert adapter.read_calls == ["photos/img.jpg"]


def test_compute_falls_through_when_metadata_wrong_length() -> None:
    payload = b"abc"
    expected = hashlib.sha256(payload).hexdigest()

    meta = _FakeMeta(
        key="k",
        size_bytes=len(payload),
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={"mcps-content-sha256": "a" * 63},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"k": payload})
    catalog = Catalog()

    assert compute_content_hash(adapter, meta, catalog) == expected
    assert adapter.read_calls == ["k"]


def test_compute_falls_through_when_metadata_non_hex() -> None:
    payload = b"abc"
    expected = hashlib.sha256(payload).hexdigest()

    meta = _FakeMeta(
        key="k",
        size_bytes=len(payload),
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={"mcps-content-sha256": "z" * 64},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"k": payload})
    catalog = Catalog()

    assert compute_content_hash(adapter, meta, catalog) == expected
    assert adapter.read_calls == ["k"]


def test_compute_falls_through_when_metadata_absent() -> None:
    payload = b"abc"
    expected = hashlib.sha256(payload).hexdigest()

    meta = _FakeMeta(
        key="k",
        size_bytes=len(payload),
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={},  # mcps-content-sha256 missing
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"k": payload})
    catalog = Catalog()

    assert compute_content_hash(adapter, meta, catalog) == expected
    assert adapter.read_calls == ["k"]


def test_compute_uses_catalog_cache_hit_and_skips_read() -> None:
    """Step 2 (req 3.7): when metadata is missing/invalid but the Catalog
    has a record matching `(source, key, size, last_modified)`, the cached
    `content_hash` is returned and `adapter.read_bytes` is not called."""
    cached = "c" * 64
    rec = ObjectRecord(
        source="s3-prod",
        key="photos/img.jpg",
        content_hash=cached,
        size_bytes=2048,
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2024-01-02T03:04:05Z",
        content_type="image/jpeg",
    )
    catalog = _seed_catalog_with(rec)

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=2048,
        last_modified="2024-01-02T03:04:05Z",
        user_metadata={},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"photos/img.jpg": b"unused"})

    result = compute_content_hash(adapter, meta, catalog)

    assert result == cached
    assert adapter.read_calls == []


def test_compute_streams_when_cache_misses_on_size() -> None:
    """Cache entry exists for `(source, key)` but `size_bytes` differs ->
    cached hash is stale (req 3.8) and we must re-stream."""
    payload = b"updated content"
    expected = hashlib.sha256(payload).hexdigest()

    rec = ObjectRecord(
        source="s3-prod",
        key="photos/img.jpg",
        content_hash="d" * 64,
        size_bytes=999,  # stale size
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2024-01-02T03:04:05Z",
        content_type="image/jpeg",
    )
    catalog = _seed_catalog_with(rec)

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=len(payload),  # differs from catalog
        last_modified="2024-01-02T03:04:05Z",
        user_metadata={},
    )
    adapter = _FakeAdapter(
        name="s3-prod", blobs={"photos/img.jpg": payload}
    )

    result = compute_content_hash(adapter, meta, catalog)

    assert result == expected
    assert adapter.read_calls == ["photos/img.jpg"]


def test_compute_streams_when_cache_misses_on_last_modified() -> None:
    """Cache entry exists for `(source, key)` but `last_modified` differs ->
    cached hash is stale (req 3.8) and we must re-stream."""
    payload = b"another update"
    expected = hashlib.sha256(payload).hexdigest()

    rec = ObjectRecord(
        source="gcs-archive",
        key="photos/img.jpg",
        content_hash="e" * 64,
        size_bytes=len(payload),
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2023-12-01T00:00:00Z",  # stale mtime
        content_type="image/jpeg",
    )
    catalog = _seed_catalog_with(rec)

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=len(payload),
        last_modified="2024-06-15T12:00:00Z",  # differs
        user_metadata={},
    )
    adapter = _FakeAdapter(
        name="gcs-archive", blobs={"photos/img.jpg": payload}
    )

    result = compute_content_hash(adapter, meta, catalog)

    assert result == expected
    assert adapter.read_calls == ["photos/img.jpg"]


def test_compute_streams_when_catalog_record_belongs_to_other_source() -> None:
    """A Catalog hit on the same `(key, size, last_modified)` but a
    different `source` must NOT short-circuit step 2: the four-tuple
    predicate is `(source, key, size, last_modified)`."""
    payload = b"per-source bytes"
    expected = hashlib.sha256(payload).hexdigest()

    rec = ObjectRecord(
        source="s3-prod",
        key="photos/img.jpg",
        content_hash="f" * 64,
        size_bytes=len(payload),
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2024-01-02T03:04:05Z",
        content_type="image/jpeg",
    )
    catalog = _seed_catalog_with(rec)

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=len(payload),
        last_modified="2024-01-02T03:04:05Z",
        user_metadata={},
    )
    adapter = _FakeAdapter(
        name="gcs-archive",  # different source
        blobs={"photos/img.jpg": payload},
    )

    result = compute_content_hash(adapter, meta, catalog)

    assert result == expected
    assert adapter.read_calls == ["photos/img.jpg"]


def test_compute_metadata_shortcut_takes_precedence_over_cache_hit() -> None:
    """Step 1 takes precedence over step 2: a record exists in the Catalog
    AND a valid `mcps-content-sha256` is set; the metadata wins and the
    adapter is not called."""
    metadata_hash = "1" * 64
    cached_hash = "2" * 64

    rec = ObjectRecord(
        source="s3-prod",
        key="photos/img.jpg",
        content_hash=cached_hash,
        size_bytes=1024,
        last_seen_at="2024-01-01T00:00:00Z",
        last_modified="2024-01-02T03:04:05Z",
        content_type="image/jpeg",
    )
    catalog = _seed_catalog_with(rec)

    meta = _FakeMeta(
        key="photos/img.jpg",
        size_bytes=1024,
        last_modified="2024-01-02T03:04:05Z",
        user_metadata={"mcps-content-sha256": metadata_hash},
    )
    adapter = _FakeAdapter(
        name="s3-prod", blobs={"photos/img.jpg": b"unused"}
    )

    result = compute_content_hash(adapter, meta, catalog)

    assert result == metadata_hash
    assert adapter.read_calls == []


def test_compute_streams_chunks_through_adapter_for_empty_object() -> None:
    """An empty Object yields the well-known empty-input digest via step 3."""
    meta = _FakeMeta(
        key="empty.bin",
        size_bytes=0,
        last_modified="2024-01-01T00:00:00Z",
        user_metadata={},
    )
    adapter = _FakeAdapter(name="s3-prod", blobs={"empty.bin": b""})
    catalog = Catalog()

    result = compute_content_hash(adapter, meta, catalog)

    assert result == EMPTY_SHA256
    assert adapter.read_calls == ["empty.bin"]
