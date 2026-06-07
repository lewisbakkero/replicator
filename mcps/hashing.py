"""Streamed SHA-256 helpers and the `compute_content_hash` priority chain.

The Catalog identifies Objects by their lowercase-hex SHA-256 Content_Hash
(req 2.2-2.4). Listing every Object end-to-end on every Sync_Run is
unaffordable, so the listing path resolves a record's Content_Hash through a
three-step priority documented in `design.md`:

    1. Trust the round-tripped `mcps-content-sha256` user-metadata that an
       earlier Sync_Run wrote when it replicated the Object (req 7.1). The
       value is validated as a 64-char lowercase hex string before use; a
       malformed value falls through.
    2. Otherwise, look the Object up in the in-memory Catalog by
       `(source, key, size, last_modified)`. A hit means the provider
       reports byte-identical size and mtime since we last hashed the
       Object, so the cached Content_Hash is still valid (req 3.7).
    3. Otherwise, stream `adapter.read_bytes(meta.key)` through SHA-256 in
       1 MiB chunks, never materialising the full Object on disk (req 2.2,
       2.4, 7.2).

This module is import-safe before the `SourceAdapter` ABC lands in task 16:
the `adapter` and `meta` arguments are typed via small `Protocol`s that the
real implementations will satisfy structurally.

Validates: Requirements 2.2, 2.3, 2.4, 3.7, 7.1, 7.2.
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Iterator, Mapping, Optional, Protocol, runtime_checkable


CHUNK_SIZE = 1024 * 1024  # 1 MiB
"""Chunk size for streamed reads. 1 MiB matches the design.md sketch and is
large enough to keep per-chunk overhead negligible while small enough that
no single allocation dominates resident memory."""


# ---------------------------------------------------------------------------
# Structural typing for adapter and meta
# ---------------------------------------------------------------------------

@runtime_checkable
class _ObjectMetaLike(Protocol):
    """Subset of `mcps.sources.base.ObjectMeta` that this module touches.

    Defined as a Protocol so this module compiles before task 16 introduces
    the real `ObjectMeta`. The fields here are the strict superset required
    by `compute_content_hash`; any future ObjectMeta will satisfy this
    protocol structurally without further code changes.
    """

    key: str
    size_bytes: int
    last_modified: str
    user_metadata: Mapping[str, str]


@runtime_checkable
class _SourceAdapterLike(Protocol):
    """Subset of `mcps.sources.base.SourceAdapter` that this module touches.

    Only `name` (used for the Catalog cache key) and `read_bytes` (used for
    the streaming-hash fallback) are needed here.
    """

    name: str

    def read_bytes(self, key: str) -> Iterator[bytes]: ...


@runtime_checkable
class _CatalogLike(Protocol):
    """Subset of `mcps.catalog.model.Catalog` that this module touches.

    Only `cache_lookup` is needed; phrased as a Protocol so unit tests can
    pass a minimal fake without constructing a full Catalog.
    """

    def cache_lookup(
        self,
        source: str,
        key: str,
        size: int,
        last_modified: str,
    ) -> Optional[str]: ...


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def stream_sha256(chunks: Iterable[bytes]) -> str:
    """Return the lowercase-hex SHA-256 over the concatenation of `chunks`.

    The function never holds more than one chunk in memory at a time, which
    is the property the listing path relies on for arbitrarily large media
    objects (req 2.2, 2.4, 7.2). For an empty iterable the result is the
    well-known empty-input digest
    ``e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855``.
    """
    h = hashlib.sha256()
    for chunk in chunks:
        h.update(chunk)
    return h.hexdigest()


def s3_etag_is_singlepart(etag: Optional[str]) -> bool:
    """Return True iff `etag` looks like a single-part S3 upload ETag.

    S3 returns ETags surrounded by literal double quotes
    (``"d41d8cd98f00b204e9800998ecf8427e"``); we strip those before
    inspecting the body. A single-part upload has an ETag that is exactly
    32 lowercase hex characters with no `-N` suffix; the multipart form
    appends `-N` where `N` is the part count and is therefore explicitly
    excluded.

    The single-part ETag equals the MD5 of the bytes, but this function is
    *not* a content-hash decision. Its only role is the optimisation in
    `S3SourceAdapter` (task 17) that skips streaming SHA-256 when two S3
    ETags already prove the bytes differ. A return value of True means
    "this ETag is suitable for the optimisation", not "use this as
    Content_Hash" (req 2.2 forbids the latter).

    Returns False for None or empty input, for any value containing `-`,
    for any length other than 32, and for any character outside
    ``0-9a-f`` (uppercase hex is rejected to keep the round-trip with
    `Catalog_Printer`'s lowercase-only output).
    """
    if not etag:
        return False
    body = etag.strip()
    if body.startswith('"') and body.endswith('"') and len(body) >= 2:
        body = body[1:-1]
    if not body or "-" in body:
        return False
    if len(body) != 32:
        return False
    return all(c in "0123456789abcdef" for c in body)


def is_valid_content_hash(value: Optional[str]) -> bool:
    """Return True iff `value` is exactly 64 lowercase hex characters.

    This is the validation gate for the `mcps-content-sha256` user-metadata
    shortcut (req 7.1) and for any other place in the codebase that needs
    to assert the canonical Content_Hash shape. Uppercase hex is rejected
    on purpose: the Catalog_Printer emits lowercase, the Catalog_Parser
    rejects anything else, and accepting uppercase here would let a
    malformed metadata value slip past the listing path and corrupt the
    Catalog on the next round-trip.
    """
    if value is None:
        return False
    if len(value) != 64:
        return False
    return all(c in "0123456789abcdef" for c in value)


# ---------------------------------------------------------------------------
# The three-step priority chain
# ---------------------------------------------------------------------------

def compute_content_hash(
    adapter: _SourceAdapterLike,
    meta: _ObjectMetaLike,
    catalog: _CatalogLike,
) -> str:
    """Resolve the Content_Hash for `meta` via the design.md priority chain.

    Step 1 — trust `mcps-content-sha256` user-metadata. The Replicator (task
    23) writes this on every replicated Object (req 6.4); subsequent runs
    can trust the value without re-hashing the bytes (req 7.1). The value
    is validated as 64-char lowercase hex; a malformed value falls through
    to step 2 and the caller is expected to log a `hash-recomputed` entry
    against the Object (req 7.2). This function does not emit Manifest
    records — it is a pure value-returning helper invoked under the
    listing path.

    Step 2 — Catalog cache hit. If the Object's `(adapter.name, key,
    size_bytes, last_modified)` matches a prior `ObjectRecord`'s
    `(source, key, size_bytes, last_modified)`, the catalogued
    `content_hash` is still valid (req 3.7) and is returned without
    streaming any bytes.

    Step 3 — streamed SHA-256. With neither shortcut available, we open a
    streaming read via `adapter.read_bytes(meta.key)` and hash it through
    `stream_sha256`. This is the only branch that touches the network,
    which is why steps 1 and 2 exist at all (req 7.2).

    The function is intentionally short on side-effects: it does not log,
    write to the Catalog, or produce a Manifest record. Composition with
    the Manifest is the caller's responsibility (the listing pipeline in
    task 30).
    """
    # Step 1: mcps-content-sha256 user-metadata shortcut.
    candidate = meta.user_metadata.get("mcps-content-sha256")
    if is_valid_content_hash(candidate):
        # ``candidate`` is guaranteed non-None and 64-char lowercase hex by
        # the validator; cast away the Optional for the type checker.
        assert candidate is not None
        return candidate

    # Step 2: Catalog cache hit on (source, key, size, last_modified).
    cached = catalog.cache_lookup(
        adapter.name,
        meta.key,
        meta.size_bytes,
        meta.last_modified,
    )
    if cached is not None:
        return cached

    # Step 3: streamed SHA-256 fallback.
    return stream_sha256(adapter.read_bytes(meta.key))


__all__ = [
    "CHUNK_SIZE",
    "stream_sha256",
    "s3_etag_is_singlepart",
    "is_valid_content_hash",
    "compute_content_hash",
]
