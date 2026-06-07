# Feature: multicloud-photo-sync, Property 9: Streamed SHA-256 identity over SourceAdapter
"""Streamed SHA-256 identity property test.

Property under test (design.md, "Correctness Properties — Property 9:
Streamed SHA-256 identity over SourceAdapter"):

  For any `SourceAdapter` populated with arbitrary ``{key: bytes}``
  Objects, listing the Source via ``list_objects`` and resolving each
  record's Content_Hash via ``compute_content_hash`` MUST yield, for
  every record, a value equal to ``sha256(bytes).hexdigest()``. The
  identity holds across both the ``mcps-content-sha256`` user-metadata
  shortcut path (req 7.1) and the streamed-hash fallback path (req 7.2,
  also reached when the shortcut is absent or malformed → the
  `hash-recomputed` Manifest entry is logged by the listing pipeline,
  which the test does not exercise; the underlying value identity is
  what this property guards).

The test:

1. Generates a ``{key: bytes}`` population. For each key, a per-key
   ``shortcut_kind`` decides whether the listing-side `ObjectMeta`
   carries a valid ``mcps-content-sha256`` user-metadata value
   (shortcut path), a *malformed* one (forces fallback), or no value
   at all (forces fallback).
2. Builds a `FakeSourceAdapter` with the bytes and the chosen
   user-metadata. Iterates the listing across multiple page sizes
   (1, 2, and "all") to exercise the paginated-listing path
   (req 2.5) — the adapter's own ``list_objects`` is one stream;
   the test slices that stream into pages to simulate the per-page
   iteration the real adapters would do.
3. For each record on each page, computes the Content_Hash via
   ``compute_content_hash`` against an empty Catalog (so the cache
   branch never fires, leaving only the shortcut and the streamed
   fallback as live branches), and asserts equality with
   ``sha256(bytes).hexdigest()``.
4. Asserts the result is identical regardless of page size — the
   listing pagination boundaries do not change record identity
   (req 2.5).

Because the test never engages the Catalog cache hit, every
non-shortcut record exercises the streamed-hash fallback. To prove
both branches participate, the test counts how many records were
served by each branch and asserts both observed counts match the
generator's ``shortcut_kind`` decisions.

Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2.
"""

from __future__ import annotations

import hashlib
from itertools import islice
from typing import Iterable, List, Tuple

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog
from mcps.hashing import compute_content_hash
from mcps.sources.base import ObjectMeta
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


# Three flavours of ``mcps-content-sha256`` user-metadata, one per record:
#
# * ``"valid"``    — the metadata carries the canonical 64-char lowercase
#   hex digest of the bytes. ``compute_content_hash`` returns it directly
#   (req 7.1, shortcut path).
# * ``"malformed"`` — the metadata carries an invalid string (e.g.
#   uppercase hex). The shortcut validator rejects it and the chain falls
#   through to the streamed fallback (req 7.2, the `hash-recomputed`
#   path).
# * ``"absent"``    — no ``mcps-content-sha256`` entry at all. Fallback
#   path (req 7.2).
_ShortcutKind = st.sampled_from(("valid", "malformed", "absent"))


# Bytes are drawn from a small pool so cross-record byte collisions are
# common, exercising the case where two distinct keys map to the same
# Content_Hash via different shortcut_kinds.
_BYTES_POOL: tuple[bytes, ...] = (
    b"",
    b"a",
    b"ab",
    b"abc",
    b"hello world",
    b"\x00",
    b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09",
    b"the quick brown fox jumps over the lazy dog",
    bytes(range(256)),
)

# Keys are short and printable so the property output is debuggable; the
# adapter sorts on iteration so insertion order does not matter.
_KEY_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        whitelist_characters=("/", "-", "_", "."),
    ),
    min_size=1,
    max_size=20,
)


@st.composite
def _populations(draw) -> List[Tuple[str, bytes, str]]:
    """Draw a list of (key, payload, shortcut_kind) triples.

    Keys are unique within a single example so the FakeSourceAdapter's
    dict-backed store is well-defined. ``shortcut_kind`` is per-record so
    a single example exercises both code paths.
    """
    n = draw(st.integers(min_value=0, max_value=8))
    keys = draw(
        st.lists(_KEY_TEXT, min_size=n, max_size=n, unique=True),
    )
    triples: List[Tuple[str, bytes, str]] = []
    for key in keys:
        payload = draw(st.sampled_from(_BYTES_POOL))
        kind = draw(_ShortcutKind)
        triples.append((key, payload, kind))
    return triples


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_adapter_and_expected(
    triples: List[Tuple[str, bytes, str]],
) -> Tuple[FakeSourceAdapter, dict[str, str], dict[str, str]]:
    """Build the FakeSourceAdapter, the expected-hash map, and the
    per-key shortcut classification.

    Returns:

    * ``adapter`` — a `FakeSourceAdapter` of kind ``"s3"`` seeded with
      every (key, payload) pair, plus a per-key user-metadata dict
      whose ``mcps-content-sha256`` entry depends on ``shortcut_kind``.
    * ``expected`` — ``{key: sha256(payload).hexdigest()}`` for every
      key, computed once with `hashlib.sha256` so the test never
      depends on the implementation under test.
    * ``shortcut_classification`` — ``{key: shortcut_kind}`` so the
      assertion phase can confirm both branches are exercised on every
      example.
    """
    records: dict[str, bytes] = {}
    metadata: dict[str, dict[str, str]] = {}
    expected: dict[str, str] = {}
    classification: dict[str, str] = {}

    for key, payload, kind in triples:
        records[key] = payload
        true_hash = hashlib.sha256(payload).hexdigest()
        expected[key] = true_hash
        classification[key] = kind

        per_key_meta: dict[str, str] = {}
        if kind == "valid":
            per_key_meta["mcps-content-sha256"] = true_hash
        elif kind == "malformed":
            # Use uppercase hex of the correct digest so the validator
            # rejects it (it requires lowercase hex per req 7.1) and
            # the chain falls through to the streamed fallback. The
            # value is not the same as the canonical hash — uppercase
            # hex is always rejected by `is_valid_content_hash`.
            per_key_meta["mcps-content-sha256"] = true_hash.upper()
        # ``absent`` -> leave the metadata dict empty.

        if per_key_meta:
            metadata[key] = per_key_meta

    adapter = FakeSourceAdapter(
        name="s3",
        kind="s3",
        records=records,
        metadata=metadata,
    )
    return adapter, expected, classification


def _paginate(stream: Iterable[ObjectMeta], page_size: int) -> List[List[ObjectMeta]]:
    """Slice ``stream`` into pages of ``page_size`` entries each.

    A ``page_size`` of 0 is treated as "all in one page" so the caller
    can use it to mean "no pagination" without a separate code path.
    The list-of-lists return preserves enough structure for the test to
    assert that pagination does not change record identity.
    """
    if page_size <= 0:
        return [list(stream)]

    pages: List[List[ObjectMeta]] = []
    iterator = iter(stream)
    while True:
        page = list(islice(iterator, page_size))
        if not page:
            break
        pages.append(page)
    return pages


# ---------------------------------------------------------------------------
# The Property 9 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(triples=_populations())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_streamed_sha256_identity_over_source_adapter(
    triples: List[Tuple[str, bytes, str]],
) -> None:
    """Per-record Content_Hash equals ``sha256(bytes).hexdigest()`` across
    every page size, regardless of which hash-priority branch served the
    record.

    Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2.
    """
    adapter, expected, classification = _build_adapter_and_expected(triples)

    # An empty Catalog so `compute_content_hash` never reaches the cache
    # branch; only the shortcut (req 7.1) and the streamed fallback
    # (req 7.2) are live. This is the precise reduction the property
    # statement is about.
    catalog = Catalog()

    # Iterate over multiple page sizes so the property's "across
    # paginated listings" clause is exercised. ``0`` means "all in one
    # page"; ``1`` and ``2`` exercise small-page boundaries the way a
    # provider would surface them via continuation tokens.
    page_sizes = (0, 1, 2)
    per_page_size_results: dict[int, dict[str, str]] = {}

    for page_size in page_sizes:
        # ``adapter.list_objects()`` is a fresh iterator on every call;
        # the FakeSourceAdapter does not cache pagination state across
        # calls, mirroring the real adapters that re-issue list calls
        # for each Sync_Run.
        pages = _paginate(adapter.list_objects(), page_size)
        observed: dict[str, str] = {}
        for page in pages:
            for meta in page:
                content_hash = compute_content_hash(adapter, meta, catalog)
                # Per-record assertion: identity with hashlib.sha256.
                assert content_hash == expected[meta.key], (
                    f"page_size={page_size!r}: content_hash mismatch for "
                    f"key={meta.key!r}, kind={classification[meta.key]!r}: "
                    f"expected={expected[meta.key]!r}, got={content_hash!r}"
                )
                observed[meta.key] = content_hash
        per_page_size_results[page_size] = observed

    # Cross-page-size invariant: the per-key result is the same
    # regardless of how the listing was paginated. This is a direct
    # restatement of req 2.5 — pagination MUST NOT alter the resulting
    # `Object_Records`.
    reference = per_page_size_results[page_sizes[0]]
    for page_size in page_sizes[1:]:
        assert per_page_size_results[page_size] == reference, (
            f"pagination at page_size={page_size!r} produced a different "
            f"result map than page_size={page_sizes[0]!r}: "
            f"reference={reference!r}, observed={per_page_size_results[page_size]!r}"
        )

    # Every key was observed exactly once on the "all in one page" run.
    assert set(reference.keys()) == set(expected.keys()), (
        f"adapter.list_objects() did not yield every seeded key: "
        f"expected={sorted(expected.keys())!r}, got={sorted(reference.keys())!r}"
    )

    # Branch-coverage assertion: the test deliberately spans both
    # branches the property reasons about. We do NOT require both
    # branches to fire on every example (Hypothesis may shrink to a
    # population that uses only one), but we DO require that the
    # branches align with the generator's per-key classification —
    # ``valid`` keys take the shortcut, ``malformed``/``absent`` keys
    # take the fallback, and the resulting hash equals
    # ``sha256(bytes)`` in either case. The per-record assertion above
    # already establishes the equality; this block proves that the
    # `read_bytes` call surface lines up with the generator's intent
    # so future regressions can't silently change which branch is
    # exercised.
    fallback_keys = {
        k for k, kind in classification.items() if kind != "valid"
    }
    shortcut_keys = {
        k for k, kind in classification.items() if kind == "valid"
    }

    # Only count `read_bytes` calls from the final pass (the "all in
    # one page" run is replayed last, but call_log is cumulative across
    # every list_objects iteration; instead we check membership). The
    # FakeSourceAdapter records a call per `read_bytes` invocation; the
    # listing path never calls `read_bytes` for shortcut-served
    # records, so any shortcut key appearing in the read-call log is a
    # bug.
    read_bytes_keys = {
        kwargs["key"]
        for method, kwargs in adapter.call_log
        if method == "read_bytes"
    }
    assert read_bytes_keys.isdisjoint(shortcut_keys), (
        f"shortcut-eligible keys were unexpectedly streamed: "
        f"{sorted(read_bytes_keys & shortcut_keys)!r}"
    )
    # Every fallback key was streamed at least once across the three
    # iterations.
    missing_fallback = fallback_keys - read_bytes_keys
    assert not missing_fallback, (
        f"fallback-required keys were not streamed: "
        f"{sorted(missing_fallback)!r}"
    )
