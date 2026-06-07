# Feature: multicloud-photo-sync, Property 6: Replication eventual-consistency
"""Replication eventual-consistency property test.

Property under test (design.md, "Correctness Properties — Property 6:
Replication eventual-consistency"):

  For any pair of Replicated_Sources `(A, B)` with arbitrary
  `ObjectRecord` populations, after a successful Sync_Run with
  `delete_propagation=none` and no key conflicts, the set of
  Content_Hashes present in `A` equals the set of Content_Hashes
  present in `B`, modulo Content_Hashes whose copy operations recorded
  a `replication-error` Manifest entry.

The test:

1. Generates two non-overlapping Source populations seeded into a
   `Catalog` and a pair of `FakeSourceAdapter` instances.
2. Builds a `Replicator` with ``on_key_conflict="rename"`` (so the
   property's "no key conflicts" precondition holds — a different
   hash at the same key is moved aside under a suffixed key, which
   counts as a successful copy of that hash to the destination).
3. Computes ``plan`` for both ordered pairs ``(A, B)`` and ``(B, A)``
   and runs ``replicate`` against both plans against a single shared
   Manifest.
4. Asserts the Content_Hash sets of A and B agree, modulo any hash
   that recorded a ``REPLICATION_ERROR`` Manifest entry.

The property uses small pools (5 hashes, 3 keys per hash) so the
state space is tractable for 200 examples.

Validates: Requirements 6.1, 6.2, 6.6, 6.7, 7.4, 11.5.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.manifest.model import Action
from mcps.manifest.parser import parse_manifest_file
from mcps.manifest.writer import ManifestWriter
from mcps.replication import (
    MCPS_CONTENT_SHA256_KEY,
    MCPS_SOURCE_KEY,
    Replicator,
)
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# Small pool of 64-char lowercase hex hashes so cross-source overlap is
# common and the property exercises both "absent-on-dst" and "already-
# present-on-dst" branches.
_HASH_POOL: tuple[str, ...] = tuple(chr(ord("a") + i) * 64 for i in range(5))

# Small pool of keys so collisions on the destination side happen
# regularly. ``rename`` is the on-key-conflict policy (see module
# docstring) so collisions count as successful copies.
_KEY_POOL: tuple[str, ...] = ("k1", "k2", "k3")

_SOURCE_NAMES: tuple[str, str] = ("s3", "gcs")


@st.composite
def _source_population(draw, source_name: str) -> dict[str, str]:
    """Draw a ``{key: content_hash}`` dict for one Source.

    Keys are drawn from `_KEY_POOL` without replacement; each key has
    one hash in this Source. The result is the canonical view of the
    Source's content as the Replicator's `plan()` step sees it.
    """
    n = draw(st.integers(min_value=0, max_value=len(_KEY_POOL)))
    keys = draw(
        st.lists(
            st.sampled_from(_KEY_POOL),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    population: dict[str, str] = {}
    for key in keys:
        population[key] = draw(st.sampled_from(_HASH_POOL))
    return population


@st.composite
def _both_populations(draw) -> tuple[dict[str, str], dict[str, str]]:
    a = draw(_source_population("s3"))
    b = draw(_source_population("gcs"))
    return a, b


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build(
    population: dict[str, str], *, source_name: str, kind: str
) -> tuple[FakeSourceAdapter, list[ObjectRecord]]:
    """Build a FakeSourceAdapter + ObjectRecord list seeded with
    ``population``.

    Each entry in ``population`` is materialised both in the adapter's
    ``records`` (the bytes are a deterministic short payload encoding
    the hash so post-write verification round-trips) and as an
    ``ObjectRecord`` in the returned list.

    The ``user_metadata`` for each key carries
    ``mcps-content-sha256=<hash>`` and ``mcps-source=<source_name>``
    so the loop guard does not fire on records that originated in the
    *current* Source — only on records that the Replicator would have
    just written from the peer.
    """
    records_dict: dict[str, bytes] = {}
    metadata: dict[str, dict[str, str]] = {}
    obj_records: list[ObjectRecord] = []
    for key, content_hash in population.items():
        # Use a fixed-length payload that is unique per (key, hash) so
        # the destination's post-write verification compares against
        # the size we recorded in the ObjectRecord.
        payload = f"payload:{source_name}:{key}:{content_hash[:8]}".encode("utf-8")
        records_dict[key] = payload
        metadata[key] = {
            MCPS_SOURCE_KEY: source_name,
            MCPS_CONTENT_SHA256_KEY: content_hash,
        }
        obj_records.append(
            ObjectRecord(
                source=source_name,
                key=key,
                content_hash=content_hash,
                size_bytes=len(payload),
                last_seen_at="2024-01-01T00:00:00Z",
                last_modified="2023-12-31T23:59:59Z",
                content_type=None,
                mcps_source_meta=source_name,
            )
        )
    adapter = FakeSourceAdapter(
        name=source_name,
        kind=kind,
        records=records_dict,
        metadata=metadata,
    )
    return adapter, obj_records


def _hashes_in(adapter: FakeSourceAdapter) -> frozenset[str]:
    """Return every Content_Hash currently present in the adapter.

    Reads the value from the adapter's ``user_metadata`` because that
    is what the Replicator wrote (and what the post-write verification
    compared). Falls back to skipping records whose metadata was lost.
    """
    hashes: set[str] = set()
    for key in adapter.records:
        meta = adapter.user_metadata.get(key, {})
        h = meta.get(MCPS_CONTENT_SHA256_KEY)
        if h:
            hashes.add(h)
    return frozenset(hashes)


def _replication_error_hashes(manifest_path) -> frozenset[str]:
    """Return the set of Content_Hashes that recorded a REPLICATION_ERROR."""
    records, errors = parse_manifest_file(str(manifest_path))
    assert errors == []
    err_hashes: set[str] = set()
    for r in records:
        if r.action != Action.REPLICATION_ERROR:
            continue
        if r.content_hash:
            err_hashes.add(r.content_hash)
    return frozenset(err_hashes)


# ---------------------------------------------------------------------------
# The Property 6 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(populations=_both_populations())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_replication_eventual_consistency(
    populations: tuple[dict[str, str], dict[str, str]],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """After running the Replicator both ways between two
    Replicated_Sources, the Content_Hash sets agree modulo replication
    errors.

    Validates: Requirements 6.1, 6.2, 6.6, 6.7, 7.4, 11.5.
    """
    pop_a, pop_b = populations

    a_adapter, a_records = _build(pop_a, source_name="s3", kind="s3")
    b_adapter, b_records = _build(pop_b, source_name="gcs", kind="gcs")

    catalog = Catalog()
    for r in a_records + b_records:
        catalog = catalog.upsert(r)

    adapters = {"s3": a_adapter, "gcs": b_adapter}

    rep = Replicator(
        adapters=adapters,
        on_key_conflict="rename",  # keep "no conflicts" precondition
        now=lambda: _FIXED_NOW,
        run_id="property6",
    )

    manifest_dir = tmp_path_factory.mktemp("manifest")
    manifest_path = manifest_dir / "manifest.jsonl"

    # Plan and run both directions against the same Manifest. The
    # design's eventual-consistency property is over the union of
    # ordered pairs, not each direction in isolation.
    with ManifestWriter(str(manifest_path)) as mw:
        plan = rep.plan(catalog, replicated_source_names=_SOURCE_NAMES)
        rep.replicate(plan, manifest_writer=mw)

    err_hashes = _replication_error_hashes(manifest_path)
    a_hashes = _hashes_in(a_adapter)
    b_hashes = _hashes_in(b_adapter)

    # Every hash in A that is NOT in the error set must appear in B,
    # and vice versa (req 6.6 modulo replication errors).
    assert (a_hashes - err_hashes) <= b_hashes, (
        f"A has hashes missing from B: "
        f"{sorted((a_hashes - err_hashes) - b_hashes)!r}"
    )
    assert (b_hashes - err_hashes) <= a_hashes, (
        f"B has hashes missing from A: "
        f"{sorted((b_hashes - err_hashes) - a_hashes)!r}"
    )
