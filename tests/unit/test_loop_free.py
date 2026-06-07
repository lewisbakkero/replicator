# Feature: multicloud-photo-sync, Property 8: Loop-free behaviour
"""Loop-free replication property test.

Property under test (design.md, "Correctness Properties — Property 8:
Loop-free behaviour"):

  For any `ObjectRecord` `r` whose `mcps-source` user-metadata equals
  the target Replicated_Source name, the Replicator performs no write
  to that target for `r` and emits exactly one `loop-skip` Manifest
  entry; for any `r` with missing `mcps-source`, the Replicator either
  skips it (if the destination has it) or writes it with `mcps-source`
  set to its originating Source name.

The test:

1. Generates a population of records on a single source ``A``,
   each tagged with one of three states for the live ``mcps-source``
   user-metadata: equal to the destination name (``loop`` records),
   equal to the source name itself (``self`` records), or missing
   entirely (``untagged`` records).
2. Runs the Replicator from A to B (B starts empty).
3. Asserts:
   * Every ``loop`` record produces exactly one ``LOOP_SKIP`` Manifest
     entry and no ``write_bytes`` call against B for its key.
   * Every ``untagged`` record produces a ``SOURCE_TAGGED`` Manifest
     entry **and** a ``write_bytes`` call to B that carries
     ``mcps-source = A``.
   * No record produces both a ``LOOP_SKIP`` and a ``REPLICATE`` for
     the same key.

Validates: Requirements 7.1, 7.3, 7.4.
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

_SRC_NAME = "s3"
_DST_NAME = "gcs"

_HASH_POOL: tuple[str, ...] = tuple(chr(ord("a") + i) * 64 for i in range(5))

# We allow up to 6 records per example so the property exercises a
# variety of state mixtures while keeping each example fast.
_KEY_POOL: tuple[str, ...] = tuple(f"k{i}" for i in range(6))


# A record's loop state determines what the live mcps-source value will
# be on the source-side adapter.
_LoopState = st.sampled_from(("loop", "self", "untagged"))


@st.composite
def _record_specs(draw) -> list[tuple[str, str, str]]:
    """Generate a list of (key, content_hash, loop_state) triples.

    Keys are unique within an example; hashes are sampled from
    `_HASH_POOL` (collisions across keys are allowed and exercise the
    plan's "first canonical wins per hash" branch).
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
    triples: list[tuple[str, str, str]] = []
    for key in keys:
        content_hash = draw(st.sampled_from(_HASH_POOL))
        loop_state = draw(_LoopState)
        triples.append((key, content_hash, loop_state))
    return triples


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _live_mcps_source_for(state: str) -> Optional[str]:
    """Translate a loop-state token into the live ``mcps-source`` value
    on the source-side adapter."""
    if state == "loop":
        return _DST_NAME
    if state == "self":
        return _SRC_NAME
    return None  # "untagged"


def _build_adapters_and_catalog(
    triples: list[tuple[str, str, str]],
) -> tuple[FakeSourceAdapter, FakeSourceAdapter, Catalog]:
    """Build the source/destination adapters and a Catalog from the
    generated triples.

    The destination adapter starts empty; the source adapter is seeded
    with one byte payload per (key, hash) pair so post-write
    verification can round-trip.
    """
    src_records: dict[str, bytes] = {}
    src_metadata: dict[str, dict[str, str]] = {}
    catalog = Catalog()
    for key, content_hash, state in triples:
        payload = f"payload:{key}:{content_hash[:8]}".encode("utf-8")
        src_records[key] = payload
        live_meta: dict[str, str] = {MCPS_CONTENT_SHA256_KEY: content_hash}
        live_value = _live_mcps_source_for(state)
        if live_value is not None:
            live_meta[MCPS_SOURCE_KEY] = live_value
        src_metadata[key] = live_meta
        # The Catalog's mcps_source_meta mirrors the live value so the
        # plan() step's loop check matches what the per-object pipeline
        # observes via get_metadata.
        catalog = catalog.upsert(
            ObjectRecord(
                source=_SRC_NAME,
                key=key,
                content_hash=content_hash,
                size_bytes=len(payload),
                last_seen_at="2024-01-01T00:00:00Z",
                last_modified="2023-12-31T23:59:59Z",
                content_type=None,
                mcps_source_meta=live_value,
            )
        )

    src_adapter = FakeSourceAdapter(
        name=_SRC_NAME, kind="s3", records=src_records, metadata=src_metadata
    )
    dst_adapter = FakeSourceAdapter(name=_DST_NAME, kind="gcs", records={})
    return src_adapter, dst_adapter, catalog


def _manifest_records(path):
    records, errors = parse_manifest_file(str(path))
    assert errors == []
    return records


# ---------------------------------------------------------------------------
# The Property 8 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(triples=_record_specs())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_loop_free_replication_behaviour(
    triples: list[tuple[str, str, str]],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Loop guard fires for ``loop`` records; ``untagged`` records get
    ``SOURCE_TAGGED`` and a ``mcps-source = src`` write.

    Validates: Requirements 7.1, 7.3, 7.4.
    """
    src_adapter, dst_adapter, catalog = _build_adapters_and_catalog(triples)
    adapters = {_SRC_NAME: src_adapter, _DST_NAME: dst_adapter}

    rep = Replicator(
        adapters=adapters,
        on_key_conflict="skip",
        now=lambda: _FIXED_NOW,
        run_id="property8",
    )

    manifest_dir = tmp_path_factory.mktemp("manifest")
    manifest_path = manifest_dir / "manifest.jsonl"

    with ManifestWriter(str(manifest_path)) as mw:
        plan = rep.plan(
            catalog, replicated_source_names=(_SRC_NAME, _DST_NAME)
        )
        rep.replicate(plan, manifest_writer=mw)

    records = _manifest_records(manifest_path)

    # Index Manifest entries by (action, key) so we can count without
    # re-walking the list per record.
    loop_skip_keys = [r.key for r in records if r.action == Action.LOOP_SKIP]
    source_tagged_keys = [
        r.key for r in records if r.action == Action.SOURCE_TAGGED
    ]
    replicate_keys = [r.key for r in records if r.action == Action.REPLICATE]

    # Index the destination adapter's write_bytes calls by key + the
    # mcps-source value they carried.
    write_calls = [c for c in dst_adapter.call_log if c[0] == "write_bytes"]
    write_keys = [c[1]["key"] for c in write_calls]
    write_metadata_by_key: dict[str, dict[str, str]] = {
        c[1]["key"]: dict(c[1]["user_metadata"]) for c in write_calls
    }

    # The plan picks one canonical record per (src, dst, hash). Two
    # records sharing a hash on src may map to a single plan entry. To
    # express the per-hash quantification precisely, we group inputs by
    # hash and reason about the *canonical chosen* record for each
    # hash.
    by_hash: dict[str, list[tuple[str, str, str]]] = {}
    for triple in triples:
        by_hash.setdefault(triple[1], []).append(triple)

    # Build the destination side from the perspective of the plan: for
    # every hash, the plan picks one canonical record; that record's
    # state determines the expected outcome.
    plan_pairs = {(p[2], p[3].key) for p in plan.pairs}  # (hash, src_key)

    expected_loop_skip_keys: set[str] = set()
    expected_source_tagged_keys: set[str] = set()
    expected_replicate_keys: set[str] = set()

    # Walk the plan's chosen records (the only ones the Replicator
    # actually processes).
    chosen_by_hash: dict[str, tuple[str, str, str]] = {}
    for content_hash, key in plan_pairs:
        # Find the original triple for this (hash, key).
        for triple in by_hash.get(content_hash, []):
            if triple[0] == key:
                chosen_by_hash[content_hash] = triple
                break

    for content_hash, (key, _, state) in chosen_by_hash.items():
        if state == "loop":
            expected_loop_skip_keys.add(key)
        elif state == "untagged":
            expected_source_tagged_keys.add(key)
            expected_replicate_keys.add(key)
        else:  # "self"
            expected_replicate_keys.add(key)

    # Property 8 (a): every "loop" record produces exactly one LOOP_SKIP
    # Manifest entry and zero writes to the destination.
    assert sorted(loop_skip_keys) == sorted(expected_loop_skip_keys), (
        f"LOOP_SKIP keys mismatch: expected={sorted(expected_loop_skip_keys)!r} "
        f"got={sorted(loop_skip_keys)!r}"
    )
    for key in expected_loop_skip_keys:
        assert (
            loop_skip_keys.count(key) == 1
        ), f"expected exactly one LOOP_SKIP for {key!r}, got {loop_skip_keys.count(key)}"
        assert key not in write_keys, (
            f"key {key!r} was written to dst despite being a loop record"
        )

    # Property 8 (b): every "untagged" record produces a SOURCE_TAGGED
    # entry and a write whose user_metadata contains mcps-source = src.
    assert sorted(source_tagged_keys) == sorted(expected_source_tagged_keys), (
        f"SOURCE_TAGGED keys mismatch: "
        f"expected={sorted(expected_source_tagged_keys)!r} "
        f"got={sorted(source_tagged_keys)!r}"
    )
    for key in expected_source_tagged_keys:
        assert key in write_keys, (
            f"key {key!r} was source-tagged but no write_bytes was issued"
        )
        attached = write_metadata_by_key.get(key, {})
        assert attached.get(MCPS_SOURCE_KEY) == _SRC_NAME, (
            f"write for {key!r} did not carry mcps-source={_SRC_NAME!r}: {attached!r}"
        )

    # Sanity: REPLICATE keys are exactly the union of self + untagged
    # plan picks (any record that was actually written).
    assert sorted(replicate_keys) == sorted(expected_replicate_keys), (
        f"REPLICATE keys mismatch: expected={sorted(expected_replicate_keys)!r} "
        f"got={sorted(replicate_keys)!r}"
    )

    # No key appears in both LOOP_SKIP and REPLICATE.
    assert set(loop_skip_keys).isdisjoint(replicate_keys)
