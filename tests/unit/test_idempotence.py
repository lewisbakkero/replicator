# Feature: multicloud-photo-sync, Property 7: Idempotence — second run is a no-op
"""Idempotence property test.

Property under test (design.md, "Correctness Properties — Property 7:
Idempotence — second run is a no-op"):

  Given an unchanged set of Replicated_Sources and an unchanged set of
  Objects, after the first Sync_Run completes, every subsequent Sync_Run
  performs zero `write_bytes`, `delete`, and `set_tag` calls against any
  source adapter, and produces a Manifest containing exactly one SUMMARY
  entry and zero non-DISCOVERED actions (DISCOVERED entries describe
  per-Object listing decisions and are therefore allowed on every run).

The test:

1. Builds two `FakeSourceAdapter` instances seeded with disjoint key
   spaces (each Source's keys are prefixed with its own name) and
   arbitrary byte payloads drawn by Hypothesis.
2. Runs a minimal end-to-end pipeline (`_run_pipeline`) over the two
   adapters with an initially-empty Catalog. The pipeline lists each
   Source, computes Content_Hashes via `compute_content_hash`, builds an
   `ObjectRecord` per listed Object (writing one DISCOVERED Manifest
   entry per Object), executes the `Replicator`, refreshes the Catalog
   from the post-replicate adapter state, and emits the per-run SUMMARY
   record. The pipeline is deliberately small but covers every Manifest
   action surface the property reasons about (DISCOVERED, REPLICATE /
   REPLICATE_SKIP / KEY_CONFLICT / RENAME / SOURCE_TAGGED / etc.,
   SUMMARY).
3. Snapshots the post-run-1 Catalog and resets each adapter's
   `call_log`.
4. Re-runs the same pipeline with the post-run-1 Catalog and the
   adapters in their post-run-1 state. Because run 1 has already
   converged the Content_Hash sets across both Sources (the test uses
   ``on_key_conflict="rename"`` so any incidental collisions resolve
   into a separate destination key rather than blocking convergence),
   the per-pair Replication Plan is empty and the second run performs
   no mutating work.
5. Asserts the property: zero `write_bytes`, `delete`, `set_tag` calls
   on the second run; exactly one SUMMARY entry in the second-run
   Manifest; and zero non-DISCOVERED, non-SUMMARY entries in that
   Manifest.

The test exercises both the listing path (req 2.1) and the loop /
no-op path (req 7.5) — by construction, the second run lists every
Object via `compute_content_hash`'s shortcut/cache branches (req 7.1,
7.2) and every plan diff is empty (req 11.1, 11.2).

Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2, 7.5,
11.1, 11.2.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Mapping, Optional, Tuple

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.hashing import compute_content_hash
from mcps.manifest.model import Action, ManifestRecord, Result
from mcps.manifest.parser import parse_manifest_file
from mcps.manifest.writer import ManifestWriter
from mcps.replication import Replicator
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Fixed clock + Source identifiers
# ---------------------------------------------------------------------------

# All Manifest timestamps stamped during the test use this fixed clock so
# the property remains deterministic across re-runs and so the Catalog's
# ``last_seen_at`` field is identical between run 1 and run 2 (the
# ``last_modified`` and cache-lookup fields are sourced from the
# adapter, not the clock, and are stable for the same reasons).
_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_NOW_ISO_SECONDS = "2024-06-01T12:00:00Z"
_NOW_ISO_MS = "2024-06-01T12:00:00.000Z"

_SRC_A = "s3"
_SRC_B = "gcs"
_REPLICATED_SOURCES: tuple[str, str] = (_SRC_A, _SRC_B)


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Bytes are drawn from a tiny pool so cross-Source byte collisions (and
# therefore Content_Hash collisions, which exercise the
# REPLICATE_SKIP_EXISTING branch on run 1) happen routinely.
_BYTE_PAYLOAD_POOL: tuple[bytes, ...] = (
    b"",
    b"a",
    b"alpha",
    b"beta",
    b"gamma",
    b"the quick brown fox",
    b"\x00\x01\x02\x03",
)


@st.composite
def _per_source_population(draw, source_name: str) -> dict[str, bytes]:
    """Draw a ``{key: bytes}`` mapping for one Source.

    Keys are namespaced with the Source name (``"s3/<id>"`` or
    ``"gcs/<id>"``) so the two Sources never share a key. This keeps the
    property's "no key conflicts on convergence" precondition trivially
    true: Replication writes to the peer Source land on a key that does
    not collide with any pre-existing key, so the post-run-1 hash sets
    of the two Sources are equal and the second run's plan is empty.
    """
    n = draw(st.integers(min_value=0, max_value=4))
    ids = draw(
        st.lists(
            st.integers(min_value=0, max_value=99),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    population: dict[str, bytes] = {}
    for key_id in ids:
        population[f"{source_name}/{key_id:02d}"] = draw(
            st.sampled_from(_BYTE_PAYLOAD_POOL)
        )
    return population


@st.composite
def _both_populations(
    draw,
) -> Tuple[dict[str, bytes], dict[str, bytes]]:
    pop_a = draw(_per_source_population(_SRC_A))
    pop_b = draw(_per_source_population(_SRC_B))
    return pop_a, pop_b


# ---------------------------------------------------------------------------
# Pipeline helper — minimal "Sync_Run" surface used by both runs
# ---------------------------------------------------------------------------


def _list_and_discover(
    *,
    adapter: FakeSourceAdapter,
    catalog: Catalog,
    manifest_writer: Optional[ManifestWriter],
    run_id: str,
) -> Catalog:
    """List ``adapter`` and upsert the resulting `ObjectRecord` set.

    For each listed `ObjectMeta`:

    * Resolve the Content_Hash via ``compute_content_hash`` (req 7.1,
      7.2, 3.7) — the shortcut / cache / streamed-fallback chain.
    * Build a fresh `ObjectRecord` and ``upsert`` it into the Catalog
      (req 11.5: at most one record per ``(source, key)``).
    * If a `manifest_writer` is supplied, append a DISCOVERED
      ManifestRecord per Object — these are listing/catalog updates and
      Property 7 explicitly excludes them from the "non-DISCOVERED
      action" prohibition.

    The function returns the new Catalog. ``manifest_writer=None`` is
    used by the post-replicate refresh to avoid emitting a second wave
    of DISCOVERED entries for the same Objects.
    """
    new_catalog = catalog
    for meta in adapter.list_objects():
        content_hash = compute_content_hash(adapter, meta, new_catalog)
        rec = ObjectRecord(
            source=adapter.name,
            key=meta.key,
            content_hash=content_hash,
            size_bytes=meta.size_bytes,
            last_seen_at=_NOW_ISO_SECONDS,
            last_modified=meta.last_modified,
            content_type=meta.content_type,
            mcps_source_meta=meta.user_metadata.get("mcps-source"),
        )
        new_catalog = new_catalog.upsert(rec)

        if manifest_writer is not None:
            manifest_writer.append(
                ManifestRecord(
                    timestamp=_NOW_ISO_MS,
                    run_id=run_id,
                    action=Action.DISCOVERED,
                    result=Result.SUCCESS,
                    source=adapter.name,
                    key=meta.key,
                    content_hash=content_hash,
                    size_bytes=meta.size_bytes,
                )
            )

    return new_catalog


def _run_pipeline(
    *,
    adapters: Mapping[str, FakeSourceAdapter],
    catalog: Catalog,
    manifest_writer: ManifestWriter,
    run_id: str,
) -> Catalog:
    """One end-to-end Sync_Run iteration.

    Sequence:

    1. **List + DISCOVER + upsert** every Source. The Catalog grows to
       cover everything visible at run-start time (req 2.1, 2.5).
    2. **Replicate** via the `Replicator`. Per-pair diffs are computed
       from the Catalog populated in step 1; canonical records are
       picked deterministically (req 5.1, 6.1, 6.2).
    3. **Refresh** the Catalog from the post-replicate adapter state.
       This step does NOT emit DISCOVERED entries — the Manifest entries
       written in step 1 already describe the run's listing decisions;
       this refresh exists only to keep the in-memory Catalog
       consistent with the post-replicate side effect of step 2 so the
       *next* run's plan is computed against an accurate Catalog.
    4. **SUMMARY** — append a single SUMMARY ManifestRecord. Property 7
       requires exactly one such entry per run.
    """
    # Step 1: list every source and upsert into the Catalog, emitting
    # one DISCOVERED entry per Object.
    current = catalog
    for name in _REPLICATED_SOURCES:
        current = _list_and_discover(
            adapter=adapters[name],
            catalog=current,
            manifest_writer=manifest_writer,
            run_id=run_id,
        )

    # Step 2: build a Replicator and run the per-pair plan.
    rep = Replicator(
        adapters=adapters,
        # ``rename`` keeps the convergence invariant when the two
        # populations happen to collide on a key (with our disjoint-key
        # generator they never do, but ``rename`` is the safer choice
        # for the property: it cannot block convergence).
        on_key_conflict="rename",
        run_id=run_id,
        now=lambda: _FIXED_NOW,
    )
    plan = rep.plan(current, replicated_source_names=_REPLICATED_SOURCES)
    rep.replicate(plan, manifest_writer=manifest_writer)

    # Step 3: post-replicate refresh — no DISCOVERED entries this time.
    for name in _REPLICATED_SOURCES:
        current = _list_and_discover(
            adapter=adapters[name],
            catalog=current,
            manifest_writer=None,
            run_id=run_id,
        )

    # Step 4: emit the SUMMARY entry. Exactly one per run.
    manifest_writer.append(
        ManifestRecord(
            timestamp=_NOW_ISO_MS,
            run_id=run_id,
            action=Action.SUMMARY,
            result=Result.SUCCESS,
        )
    )

    return current


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_adapters(
    pop_a: dict[str, bytes], pop_b: dict[str, bytes]
) -> dict[str, FakeSourceAdapter]:
    """Build the two `FakeSourceAdapter` instances seeded with the populations.

    Neither population carries any ``mcps-*`` metadata at construction
    time — the Replicator attaches ``mcps-content-sha256`` /
    ``mcps-source`` / ``mcps-replicated-at`` to records *it* writes
    during run 1, mirroring the real adapters. This means run 2's
    listing path exercises the full hash-priority chain: cached records
    on the originating side use the Catalog cache hit (req 3.7), and
    replicated records on the peer side use the
    ``mcps-content-sha256`` shortcut (req 7.1).
    """
    return {
        _SRC_A: FakeSourceAdapter(
            name=_SRC_A, kind="s3", records=dict(pop_a)
        ),
        _SRC_B: FakeSourceAdapter(
            name=_SRC_B, kind="gcs", records=dict(pop_b)
        ),
    }


def _mutating_call_count(adapter: FakeSourceAdapter) -> int:
    """Number of `write_bytes`, `delete`, or `set_tag` calls on ``adapter``.

    ``call_log`` records every public method call as
    ``(method_name, kwargs)``. The property under test forbids any of
    those three mutating calls on the second run.
    """
    return sum(
        1
        for method, _kwargs in adapter.call_log
        if method in ("write_bytes", "delete", "set_tag")
    )


# ---------------------------------------------------------------------------
# The Property 7 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(populations=_both_populations())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_idempotent_second_run_is_a_no_op(
    populations: Tuple[dict[str, bytes], dict[str, bytes]],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """A second Sync_Run with unchanged inputs performs no mutating work.

    Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2, 7.5,
    11.1, 11.2.
    """
    pop_a, pop_b = populations
    adapters = _build_adapters(pop_a, pop_b)

    # ------------------------------------------------------------------
    # Run 1 — bring the system to convergence.
    # ------------------------------------------------------------------
    manifest_dir = tmp_path_factory.mktemp("manifest")
    run1_path = manifest_dir / "run1.jsonl"
    with ManifestWriter(str(run1_path)) as mw1:
        catalog_after_run1 = _run_pipeline(
            adapters=adapters,
            catalog=Catalog(),
            manifest_writer=mw1,
            run_id="property7-run1",
        )

    # ------------------------------------------------------------------
    # Reset the per-adapter call_logs so run 2's call surface is
    # observable in isolation. The adapters retain their bytes /
    # user_metadata / tags state from run 1 — which is precisely the
    # post-run-1 state the property reasons about.
    # ------------------------------------------------------------------
    for adapter in adapters.values():
        adapter.call_log.clear()

    # ------------------------------------------------------------------
    # Run 2 — same inputs, post-run-1 Catalog. Should be a no-op.
    # ------------------------------------------------------------------
    run2_path = manifest_dir / "run2.jsonl"
    with ManifestWriter(str(run2_path)) as mw2:
        _run_pipeline(
            adapters=adapters,
            catalog=catalog_after_run1,
            manifest_writer=mw2,
            run_id="property7-run2",
        )

    # ------------------------------------------------------------------
    # Property 7 (a): zero mutating calls on either adapter.
    # ------------------------------------------------------------------
    a_mutations = _mutating_call_count(adapters[_SRC_A])
    b_mutations = _mutating_call_count(adapters[_SRC_B])
    assert a_mutations == 0, (
        f"second run made {a_mutations} mutating calls against {_SRC_A!r}: "
        f"{[c for c in adapters[_SRC_A].call_log if c[0] in ('write_bytes', 'delete', 'set_tag')]!r}"
    )
    assert b_mutations == 0, (
        f"second run made {b_mutations} mutating calls against {_SRC_B!r}: "
        f"{[c for c in adapters[_SRC_B].call_log if c[0] in ('write_bytes', 'delete', 'set_tag')]!r}"
    )

    # ------------------------------------------------------------------
    # Property 7 (b): exactly one SUMMARY and zero non-DISCOVERED,
    # non-SUMMARY actions in the second-run Manifest.
    # ------------------------------------------------------------------
    records, errors = parse_manifest_file(str(run2_path))
    assert errors == [], f"run-2 manifest had parse errors: {errors!r}"

    summary_count = sum(1 for r in records if r.action == Action.SUMMARY)
    non_discovered_count = sum(
        1
        for r in records
        if r.action not in (Action.DISCOVERED, Action.SUMMARY)
    )

    assert summary_count == 1, (
        f"expected exactly one SUMMARY in run-2 manifest, got {summary_count}; "
        f"actions present: {[r.action.value for r in records]!r}"
    )
    assert non_discovered_count == 0, (
        f"expected zero non-DISCOVERED actions in run-2 manifest, got "
        f"{non_discovered_count}; offending entries: "
        f"{[r for r in records if r.action not in (Action.DISCOVERED, Action.SUMMARY)]!r}"
    )
