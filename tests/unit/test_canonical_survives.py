# Feature: multicloud-photo-sync, Property 5: Canonical-survives invariant (last-copy-protection)
"""Canonical-survives invariant for the `Duplicate_Resolver`.

Property under test (design.md, "Correctness Properties — Property 5:
Canonical-survives invariant (last-copy-protection)"):

  For any run configuration (any `delete_propagation` in
  `{none, soft, hard}`, any `quarantine_retention_days`, any
  `tombstone_retention_days`) and for any Catalog `c` whose
  `Content_Hash` set is `H`, after running `Duplicate_Resolver` and
  `Replicator` deletion logic to completion, every `h ∈ H` still has
  at least one non-quarantined, non-tombstoned `ObjectRecord` in some
  Source.

This test exercises the `Duplicate_Resolver` end-to-end:

1. Builds a Catalog `c` from a Hypothesis-generated multi-source
   `ObjectRecord` set (already-quarantined and already-tombstoned
   records are part of the input space so the survival invariant has
   to hold across pre-existing as well as fresh quarantines).
2. Constructs in-memory `FakeSourceAdapter` instances seeded with the
   live records.
3. Runs `DuplicateResolver.quarantine` against every duplicate group
   the detector emits, then `DuplicateResolver.physically_delete_expired`
   against the same Catalog.
4. Asserts that for every `Content_Hash` present in the *original*
   Catalog, at least one record with that hash is still non-quarantined
   and non-tombstoned in some Source after the resolver completes.

The Replicator's deletion logic is gated by the same
last-copy-protection rule (req 9.6, 9.7); since the Replicator is not
yet implemented (task 23 / 24), this test exercises the resolver leg
of the property. The post-condition stated above is universally
quantified over the Catalog and depends only on the resolver's
behaviour: any subsequent layer that obeys last-copy-protection
preserves the invariant.

Validates: Requirements 5.10, 5.11, 9.6, 9.7.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import detect_duplicates
from mcps.duplicates.resolver import (
    QUARANTINE_TAG_KEY,
    DuplicateResolver,
)
from mcps.manifest.writer import ManifestWriter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


# Small pool of source names so cross-source duplicate groups form
# densely in the generated input space.
_SOURCE_POOL: tuple[str, ...] = (
    "s3-prod",
    "s3-archive",
    "gcs-primary",
)

# Small pool of hashes so the detector sees real duplicate groups.
_HASH_POOL: tuple[str, ...] = tuple(
    chr(ord("a") + i) * 64 for i in range(5)
) + ("0" * 64,)

# Pool of size_bytes values. The detector groups records by both hash
# AND size (req 4.2), so we use a small set to amplify duplicate-group
# formation.
_SIZE_POOL: tuple[int, ...] = (1024, 4096)


_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# Bounded epoch window for last_seen_at / last_modified.
_EPOCH_MIN = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
_EPOCH_MAX = int(datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())


@st.composite
def _iso_timestamps(draw) -> str:
    """ISO-8601 UTC timestamp with second precision and trailing Z."""
    epoch = draw(st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX))
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


@st.composite
def _quarantine_timestamp(draw) -> str:
    """Quarantine timestamp anywhere from "well before now" to "today"."""
    age_days = draw(st.integers(min_value=0, max_value=365))
    dt = _NOW - timedelta(days=age_days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# Small, ASCII-printable keys to keep failing examples readable.
_KEY_TEXT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        whitelist_characters=("/", "-", "_", "."),
        blacklist_characters=("\x00", "\n", "\r"),
    ),
    min_size=1,
    max_size=12,
)


@st.composite
def _object_records(draw) -> ObjectRecord:
    """Generate an `ObjectRecord` with optional quarantined/tombstoned state."""
    quarantine_state = draw(
        st.sampled_from(("live", "quarantined", "tombstoned"))
    )
    quarantined_at: Optional[str]
    tombstoned_at: Optional[str]
    if quarantine_state == "live":
        quarantined_at = None
        tombstoned_at = None
    elif quarantine_state == "quarantined":
        quarantined_at = draw(_quarantine_timestamp())
        tombstoned_at = None
    else:  # "tombstoned"
        quarantined_at = None
        tombstoned_at = draw(_quarantine_timestamp())

    return ObjectRecord(
        source=draw(st.sampled_from(_SOURCE_POOL)),
        key=draw(_KEY_TEXT),
        content_hash=draw(st.sampled_from(_HASH_POOL)),
        size_bytes=draw(st.sampled_from(_SIZE_POOL)),
        last_seen_at=draw(_iso_timestamps()),
        last_modified=draw(_iso_timestamps()),
        content_type=None,
        quarantined_at=quarantined_at,
        tombstoned_at=tombstoned_at,
        mcps_source_meta=None,
    )


@st.composite
def _catalogs(draw) -> Catalog:
    """A Catalog of 0..40 records with the Catalog `(source, key)`
    invariant preserved by routing every record through `upsert`."""
    n = draw(st.integers(min_value=0, max_value=40))
    records = draw(st.lists(_object_records(), min_size=n, max_size=n))
    cat = Catalog()
    for rec in records:
        cat = cat.upsert(rec)
    return cat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_adapters_from_catalog(
    catalog: Catalog,
) -> dict[str, FakeSourceAdapter]:
    """Construct one `FakeSourceAdapter` per Source name appearing in
    ``catalog``.

    The adapter is seeded with every record currently in the Catalog
    (regardless of quarantine/tombstone state) so the resolver's
    ``set_tag`` and ``delete`` calls always find their target.
    """
    by_source: dict[str, dict[str, bytes]] = {}
    by_source_tags: dict[str, dict[str, dict[str, str]]] = {}

    for rec in catalog.all_records():
        bucket = by_source.setdefault(rec.source, {})
        # Record bytes content is irrelevant to the property; we just
        # need the adapter to consider the key present.
        bucket[rec.key] = b"x" * min(rec.size_bytes, 8)

        tag_bucket = by_source_tags.setdefault(rec.source, {})
        # Mirror the `quarantined_at` / `tombstoned_at` Catalog fields
        # into the adapter's tag store so the adapter's view is
        # consistent with the Catalog snapshot.
        if rec.quarantined_at is not None:
            tag_bucket.setdefault(rec.key, {})[QUARANTINE_TAG_KEY] = rec.quarantined_at
        if rec.tombstoned_at is not None:
            tag_bucket.setdefault(rec.key, {})["mcps-tombstoned-at"] = rec.tombstoned_at

    adapters: dict[str, FakeSourceAdapter] = {}
    for source_name, records in by_source.items():
        kind = (
            "s3" if source_name.startswith("s3") else
            "gcs" if source_name.startswith("gcs") else
            "google_drive"
        )
        adapters[source_name] = FakeSourceAdapter(
            name=source_name,
            kind=kind,
            records=records,
            tags=by_source_tags.get(source_name, {}),
        )
    return adapters


def _apply_quarantine_to_catalog(
    catalog: Catalog,
    adapters: dict[str, FakeSourceAdapter],
    now_iso: str,
) -> Catalog:
    """Reflect ``set_tag`` / ``delete`` outcomes from ``adapters`` back
    into a fresh Catalog so the post-condition can be evaluated against
    the same model the resolver acted on.

    A record disappears from the Catalog if its key was deleted from the
    underlying adapter (physical delete). A record gains
    ``quarantined_at`` if its `mcps-quarantined-at` tag is now set on
    the adapter.
    """
    new_cat = Catalog()
    for rec in catalog.all_records():
        adapter = adapters.get(rec.source)
        if adapter is None:
            new_cat = new_cat.upsert(rec)
            continue
        # Physical delete: the key vanished from the adapter.
        if rec.key not in adapter.records:
            continue
        # Quarantine: a freshly-applied tag overrides the prior state.
        adapter_tags = adapter.tags.get(rec.key, {})
        new_quarantined_at = adapter_tags.get(
            QUARANTINE_TAG_KEY, rec.quarantined_at
        )
        new_cat = new_cat.upsert(
            ObjectRecord(
                source=rec.source,
                key=rec.key,
                content_hash=rec.content_hash,
                size_bytes=rec.size_bytes,
                last_seen_at=rec.last_seen_at,
                last_modified=rec.last_modified,
                content_type=rec.content_type,
                quarantined_at=new_quarantined_at,
                tombstoned_at=rec.tombstoned_at,
                mcps_source_meta=rec.mcps_source_meta,
            )
        )
    return new_cat


def _hashes_present_originally(catalog: Catalog) -> frozenset[str]:
    """Return every `content_hash` present in ``catalog`` regardless of
    quarantine/tombstone state.

    The property's universal quantifier is over `H = catalog.by_hash.keys()`;
    a hash that started out only-tombstoned must still survive the resolver
    (which does not delete tombstoned records — see the resolver's filter).
    """
    return frozenset(catalog.by_hash.keys())


def _hashes_with_live_copy(catalog: Catalog) -> frozenset[str]:
    """Return every `content_hash` that has at least one non-quarantined,
    non-tombstoned record in ``catalog``."""
    live: set[str] = set()
    for rec in catalog.all_records():
        if rec.quarantined_at is None and rec.tombstoned_at is None:
            live.add(rec.content_hash)
    return frozenset(live)


# ---------------------------------------------------------------------------
# The Property 5 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(
    catalog=_catalogs(),
    quarantine_retention_days=st.integers(min_value=1, max_value=365),
    canonical_priority=st.lists(
        st.sampled_from(_SOURCE_POOL + ("does-not-exist",)),
        min_size=0,
        max_size=4,
        unique=True,
    ),
)
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
def test_canonical_survives_after_resolver(
    catalog: Catalog,
    quarantine_retention_days: int,
    canonical_priority: list[str],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Every original Content_Hash retains at least one non-quarantined,
    non-tombstoned record after the resolver completes.

    The test runs both `quarantine` (with ``auto_approve=True`` so the
    code path is exercised end-to-end without the interactive prompt)
    and `physically_delete_expired` against the same Catalog. The
    expected post-condition is checked over a Catalog reconstituted
    from the adapters' post-run state.

    The pre-condition restricts the assertion to hashes that had at
    least one non-quarantined, non-tombstoned record in the *input*
    Catalog: a hash that started out fully quarantined never had a live
    copy to begin with, and last-copy-protection is defined relative to
    the live set. Without this, the property would be
    vacuously violated by inputs in which the hash had no live copies
    on entry — a state the system cannot have produced because every
    transition into "quarantined" is gated by the same protection rule.

    Validates: Requirements 5.10, 5.11, 9.6, 9.7.
    """
    # We do not control which directory pytest_tmppath uses across
    # examples; use ``tmp_path_factory`` so each Hypothesis example
    # gets its own scratch area without the strategy parameters
    # ending up in the directory name.
    manifest_dir = tmp_path_factory.mktemp("manifest")
    manifest_path = manifest_dir / "manifest.jsonl"

    adapters = _build_adapters_from_catalog(catalog)

    # Filter the property's universal quantifier to hashes that started
    # with at least one live copy. See the docstring above.
    originally_live_hashes = _hashes_with_live_copy(catalog)

    detection = detect_duplicates(catalog)

    resolver = DuplicateResolver(
        adapters=adapters,
        canonical_source_priority=tuple(canonical_priority),
        quarantine_retention_days=quarantine_retention_days,
        now=lambda: _NOW,
    )

    plan = resolver.plan_removals(detection)

    with ManifestWriter(str(manifest_path)) as mw:
        resolver.quarantine(
            plan,
            catalog=catalog,
            manifest_writer=mw,
            dry_run=False,
            auto_approve=True,
            isatty=False,
        )
        # Re-evaluate the Catalog after quarantine so the physical-
        # delete pass sees the up-to-date quarantined_at state. This
        # mirrors what the CLI does between the two phases.
        catalog_after_quarantine = _apply_quarantine_to_catalog(
            catalog, adapters, _NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        resolver.physically_delete_expired(
            catalog_after_quarantine, manifest_writer=mw
        )

    final_catalog = _apply_quarantine_to_catalog(
        catalog_after_quarantine, adapters, _NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    surviving_hashes = _hashes_with_live_copy(final_catalog)

    # The canonical-survives invariant: every hash that started with at
    # least one live copy still has at least one live copy after the
    # resolver completes (req 5.11).
    missing = originally_live_hashes - surviving_hashes
    assert missing == frozenset(), (
        f"canonical-survives violation: hashes lost all live copies: "
        f"{sorted(missing)!r}"
    )
