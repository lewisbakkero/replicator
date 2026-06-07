"""`Replicator` — bidirectional replication between Replicated_Sources.

This module implements the per-pair Content_Hash diff, the replication
plan, and the per-object replication pipeline described in design.md
and Requirements 6, 7, and 8.

The high-level flow:

* ``Replicator.plan(catalog, replicated_source_names)`` walks every
  ordered pair ``(src, dst)`` of distinct Replicated_Source names, finds
  the Content_Hashes present in ``src`` but absent from ``dst`` (req 6.1),
  and picks one canonical `ObjectRecord` per such hash via the same
  deterministic tie-break used by the Duplicate_Resolver (req 5.1).
  The result is a frozen, deterministically-sorted `ReplicationPlan`.

* ``Replicator.replicate(plan, manifest_writer)`` processes each plan
  entry through `_replicate_one`. The pipeline implements the full
  per-object contract:

  1. **Loop check (req 7.3)** — if the source-side `mcps-source`
     user-metadata equals the destination's name, emit a single
     ``LOOP_SKIP`` Manifest entry and skip.
  2. **Source-tag check (req 7.4)** — if the source record's
     ``mcps-source`` is missing or empty, emit a ``SOURCE_TAGGED``
     entry recording that the copy will be tagged with the originating
     Source name.
  3. **Key-conflict policy (req 8.1-8.5)** — if the destination
     already has the key, branch on ``on_key_conflict``:
     * existing destination has the same Content_Hash → emit
       ``REPLICATE_SKIP`` and return (req 6.7).
     * different hash + ``skip`` → emit ``KEY_CONFLICT`` and return.
     * different hash + ``rename`` → write to ``key.<hash8>`` and emit
       ``RENAME``.
     * different hash + ``overwrite`` → if ``destructive_writes_allowed``
       is True write to the original key and emit ``OVERWRITE``;
       otherwise treat as ``skip`` and emit ``KEY_CONFLICT`` (the
       Cold_Start first-pass-confirmed safety guard, req 18.3).
  4. **Stream copy (req 6.3, 6.4)** — `read_bytes` from the source,
     `write_bytes` to the destination with ``mcps-source``,
     ``mcps-content-sha256``, ``mcps-replicated-at`` metadata.
  5. **Post-write verification (req 6.5)** — `get_metadata` on the
     destination and compare ``size_bytes`` and the round-tripped
     ``mcps-content-sha256``. On mismatch, ``delete`` the destination
     object and emit ``REPLICATION_ERROR``.

* ``ReplicationStats`` is a frozen counter struct returned by
  ``replicate(...)`` so the CLI can fold it into the SUMMARY record
  without re-walking the Manifest.

The Replicator is stateless across invocations — ``replicate`` does
not mutate ``self`` — and delegates every side effect to the supplied
adapters / manifest writer. This makes the property tests trivial:
they construct a Catalog, two FakeSourceAdapters, run the Replicator,
and assert on the adapter call logs and the Manifest contents.

Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.1, 7.2,
7.3, 7.4, 7.5, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping, Optional

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.errors import ReadOnlySourceError
from mcps.manifest.model import Action, ManifestRecord, Result
from mcps.manifest.writer import ManifestWriter
from mcps.sources.base import ObjectMeta, SourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


MCPS_SOURCE_KEY = "mcps-source"
MCPS_CONTENT_SHA256_KEY = "mcps-content-sha256"
MCPS_REPLICATED_AT_KEY = "mcps-replicated-at"
"""User-metadata keys attached by the Replicator at write time
(req 6.4, 7.4)."""

MCPS_TOMBSTONED_AT_KEY = "mcps-tombstoned-at"
"""Tag/user-metadata key used by ``propagate_deletions`` under
``delete_propagation=soft`` to mark a peer Object as tombstoned in
response to its sibling Content_Hash being absent from its originating
Replicated_Source this run (req 9.3)."""


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplicationPlan:
    """The frozen output of ``Replicator.plan``.

    ``pairs`` is a deterministically-sorted tuple of
    ``(src_name, dst_name, content_hash, canonical_record)`` quadruples,
    where ``canonical_record`` is the `ObjectRecord` chosen on the
    source side per the deterministic tie-break.

    The tuple is sorted by ``(src_name, dst_name, content_hash)`` so
    two runs over the same Catalog emit the same plan (req 4.4 for
    replication plans).
    """

    pairs: tuple[tuple[str, str, str, ObjectRecord], ...]


@dataclass(frozen=True)
class ReplicationStats:
    """Counter struct returned by ``Replicator.replicate``.

    Fields:

    * ``replicate`` — count of successful clean writes (no key
      collision on the destination).
    * ``replicate_skip_existing`` — count of records skipped because
      the destination already had the same content_hash at the same
      key (req 6.7).
    * ``loop_skip`` — count of records skipped by the loop guard
      (req 7.3).
    * ``key_conflict`` — count of records skipped because of a
      different-hash collision and ``on_key_conflict=skip`` (or the
      ``destructive_writes_allowed=False`` first-pass guard for
      ``overwrite``) (req 8.1).
    * ``overwrite`` — count of records written under
      ``on_key_conflict=overwrite`` (req 8.2).
    * ``rename`` — count of records written under
      ``on_key_conflict=rename`` to the suffixed key (req 8.3).
    * ``replication_error`` — count of records that failed post-write
      verification (req 6.5) or whose source bytes could not be read
      after retries (req 6.2).
    * ``source_tagged`` — count of records that had the originating
      Source's name written into ``mcps-source`` because it was
      missing on the source side (req 7.4).
    * ``tombstone`` — count of peer records successfully tagged with
      ``mcps-tombstoned-at`` under ``delete_propagation=soft``
      (req 9.3).
    * ``physical_delete`` — count of tombstoned records physically
      deleted under ``delete_propagation=hard`` once their
      ``tombstoned_at`` age exceeded ``tombstone_retention_days``
      (req 9.5).
    * ``last_copy_guard`` — count of tombstone- or physical-delete
      operations refused by last-copy-protection (req 9.6, 9.7). The
      guard runs unconditionally, so this counter can be non-zero
      even under ``delete_propagation=none`` if some external mechanism
      has already tombstoned/quarantined records.
    """

    replicate: int = 0
    replicate_skip_existing: int = 0
    loop_skip: int = 0
    key_conflict: int = 0
    overwrite: int = 0
    rename: int = 0
    replication_error: int = 0
    source_tagged: int = 0
    tombstone: int = 0
    physical_delete: int = 0
    last_copy_guard: int = 0


@dataclass
class _MutableStats:
    """Internal mutable counterpart of `ReplicationStats`.

    Kept private; ``Replicator.replicate`` returns a frozen
    `ReplicationStats` to its caller.
    """

    replicate: int = 0
    replicate_skip_existing: int = 0
    loop_skip: int = 0
    key_conflict: int = 0
    overwrite: int = 0
    rename: int = 0
    replication_error: int = 0
    source_tagged: int = 0
    tombstone: int = 0
    physical_delete: int = 0
    last_copy_guard: int = 0

    def freeze(self) -> ReplicationStats:
        return ReplicationStats(
            replicate=self.replicate,
            replicate_skip_existing=self.replicate_skip_existing,
            loop_skip=self.loop_skip,
            key_conflict=self.key_conflict,
            overwrite=self.overwrite,
            rename=self.rename,
            replication_error=self.replication_error,
            source_tagged=self.source_tagged,
            tombstone=self.tombstone,
            physical_delete=self.physical_delete,
            last_copy_guard=self.last_copy_guard,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso_seconds(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ``YYYY-MM-DDTHH:MM:SSZ`` (UTC, second precision)."""
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_iso_ms(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ISO-8601 millisecond UTC with trailing Z."""
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _parse_iso_seconds(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 string used for ``last_seen_at`` ordering.

    Returns ``None`` when the value cannot be parsed; callers treat
    unparseable timestamps as "later than any parseable value" so a
    malformed entry never silently wins the canonical tie-break.
    """
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _select_canonical(
    candidates: list[ObjectRecord],
    canonical_source_priority: tuple[str, ...],
) -> ObjectRecord:
    """Pick the canonical source record for a given content_hash.

    Mirrors the duplicate-resolver's tie-break (req 5.1):

    (a) the first Source listed in ``canonical_source_priority`` that
        owns at least one candidate;
    (b) earliest ``last_seen_at`` at second precision;
    (c) lexicographically smallest ``key`` byte order.
    """
    if not candidates:
        raise ValueError("no candidates supplied to _select_canonical")

    # Rule (a): if any priority Source matches, restrict to that subset.
    chosen_subset: list[ObjectRecord] = candidates
    for priority_name in canonical_source_priority:
        subset = [c for c in candidates if c.source == priority_name]
        if subset:
            chosen_subset = subset
            break

    def sort_key(rec: ObjectRecord) -> tuple[int, datetime, bytes]:
        parsed = _parse_iso_seconds(rec.last_seen_at)
        if parsed is None:
            return (
                1,
                datetime.max.replace(tzinfo=timezone.utc),
                rec.key.encode("utf-8"),
            )
        return (0, parsed, rec.key.encode("utf-8"))

    return min(chosen_subset, key=sort_key)


def _records_by_hash_for_source(
    catalog: Catalog, source_name: str
) -> dict[str, list[ObjectRecord]]:
    """Build ``content_hash -> [records]`` for a single Source.

    Skips records that are tombstoned or quarantined: those are not
    eligible to be replicated (the design treats them as not-live for
    the purposes of last-copy / replication decisions).
    """
    by_hash: dict[str, list[ObjectRecord]] = {}
    for rec in catalog.records_for_source(source_name):
        if rec.tombstoned_at is not None or rec.quarantined_at is not None:
            continue
        by_hash.setdefault(rec.content_hash, []).append(rec)
    return by_hash


# ---------------------------------------------------------------------------
# Replicator
# ---------------------------------------------------------------------------


class Replicator:
    """Per-Sync_Run replicator.

    Constructor parameters (all keyword-only):

    * ``adapters`` — mapping from Source name to `SourceAdapter`.
      ``replicate`` looks up the source and destination adapters by
      name; a missing entry results in a ``REPLICATION_ERROR`` Manifest
      entry for every plan row referring to that name.
    * ``canonical_source_priority`` — same priority tuple used by the
      Duplicate_Resolver for canonical picks (req 5.1).
    * ``on_key_conflict`` — ``"skip"``, ``"rename"`` or ``"overwrite"``
      (req 8.6 with the ``skip`` default).
    * ``fail_on_conflict`` — surfaced to the caller via
      ``ReplicationStats.key_conflict``; the CLI maps the run's exit
      code (req 8.4).
    * ``destructive_writes_allowed`` — when ``False``, the
      ``overwrite`` arm of ``on_key_conflict`` is forced to behave
      like ``skip``. Used by the Cold_Start first-pass guard (req
      18.3).
    * ``delete_propagation`` — ``"none"`` (default), ``"soft"``, or
      ``"hard"`` (req 9.1). Drives the behaviour of
      ``propagate_deletions`` and ``physically_delete_tombstoned``.
      Last-copy-protection (req 9.6, 9.7) runs unconditionally
      regardless of this setting.
    * ``tombstone_retention_days`` — minimum age (in days) of an
      ``mcps-tombstoned-at`` tag before its record is eligible for
      physical delete under ``delete_propagation=hard`` (req 9.4,
      9.5). Defaults to 30.
    * ``run_id`` — UUIDv4 hex shared by every Manifest entry the
      Replicator emits during this Sync_Run.
    * ``now`` — wall-clock callable returning a UTC `datetime`. The
      Replicator stamps Manifest timestamps and the
      ``mcps-replicated-at`` user-metadata via this clock so property
      tests remain deterministic.
    """

    def __init__(
        self,
        *,
        adapters: Mapping[str, SourceAdapter],
        canonical_source_priority: tuple[str, ...] = (),
        on_key_conflict: Literal["skip", "rename", "overwrite"] = "skip",
        fail_on_conflict: bool = False,
        destructive_writes_allowed: bool = True,
        delete_propagation: Literal["none", "soft", "hard"] = "none",
        tombstone_retention_days: int = 30,
        run_id: str = "00000000",
        now: Callable[[], datetime] = _default_now,
    ) -> None:
        if on_key_conflict not in ("skip", "rename", "overwrite"):
            raise ValueError(
                f"on_key_conflict must be one of skip/rename/overwrite, "
                f"got {on_key_conflict!r}"
            )
        if delete_propagation not in ("none", "soft", "hard"):
            raise ValueError(
                f"delete_propagation must be one of none/soft/hard, "
                f"got {delete_propagation!r}"
            )
        self._adapters: Mapping[str, SourceAdapter] = adapters
        self._canonical_source_priority = canonical_source_priority
        self._on_key_conflict = on_key_conflict
        self._fail_on_conflict = fail_on_conflict
        self._destructive_writes_allowed = destructive_writes_allowed
        self._delete_propagation = delete_propagation
        self._tombstone_retention_days = tombstone_retention_days
        self._run_id = run_id
        self._now = now

    # ------------------------------------------------------------------
    # Plan
    # ------------------------------------------------------------------

    def plan(
        self,
        catalog: Catalog,
        *,
        replicated_source_names: tuple[str, ...],
    ) -> ReplicationPlan:
        """Return a deterministic `ReplicationPlan` for ``catalog``.

        For each ordered pair ``(src, dst)`` of distinct names in
        ``replicated_source_names``:

        1. Compute the set of Content_Hashes present in ``src`` but
           absent from ``dst`` (req 6.1).
        2. For each such hash, pick the canonical record from the
           ``src``-side records using the canonical priority + earliest
           ``last_seen_at`` + lex smallest ``key`` tie-break.
        3. Append the ``(src, dst, hash, record)`` quadruple to the
           plan.

        The final tuple is sorted by ``(src, dst, hash)`` so the
        plan is permutation-invariant given the same Catalog
        (Property 4 — order-independence).
        """
        # Pre-compute per-source views once so the inner loop is cheap
        # for an N×N pair set.
        by_source_hashes: dict[str, dict[str, list[ObjectRecord]]] = {}
        by_source_hash_set: dict[str, frozenset[str]] = {}
        for name in replicated_source_names:
            recs = _records_by_hash_for_source(catalog, name)
            by_source_hashes[name] = recs
            by_source_hash_set[name] = frozenset(recs.keys())

        pairs: list[tuple[str, str, str, ObjectRecord]] = []
        for src in replicated_source_names:
            src_hashes = by_source_hash_set[src]
            for dst in replicated_source_names:
                if src == dst:
                    continue
                dst_hashes = by_source_hash_set[dst]
                missing = src_hashes - dst_hashes
                for content_hash in missing:
                    candidates = by_source_hashes[src].get(content_hash, [])
                    if not candidates:
                        continue
                    canonical = _select_canonical(
                        candidates, self._canonical_source_priority
                    )
                    pairs.append((src, dst, content_hash, canonical))

        pairs.sort(key=lambda p: (p[0], p[1], p[2]))
        return ReplicationPlan(pairs=tuple(pairs))

    # ------------------------------------------------------------------
    # Replicate
    # ------------------------------------------------------------------

    def replicate(
        self,
        plan: ReplicationPlan,
        *,
        manifest_writer: ManifestWriter,
    ) -> ReplicationStats:
        """Execute ``plan`` against the configured adapters.

        Each entry runs through `_replicate_one`. Errors inside a
        single per-object pipeline are caught, recorded as
        ``REPLICATION_ERROR`` Manifest entries, and the run continues
        (the design's "anything inside the per-object loop … continues"
        rule).
        """
        stats = _MutableStats()
        for src_name, dst_name, content_hash, record in plan.pairs:
            try:
                self._replicate_one(
                    src_name=src_name,
                    dst_name=dst_name,
                    content_hash=content_hash,
                    record=record,
                    manifest_writer=manifest_writer,
                    stats=stats,
                )
            except ReadOnlySourceError as exc:
                # Writing to a read-only adapter (e.g. Drive) — surface
                # as a per-record replication error and continue.
                self._emit_replication_error(
                    manifest_writer,
                    src=src_name,
                    dst=dst_name,
                    key=record.key,
                    expected=content_hash,
                    observed=None,
                    error=repr(exc),
                )
                stats.replication_error += 1
            except Exception as exc:  # noqa: BLE001 — provider-mapped
                self._emit_replication_error(
                    manifest_writer,
                    src=src_name,
                    dst=dst_name,
                    key=record.key,
                    expected=content_hash,
                    observed=None,
                    error=repr(exc),
                )
                stats.replication_error += 1

        return stats.freeze()

    # ------------------------------------------------------------------
    # Per-object pipeline
    # ------------------------------------------------------------------

    def _replicate_one(
        self,
        *,
        src_name: str,
        dst_name: str,
        content_hash: str,
        record: ObjectRecord,
        manifest_writer: ManifestWriter,
        stats: _MutableStats,
    ) -> None:
        """Implement the single-record replication pipeline.

        Sequence:

        1. Resolve adapters for ``src_name`` and ``dst_name``. A
           missing adapter is a per-record error.
        2. Loop check (req 7.3): if the source-side ``mcps-source``
           equals ``dst_name``, emit ``LOOP_SKIP`` and return.
        3. Source-tag check (req 7.4): if the source-side
           ``mcps-source`` is missing, emit ``SOURCE_TAGGED`` and
           continue (the eventual write attaches ``mcps-source =
           src_name``).
        4. Probe the destination via ``get_metadata`` to detect
           pre-existing keys.
        5. Branch:
           * destination absent → clean write via `_write_record`.
           * destination same hash → ``REPLICATE_SKIP``.
           * destination different hash → branch on ``on_key_conflict``:
             ``skip`` / ``rename`` / ``overwrite``.
        """
        src_adapter = self._adapters.get(src_name)
        dst_adapter = self._adapters.get(dst_name)
        if src_adapter is None:
            self._emit_replication_error(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                key=record.key,
                expected=content_hash,
                observed=None,
                error=f"no adapter configured for source {src_name!r}",
            )
            stats.replication_error += 1
            return
        if dst_adapter is None:
            self._emit_replication_error(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                key=record.key,
                expected=content_hash,
                observed=None,
                error=f"no adapter configured for destination {dst_name!r}",
            )
            stats.replication_error += 1
            return

        # Step 1: read the source-side metadata so we can apply the
        # loop guard against the *live* user_metadata, not just the
        # cached value on the ObjectRecord. The Catalog's
        # ``mcps_source_meta`` is the value at the previous run; the
        # adapter is the source of truth for *this* run (req 7.3).
        try:
            src_meta = src_adapter.get_metadata(record.key)
        except FileNotFoundError:
            # Source object disappeared between listing and replication
            # — record the failure and continue.
            self._emit_replication_error(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                key=record.key,
                expected=content_hash,
                observed=None,
                error="source object missing at replication time",
            )
            stats.replication_error += 1
            return

        live_mcps_source = src_meta.user_metadata.get(MCPS_SOURCE_KEY) or ""
        cached_mcps_source = record.mcps_source_meta or ""

        # Step 2: loop guard (req 7.3). The check is satisfied if
        # *either* the live metadata or the cached value flags this
        # record as originating at the destination — both signals are
        # treated as authoritative, but the live one wins on conflict.
        if (
            live_mcps_source == dst_name
            or (not live_mcps_source and cached_mcps_source == dst_name)
        ):
            self._emit_loop_skip(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                record=record,
            )
            stats.loop_skip += 1
            return

        # Step 3: source-tag check (req 7.4). Missing or empty
        # ``mcps-source`` on the source object means the eventual write
        # will set it to ``src_name``; emit a SOURCE_TAGGED Manifest
        # entry so the operator can audit the tagging.
        if not live_mcps_source and not cached_mcps_source:
            self._emit_source_tagged(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                record=record,
            )
            stats.source_tagged += 1

        # Step 4: probe the destination key.
        existing_meta: Optional[ObjectMeta]
        try:
            existing_meta = dst_adapter.get_metadata(record.key)
        except FileNotFoundError:
            existing_meta = None

        if existing_meta is None:
            # Step 5a: clean write (req 6.2-6.5).
            self._write_record(
                src_adapter=src_adapter,
                dst_adapter=dst_adapter,
                src_name=src_name,
                dst_name=dst_name,
                dst_key=record.key,
                record=record,
                content_hash=content_hash,
                action_on_success=Action.REPLICATE,
                manifest_writer=manifest_writer,
                stats=stats,
                stats_field="replicate",
            )
            return

        # Step 5b/c: destination already has the key.
        existing_hash = existing_meta.user_metadata.get(MCPS_CONTENT_SHA256_KEY)
        if existing_hash == content_hash:
            # Same content already at the same key (req 6.7).
            self._emit_replicate_skip(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                record=record,
                content_hash=content_hash,
            )
            stats.replicate_skip_existing += 1
            return

        # Different hash on the destination → conflict policy.
        policy = self._on_key_conflict
        if policy == "skip":
            self._emit_key_conflict(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                record=record,
                expected=content_hash,
                observed=existing_hash,
            )
            stats.key_conflict += 1
            return

        if policy == "rename":
            renamed_key = f"{record.key}.{content_hash[:8]}"
            # Probe the renamed key once. If something already exists
            # at the renamed key the design's recursion is one level:
            # treat the renamed key the same way the original was
            # treated — same hash → REPLICATE_SKIP, different hash →
            # KEY_CONFLICT (we do not rename twice).
            try:
                renamed_meta: Optional[ObjectMeta] = dst_adapter.get_metadata(
                    renamed_key
                )
            except FileNotFoundError:
                renamed_meta = None
            if renamed_meta is not None:
                renamed_hash = renamed_meta.user_metadata.get(
                    MCPS_CONTENT_SHA256_KEY
                )
                if renamed_hash == content_hash:
                    self._emit_replicate_skip(
                        manifest_writer,
                        src=src_name,
                        dst=dst_name,
                        record=record,
                        content_hash=content_hash,
                        renamed_key=renamed_key,
                    )
                    stats.replicate_skip_existing += 1
                    return
                self._emit_key_conflict(
                    manifest_writer,
                    src=src_name,
                    dst=dst_name,
                    record=record,
                    expected=content_hash,
                    observed=renamed_hash,
                    renamed_key=renamed_key,
                )
                stats.key_conflict += 1
                return
            # Renamed key absent → write under the suffixed key.
            self._write_record(
                src_adapter=src_adapter,
                dst_adapter=dst_adapter,
                src_name=src_name,
                dst_name=dst_name,
                dst_key=renamed_key,
                record=record,
                content_hash=content_hash,
                action_on_success=Action.RENAME,
                manifest_writer=manifest_writer,
                stats=stats,
                stats_field="rename",
                # The Manifest RENAME entry references both keys.
                rename_original_key=record.key,
            )
            return

        # policy == "overwrite"
        if not self._destructive_writes_allowed:
            # Cold_Start first-pass guard (req 18.3): treat overwrite
            # as skip and emit a KEY_CONFLICT entry.
            self._emit_key_conflict(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                record=record,
                expected=content_hash,
                observed=existing_hash,
            )
            stats.key_conflict += 1
            return

        self._write_record(
            src_adapter=src_adapter,
            dst_adapter=dst_adapter,
            src_name=src_name,
            dst_name=dst_name,
            dst_key=record.key,
            record=record,
            content_hash=content_hash,
            action_on_success=Action.OVERWRITE,
            manifest_writer=manifest_writer,
            stats=stats,
            stats_field="overwrite",
            previous_hash=existing_hash,
        )

    # ------------------------------------------------------------------
    # Stream copy + post-write verification
    # ------------------------------------------------------------------

    def _write_record(
        self,
        *,
        src_adapter: SourceAdapter,
        dst_adapter: SourceAdapter,
        src_name: str,
        dst_name: str,
        dst_key: str,
        record: ObjectRecord,
        content_hash: str,
        action_on_success: Action,
        manifest_writer: ManifestWriter,
        stats: _MutableStats,
        stats_field: str,
        rename_original_key: Optional[str] = None,
        previous_hash: Optional[str] = None,
    ) -> None:
        """Stream the source bytes to the destination and verify.

        On success emits a Manifest record with ``action_on_success`` and
        increments ``stats.<stats_field>`` by 1. On post-write
        verification mismatch deletes the destination object and emits
        ``REPLICATION_ERROR``.
        """
        mcps_metadata = {
            MCPS_SOURCE_KEY: src_name,
            MCPS_CONTENT_SHA256_KEY: content_hash,
            MCPS_REPLICATED_AT_KEY: _now_iso_seconds(self._now),
        }

        chunks = src_adapter.read_bytes(record.key)
        dst_adapter.write_bytes(
            dst_key,
            chunks,
            record.size_bytes,
            record.content_type,
            mcps_metadata,
        )

        # Post-write verification (req 6.5).
        try:
            dst_meta = dst_adapter.get_metadata(dst_key)
        except FileNotFoundError:
            # The write apparently succeeded but the object is missing
            # — treat as verification failure.
            self._emit_replication_error(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                key=dst_key,
                expected=content_hash,
                observed=None,
                error="post-write get_metadata returned not found",
            )
            stats.replication_error += 1
            return

        observed_hash = dst_meta.user_metadata.get(MCPS_CONTENT_SHA256_KEY)
        if (
            dst_meta.size_bytes != record.size_bytes
            or observed_hash != content_hash
        ):
            # Roll back the bad write before recording the error so
            # the destination never ends up with a partial object
            # (req 6.5).
            try:
                dst_adapter.delete(dst_key)
            except Exception:  # noqa: BLE001 — best-effort rollback
                # Even if the rollback fails we still surface the
                # original verification mismatch as the user-facing
                # error; rollback failure is logged separately by the
                # adapter itself.
                pass
            self._emit_replication_error(
                manifest_writer,
                src=src_name,
                dst=dst_name,
                key=dst_key,
                expected=content_hash,
                observed=observed_hash,
                error=(
                    f"post-write verify mismatch: "
                    f"size_bytes expected={record.size_bytes!r} "
                    f"observed={dst_meta.size_bytes!r}; "
                    f"content_hash expected={content_hash!r} "
                    f"observed={observed_hash!r}"
                ),
            )
            stats.replication_error += 1
            return

        # Successful write.
        extra: dict[str, str] = {}
        if rename_original_key is not None:
            extra["original_key"] = rename_original_key
            extra["renamed_key"] = dst_key
        if previous_hash is not None:
            extra["previous_content_hash"] = previous_hash or ""

        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=action_on_success,
                result=Result.SUCCESS,
                source=src_name,
                target=dst_name,
                key=dst_key,
                content_hash=content_hash,
                size_bytes=record.size_bytes,
                extra=extra,
            )
        )
        setattr(stats, stats_field, getattr(stats, stats_field) + 1)

    # ------------------------------------------------------------------
    # Manifest emission helpers
    # ------------------------------------------------------------------

    def _emit_loop_skip(
        self,
        manifest_writer: ManifestWriter,
        *,
        src: str,
        dst: str,
        record: ObjectRecord,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.LOOP_SKIP,
                result=Result.SKIPPED,
                source=src,
                target=dst,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
            )
        )

    def _emit_source_tagged(
        self,
        manifest_writer: ManifestWriter,
        *,
        src: str,
        dst: str,
        record: ObjectRecord,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.SOURCE_TAGGED,
                result=Result.SUCCESS,
                source=src,
                target=dst,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
                extra={"mcps_source": src},
            )
        )

    def _emit_replicate_skip(
        self,
        manifest_writer: ManifestWriter,
        *,
        src: str,
        dst: str,
        record: ObjectRecord,
        content_hash: str,
        renamed_key: Optional[str] = None,
    ) -> None:
        extra: dict[str, str] = {}
        if renamed_key is not None:
            extra["renamed_key"] = renamed_key
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.REPLICATE_SKIP,
                result=Result.SKIPPED,
                source=src,
                target=dst,
                key=record.key,
                content_hash=content_hash,
                size_bytes=record.size_bytes,
                extra=extra,
            )
        )

    def _emit_key_conflict(
        self,
        manifest_writer: ManifestWriter,
        *,
        src: str,
        dst: str,
        record: ObjectRecord,
        expected: str,
        observed: Optional[str],
        renamed_key: Optional[str] = None,
    ) -> None:
        extra: dict[str, str] = {
            "expected_content_hash": expected,
            "observed_content_hash": observed or "",
        }
        if renamed_key is not None:
            extra["renamed_key"] = renamed_key
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.KEY_CONFLICT,
                result=Result.SKIPPED,
                source=src,
                target=dst,
                key=record.key,
                content_hash=expected,
                size_bytes=record.size_bytes,
                extra=extra,
            )
        )

    def _emit_replication_error(
        self,
        manifest_writer: ManifestWriter,
        *,
        src: str,
        dst: str,
        key: str,
        expected: str,
        observed: Optional[str],
        error: str,
    ) -> None:
        extra: dict[str, str] = {
            "expected_content_hash": expected,
            "observed_content_hash": observed or "",
        }
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.REPLICATION_ERROR,
                result=Result.ERROR,
                source=src,
                target=dst,
                key=key,
                content_hash=expected,
                error=error,
                extra=extra,
            )
        )

    # ------------------------------------------------------------------
    # Deletion handling (req 9.1-9.7)
    # ------------------------------------------------------------------

    def propagate_deletions(
        self,
        catalog_at_start: Catalog,
        current_records: Mapping[str, list[ObjectRecord]],
        *,
        replicated_source_names: tuple[str, ...],
        reachable: set[str],
        manifest_writer: ManifestWriter,
    ) -> ReplicationStats:
        """Propagate per-Source deletions across Replicated_Sources.

        Behaviour by ``delete_propagation``:

        * ``"none"`` (req 9.2) — no tombstone propagation occurs. The
          method short-circuits and returns zero counters. Last-copy
          protection is still computed defensively but cannot reduce
          the live count because no tombstone is applied here.
        * ``"soft"`` (req 9.3) — for every record in
          ``catalog_at_start`` whose ``source`` is a Replicated_Source
          and whose ``(source, key)`` is NOT present in
          ``current_records[source]`` (i.e. it has gone missing this
          run), peer records sharing the same ``content_hash`` in
          *other* Replicated_Sources receive an ``mcps-tombstoned-at``
          tag, gated by last-copy-protection.
        * ``"hard"`` — same as ``soft`` for the propagation step;
          ``physically_delete_tombstoned`` is the separate phase that
          actually removes bytes once the tombstone is old enough.

        ``reachable`` is the set of Source names whose listing
        succeeded this Sync_Run. Records whose originating Source is
        absent from ``reachable`` are NEVER treated as missing — a
        failed listing must not be allowed to drop bytes
        (design.md "Treat unreachable Sources as 'not absent'").

        Last-copy-protection (req 9.6, 9.7): a tombstone is refused
        when it would reduce the count of non-tombstoned,
        non-quarantined records sharing the same ``content_hash``
        across all Replicated_Sources to zero. This guard runs
        unconditionally, including under ``delete_propagation=none``
        (defence in depth — even though no tombstones are applied
        under ``"none"``, the method's contract still emits zero
        side-effecting calls and is consistent with the rest of the
        run).

        Returns a frozen `ReplicationStats` with only the
        deletion-related counters populated; replication-side counters
        are left at zero so the caller can fold them into the run-wide
        stats.
        """
        stats = _MutableStats()

        # Under "none" we still walk the catalog but never tombstone:
        # the early return keeps the path cheap and makes the behaviour
        # explicit. LCP runs unconditionally as a safeguard, but
        # because we make no calls there is nothing to guard here.
        if self._delete_propagation == "none":
            return stats.freeze()

        replicated = set(replicated_source_names)
        # ``current_records`` is keyed by Source name. Build a per-Source
        # set of keys so the "is absent this run" check is O(1).
        current_keys_by_source: dict[str, set[str]] = {
            name: {r.key for r in current_records.get(name, [])}
            for name in replicated_source_names
        }

        # Initial live counts — non-tombstoned, non-quarantined records
        # in any Replicated_Source per the catalog snapshot. The
        # snapshot is "ground truth" for survival decisions because
        # the in-memory catalog is not yet rewritten with this run's
        # listing observations and the Replicator is the entity making
        # the change. Decrementing the count after each successful
        # tombstone/delete keeps LCP correct across the whole batch
        # (req 9.6, 9.7; defence in depth even under
        # ``delete_propagation=none``, though that branch returns
        # early before any work is done).
        live_counts = self._compute_live_counts(catalog_at_start, replicated)

        # Records we've already tombstoned in this batch — used to
        # filter peer candidates so a record never receives two
        # tombstone tags during one propagation phase.
        tombstoned_this_run: set[tuple[str, str]] = set()

        now_iso_tag = _now_iso_seconds(self._now)

        # Collect candidates deterministically so the Manifest output
        # is stable across runs.
        candidates: list[ObjectRecord] = sorted(
            (
                rec
                for rec in catalog_at_start.all_records()
                if rec.source in replicated
                and rec.tombstoned_at is None
                and rec.quarantined_at is None
                and rec.source in reachable
                and rec.key not in current_keys_by_source.get(rec.source, set())
            ),
            key=lambda r: (r.source, r.key, r.content_hash),
        )

        for missing in candidates:
            # Identify peer records (same content_hash) on *other*
            # Replicated_Sources that are still live (non-tombstoned,
            # non-quarantined) per the catalog snapshot AND have not
            # already been tombstoned earlier in this same batch.
            # These are the records that would receive the tombstone
            # tag.
            peers = sorted(
                (
                    peer
                    for peer in catalog_at_start.all_records()
                    if peer.content_hash == missing.content_hash
                    and peer.source in replicated
                    and peer.source != missing.source
                    and peer.tombstoned_at is None
                    and peer.quarantined_at is None
                    and (peer.source, peer.key) not in tombstoned_this_run
                ),
                key=lambda r: (r.source, r.key),
            )

            for peer in peers:
                # Last-copy-protection: tombstoning ``peer`` is allowed
                # only if at least one live copy of this content_hash
                # remains anywhere in the Replicated_Sources after the
                # tag is applied. ``live_counts[hash]`` includes
                # ``peer`` itself, so we need >= 2 to leave >= 1 alive.
                if not self._would_survive(live_counts, peer.content_hash):
                    self._emit_last_copy_guard(
                        manifest_writer,
                        record=peer,
                        intended_action=Action.TOMBSTONE,
                    )
                    stats.last_copy_guard += 1
                    continue

                adapter = self._adapters.get(peer.source)
                if adapter is None:
                    # No adapter configured for this peer's Source.
                    # Surface the issue but do not adjust live counts.
                    self._emit_tombstone_error(
                        manifest_writer,
                        record=peer,
                        error=(
                            f"no adapter configured for source "
                            f"{peer.source!r}"
                        ),
                    )
                    continue

                try:
                    adapter.set_tag(
                        peer.key, MCPS_TOMBSTONED_AT_KEY, now_iso_tag
                    )
                except Exception as exc:  # noqa: BLE001 — provider error
                    self._emit_tombstone_error(
                        manifest_writer, record=peer, error=repr(exc)
                    )
                    continue

                # Successful tombstone: decrement the live count and
                # emit the SUCCESS Manifest entry.
                live_counts[peer.content_hash] = (
                    live_counts.get(peer.content_hash, 0) - 1
                )
                tombstoned_this_run.add((peer.source, peer.key))
                self._emit_tombstone_success(
                    manifest_writer,
                    record=peer,
                    triggering_record=missing,
                )
                stats.tombstone += 1

        return stats.freeze()

    def physically_delete_tombstoned(
        self,
        catalog: Catalog,
        *,
        manifest_writer: ManifestWriter,
    ) -> ReplicationStats:
        """Physically delete tombstoned records past their retention.

        Only acts when ``delete_propagation == "hard"`` (req 9.5);
        otherwise short-circuits and returns zero counters.

        For each record in ``catalog`` whose ``tombstoned_at`` is at
        least ``tombstone_retention_days`` in the past, the Replicator
        calls ``adapter.delete(key)`` under last-copy-protection: the
        deletion is refused if it would leave the record's
        ``content_hash`` with zero non-tombstoned, non-quarantined
        copies in any Replicated_Source (req 9.6, 9.7).

        Successful deletes emit a ``PHYSICAL_DELETE`` ``DELETED`` entry;
        ``adapter.delete`` failures emit a ``PHYSICAL_DELETE`` ``ERROR``
        entry and the run continues. Records with malformed or
        unparseable ``tombstoned_at`` timestamps are treated as
        not-yet-expired so a malformed value never silently leads to
        deletion.
        """
        stats = _MutableStats()

        if self._delete_propagation != "hard":
            return stats.freeze()

        replicated = {r.source for r in catalog.all_records()}
        live_counts = self._compute_live_counts(catalog, replicated)

        cutoff = self._now()
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        else:
            cutoff = cutoff.astimezone(timezone.utc)

        retention_seconds = self._tombstone_retention_days * 86400

        candidates = sorted(
            (
                r
                for r in catalog.all_records()
                if r.tombstoned_at is not None and r.quarantined_at is None
            ),
            key=lambda r: (r.source, r.key),
        )

        for record in candidates:
            tombstoned_at = _parse_iso_seconds(record.tombstoned_at or "")
            if tombstoned_at is None:
                # Cannot parse → treat as "not yet expired" so we never
                # delete a record whose tag is malformed.
                continue
            age = cutoff - tombstoned_at
            if age.total_seconds() < retention_seconds:
                continue

            # The record itself is already tombstoned and therefore
            # does NOT contribute to ``live_counts``. Survival check:
            # the hash must still have at least one live copy
            # somewhere.
            if live_counts.get(record.content_hash, 0) < 1:
                self._emit_last_copy_guard(
                    manifest_writer,
                    record=record,
                    intended_action=Action.PHYSICAL_DELETE,
                )
                stats.last_copy_guard += 1
                continue

            adapter = self._adapters.get(record.source)
            if adapter is None:
                self._emit_physical_delete_error(
                    manifest_writer,
                    record=record,
                    error=(
                        f"no adapter configured for source "
                        f"{record.source!r}"
                    ),
                )
                continue

            try:
                adapter.delete(record.key)
            except Exception as exc:  # noqa: BLE001 — provider error
                self._emit_physical_delete_error(
                    manifest_writer, record=record, error=repr(exc)
                )
                continue

            self._emit_physical_delete_success(
                manifest_writer, record=record
            )
            stats.physical_delete += 1

        return stats.freeze()

    # ------------------------------------------------------------------
    # Last-copy-protection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_live_counts(
        catalog: Catalog, replicated: set[str]
    ) -> dict[str, int]:
        """Return ``content_hash -> count of live copies across `replicated```.

        A copy is live when neither ``quarantined_at`` nor
        ``tombstoned_at`` is set. Restricting the count to records
        whose ``source`` is in ``replicated`` matches the design's
        scope: last-copy-protection guards against losing the last
        copy *across all Replicated_Sources*, not e.g. across a Drive
        Pull_Only_Source the Replicator cannot write to anyway.
        """
        counts: dict[str, int] = {}
        for r in catalog.all_records():
            if r.source not in replicated:
                continue
            if r.quarantined_at is not None or r.tombstoned_at is not None:
                continue
            counts[r.content_hash] = counts.get(r.content_hash, 0) + 1
        return counts

    @staticmethod
    def _would_survive(
        live_counts: dict[str, int], content_hash: str
    ) -> bool:
        """True iff tombstoning one more live copy still leaves >= 1 live.

        ``live_counts[content_hash]`` is the current count *including*
        the record about to be tombstoned, so we need >= 2 for the
        post-action count to be >= 1.
        """
        return live_counts.get(content_hash, 0) >= 2

    # ------------------------------------------------------------------
    # Deletion-related Manifest emission helpers
    # ------------------------------------------------------------------

    def _emit_tombstone_success(
        self,
        manifest_writer: ManifestWriter,
        *,
        record: ObjectRecord,
        triggering_record: ObjectRecord,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.TOMBSTONE,
                result=Result.SUCCESS,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
                extra={
                    "triggering_source": triggering_record.source,
                    "triggering_key": triggering_record.key,
                },
            )
        )

    def _emit_tombstone_error(
        self,
        manifest_writer: ManifestWriter,
        *,
        record: ObjectRecord,
        error: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.TOMBSTONE,
                result=Result.ERROR,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
                error=error,
            )
        )

    def _emit_last_copy_guard(
        self,
        manifest_writer: ManifestWriter,
        *,
        record: ObjectRecord,
        intended_action: Action,
    ) -> None:
        """Emit a ``LAST_COPY_GUARD`` Manifest entry.

        The intended action (``TOMBSTONE`` or ``PHYSICAL_DELETE``) is
        recorded under ``extra.intended_action`` so downstream tools
        can attribute the skip to the propagation phase or the
        physical-delete phase that triggered it (mirrors
        `DuplicateResolver._emit_last_copy_guard`).
        """
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.LAST_COPY_GUARD,
                result=Result.SKIPPED,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
                extra={"intended_action": intended_action.value},
            )
        )

    def _emit_physical_delete_success(
        self,
        manifest_writer: ManifestWriter,
        *,
        record: ObjectRecord,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.PHYSICAL_DELETE,
                result=Result.DELETED,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
            )
        )

    def _emit_physical_delete_error(
        self,
        manifest_writer: ManifestWriter,
        *,
        record: ObjectRecord,
        error: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.PHYSICAL_DELETE,
                result=Result.ERROR,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
                error=error,
            )
        )


__all__ = [
    "MCPS_SOURCE_KEY",
    "MCPS_CONTENT_SHA256_KEY",
    "MCPS_REPLICATED_AT_KEY",
    "MCPS_TOMBSTONED_AT_KEY",
    "ReplicationPlan",
    "ReplicationStats",
    "Replicator",
]
