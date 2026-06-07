"""`Duplicate_Resolver` — canonical pick, quarantine, and physical delete
under last-copy-protection.

This module implements the three stages of duplicate resolution
described in design.md and Requirement 5:

1. ``pick_canonical(group, *, canonical_source_priority)`` runs the
   deterministic tie-break (priority Source → earliest ``last_seen_at``
   at millisecond precision → lexicographically smallest ``key`` byte
   order) and returns a `CanonicalChoice` carrying the canonical record,
   the removable records, and a ``priority_warning`` flag set when the
   configured priority list was missing/empty or no entry matched a
   group member's Source (req 5.1, 5.2).

2. ``DuplicateResolver.quarantine(removals, ...)`` performs the
   interactive confirmation (req 5.5/5.6), then per removable record:

   * checks last-copy-protection: at least one non-quarantined,
     non-tombstoned record with the same ``content_hash`` must remain
     elsewhere after this batch is applied (req 5.10, 5.11, 9.6, 9.7).
     If protection would be violated the record is skipped and a
     ``LAST_COPY_GUARD`` Manifest entry is emitted.
   * else calls ``adapter.set_tag(key, "mcps-quarantined-at", now_iso)``
     and emits a ``QUARANTINE`` Manifest entry with ``QUARANTINED``
     (req 5.7) or ``ERROR`` (req 5.8) on failure.

   In ``--dry-run`` (``dry_run=True``) every removable record produces a
   ``QUARANTINE`` Manifest entry with ``PLANNED`` and zero side effects
   are observed on any adapter (req 5.4).

3. ``DuplicateResolver.physically_delete_expired(catalog, ...)`` walks
   the Catalog for records whose ``quarantined_at`` is older than
   ``quarantine_retention_days`` and deletes them under the same
   last-copy-protection check (req 5.9, 5.10).

The resolver itself talks only to `SourceAdapter` instances and the
`ManifestWriter`; it never reads the network directly. All decisions
are driven by `ObjectRecord` values supplied by the caller. The
interactive prompt and the wall-clock are injected as callables so the
property and unit tests can drive both deterministically.

Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9,
5.10, 5.11, 9.6, 9.7.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping, Optional

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import DetectionResult, DuplicateGroup
from mcps.errors import InteractiveConfirmationRequired
from mcps.manifest.model import Action, ManifestRecord, Result
from mcps.manifest.writer import ManifestWriter
from mcps.sources.base import SourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


QUARANTINE_TAG_KEY = "mcps-quarantined-at"
"""User-metadata / tag key used to mark a record for quarantine
(req 5.7)."""


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CanonicalChoice:
    """The output of `pick_canonical` for one `DuplicateGroup`.

    Fields:

    * ``canonical`` — the winning `ObjectRecord` that survives the run.
    * ``removable`` — every other group member, sorted by ``(source,
      key)`` so the on-disk Manifest is byte-deterministic.
    * ``priority_warning`` — True when ``canonical_source_priority`` was
      missing/empty or no entry matched any member's Source. The CLI
      records this in the Manifest so operators see which groups fell
      back to rule (b) (req 5.2).
    """

    canonical: ObjectRecord
    removable: tuple[ObjectRecord, ...]
    priority_warning: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso_seconds(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ``YYYY-MM-DDTHH:MM:SSZ`` (UTC, second precision).

    The clock is injected so tests can pin the timestamp deterministically.
    The returned format matches the design's tag value contract for
    ``mcps-quarantined-at`` (req 5.7).
    """
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_iso_ms(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ISO-8601 with millisecond precision and trailing Z.

    Manifest records use millisecond precision for their ``timestamp``
    field (design.md "Manifest_Record"). The `Manifest_Writer` does not
    stamp records itself — every emitter passes a pre-formatted string.
    """
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    # ``%f`` gives microseconds; truncate to milliseconds.
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _parse_iso_seconds(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 UTC timestamp with optional fractional seconds.

    Accepts both the ``YYYY-MM-DDTHH:MM:SSZ`` and the
    ``YYYY-MM-DDTHH:MM:SS.fffZ`` shapes the resolver itself emits, plus
    any other ``datetime.fromisoformat``-compatible variant. Returns
    ``None`` if the string cannot be parsed; callers treat unparseable
    timestamps as "do not delete" so a malformed tag never leads to
    data loss.
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


# ---------------------------------------------------------------------------
# pick_canonical
# ---------------------------------------------------------------------------


def pick_canonical(
    group: DuplicateGroup,
    *,
    canonical_source_priority: tuple[str, ...],
) -> CanonicalChoice:
    """Choose the canonical `ObjectRecord` for ``group``.

    Tie-break, applied in order (req 5.1):

    (a) The first Source listed in ``canonical_source_priority`` that
        owns at least one member of ``group``.
    (b) The earliest ``last_seen_at`` timestamp at millisecond
        precision; ties propagate to (c).
    (c) The lexicographically smallest ``key`` compared byte-for-byte
        in UTF-8.

    Rule (a) is skipped when ``canonical_source_priority`` is empty or
    when none of its entries match any member's Source. In that case
    ``priority_warning=True`` is set on the returned `CanonicalChoice`
    so the CLI can emit the warning required by req 5.2.

    The ``removable`` tuple is sorted by ``(source, key)`` so the
    Manifest output is deterministic.
    """
    members = list(group.members)
    if not members:
        # The detector never emits empty groups (req 4.1: count >= 2),
        # but defensive code here keeps unit tests honest.
        raise ValueError("DuplicateGroup must have at least one member")

    priority_winner = _apply_priority_rule(members, canonical_source_priority)
    if priority_winner is not None:
        canonical = priority_winner
        priority_warning = False
    else:
        canonical = _apply_timestamp_then_key_rule(members)
        priority_warning = True

    removable = tuple(
        sorted(
            (m for m in members if m is not canonical),
            key=lambda r: (r.source, r.key),
        )
    )
    return CanonicalChoice(
        canonical=canonical,
        removable=removable,
        priority_warning=priority_warning,
    )


def _apply_priority_rule(
    members: list[ObjectRecord],
    priority: tuple[str, ...],
) -> Optional[ObjectRecord]:
    """Return the priority-source winner, or ``None`` if rule (a) does not apply."""
    if not priority:
        return None
    for source_name in priority:
        # Every member sharing the priority Source is a candidate; if more
        # than one matches we still need (b) and (c) to break the tie.
        candidates = [m for m in members if m.source == source_name]
        if not candidates:
            continue
        return _apply_timestamp_then_key_rule(candidates)
    return None


def _apply_timestamp_then_key_rule(members: list[ObjectRecord]) -> ObjectRecord:
    """Apply tie-break (b) then (c) to ``members``.

    ``last_seen_at`` is an ISO-8601 string; the resolver compares the
    parsed timestamp at millisecond precision (req 5.1 (b)). Records
    whose timestamp cannot be parsed sort last so a malformed value
    does not silently win the tie-break.
    """

    def sort_key(rec: ObjectRecord) -> tuple[int, datetime, bytes]:
        parsed = _parse_iso_seconds(rec.last_seen_at)
        if parsed is None:
            # Use ``datetime.max`` so unparseable timestamps lose to any
            # parseable one. The leading 1 vs 0 ensures the ordering is
            # stable across implementations.
            return (1, datetime.max.replace(tzinfo=timezone.utc), rec.key.encode("utf-8"))
        # Truncate to millisecond precision so two timestamps differing
        # only in microseconds compare equal at this stage; rule (c)
        # then breaks the tie.
        truncated = parsed.replace(microsecond=(parsed.microsecond // 1000) * 1000)
        return (0, truncated, rec.key.encode("utf-8"))

    return min(members, key=sort_key)


# ---------------------------------------------------------------------------
# DuplicateResolver
# ---------------------------------------------------------------------------


class DuplicateResolver:
    """Stateful coordinator for canonical pick, quarantine, and physical
    delete.

    Constructor parameters:

    * ``adapters`` — a mapping from Source name to `SourceAdapter`. The
      resolver looks up the adapter for each removable record's Source
      to call ``set_tag`` / ``delete``. Records whose Source has no
      adapter in the mapping are skipped (with a Manifest ERROR entry)
      so a partially-configured run cannot accidentally drop bytes.
    * ``canonical_source_priority`` — the configured priority tuple
      from `DuplicatesConfig.canonical_source_priority` (req 5.2).
    * ``quarantine_retention_days`` — quarantine grace period before
      physical delete (req 5.9). Defaults to 30.
    * ``run_id`` — UUIDv4 hex shared by every Manifest entry the
      resolver emits during this Sync_Run.
    * ``now`` — wall-clock callable returning a UTC `datetime`.
      Injectable so property tests can pin time deterministically.
    * ``confirm`` — interactive confirmation hook called as
      ``confirm(count, total_bytes)`` and expected to return ``True``
      iff the operator approves the batch (req 5.5). The default
      ``lambda count, total_bytes: True`` matches ``--auto-approve``
      semantics; the CLI passes a real input-prompt function.
    """

    def __init__(
        self,
        *,
        adapters: Mapping[str, SourceAdapter],
        canonical_source_priority: tuple[str, ...] = (),
        quarantine_retention_days: int = 30,
        run_id: str = "00000000",
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        confirm: Callable[[int, int], bool] = lambda count, total_bytes: True,
    ) -> None:
        self._adapters: Mapping[str, SourceAdapter] = adapters
        self._canonical_source_priority = canonical_source_priority
        self._quarantine_retention_days = quarantine_retention_days
        self._run_id = run_id
        self._now = now
        self._confirm = confirm

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------

    def plan_removals(
        self, detection_result: DetectionResult
    ) -> list[CanonicalChoice]:
        """Return one `CanonicalChoice` per duplicate group.

        Pure function over the detector output (req 5.3). The CLI uses
        this to render the removal plan for ``--dry-run`` operators
        before any confirmation is gathered.
        """
        return [
            pick_canonical(
                group,
                canonical_source_priority=self._canonical_source_priority,
            )
            for group in detection_result.groups
        ]

    # ------------------------------------------------------------------
    # Quarantine
    # ------------------------------------------------------------------

    def quarantine(
        self,
        removals: list[CanonicalChoice],
        *,
        catalog: Catalog,
        manifest_writer: ManifestWriter,
        dry_run: bool,
        auto_approve: bool,
        isatty: bool,
    ) -> None:
        """Apply the quarantine plan in ``removals``.

        Behaviour:

        * In ``dry_run`` mode every removable record produces one
          ``QUARANTINE`` Manifest entry with ``PLANNED`` and zero
          side effects on adapters (req 5.4).
        * In apply mode, when ``auto_approve`` is True the run proceeds
          immediately; when ``auto_approve`` is False:
            - If ``isatty`` is True, ``confirm(count, total_bytes)`` is
              called; when it returns False the run aborts with no
              calls (req 5.5).
            - If ``isatty`` is False, raises
              :class:`InteractiveConfirmationRequired` (req 5.6).
        * For each removable record, last-copy-protection is checked:
          if the record's ``content_hash`` would have at least one
          non-quarantined, non-tombstoned live copy elsewhere after
          this batch is applied, the record is tagged; otherwise a
          ``LAST_COPY_GUARD`` Manifest entry is emitted and the
          tagging is skipped (req 5.10, 5.11).
        * On a successful tag a ``QUARANTINE`` ``QUARANTINED`` entry is
          emitted; on a tagging failure a ``QUARANTINE`` ``ERROR``
          entry is emitted and the run continues (req 5.7, 5.8).

        ``catalog`` is used as the snapshot against which last-copy
        survival is computed; the resolver does not mutate it. The
        on-disk Catalog is rewritten by the CLI at the end of the
        Sync_Run from `ObjectRecord` updates that include the new
        ``quarantined_at`` timestamp.
        """
        # Flatten the per-group removable lists into one ordered list
        # for deterministic processing across groups.
        flat_removables: list[ObjectRecord] = []
        for choice in removals:
            flat_removables.extend(choice.removable)

        if dry_run:
            for record in flat_removables:
                self._emit_quarantine_planned(manifest_writer, record)
            return

        # Apply mode: gate on confirmation.
        if not auto_approve:
            if isatty:
                count = len(flat_removables)
                total_bytes = sum(r.size_bytes for r in flat_removables)
                if not self._confirm(count, total_bytes):
                    # Operator declined; no calls, no records (req 5.5).
                    return
            else:
                raise InteractiveConfirmationRequired(
                    "interactive confirmation is required for quarantine "
                    "in apply mode without --auto-approve"
                )

        # Last-copy-protection counter. Initial value: number of records
        # per content_hash that are currently live (i.e. neither
        # quarantined nor tombstoned). Each successful quarantine action
        # decrements the counter; a quarantine that would push the
        # counter below 1 is skipped (req 5.10, 5.11).
        live_counts = self._compute_live_counts(catalog)

        now_iso_tag = _now_iso_seconds(self._now)

        for record in flat_removables:
            if not self._would_survive(live_counts, record.content_hash):
                self._emit_last_copy_guard(manifest_writer, record)
                continue

            adapter = self._adapters.get(record.source)
            if adapter is None:
                # Misconfigured run: no adapter for this Source. Refuse
                # to act and surface the error in the Manifest. The
                # last-copy counter is left untouched because no
                # destructive call was made.
                self._emit_quarantine_error(
                    manifest_writer,
                    record,
                    error=f"no adapter configured for source {record.source!r}",
                )
                continue

            try:
                adapter.set_tag(record.key, QUARANTINE_TAG_KEY, now_iso_tag)
            except Exception as exc:  # noqa: BLE001 — surface any provider error
                # Tag failure: leave the Object unchanged, record the
                # failure, do NOT decrement the live count (the record
                # is still a live copy of its hash) (req 5.8).
                self._emit_quarantine_error(
                    manifest_writer, record, error=repr(exc)
                )
                continue

            # Successful quarantine: decrement live count and record success.
            live_counts[record.content_hash] = (
                live_counts.get(record.content_hash, 0) - 1
            )
            self._emit_quarantine_quarantined(manifest_writer, record)

    # ------------------------------------------------------------------
    # Physical delete of expired-quarantine records
    # ------------------------------------------------------------------

    def physically_delete_expired(
        self,
        catalog: Catalog,
        *,
        manifest_writer: ManifestWriter,
    ) -> None:
        """Physically delete every record whose quarantine has expired.

        A record is considered expired when ``quarantined_at`` is at
        least ``quarantine_retention_days`` in the past relative to
        ``now()`` (req 5.9). Each candidate is checked under the same
        last-copy-protection rule as quarantine (req 5.10, 5.11): if
        deleting the record would leave its ``content_hash`` with zero
        live copies anywhere, the deletion is skipped and a
        ``LAST_COPY_GUARD`` entry is emitted.

        Successful deletes emit a ``PHYSICAL_DELETE`` ``DELETED`` entry;
        ``adapter.delete`` failures emit a ``PHYSICAL_DELETE`` ``ERROR``
        entry and the run continues.
        """
        cutoff = self._now()
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        else:
            cutoff = cutoff.astimezone(timezone.utc)

        # Walk every quarantined record in deterministic order so the
        # Manifest output is stable across runs.
        candidates = sorted(
            (
                r
                for r in catalog.all_records()
                if r.quarantined_at is not None and r.tombstoned_at is None
            ),
            key=lambda r: (r.source, r.key),
        )

        live_counts = self._compute_live_counts(catalog)

        for record in candidates:
            quarantined_at = _parse_iso_seconds(record.quarantined_at or "")
            if quarantined_at is None:
                # Cannot parse → treat as "not yet expired" so we never
                # delete a record whose tag is malformed.
                continue
            age = cutoff - quarantined_at
            if age.total_seconds() < self._quarantine_retention_days * 86400:
                continue

            # The record itself is already quarantined, so it does NOT
            # contribute to its hash's live count. Survival check: the
            # hash must still have ≥ 1 live (non-quarantined) copy.
            if not self._would_survive_after_delete(live_counts, record):
                self._emit_last_copy_guard(
                    manifest_writer, record, action=Action.PHYSICAL_DELETE
                )
                continue

            adapter = self._adapters.get(record.source)
            if adapter is None:
                self._emit_physical_delete_error(
                    manifest_writer,
                    record,
                    error=f"no adapter configured for source {record.source!r}",
                )
                continue

            try:
                adapter.delete(record.key)
            except Exception as exc:  # noqa: BLE001 — provider-mapped error
                self._emit_physical_delete_error(
                    manifest_writer, record, error=repr(exc)
                )
                continue

            self._emit_physical_delete_deleted(manifest_writer, record)

    # ------------------------------------------------------------------
    # Last-copy-protection
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_live_counts(catalog: Catalog) -> dict[str, int]:
        """Return ``content_hash -> count of live copies`` over ``catalog``.

        A copy is live when neither ``quarantined_at`` nor
        ``tombstoned_at`` is set. The count is mutated in place by
        `quarantine` as records are successfully tagged so the survival
        check stays correct across the batch.
        """
        counts: dict[str, int] = {}
        for r in catalog.all_records():
            if r.quarantined_at is not None or r.tombstoned_at is not None:
                continue
            counts[r.content_hash] = counts.get(r.content_hash, 0) + 1
        return counts

    @staticmethod
    def _would_survive(live_counts: dict[str, int], content_hash: str) -> bool:
        """Return True iff quarantining one more copy still leaves ≥ 1 live.

        ``live_counts[content_hash]`` is the current count *including*
        the record about to be quarantined; we therefore need ``>= 2``
        for the post-action count to be ``>= 1``.
        """
        return live_counts.get(content_hash, 0) >= 2

    @staticmethod
    def _would_survive_after_delete(
        live_counts: dict[str, int], record: ObjectRecord
    ) -> bool:
        """Survival check for a physical-delete on a quarantined record.

        The record is already quarantined, so it does *not* contribute
        to ``live_counts``. The hash survives iff at least one live
        copy remains in the Catalog.
        """
        return live_counts.get(record.content_hash, 0) >= 1

    # ------------------------------------------------------------------
    # Manifest emission helpers
    # ------------------------------------------------------------------

    def _emit_quarantine_planned(
        self, manifest_writer: ManifestWriter, record: ObjectRecord
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.QUARANTINE,
                result=Result.PLANNED,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
            )
        )

    def _emit_quarantine_quarantined(
        self, manifest_writer: ManifestWriter, record: ObjectRecord
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.QUARANTINE,
                result=Result.QUARANTINED,
                source=record.source,
                key=record.key,
                content_hash=record.content_hash,
                size_bytes=record.size_bytes,
            )
        )

    def _emit_quarantine_error(
        self,
        manifest_writer: ManifestWriter,
        record: ObjectRecord,
        *,
        error: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.QUARANTINE,
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
        record: ObjectRecord,
        *,
        action: Action = Action.QUARANTINE,
    ) -> None:
        """Emit a LAST_COPY_GUARD Manifest entry.

        ``action`` is preserved as ``LAST_COPY_GUARD`` itself so the
        operator can distinguish the guard from the QUARANTINE /
        PHYSICAL_DELETE family. The original action is recorded in
        ``extra`` so downstream tools can still attribute the skip to
        the quarantine path or the delete path that triggered it.
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
                extra={"intended_action": action.value},
            )
        )

    def _emit_physical_delete_deleted(
        self, manifest_writer: ManifestWriter, record: ObjectRecord
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
        record: ObjectRecord,
        *,
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
    "QUARANTINE_TAG_KEY",
    "CanonicalChoice",
    "DuplicateResolver",
    "pick_canonical",
]
