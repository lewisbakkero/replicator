"""Unit tests for `mcps.duplicates.resolver`.

Example-based coverage of the `Duplicate_Resolver` per task 22:

* ``pick_canonical`` priority match (req 5.1 (a))
* ``pick_canonical`` empty priority falls back to earliest ``last_seen_at``
  (req 5.1 (b), 5.2)
* ``pick_canonical`` priority pointing to a Source not in the group falls
  through with ``priority_warning=True`` (req 5.2)
* ``pick_canonical`` tie-break (c) — same source, same ``last_seen_at`` →
  lex-smallest ``key`` (req 5.1 (c))
* ``DuplicateResolver.quarantine`` in dry-run: Manifest carries QUARANTINE
  PLANNED, no ``set_tag`` calls (req 5.4)
* ``quarantine`` in apply with ``auto_approve=True`` calls ``set_tag``
  (req 5.7)
* ``quarantine`` in apply, ``isatty=True``, confirm returns False: no
  calls (req 5.5)
* ``quarantine`` in apply, not ``isatty``, no ``auto_approve``: raises
  :class:`InteractiveConfirmationRequired` (req 5.6)
* ``quarantine`` where ``set_tag`` raises: Manifest entry with
  ``ERROR`` (req 5.8)
* ``physically_delete_expired``: only removes records past retention
  (req 5.9)
* ``physically_delete_expired`` with last-copy-guard: skips and emits
  LAST_COPY_GUARD (req 5.9, 5.10)
* ``quarantine`` last-copy-protection skip (req 5.10)

Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9,
5.10, 5.11, 9.6, 9.7.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import DuplicateGroup, detect_duplicates
from mcps.duplicates.resolver import (
    QUARANTINE_TAG_KEY,
    CanonicalChoice,
    DuplicateResolver,
    pick_canonical,
)
from mcps.errors import ExitCode, InteractiveConfirmationRequired
from mcps.manifest.model import Action, Result
from mcps.manifest.parser import parse_manifest_file
from mcps.manifest.writer import ManifestWriter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def make_record(
    *,
    source: str = "s3-prod",
    key: str = "photos/IMG_0001.jpg",
    content_hash: str = HASH_A,
    size_bytes: int = 1024,
    last_seen_at: str = "2024-01-01T00:00:00Z",
    last_modified: str = "2023-12-31T23:59:59Z",
    content_type: Optional[str] = "image/jpeg",
    quarantined_at: Optional[str] = None,
    tombstoned_at: Optional[str] = None,
    mcps_source_meta: Optional[str] = None,
) -> ObjectRecord:
    return ObjectRecord(
        source=source,
        key=key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        last_seen_at=last_seen_at,
        last_modified=last_modified,
        content_type=content_type,
        quarantined_at=quarantined_at,
        tombstoned_at=tombstoned_at,
        mcps_source_meta=mcps_source_meta,
    )


def build_group(
    *records: ObjectRecord, label: str = "cross-source"
) -> DuplicateGroup:
    """Build a `DuplicateGroup` directly. The tests need to drive the
    resolver with records that already share a content_hash; using the
    detector would tightly couple the test to the detector."""
    members = tuple(sorted(records, key=lambda r: (r.source, r.key)))
    return DuplicateGroup(
        content_hash=records[0].content_hash,
        members=members,
        label=label,  # type: ignore[arg-type]
        total_size_bytes=sum(r.size_bytes for r in records),
    )


def fixed_now(value: datetime):
    """Return a callable that always yields ``value`` (with UTC tzinfo)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    def _now() -> datetime:
        return value

    return _now


def manifest_records_at(path):
    """Return the list of `ManifestRecord`s parsed from ``path``."""
    records, errors = parse_manifest_file(str(path))
    assert errors == []
    return records


# ---------------------------------------------------------------------------
# pick_canonical
# ---------------------------------------------------------------------------


class TestPickCanonical:
    def test_priority_matches_first_listed_source(self):
        """Rule (a): canonical is the priority Source's record (req 5.1)."""
        gcs_record = make_record(
            source="gcs-archive", key="a", last_seen_at="2024-01-01T00:00:00Z"
        )
        s3_record = make_record(
            source="s3-prod", key="a", last_seen_at="2023-01-01T00:00:00Z"
        )
        group = build_group(gcs_record, s3_record)

        choice = pick_canonical(
            group, canonical_source_priority=("s3-prod", "gcs-archive")
        )
        assert choice.canonical == s3_record
        assert choice.removable == (gcs_record,)
        assert choice.priority_warning is False

    def test_empty_priority_falls_back_to_earliest_last_seen_at(self):
        """Rule (b) under an empty priority list: earlier wins (req 5.1, 5.2)."""
        early = make_record(
            source="s3-prod", key="a", last_seen_at="2023-01-01T00:00:00Z"
        )
        late = make_record(
            source="gcs-archive", key="b", last_seen_at="2024-06-15T12:00:00Z"
        )
        group = build_group(early, late)

        choice = pick_canonical(group, canonical_source_priority=())
        assert choice.canonical == early
        assert choice.removable == (late,)
        # Empty priority → priority_warning is set per req 5.2.
        assert choice.priority_warning is True

    def test_priority_with_no_matching_source_raises_warning_and_falls_through(self):
        """Priority lists a Source not present in the group: rule (a) is
        skipped and ``priority_warning=True`` (req 5.2)."""
        early = make_record(
            source="s3-prod", key="a", last_seen_at="2023-01-01T00:00:00Z"
        )
        late = make_record(
            source="gcs-archive", key="b", last_seen_at="2024-06-15T12:00:00Z"
        )
        group = build_group(early, late)

        choice = pick_canonical(
            group, canonical_source_priority=("does-not-exist",)
        )
        assert choice.canonical == early
        assert choice.priority_warning is True

    def test_tie_break_rule_c_lex_smallest_key(self):
        """Same source, same ``last_seen_at`` → lex-smallest ``key`` wins."""
        rec_z = make_record(
            source="s3-prod",
            key="zzz",
            last_seen_at="2024-01-01T00:00:00Z",
        )
        rec_a = make_record(
            source="s3-prod",
            key="aaa",
            last_seen_at="2024-01-01T00:00:00Z",
        )
        group = build_group(rec_z, rec_a)

        # Empty priority: rule (b) ties (same timestamp), rule (c) breaks it.
        choice = pick_canonical(group, canonical_source_priority=())
        assert choice.canonical == rec_a
        assert choice.removable == (rec_z,)

    def test_priority_chooses_earliest_within_priority_source(self):
        """When multiple records share the priority Source, rule (b) breaks the tie
        within that Source — not against records outside the priority Source."""
        priority_late = make_record(
            source="s3-prod", key="z", last_seen_at="2024-06-01T00:00:00Z"
        )
        priority_early = make_record(
            source="s3-prod", key="y", last_seen_at="2023-01-01T00:00:00Z"
        )
        non_priority = make_record(
            source="gcs-archive", key="a", last_seen_at="2022-01-01T00:00:00Z"
        )
        group = build_group(priority_late, priority_early, non_priority)

        choice = pick_canonical(
            group, canonical_source_priority=("s3-prod",)
        )
        # Even though `non_priority` has the earliest timestamp overall,
        # the priority rule pins the canonical to s3-prod, and within
        # s3-prod the earlier record wins.
        assert choice.canonical == priority_early
        assert choice.priority_warning is False
        assert non_priority in choice.removable

    def test_removable_sorted_by_source_then_key(self):
        """Removables are sorted (source, key) for Manifest determinism."""
        a = make_record(source="s3-prod", key="bbb", last_seen_at="2023-01-01T00:00:00Z")
        b = make_record(source="s3-prod", key="aaa", last_seen_at="2024-01-01T00:00:00Z")
        c = make_record(source="gcs-archive", key="ccc", last_seen_at="2024-01-01T00:00:00Z")
        group = build_group(a, b, c)

        # `a` is canonical: earliest timestamp.
        choice = pick_canonical(group, canonical_source_priority=())
        assert choice.canonical == a
        assert choice.removable == (c, b)


# ---------------------------------------------------------------------------
# DuplicateResolver — quarantine
# ---------------------------------------------------------------------------


class TestQuarantineDryRun:
    def test_dry_run_emits_planned_entries_and_no_set_tag_calls(self, tmp_path):
        """Req 5.4: ``--dry-run`` writes the plan to the Manifest and
        makes no destructive call."""
        rec_canonical = make_record(
            source="s3-prod", key="canonical", last_seen_at="2023-01-01T00:00:00Z"
        )
        rec_removable = make_record(
            source="gcs-archive", key="dup", last_seen_at="2024-01-01T00:00:00Z"
        )

        s3 = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            records={rec_canonical.key: b"hello"},
        )
        gcs = FakeSourceAdapter(
            name="gcs-archive",
            kind="gcs",
            records={rec_removable.key: b"hello"},
        )

        catalog = Catalog().upsert(rec_canonical).upsert(rec_removable)
        choice = pick_canonical(
            DuplicateGroup(
                content_hash=HASH_A,
                members=(rec_canonical, rec_removable),
                label="cross-source",
                total_size_bytes=2 * rec_canonical.size_bytes,
            ),
            canonical_source_priority=("s3-prod",),
        )

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            canonical_source_priority=("s3-prod",),
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                [choice],
                catalog=catalog,
                manifest_writer=mw,
                dry_run=True,
                auto_approve=False,
                isatty=False,
            )

        records = manifest_records_at(manifest_path)
        assert len(records) == 1
        assert records[0].action == Action.QUARANTINE
        assert records[0].result == Result.PLANNED
        assert records[0].source == "gcs-archive"
        assert records[0].key == "dup"

        # No set_tag / delete observed on either adapter.
        assert all(call[0] != "set_tag" for call in s3.call_log)
        assert all(call[0] != "set_tag" for call in gcs.call_log)
        assert all(call[0] != "delete" for call in gcs.call_log)


class TestQuarantineApply:
    def _setup_pair(self, *, gcs_writes: bool = True):
        """Build a two-record duplicate group with adapters and a Catalog."""
        rec_canonical = make_record(
            source="s3-prod", key="canonical", last_seen_at="2023-01-01T00:00:00Z"
        )
        rec_removable = make_record(
            source="gcs-archive", key="dup", last_seen_at="2024-01-01T00:00:00Z"
        )

        s3 = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            records={rec_canonical.key: b"hello"},
        )
        gcs = FakeSourceAdapter(
            name="gcs-archive",
            kind="gcs",
            records={rec_removable.key: b"hello"},
            supports_writes=gcs_writes,
        )
        catalog = Catalog().upsert(rec_canonical).upsert(rec_removable)
        choice = CanonicalChoice(
            canonical=rec_canonical,
            removable=(rec_removable,),
            priority_warning=False,
        )
        return rec_canonical, rec_removable, s3, gcs, catalog, choice

    def test_apply_with_auto_approve_calls_set_tag(self, tmp_path):
        """Req 5.7: apply mode with ``auto_approve=True`` tags removable
        records with ``mcps-quarantined-at``."""
        _, rec_removable, s3, gcs, catalog, choice = self._setup_pair()

        manifest_path = tmp_path / "manifest.jsonl"
        fixed_dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            now=fixed_now(fixed_dt),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                [choice],
                catalog=catalog,
                manifest_writer=mw,
                dry_run=False,
                auto_approve=True,
                isatty=False,
            )

        # Tag set on the removable record only.
        tag_calls = [c for c in gcs.call_log if c[0] == "set_tag"]
        assert len(tag_calls) == 1
        kwargs = tag_calls[0][1]
        assert kwargs["key"] == rec_removable.key
        assert kwargs["tag_key"] == QUARANTINE_TAG_KEY
        assert kwargs["tag_value"] == "2024-06-01T12:00:00Z"

        # No tag on the canonical record.
        assert not any(c[0] == "set_tag" for c in s3.call_log)

        # Manifest carries QUARANTINE QUARANTINED.
        records = manifest_records_at(manifest_path)
        assert len(records) == 1
        assert records[0].action == Action.QUARANTINE
        assert records[0].result == Result.QUARANTINED

    def test_apply_isatty_confirm_no_skips_all_calls(self, tmp_path):
        """Req 5.5: when the operator declines, no destructive call is made."""
        _, _, s3, gcs, catalog, choice = self._setup_pair()

        manifest_path = tmp_path / "manifest.jsonl"

        confirm_calls: list[tuple[int, int]] = []

        def confirm(count: int, total_bytes: int) -> bool:
            confirm_calls.append((count, total_bytes))
            return False

        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            confirm=confirm,
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                [choice],
                catalog=catalog,
                manifest_writer=mw,
                dry_run=False,
                auto_approve=False,
                isatty=True,
            )

        # Operator was prompted exactly once with the right counts.
        assert len(confirm_calls) == 1
        assert confirm_calls[0] == (1, 1024)

        # Zero set_tag calls.
        assert not any(c[0] == "set_tag" for c in gcs.call_log)
        assert not any(c[0] == "set_tag" for c in s3.call_log)

        # Manifest is empty: confirm-no aborts before any record is written.
        records = manifest_records_at(manifest_path)
        assert records == []

    def test_apply_isatty_confirm_yes_calls_set_tag(self, tmp_path):
        """Affirmative confirmation lets the run proceed (req 5.5)."""
        _, rec_removable, s3, gcs, catalog, choice = self._setup_pair()

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            confirm=lambda count, total: True,
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                [choice],
                catalog=catalog,
                manifest_writer=mw,
                dry_run=False,
                auto_approve=False,
                isatty=True,
            )

        tag_calls = [c for c in gcs.call_log if c[0] == "set_tag"]
        assert len(tag_calls) == 1
        records = manifest_records_at(manifest_path)
        assert records[0].result == Result.QUARANTINED

    def test_apply_no_tty_no_auto_approve_raises(self, tmp_path):
        """Req 5.6: non-interactive apply without ``--auto-approve``
        aborts before any call."""
        _, _, s3, gcs, catalog, choice = self._setup_pair()
        manifest_path = tmp_path / "manifest.jsonl"

        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
        )
        with ManifestWriter(str(manifest_path)) as mw:
            with pytest.raises(InteractiveConfirmationRequired) as excinfo:
                resolver.quarantine(
                    [choice],
                    catalog=catalog,
                    manifest_writer=mw,
                    dry_run=False,
                    auto_approve=False,
                    isatty=False,
                )

        assert excinfo.value.exit_code == ExitCode.INTERACTIVE_REQUIRED
        # No calls made before the abort.
        assert not any(c[0] == "set_tag" for c in gcs.call_log)
        assert not any(c[0] == "set_tag" for c in s3.call_log)

    def test_set_tag_failure_records_error_and_continues(self, tmp_path):
        """Req 5.8: a tagging failure leaves the Object unchanged and
        records the failure in the Manifest while the run continues."""
        rec_canonical = make_record(
            source="s3-prod", key="canonical", last_seen_at="2023-01-01T00:00:00Z"
        )
        rec_remove_a = make_record(
            source="gcs-archive", key="dup-a", last_seen_at="2024-01-01T00:00:00Z"
        )
        rec_remove_b = make_record(
            source="gcs-archive",
            key="dup-b",
            last_seen_at="2024-02-01T00:00:00Z",
            content_hash=HASH_B,
        )
        # Different hash so they form a separate "live group" survival-wise.
        rec_remove_b_canonical = make_record(
            source="s3-prod",
            key="canonical-b",
            last_seen_at="2023-02-01T00:00:00Z",
            content_hash=HASH_B,
        )

        s3 = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            records={
                rec_canonical.key: b"hello",
                rec_remove_b_canonical.key: b"world",
            },
        )

        # GCS adapter that raises on set_tag.
        class FlakyGCS(FakeSourceAdapter):
            def set_tag(self, key, tag_key, tag_value):
                # Record the call before failing so the test can assert it
                # was attempted (matching the design's "attempted then
                # failed" Manifest semantics).
                self._record_call(
                    "set_tag", key=key, tag_key=tag_key, tag_value=tag_value
                )
                raise RuntimeError(f"boom {key}")

        gcs = FlakyGCS(
            name="gcs-archive",
            kind="gcs",
            records={
                rec_remove_a.key: b"hello",
                rec_remove_b.key: b"world",
            },
        )

        catalog = (
            Catalog()
            .upsert(rec_canonical)
            .upsert(rec_remove_a)
            .upsert(rec_remove_b_canonical)
            .upsert(rec_remove_b)
        )

        choices = [
            CanonicalChoice(
                canonical=rec_canonical,
                removable=(rec_remove_a,),
                priority_warning=False,
            ),
            CanonicalChoice(
                canonical=rec_remove_b_canonical,
                removable=(rec_remove_b,),
                priority_warning=False,
            ),
        ]

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                choices,
                catalog=catalog,
                manifest_writer=mw,
                dry_run=False,
                auto_approve=True,
                isatty=False,
            )

        # set_tag was attempted on both removables.
        tag_calls = [c for c in gcs.call_log if c[0] == "set_tag"]
        assert len(tag_calls) == 2

        records = manifest_records_at(manifest_path)
        assert len(records) == 2
        for rec in records:
            assert rec.action == Action.QUARANTINE
            assert rec.result == Result.ERROR
            assert rec.error is not None
            assert "boom" in rec.error


class TestLastCopyProtection:
    def test_quarantine_skips_when_protection_violated(self, tmp_path):
        """Req 5.10/5.11: never quarantine the last live copy of a hash.

        Two records share a hash and both are flagged removable (because
        the test passes a malformed ``CanonicalChoice``). The second
        quarantine attempt would leave zero live copies, so it must be
        skipped with a LAST_COPY_GUARD entry.
        """
        rec_a = make_record(
            source="s3-prod", key="a", last_seen_at="2023-01-01T00:00:00Z"
        )
        rec_b = make_record(
            source="gcs-archive", key="b", last_seen_at="2024-01-01T00:00:00Z"
        )

        s3 = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            records={rec_a.key: b"hello"},
        )
        gcs = FakeSourceAdapter(
            name="gcs-archive",
            kind="gcs",
            records={rec_b.key: b"hello"},
        )
        catalog = Catalog().upsert(rec_a).upsert(rec_b)

        # Pathological: claim both records are removable. Without
        # last-copy-protection both would be tagged and the hash would
        # vanish from every live copy.
        choice = CanonicalChoice(
            canonical=rec_a,  # Placeholder; resolver only iterates `removable`.
            removable=(rec_a, rec_b),
            priority_warning=False,
        )

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.quarantine(
                [choice],
                catalog=catalog,
                manifest_writer=mw,
                dry_run=False,
                auto_approve=True,
                isatty=False,
            )

        # The first quarantine succeeded (initial live count = 2 → tag
        # leaves 1). The second is rejected by last-copy-protection.
        all_tags_s3 = [c for c in s3.call_log if c[0] == "set_tag"]
        all_tags_gcs = [c for c in gcs.call_log if c[0] == "set_tag"]
        assert len(all_tags_s3) + len(all_tags_gcs) == 1

        records = manifest_records_at(manifest_path)
        actions = [(r.action, r.result) for r in records]
        assert (Action.QUARANTINE, Result.QUARANTINED) in actions
        assert (Action.LAST_COPY_GUARD, Result.SKIPPED) in actions


# ---------------------------------------------------------------------------
# physically_delete_expired
# ---------------------------------------------------------------------------


class TestPhysicallyDeleteExpired:
    def test_only_records_past_retention_are_deleted(self, tmp_path):
        """Req 5.9: a record whose quarantine has not yet expired is left
        alone; one whose quarantine is past retention is deleted."""
        # Two records sharing a hash, both *quarantined*. Their canonical
        # peers (different hashes, live) keep last-copy-protection happy.
        live_canonical_old = make_record(
            source="s3-prod", key="canonical-old", content_hash=HASH_A
        )
        live_canonical_new = make_record(
            source="s3-prod", key="canonical-new", content_hash=HASH_B
        )
        old_quarantined = make_record(
            source="gcs-archive",
            key="old",
            content_hash=HASH_A,
            quarantined_at="2024-01-01T00:00:00Z",  # well past 30 days
        )
        new_quarantined = make_record(
            source="gcs-archive",
            key="new",
            content_hash=HASH_B,
            # Only 5 days before "now" — below the 30-day default.
            quarantined_at="2024-05-27T00:00:00Z",
        )

        s3 = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            records={
                live_canonical_old.key: b"hello",
                live_canonical_new.key: b"world",
            },
        )
        gcs = FakeSourceAdapter(
            name="gcs-archive",
            kind="gcs",
            records={
                old_quarantined.key: b"hello",
                new_quarantined.key: b"world",
            },
        )

        catalog = (
            Catalog()
            .upsert(live_canonical_old)
            .upsert(live_canonical_new)
            .upsert(old_quarantined)
            .upsert(new_quarantined)
        )

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            quarantine_retention_days=30,
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.physically_delete_expired(
                catalog, manifest_writer=mw
            )

        delete_calls = [c for c in gcs.call_log if c[0] == "delete"]
        assert len(delete_calls) == 1
        assert delete_calls[0][1]["key"] == "old"

        records = manifest_records_at(manifest_path)
        assert len(records) == 1
        assert records[0].action == Action.PHYSICAL_DELETE
        assert records[0].result == Result.DELETED
        assert records[0].key == "old"

    def test_last_copy_protection_skips_delete(self, tmp_path):
        """Req 5.10: physical delete that would leave zero live copies is
        skipped and a LAST_COPY_GUARD entry is emitted."""
        # Single quarantined record; no live peer for the same hash.
        only_record = make_record(
            source="gcs-archive",
            key="only",
            content_hash=HASH_A,
            quarantined_at="2024-01-01T00:00:00Z",
        )
        gcs = FakeSourceAdapter(
            name="gcs-archive",
            kind="gcs",
            records={only_record.key: b"hello"},
        )
        catalog = Catalog().upsert(only_record)

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"gcs-archive": gcs},
            quarantine_retention_days=30,
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.physically_delete_expired(
                catalog, manifest_writer=mw
            )

        # No delete call.
        assert not any(c[0] == "delete" for c in gcs.call_log)

        records = manifest_records_at(manifest_path)
        assert len(records) == 1
        assert records[0].action == Action.LAST_COPY_GUARD
        assert records[0].result == Result.SKIPPED
        # The Manifest entry attributes the skip to the physical-delete
        # path so operators can distinguish it from the quarantine-path
        # last-copy-guard.
        assert records[0].extra.get("intended_action") == Action.PHYSICAL_DELETE.value

    def test_already_tombstoned_records_are_ignored(self, tmp_path):
        """A tombstoned record is not a quarantine candidate; the resolver
        only inspects ``quarantined_at != None and tombstoned_at == None``."""
        live = make_record(
            source="s3-prod", key="live", content_hash=HASH_A
        )
        tombstoned = make_record(
            source="gcs-archive",
            key="ts",
            content_hash=HASH_A,
            quarantined_at="2024-01-01T00:00:00Z",
            tombstoned_at="2024-01-15T00:00:00Z",
        )

        gcs = FakeSourceAdapter(
            name="gcs-archive", kind="gcs", records={tombstoned.key: b"hello"}
        )
        s3 = FakeSourceAdapter(
            name="s3-prod", kind="s3", records={live.key: b"hello"}
        )
        catalog = Catalog().upsert(live).upsert(tombstoned)

        manifest_path = tmp_path / "manifest.jsonl"
        resolver = DuplicateResolver(
            adapters={"s3-prod": s3, "gcs-archive": gcs},
            now=fixed_now(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)),
        )
        with ManifestWriter(str(manifest_path)) as mw:
            resolver.physically_delete_expired(catalog, manifest_writer=mw)

        # No delete and no Manifest entry.
        assert not any(c[0] == "delete" for c in gcs.call_log)
        assert manifest_records_at(manifest_path) == []


# ---------------------------------------------------------------------------
# plan_removals (composes pick_canonical with the detector)
# ---------------------------------------------------------------------------


def test_plan_removals_returns_one_choice_per_group():
    """Sanity: the resolver's `plan_removals` produces exactly one
    `CanonicalChoice` per duplicate group emitted by the detector."""
    rec_a1 = make_record(
        source="s3-prod", key="a1", content_hash=HASH_A,
        last_seen_at="2023-01-01T00:00:00Z",
    )
    rec_a2 = make_record(
        source="gcs-archive", key="a2", content_hash=HASH_A,
        last_seen_at="2024-01-01T00:00:00Z",
    )
    rec_b1 = make_record(
        source="s3-prod", key="b1", content_hash=HASH_B,
        last_seen_at="2023-06-01T00:00:00Z",
    )
    rec_b2 = make_record(
        source="gcs-archive", key="b2", content_hash=HASH_B,
        last_seen_at="2024-06-01T00:00:00Z",
    )
    catalog = Catalog().upsert(rec_a1).upsert(rec_a2).upsert(rec_b1).upsert(rec_b2)
    detection = detect_duplicates(catalog)
    assert len(detection.groups) == 2

    resolver = DuplicateResolver(
        adapters={},
        canonical_source_priority=("s3-prod",),
    )
    plan = resolver.plan_removals(detection)
    assert len(plan) == 2
    # With s3-prod priority every canonical comes from s3-prod and the
    # gcs-archive copy is removable.
    for choice in plan:
        assert choice.canonical.source == "s3-prod"
        assert all(r.source == "gcs-archive" for r in choice.removable)
