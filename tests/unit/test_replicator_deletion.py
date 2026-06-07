"""Example-based unit tests for `Replicator.propagate_deletions` and
`Replicator.physically_delete_tombstoned`.

This file covers the deletion-handling extensions of task 24:

* ``delete_propagation=none`` is a strict no-op even when the catalog
  contains records that have gone missing (req 9.2).
* ``delete_propagation=soft`` adds ``mcps-tombstoned-at`` to peer
  records on other Replicated_Sources for records absent from the
  current run, gated by last-copy-protection (req 9.3, 9.6, 9.7).
* ``delete_propagation=hard`` physically deletes records whose
  ``tombstoned_at`` age exceeds ``tombstone_retention_days`` (req 9.5),
  again under last-copy-protection.
* Unreachable Sources are NEVER treated as "absent" — a failed listing
  must not be allowed to drop bytes (design.md "Treat unreachable
  Sources as 'not absent'").

Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.manifest.model import Action, Result
from mcps.manifest.parser import parse_manifest_file
from mcps.manifest.writer import ManifestWriter
from mcps.replication import (
    MCPS_CONTENT_SHA256_KEY,
    MCPS_SOURCE_KEY,
    MCPS_TOMBSTONED_AT_KEY,
    Replicator,
)
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


HASH_A = "a" * 64
HASH_B = "b" * 64

FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_NOW


def _make_record(
    *,
    source: str,
    key: str,
    content_hash: str,
    size_bytes: int = 5,
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


def _manifest_records(path) -> list:
    records, errors = parse_manifest_file(str(path))
    assert errors == []
    return records


def _make_adapter(name: str, kind: str, *, records=None, metadata=None):
    """Helper that returns a FakeSourceAdapter mirroring real Replicated_Sources."""
    return FakeSourceAdapter(
        name=name,
        kind=kind,
        records=dict(records or {}),
        metadata={k: dict(v) for k, v in (metadata or {}).items()},
    )


# ---------------------------------------------------------------------------
# delete_propagation = none
# ---------------------------------------------------------------------------


class TestDeletePropagationNone:
    """Under ``delete_propagation=none`` no tombstone propagation occurs.

    Even when the catalog has a record present last run and absent this
    run, neither ``set_tag`` nor ``delete`` is called. Manifest stays
    empty for the propagate_deletions phase (req 9.2).
    """

    def test_missing_record_does_not_tombstone_peer(self, tmp_path):
        rec_s3 = _make_record(source="s3", key="a.jpg", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog_at_start = Catalog().upsert(rec_s3).upsert(rec_gcs)

        # The s3 listing this run does NOT include a.jpg → it's "missing".
        # gcs still has its peer.
        s3 = _make_adapter("s3", "s3", records={})  # empty: object gone
        gcs = _make_adapter(
            "gcs",
            "gcs",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="none",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={"s3": [], "gcs": [rec_gcs]},
                replicated_source_names=("s3", "gcs"),
                reachable={"s3", "gcs"},
                manifest_writer=mw,
            )

        assert stats.tombstone == 0
        assert stats.last_copy_guard == 0
        assert stats.physical_delete == 0

        # Adapters received zero side-effecting calls.
        for adapter in adapters.values():
            assert not any(
                c[0] in {"set_tag", "delete", "write_bytes"}
                for c in adapter.call_log
            )

        records = _manifest_records(manifest_path)
        assert records == []


# ---------------------------------------------------------------------------
# delete_propagation = soft
# ---------------------------------------------------------------------------


class TestDeletePropagationSoft:
    """Under ``soft`` peer records receive ``mcps-tombstoned-at``."""

    def test_peer_gets_tombstoned_when_source_record_disappears(self, tmp_path):
        rec_s3 = _make_record(source="s3", key="a.jpg", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        # A second hash with a live copy on gcs only — survival check
        # for HASH_A must not interfere with HASH_B.
        rec_gcs_b = _make_record(source="gcs", key="b.jpg", content_hash=HASH_B)
        catalog_at_start = (
            Catalog().upsert(rec_s3).upsert(rec_gcs).upsert(rec_gcs_b)
        )

        # s3 listing is empty this run.
        s3 = _make_adapter("s3", "s3", records={})
        gcs = _make_adapter(
            "gcs",
            "gcs",
            records={"a.jpg": b"hello", "b.jpg": b"world"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                },
                "b.jpg": {
                    MCPS_SOURCE_KEY: "gcs",
                    MCPS_CONTENT_SHA256_KEY: HASH_B,
                },
            },
        )
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            run_id="abc12345",
            delete_propagation="soft",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={"s3": [], "gcs": [rec_gcs, rec_gcs_b]},
                replicated_source_names=("s3", "gcs"),
                reachable={"s3", "gcs"},
                manifest_writer=mw,
            )

        # The single peer on gcs was tombstoned. HASH_A had two live
        # copies (s3 + gcs); under LCP it is safe to tombstone one.
        # NOTE: gcs would otherwise be the "last copy" too — but the
        # rule is about non-tombstoned, non-quarantined records *across
        # all Replicated_Sources*; with rec_s3 still live in the
        # catalog snapshot at start, the gcs peer has a live sibling
        # and may be tombstoned. (After the tombstone, only rec_s3 is
        # live for HASH_A.)
        assert stats.tombstone == 1
        assert stats.last_copy_guard == 0

        # gcs received exactly one set_tag call with the documented key.
        set_tag_calls = [c for c in gcs.call_log if c[0] == "set_tag"]
        assert len(set_tag_calls) == 1
        kwargs = set_tag_calls[0][1]
        assert kwargs["key"] == "a.jpg"
        assert kwargs["tag_key"] == MCPS_TOMBSTONED_AT_KEY
        # The tag value is the run-time clock at second precision.
        assert kwargs["tag_value"] == "2024-06-01T12:00:00Z"

        # No s3-side side effects (the missing record cannot be tagged).
        assert not any(c[0] == "set_tag" for c in s3.call_log)

        records = _manifest_records(manifest_path)
        tombstones = [r for r in records if r.action == Action.TOMBSTONE]
        assert len(tombstones) == 1
        assert tombstones[0].result == Result.SUCCESS
        assert tombstones[0].source == "gcs"
        assert tombstones[0].key == "a.jpg"
        assert tombstones[0].content_hash == HASH_A
        # The triggering record (the one that went missing) is recorded
        # in extra so the audit trail is complete.
        assert tombstones[0].extra.get("triggering_source") == "s3"
        assert tombstones[0].extra.get("triggering_key") == "a.jpg"

    def test_last_copy_protection_blocks_when_only_peer_remains(self, tmp_path):
        """When the only candidate peer is itself already tombstoned,
        no tombstone fires (the peer is filtered out) — exercising
        the "no live peer to tombstone" branch (req 9.6, 9.7).

        Setup: HASH_A has one already-tombstoned copy on s3 and one
        live copy on gcs that is going missing this run. The
        propagator's peer search for the missing rec_gcs filters out
        rec_s3 (already tombstoned), so there is no candidate to
        tombstone. The end state is "no work" — and crucially, no
        accidental tag on a stale tombstone.
        """
        rec_s3 = _make_record(
            source="s3",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at="2024-05-01T00:00:00Z",
        )
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog_at_start = Catalog().upsert(rec_s3).upsert(rec_gcs)

        s3 = _make_adapter(
            "s3",
            "s3",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                    MCPS_TOMBSTONED_AT_KEY: "2024-05-01T00:00:00Z",
                }
            },
        )
        # gcs's a.jpg disappeared this run.
        gcs = _make_adapter("gcs", "gcs", records={})
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            run_id="abc12345",
            delete_propagation="soft",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={"s3": [rec_s3], "gcs": []},
                replicated_source_names=("s3", "gcs"),
                reachable={"s3", "gcs"},
                manifest_writer=mw,
            )

        # No tombstones: the only live peer would have been s3's
        # record, but s3's record is already tombstoned, so it is
        # filtered out by the propagator before LCP even runs (peers
        # must themselves be live to be tombstone candidates). The
        # net effect is "no work" and zero last_copy_guard entries
        # — the design says LCP fires when a candidate would zero out
        # the hash, but here there is no candidate at all.
        assert stats.tombstone == 0
        # No set_tag calls anywhere.
        for adapter in adapters.values():
            assert not any(c[0] == "set_tag" for c in adapter.call_log)

    def test_last_copy_guard_fires_when_sequential_tombstones_would_zero_hash(
        self, tmp_path
    ):
        """LCP fires when sequential tombstones in the same batch
        would zero out the live count for a hash (req 9.6, 9.7).

        Setup: HASH_A has copies in four Replicated_Sources where one
        is already tombstoned; two live records go missing this run.
        Processing them in order tombstones their peers one by one
        until the live count drops to 1; the next tombstone would push
        the count to 0 and is refused.

        We keep the bytes on every adapter (the listing-side
        "missing" is signalled exclusively via ``current_records``)
        so set_tag calls succeed and the propagator's tombstone path
        runs end-to-end.
        """
        # Catalog snapshot: 4 records for HASH_A.
        # rec_s3 — live, going missing this run.
        # rec_gcs — live, going missing this run.
        # rec_s3_mirror — live (the only peer that should ultimately
        #   survive).
        # rec_extra — already tombstoned (filtered out as a peer).
        rec_s3 = _make_record(source="s3", key="a.jpg", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        rec_s3_mirror = _make_record(
            source="s3-mirror", key="a.jpg", content_hash=HASH_A
        )
        rec_extra = _make_record(
            source="s3-extra",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at="2024-05-01T00:00:00Z",
        )
        catalog_at_start = (
            Catalog()
            .upsert(rec_s3)
            .upsert(rec_gcs)
            .upsert(rec_s3_mirror)
            .upsert(rec_extra)
        )

        # Every adapter still owns the bytes — we exercise the
        # propagator with bytes-still-present so set_tag calls
        # succeed. The listing-side absence is only signalled via
        # current_records.
        def _meta_for(name: str, *, tombstoned: bool = False):
            md = {
                MCPS_SOURCE_KEY: name,
                MCPS_CONTENT_SHA256_KEY: HASH_A,
            }
            if tombstoned:
                md[MCPS_TOMBSTONED_AT_KEY] = "2024-05-01T00:00:00Z"
            return md

        s3 = _make_adapter(
            "s3",
            "s3",
            records={"a.jpg": b"hello"},
            metadata={"a.jpg": _meta_for("s3")},
        )
        gcs = _make_adapter(
            "gcs",
            "gcs",
            records={"a.jpg": b"hello"},
            metadata={"a.jpg": _meta_for("gcs")},
        )
        s3_mirror = _make_adapter(
            "s3-mirror",
            "s3",
            records={"a.jpg": b"hello"},
            metadata={"a.jpg": _meta_for("s3-mirror")},
        )
        s3_extra = _make_adapter(
            "s3-extra",
            "s3",
            records={"a.jpg": b"hello"},
            metadata={"a.jpg": _meta_for("s3-extra", tombstoned=True)},
        )
        adapters = {
            "s3": s3,
            "gcs": gcs,
            "s3-mirror": s3_mirror,
            "s3-extra": s3_extra,
        }

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            run_id="abc12345",
            delete_propagation="soft",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={
                    "s3": [],
                    "gcs": [],
                    "s3-mirror": [rec_s3_mirror],
                    "s3-extra": [rec_extra],
                },
                replicated_source_names=(
                    "s3",
                    "gcs",
                    "s3-mirror",
                    "s3-extra",
                ),
                reachable={"s3", "gcs", "s3-mirror", "s3-extra"},
                manifest_writer=mw,
            )

        # Initial live count for HASH_A = 3 (s3 + gcs + s3-mirror;
        # s3-extra is tombstoned and filtered). Candidates (records
        # gone missing, sorted by ``(source, key, hash)``) are
        # processed in order: gcs, s3.
        # First missing = gcs. Peers: rec_s3, rec_s3_mirror, rec_extra
        # — rec_extra is filtered (already tombstoned). For rec_s3:
        # live count 3 -> 2, safe. For rec_s3_mirror: live count
        # 2 -> 1, safe.
        # Second missing = s3. Peers: rec_gcs, rec_s3_mirror,
        # rec_extra. rec_s3_mirror was tombstoned this run so filtered.
        # rec_extra was tombstoned previously so filtered. Only
        # rec_gcs remains. LCP at decision: live count 1 — survival
        # check (1 >= 2) is False → LCP fires.
        assert stats.tombstone == 2
        assert stats.last_copy_guard == 1

        records = _manifest_records(manifest_path)
        tombstones = [
            r for r in records if r.action == Action.TOMBSTONE
        ]
        guards = [r for r in records if r.action == Action.LAST_COPY_GUARD]
        assert len(tombstones) == 2
        assert len(guards) == 1
        assert guards[0].extra.get("intended_action") == Action.TOMBSTONE.value


# ---------------------------------------------------------------------------
# Unreachable-Source guard
# ---------------------------------------------------------------------------


class TestUnreachableSourceGuard:
    """A failed listing must NEVER produce tombstones — req design
    "Treat unreachable Sources as 'not absent'"."""

    def test_unreachable_source_does_not_tombstone(self, tmp_path):
        rec_s3 = _make_record(source="s3", key="a.jpg", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog_at_start = Catalog().upsert(rec_s3).upsert(rec_gcs)

        # s3 listing failed this run; current_records["s3"] is empty,
        # but the CLI also signals reachability via the ``reachable``
        # set, which DOES NOT include "s3".
        s3 = _make_adapter("s3", "s3", records={"a.jpg": b"hello"})
        gcs = _make_adapter(
            "gcs",
            "gcs",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="soft",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={"s3": [], "gcs": [rec_gcs]},
                replicated_source_names=("s3", "gcs"),
                # s3 listing failed; only gcs is reachable.
                reachable={"gcs"},
                manifest_writer=mw,
            )

        # Even though current_records["s3"] is empty, the s3-side
        # record is treated as "not absent" because s3 is unreachable.
        # No tombstones, no LCP fires.
        assert stats.tombstone == 0
        assert stats.last_copy_guard == 0

        for adapter in adapters.values():
            assert not any(c[0] == "set_tag" for c in adapter.call_log)

        records = _manifest_records(manifest_path)
        assert records == []

    def test_unreachable_source_under_hard_mode_also_does_not_tombstone(
        self, tmp_path
    ):
        """The unreachable-Source guard applies under ``hard`` too —
        the propagation phase under ``hard`` is identical to ``soft``,
        and the physical-delete phase is independent."""
        rec_s3 = _make_record(source="s3", key="a.jpg", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog_at_start = Catalog().upsert(rec_s3).upsert(rec_gcs)

        s3 = _make_adapter("s3", "s3", records={"a.jpg": b"hello"})
        gcs = _make_adapter(
            "gcs",
            "gcs",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="hard",
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.propagate_deletions(
                catalog_at_start,
                current_records={"s3": [], "gcs": [rec_gcs]},
                replicated_source_names=("s3", "gcs"),
                reachable={"gcs"},
                manifest_writer=mw,
            )

        assert stats.tombstone == 0
        assert stats.last_copy_guard == 0
        assert not any(c[0] == "set_tag" for c in gcs.call_log)


# ---------------------------------------------------------------------------
# delete_propagation = hard (physically_delete_tombstoned)
# ---------------------------------------------------------------------------


class TestPhysicallyDeleteTombstoned:
    """Tombstoned records older than retention are physically deleted."""

    def test_old_tombstone_is_physically_deleted(self, tmp_path):
        # Tombstoned 60 days before "now" (FIXED_NOW = 2024-06-01).
        old = (FIXED_NOW - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Two copies of HASH_A: one tombstoned old enough to delete,
        # one still live so LCP allows the delete.
        rec_s3 = _make_record(
            source="s3",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at=old,
        )
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog = Catalog().upsert(rec_s3).upsert(rec_gcs)

        s3 = _make_adapter(
            "s3",
            "s3",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                    MCPS_TOMBSTONED_AT_KEY: old,
                }
            },
        )
        gcs = _make_adapter(
            "gcs", "gcs", records={"a.jpg": b"hello"}
        )
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="hard",
            tombstone_retention_days=30,
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.physically_delete_tombstoned(
                catalog, manifest_writer=mw
            )

        assert stats.physical_delete == 1
        assert stats.last_copy_guard == 0

        delete_calls = [c for c in s3.call_log if c[0] == "delete"]
        assert len(delete_calls) == 1
        assert delete_calls[0][1]["key"] == "a.jpg"
        assert "a.jpg" not in s3.records

        records = _manifest_records(manifest_path)
        deletions = [r for r in records if r.action == Action.PHYSICAL_DELETE]
        assert len(deletions) == 1
        assert deletions[0].result == Result.DELETED
        assert deletions[0].source == "s3"
        assert deletions[0].key == "a.jpg"

    def test_recent_tombstone_is_not_deleted(self, tmp_path):
        # Tombstoned 5 days ago — within the 30-day retention.
        recent = (FIXED_NOW - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rec_s3 = _make_record(
            source="s3",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at=recent,
        )
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog = Catalog().upsert(rec_s3).upsert(rec_gcs)

        s3 = _make_adapter(
            "s3",
            "s3",
            records={"a.jpg": b"hello"},
        )
        gcs = _make_adapter("gcs", "gcs", records={"a.jpg": b"hello"})
        adapters = {"s3": s3, "gcs": gcs}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="hard",
            tombstone_retention_days=30,
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.physically_delete_tombstoned(
                catalog, manifest_writer=mw
            )

        assert stats.physical_delete == 0
        assert not any(c[0] == "delete" for c in s3.call_log)
        assert "a.jpg" in s3.records  # untouched

        records = _manifest_records(manifest_path)
        # No PHYSICAL_DELETE entries emitted for non-expired tombstones.
        assert [
            r for r in records if r.action == Action.PHYSICAL_DELETE
        ] == []

    def test_last_copy_guard_blocks_physical_delete_of_last_copy(
        self, tmp_path
    ):
        """If the tombstoned record is the last copy of its hash
        anywhere, deleting it would zero out the hash. LCP must refuse
        (req 9.6, 9.7)."""
        old = (FIXED_NOW - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Only ONE record for HASH_A — and it's tombstoned. Live count
        # for HASH_A across the catalog is 0; deleting would still
        # leave 0, but the design treats "no live copy anywhere" as a
        # last-copy violation regardless: there is no surviving
        # representative for the content_hash.
        rec_only = _make_record(
            source="s3",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at=old,
        )
        catalog = Catalog().upsert(rec_only)

        s3 = _make_adapter(
            "s3",
            "s3",
            records={"a.jpg": b"hello"},
        )
        adapters = {"s3": s3}

        rep = Replicator(
            adapters=adapters,
            now=_fixed_now,
            delete_propagation="hard",
            tombstone_retention_days=30,
        )

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.physically_delete_tombstoned(
                catalog, manifest_writer=mw
            )

        assert stats.physical_delete == 0
        assert stats.last_copy_guard == 1
        assert not any(c[0] == "delete" for c in s3.call_log)

        records = _manifest_records(manifest_path)
        guards = [r for r in records if r.action == Action.LAST_COPY_GUARD]
        assert len(guards) == 1
        assert guards[0].extra.get("intended_action") == Action.PHYSICAL_DELETE.value

    def test_under_none_or_soft_physically_delete_is_a_noop(self, tmp_path):
        """``physically_delete_tombstoned`` only acts under ``hard``."""
        old = (FIXED_NOW - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
        rec_s3 = _make_record(
            source="s3",
            key="a.jpg",
            content_hash=HASH_A,
            tombstoned_at=old,
        )
        rec_gcs = _make_record(source="gcs", key="a.jpg", content_hash=HASH_A)
        catalog = Catalog().upsert(rec_s3).upsert(rec_gcs)

        s3 = _make_adapter("s3", "s3", records={"a.jpg": b"hello"})
        gcs = _make_adapter("gcs", "gcs", records={"a.jpg": b"hello"})
        adapters = {"s3": s3, "gcs": gcs}

        for mode in ("none", "soft"):
            rep = Replicator(
                adapters=adapters,
                now=_fixed_now,
                delete_propagation=mode,
                tombstone_retention_days=30,
            )
            manifest_path = tmp_path / f"manifest-{mode}.jsonl"
            with ManifestWriter(str(manifest_path)) as mw:
                stats = rep.physically_delete_tombstoned(
                    catalog, manifest_writer=mw
                )
            assert stats.physical_delete == 0
            # No delete calls under non-hard modes.
            assert not any(c[0] == "delete" for c in s3.call_log)
            assert not any(c[0] == "delete" for c in gcs.call_log)
