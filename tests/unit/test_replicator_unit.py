"""Example-based unit tests for `mcps.replication.Replicator`.

The three Hypothesis property tests in `tests/unit/` cover the
universally-quantified properties (eventual consistency, loop-free
behaviour, conflict-resolution table). This file covers the
example-based scenarios that the property tests cannot directly
target:

* Post-write verification mismatch deletes the destination object and
  emits ``REPLICATION_ERROR`` (req 6.5).
* ``mcps-source`` user-metadata is set on the copy (req 7.4).
* The Manifest entry for ``SOURCE_TAGGED`` carries the originating
  Source name and is emitted before the actual write (req 7.4).
* Loop guard reads the *live* source-side metadata via
  ``get_metadata`` rather than relying on the cached value alone
  (req 7.3).
* ``ReplicationPlan`` is sorted deterministically.

Validates: Requirements 6.4, 6.5, 7.3, 7.4.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, Mapping, Optional

import pytest

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.manifest.model import Action, Result
from mcps.manifest.parser import parse_manifest_file
from mcps.manifest.writer import ManifestWriter
from mcps.replication import (
    MCPS_CONTENT_SHA256_KEY,
    MCPS_REPLICATED_AT_KEY,
    MCPS_SOURCE_KEY,
    ReplicationPlan,
    Replicator,
)
from mcps.sources.base import ObjectMeta
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64

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
        mcps_source_meta=mcps_source_meta,
    )


def _manifest_records(path) -> list:
    records, errors = parse_manifest_file(str(path))
    assert errors == []
    return records


# ---------------------------------------------------------------------------
# Plan determinism
# ---------------------------------------------------------------------------


class TestPlan:
    def test_plan_is_sorted_by_src_dst_hash(self):
        rec_a_in_s3 = _make_record(source="s3", key="a", content_hash=HASH_A)
        rec_b_in_s3 = _make_record(source="s3", key="b", content_hash=HASH_B)
        rec_c_in_gcs = _make_record(source="gcs", key="c", content_hash=HASH_C)

        catalog = Catalog().upsert(rec_a_in_s3).upsert(rec_b_in_s3).upsert(rec_c_in_gcs)

        adapters = {
            "s3": FakeSourceAdapter(name="s3", kind="s3", records={"a": b"x", "b": b"x"}),
            "gcs": FakeSourceAdapter(name="gcs", kind="gcs", records={"c": b"x"}),
        }
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        # Strictly ascending by (src, dst, hash).
        keys = [(p[0], p[1], p[2]) for p in plan.pairs]
        assert keys == sorted(keys)

    def test_plan_omits_destinations_that_already_have_the_hash(self):
        # Both sides have HASH_A under different keys → no plan entry
        # for HASH_A in either direction.
        rec_s3 = _make_record(source="s3", key="a", content_hash=HASH_A)
        rec_gcs = _make_record(source="gcs", key="b", content_hash=HASH_A)
        catalog = Catalog().upsert(rec_s3).upsert(rec_gcs)

        adapters = {
            "s3": FakeSourceAdapter(name="s3", kind="s3", records={"a": b"x"}),
            "gcs": FakeSourceAdapter(name="gcs", kind="gcs", records={"b": b"x"}),
        }
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))
        assert plan.pairs == ()


# ---------------------------------------------------------------------------
# write contract: mcps-* metadata is attached
# ---------------------------------------------------------------------------


class TestMetadataOnWrite:
    def test_clean_write_attaches_mcps_metadata(self, tmp_path):
        # s3 has the byte content for HASH_A; gcs is empty.
        adapters = {
            "s3": FakeSourceAdapter(
                name="s3",
                kind="s3",
                records={"photos/a.jpg": b"hello"},
                metadata={
                    "photos/a.jpg": {
                        # The source was previously tagged in s3 by another
                        # Sync_Run; the loop guard keys on dst_name, not on
                        # the presence of any mcps-source value.
                        MCPS_SOURCE_KEY: "s3",
                        MCPS_CONTENT_SHA256_KEY: HASH_A,
                    }
                },
            ),
            "gcs": FakeSourceAdapter(name="gcs", kind="gcs", records={}),
        }
        rec = _make_record(
            source="s3",
            key="photos/a.jpg",
            content_hash=HASH_A,
            size_bytes=5,
            mcps_source_meta="s3",
        )
        catalog = Catalog().upsert(rec)
        rep = Replicator(adapters=adapters, now=_fixed_now, run_id="abc12345")
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.replicate(plan, manifest_writer=mw)

        # Exactly one successful replicate.
        assert stats.replicate == 1
        assert stats.replication_error == 0

        # gcs received a write_bytes with the documented metadata keys.
        gcs = adapters["gcs"]
        write_calls = [c for c in gcs.call_log if c[0] == "write_bytes"]
        assert len(write_calls) == 1
        kwargs = write_calls[0][1]
        assert kwargs["key"] == "photos/a.jpg"
        assert kwargs["size_bytes"] == 5
        assert kwargs["user_metadata"][MCPS_SOURCE_KEY] == "s3"
        assert kwargs["user_metadata"][MCPS_CONTENT_SHA256_KEY] == HASH_A
        assert kwargs["user_metadata"][MCPS_REPLICATED_AT_KEY] == "2024-06-01T12:00:00Z"

        records = _manifest_records(manifest_path)
        replicate_recs = [r for r in records if r.action == Action.REPLICATE]
        assert len(replicate_recs) == 1
        assert replicate_recs[0].result == Result.SUCCESS
        assert replicate_recs[0].source == "s3"
        assert replicate_recs[0].target == "gcs"
        assert replicate_recs[0].key == "photos/a.jpg"
        assert replicate_recs[0].content_hash == HASH_A


# ---------------------------------------------------------------------------
# Source-tag missing-mcps-source records (req 7.4)
# ---------------------------------------------------------------------------


class TestSourceTagging:
    def test_missing_mcps_source_emits_source_tagged_then_replicate(self, tmp_path):
        # s3 has the bytes but no mcps-source tag yet (Cold_Start case).
        adapters = {
            "s3": FakeSourceAdapter(
                name="s3", kind="s3", records={"a.jpg": b"hello"}
            ),
            "gcs": FakeSourceAdapter(name="gcs", kind="gcs", records={}),
        }
        rec = _make_record(
            source="s3", key="a.jpg", content_hash=HASH_A, mcps_source_meta=None
        )
        catalog = Catalog().upsert(rec)
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.replicate(plan, manifest_writer=mw)

        assert stats.source_tagged == 1
        assert stats.replicate == 1

        records = _manifest_records(manifest_path)
        # SOURCE_TAGGED must precede REPLICATE in the Manifest.
        source_tagged_idx = next(
            i for i, r in enumerate(records) if r.action == Action.SOURCE_TAGGED
        )
        replicate_idx = next(
            i for i, r in enumerate(records) if r.action == Action.REPLICATE
        )
        assert source_tagged_idx < replicate_idx

        st = records[source_tagged_idx]
        assert st.source == "s3"
        assert st.target == "gcs"
        assert st.extra.get("mcps_source") == "s3"


# ---------------------------------------------------------------------------
# Post-write verification (req 6.5)
# ---------------------------------------------------------------------------


class _BadVerifyAdapter(FakeSourceAdapter):
    """Destination adapter whose post-write `get_metadata` lies about the
    content hash, forcing the verification step to mismatch.

    The first ``get_metadata`` call (when probing for an existing key
    on the destination) returns FileNotFoundError (key absent). The
    second ``get_metadata`` (post-write verification) returns an
    `ObjectMeta` with the wrong ``mcps-content-sha256``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._get_metadata_calls = 0

    def get_metadata(self, key):
        self._get_metadata_calls += 1
        # First call (pre-write probe) → key absent.
        if self._get_metadata_calls == 1 and key not in self.records:
            self._record_call("get_metadata", key=key)
            raise FileNotFoundError(key)
        # Subsequent call (post-write verify) → return the real meta but
        # with a tampered mcps-content-sha256 value to force a mismatch.
        meta = super().get_metadata(key)
        return ObjectMeta(
            key=meta.key,
            size_bytes=meta.size_bytes,
            last_modified=meta.last_modified,
            content_type=meta.content_type,
            user_metadata={
                **dict(meta.user_metadata),
                MCPS_CONTENT_SHA256_KEY: "f" * 64,  # wrong hash
            },
            etag=meta.etag,
            provider_hash=meta.provider_hash,
        )


class TestPostWriteVerification:
    def test_mismatch_deletes_partial_destination_and_emits_error(self, tmp_path):
        s3 = FakeSourceAdapter(
            name="s3",
            kind="s3",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        gcs = _BadVerifyAdapter(name="gcs", kind="gcs", records={})

        adapters = {"s3": s3, "gcs": gcs}
        rec = _make_record(
            source="s3", key="a.jpg", content_hash=HASH_A, mcps_source_meta="s3"
        )
        catalog = Catalog().upsert(rec)
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.replicate(plan, manifest_writer=mw)

        # Exactly one replication error counted.
        assert stats.replication_error == 1
        assert stats.replicate == 0

        # The bad destination object must have been deleted as part of
        # the rollback (req 6.5).
        assert "a.jpg" not in gcs.records
        assert any(c[0] == "delete" for c in gcs.call_log)

        records = _manifest_records(manifest_path)
        err_records = [r for r in records if r.action == Action.REPLICATION_ERROR]
        assert len(err_records) == 1
        assert err_records[0].result == Result.ERROR
        assert err_records[0].source == "s3"
        assert err_records[0].target == "gcs"
        assert err_records[0].key == "a.jpg"
        assert "post-write verify mismatch" in (err_records[0].error or "")


# ---------------------------------------------------------------------------
# Loop guard via live metadata
# ---------------------------------------------------------------------------


class TestLoopGuardLiveMetadata:
    def test_live_mcps_source_overrides_cached_value(self, tmp_path):
        """Even if the catalog says ``mcps_source_meta=None``, the live
        ``mcps-source`` user-metadata on the source object decides the
        loop check (req 7.3)."""
        # The Catalog says nothing about mcps_source_meta, but the live
        # source object on s3 has mcps-source = gcs (it was originally
        # replicated *from* gcs).
        s3 = FakeSourceAdapter(
            name="s3",
            kind="s3",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "gcs",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        gcs = FakeSourceAdapter(name="gcs", kind="gcs", records={})

        adapters = {"s3": s3, "gcs": gcs}
        rec = _make_record(
            source="s3", key="a.jpg", content_hash=HASH_A, mcps_source_meta=None
        )
        catalog = Catalog().upsert(rec)
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.replicate(plan, manifest_writer=mw)

        assert stats.loop_skip == 1
        assert stats.replicate == 0

        # No write_bytes against gcs.
        assert not any(c[0] == "write_bytes" for c in gcs.call_log)

        records = _manifest_records(manifest_path)
        loop_skips = [r for r in records if r.action == Action.LOOP_SKIP]
        assert len(loop_skips) == 1


# ---------------------------------------------------------------------------
# Same-hash destination: REPLICATE_SKIP
# ---------------------------------------------------------------------------


class TestReplicateSkipExisting:
    def test_destination_has_same_hash_at_same_key(self, tmp_path):
        """Req 6.7 — when the destination already has the same Content_Hash
        at the same key, the Replicator must skip and emit REPLICATE_SKIP."""
        # Plan generation skips when the hash is present in the destination
        # at *any* key, so to exercise REPLICATE_SKIP we need a Catalog
        # where the hash is absent from gcs (so the plan picks it up) but
        # the destination adapter, at write time, already has the same
        # hash at the same key. We accomplish this by writing the record
        # to gcs *after* plan() is called.
        s3 = FakeSourceAdapter(
            name="s3",
            kind="s3",
            records={"a.jpg": b"hello"},
            metadata={
                "a.jpg": {
                    MCPS_SOURCE_KEY: "s3",
                    MCPS_CONTENT_SHA256_KEY: HASH_A,
                }
            },
        )
        gcs = FakeSourceAdapter(name="gcs", kind="gcs", records={})
        adapters = {"s3": s3, "gcs": gcs}
        rec = _make_record(
            source="s3", key="a.jpg", content_hash=HASH_A, mcps_source_meta="s3"
        )
        catalog = Catalog().upsert(rec)
        rep = Replicator(adapters=adapters, now=_fixed_now)
        plan = rep.plan(catalog, replicated_source_names=("s3", "gcs"))

        # Now plant the same hash into gcs at the same key.
        gcs.records["a.jpg"] = b"hello"
        gcs.user_metadata["a.jpg"] = {
            MCPS_SOURCE_KEY: "s3",
            MCPS_CONTENT_SHA256_KEY: HASH_A,
        }

        manifest_path = tmp_path / "manifest.jsonl"
        with ManifestWriter(str(manifest_path)) as mw:
            stats = rep.replicate(plan, manifest_writer=mw)

        assert stats.replicate_skip_existing == 1
        assert stats.replicate == 0
        # No write to gcs.
        assert not any(c[0] == "write_bytes" for c in gcs.call_log)

        records = _manifest_records(manifest_path)
        skips = [r for r in records if r.action == Action.REPLICATE_SKIP]
        assert len(skips) == 1
        assert skips[0].result == Result.SKIPPED
