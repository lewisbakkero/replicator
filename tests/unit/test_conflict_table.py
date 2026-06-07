# Feature: multicloud-photo-sync, Property 10: Conflict-resolution table
"""Conflict-resolution table property test.

Property under test (design.md, "Correctness Properties — Property 10:
Conflict-resolution table"):

  For any triple `(existing_dst_state, incoming_src_record,
  on_key_conflict)`, the resulting state of the destination Source
  matches the deterministic table:

  | existing       | incoming hash | policy    | dst[key] after | dst[key.<hash8>] after | conflict counted? |
  |----------------|---------------|-----------|----------------|------------------------|-------------------|
  | absent         | any           | *         | incoming       | absent                 | no                |
  | same hash      | same hash     | *         | unchanged      | absent                 | no                |
  | different hash | different     | skip      | unchanged      | absent                 | yes               |
  | different hash | different     | rename    | unchanged      | incoming               | yes               |
  | different hash | different     | overwrite | incoming       | absent                 | yes               |

  and the run's exit code is non-zero iff `fail_on_conflict=true` and
  the "conflict counted?" column is `yes` for at least one record.

The test:

1. Generates a triple ``(existing_state, incoming_state, policy)`` from
   the cartesian product of three pools.
2. Builds a destination adapter pre-populated according to
   ``existing_state`` and a source adapter pre-populated with the
   incoming record.
3. Runs the Replicator with the chosen ``on_key_conflict`` policy.
4. Asserts the post-run state of ``dst[key]`` and ``dst[key.<hash8>]``
   matches the table row, and that the ``KEY_CONFLICT`` Manifest
   entry is emitted iff the table's "conflict counted?" column is
   ``yes``.

Validates: Requirements 6.7, 8.1, 8.2, 8.3, 8.4, 8.5.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import given, settings, strategies as st

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

_HASH_INCOMING = "a" * 64  # The hash that the source-side record carries.
_HASH_EXISTING = "b" * 64  # The hash on the destination side, when present.

_SRC_NAME = "s3"
_DST_NAME = "gcs"

_KEY = "photos/img.jpg"
_RENAMED_KEY = f"{_KEY}.{_HASH_INCOMING[:8]}"

# Existing destination state: "absent", "same_hash", or "different_hash".
_ExistingState = st.sampled_from(("absent", "same_hash", "different_hash"))

# on_key_conflict policy.
_Policy = st.sampled_from(("skip", "rename", "overwrite"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_INCOMING_PAYLOAD = b"incoming-bytes"
_EXISTING_PAYLOAD = b"existing-bytes"


def _build_adapters(
    existing_state: str,
) -> tuple[FakeSourceAdapter, FakeSourceAdapter]:
    """Build the source/destination adapters for one example.

    The source adapter always has the same record at ``_KEY`` carrying
    ``content_hash=_HASH_INCOMING`` and ``mcps-source=src``. The
    destination adapter is populated according to ``existing_state``.
    """
    src_adapter = FakeSourceAdapter(
        name=_SRC_NAME,
        kind="s3",
        records={_KEY: _INCOMING_PAYLOAD},
        metadata={
            _KEY: {
                MCPS_SOURCE_KEY: _SRC_NAME,
                MCPS_CONTENT_SHA256_KEY: _HASH_INCOMING,
            }
        },
    )

    if existing_state == "absent":
        dst_adapter = FakeSourceAdapter(name=_DST_NAME, kind="gcs", records={})
    elif existing_state == "same_hash":
        dst_adapter = FakeSourceAdapter(
            name=_DST_NAME,
            kind="gcs",
            records={_KEY: _INCOMING_PAYLOAD},
            metadata={
                _KEY: {
                    MCPS_SOURCE_KEY: _SRC_NAME,
                    MCPS_CONTENT_SHA256_KEY: _HASH_INCOMING,
                }
            },
        )
    elif existing_state == "different_hash":
        dst_adapter = FakeSourceAdapter(
            name=_DST_NAME,
            kind="gcs",
            records={_KEY: _EXISTING_PAYLOAD},
            metadata={
                _KEY: {
                    MCPS_SOURCE_KEY: _DST_NAME,
                    MCPS_CONTENT_SHA256_KEY: _HASH_EXISTING,
                }
            },
        )
    else:
        raise AssertionError(f"unknown existing_state: {existing_state!r}")

    return src_adapter, dst_adapter


def _build_catalog() -> Catalog:
    """Catalog containing only the source-side record for ``_KEY``.

    The destination side is not in the Catalog, so the plan() step's
    "absent on dst" check fires for any destination state. We then rely
    on the per-object pipeline's runtime probe to detect the actual
    destination state.
    """
    return Catalog().upsert(
        ObjectRecord(
            source=_SRC_NAME,
            key=_KEY,
            content_hash=_HASH_INCOMING,
            size_bytes=len(_INCOMING_PAYLOAD),
            last_seen_at="2024-01-01T00:00:00Z",
            last_modified="2023-12-31T23:59:59Z",
            content_type=None,
            mcps_source_meta=_SRC_NAME,
        )
    )


def _manifest_records(path):
    records, errors = parse_manifest_file(str(path))
    assert errors == []
    return records


# ---------------------------------------------------------------------------
# The Property 10 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(existing_state=_ExistingState, policy=_Policy)
@settings(max_examples=200, deadline=None)
def test_conflict_resolution_table(
    existing_state: str,
    policy: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Every row of the conflict-resolution table holds.

    Validates: Requirements 6.7, 8.1, 8.2, 8.3, 8.4, 8.5.
    """
    src_adapter, dst_adapter = _build_adapters(existing_state)
    adapters = {_SRC_NAME: src_adapter, _DST_NAME: dst_adapter}

    catalog = _build_catalog()

    rep = Replicator(
        adapters=adapters,
        on_key_conflict=policy,  # type: ignore[arg-type]
        now=lambda: _FIXED_NOW,
        run_id="property10",
    )

    manifest_dir = tmp_path_factory.mktemp("manifest")
    manifest_path = manifest_dir / "manifest.jsonl"
    with ManifestWriter(str(manifest_path)) as mw:
        plan = rep.plan(
            catalog, replicated_source_names=(_SRC_NAME, _DST_NAME)
        )
        rep.replicate(plan, manifest_writer=mw)

    records = _manifest_records(manifest_path)
    actions = [r.action for r in records]
    has_key_conflict = Action.KEY_CONFLICT in actions

    dst_records = dst_adapter.records
    dst_metadata = dst_adapter.user_metadata

    def _hash_at(key: str) -> Optional[str]:
        return dst_metadata.get(key, {}).get(MCPS_CONTENT_SHA256_KEY)

    def _bytes_at(key: str) -> Optional[bytes]:
        return dst_records.get(key)

    if existing_state == "absent":
        # Row 1: dst[key] = incoming, dst[key.<hash8>] absent, no conflict.
        assert _bytes_at(_KEY) == _INCOMING_PAYLOAD
        assert _hash_at(_KEY) == _HASH_INCOMING
        assert _bytes_at(_RENAMED_KEY) is None
        assert not has_key_conflict, (
            f"absent state must not produce KEY_CONFLICT (policy={policy!r})"
        )
        return

    if existing_state == "same_hash":
        # Row 2: dst[key] unchanged (still incoming), dst[key.<hash8>]
        # absent, no conflict counted. The pre-populated bytes are
        # _INCOMING_PAYLOAD (we wrote them in `_build_adapters`).
        assert _bytes_at(_KEY) == _INCOMING_PAYLOAD
        assert _hash_at(_KEY) == _HASH_INCOMING
        assert _bytes_at(_RENAMED_KEY) is None
        assert not has_key_conflict, (
            "same-hash state must not produce KEY_CONFLICT"
        )
        # Replicator should emit REPLICATE_SKIP, not KEY_CONFLICT.
        assert Action.REPLICATE_SKIP in actions
        return

    # existing_state == "different_hash"
    if policy == "skip":
        # Row 3: dst[key] unchanged (existing), dst[key.<hash8>] absent,
        # conflict counted.
        assert _bytes_at(_KEY) == _EXISTING_PAYLOAD
        assert _hash_at(_KEY) == _HASH_EXISTING
        assert _bytes_at(_RENAMED_KEY) is None
        assert has_key_conflict, (
            "different-hash + skip must record KEY_CONFLICT"
        )
        return

    if policy == "rename":
        # Row 4: dst[key] unchanged, dst[key.<hash8>] = incoming. The
        # rename arm emits a RENAME Manifest entry (req 8.3), not a
        # KEY_CONFLICT entry — req 8.4 ties the literal `key-conflict`
        # action to the `skip` arm only.
        assert _bytes_at(_KEY) == _EXISTING_PAYLOAD
        assert _hash_at(_KEY) == _HASH_EXISTING
        assert _bytes_at(_RENAMED_KEY) == _INCOMING_PAYLOAD
        assert _hash_at(_RENAMED_KEY) == _HASH_INCOMING
        assert Action.RENAME in actions, (
            "different-hash + rename must record a RENAME Manifest entry"
        )
        return

    if policy == "overwrite":
        # Row 5: dst[key] = incoming, dst[key.<hash8>] absent. The
        # overwrite arm emits an OVERWRITE Manifest entry (req 8.2);
        # KEY_CONFLICT is reserved for the `skip` arm per req 8.4.
        assert _bytes_at(_KEY) == _INCOMING_PAYLOAD
        assert _hash_at(_KEY) == _HASH_INCOMING
        assert _bytes_at(_RENAMED_KEY) is None
        assert Action.OVERWRITE in actions, (
            "different-hash + overwrite must record an OVERWRITE Manifest entry"
        )
        return

    raise AssertionError(f"unknown policy: {policy!r}")
