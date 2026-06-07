"""Unit tests for `mcps.manifest.model`.

Covers the `Action` and `Result` enum coverage, their `(str, Enum)` mixin
semantics (so ``Action.REPLICATE == "replicate"`` is true and JSON encodes
as the bare string), and the `ManifestRecord` frozen-dataclass invariants
documented in design.md.

Validates: Requirements 14.2, 15.1.
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, fields
from enum import Enum

import pytest

from mcps.manifest.model import Action, ManifestRecord, Result


# ---------------------------------------------------------------------------
# Action enum coverage
# ---------------------------------------------------------------------------

# The full mapping spelled out by design.md "Manifest_Record" section. Tests
# assert this is exactly the set of Action members; adding or removing a
# value here requires a spec update first.
EXPECTED_ACTION_VALUES: dict[str, str] = {
    "DISCOVERED": "discovered",
    "REPLICATE": "replicate",
    "REPLICATE_SKIP": "replicate-skip-existing",
    "LOOP_SKIP": "loop-skip",
    "SOURCE_TAGGED": "source-tagged",
    "HASH_RECOMPUTED": "hash-recomputed",
    "KEY_CONFLICT": "key-conflict",
    "OVERWRITE": "overwrite",
    "RENAME": "rename",
    "QUARANTINE": "quarantine",
    "PHYSICAL_DELETE": "physical-delete",
    "LAST_COPY_GUARD": "last-copy-protection",
    "TOMBSTONE": "tombstone",
    "DRIVE_SKIP_UNSUP": "drive-skip-unsupported",
    "DRIVE_SKIP_NDOC": "drive-skip-native-doc",
    "DRIVE_SKIP_EXIST": "drive-skip-existing",
    "DRIVE_IMPORT_OK": "drive-import-success",
    "DRIVE_DOWNLOAD_E": "drive-download-error",
    "DRIVE_WARN_TIME": "drive-warning-missing-created-time",
    "LIST_ERROR": "list-error",
    "HASH_ERROR": "hash-error",
    "RETRIES_EXHAUSTED": "retries-exhausted",
    "REPLICATION_ERROR": "replication-error",
    "SUMMARY": "summary",
}


def test_action_has_exactly_24_members():
    assert len(list(Action)) == 24
    assert len(EXPECTED_ACTION_VALUES) == 24


def test_action_member_names_match_design():
    assert {a.name for a in Action} == set(EXPECTED_ACTION_VALUES.keys())


def test_action_string_values_match_design_byte_for_byte():
    actual = {a.name: a.value for a in Action}
    assert actual == EXPECTED_ACTION_VALUES


def test_action_is_str_enum_so_equality_with_raw_string_works():
    """`Action` subclasses both `str` and `Enum`, so member == string holds."""
    assert isinstance(Action.REPLICATE, str)
    assert isinstance(Action.REPLICATE, Enum)
    assert Action.REPLICATE == "replicate"
    assert Action.SUMMARY == "summary"
    assert Action.DRIVE_WARN_TIME == "drive-warning-missing-created-time"


def test_action_serialises_as_bare_string_via_json_dumps():
    """``json.dumps(Action.X)`` must render as the bare string value, not as
    a structured ``{"value": "..."}`` object. This is what the JSONL
    Manifest writer relies on (req 14.2)."""
    assert json.dumps(Action.REPLICATE) == '"replicate"'
    assert json.dumps(Action.DRIVE_IMPORT_OK) == '"drive-import-success"'


# ---------------------------------------------------------------------------
# Result enum coverage
# ---------------------------------------------------------------------------


EXPECTED_RESULT_VALUES: dict[str, str] = {
    "SUCCESS": "success",
    "SKIPPED": "skipped",
    "QUARANTINED": "quarantined",
    "DELETED": "deleted",
    "PLANNED": "planned",
    "ERROR": "error",
}


def test_result_has_exactly_6_members():
    assert len(list(Result)) == 6
    assert len(EXPECTED_RESULT_VALUES) == 6


def test_result_member_names_match_design():
    assert {r.name for r in Result} == set(EXPECTED_RESULT_VALUES.keys())


def test_result_string_values_match_design_byte_for_byte():
    actual = {r.name: r.value for r in Result}
    assert actual == EXPECTED_RESULT_VALUES


def test_result_is_str_enum_so_equality_with_raw_string_works():
    assert isinstance(Result.SUCCESS, str)
    assert isinstance(Result.SUCCESS, Enum)
    assert Result.SUCCESS == "success"
    assert Result.PLANNED == "planned"
    assert Result.ERROR == "error"


def test_result_serialises_as_bare_string_via_json_dumps():
    assert json.dumps(Result.SUCCESS) == '"success"'
    assert json.dumps(Result.ERROR) == '"error"'


# ---------------------------------------------------------------------------
# ManifestRecord field shape
# ---------------------------------------------------------------------------


def test_manifest_record_has_exact_field_set_from_design():
    expected = [
        "timestamp",
        "run_id",
        "action",
        "result",
        "source",
        "target",
        "key",
        "content_hash",
        "size_bytes",
        "error",
        "extra",
    ]
    assert [f.name for f in fields(ManifestRecord)] == expected


def test_manifest_record_required_fields_have_no_default():
    """The first four fields are required; the rest default."""
    field_map = {f.name: f for f in fields(ManifestRecord)}
    from dataclasses import MISSING

    required = ["timestamp", "run_id", "action", "result"]
    for name in required:
        f = field_map[name]
        assert f.default is MISSING, f"{name} should be required"
        assert f.default_factory is MISSING, f"{name} should be required"


def test_manifest_record_optional_fields_default_to_none():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.SUMMARY,
        result=Result.SUCCESS,
    )
    assert rec.source is None
    assert rec.target is None
    assert rec.key is None
    assert rec.content_hash is None
    assert rec.size_bytes is None
    assert rec.error is None


def test_manifest_record_extra_defaults_to_empty_mapping():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.SUMMARY,
        result=Result.SUCCESS,
    )
    assert rec.extra == {}
    # And dict semantics so JSON encoding works.
    assert dict(rec.extra) == {}


def test_manifest_record_extra_default_is_not_shared_between_instances():
    """``field(default_factory=dict)`` produces a fresh dict per instance.

    If the implementation accidentally used a shared mutable default,
    mutating one instance's ``extra`` would leak into every other instance
    constructed without an explicit ``extra``.
    """
    a = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.DISCOVERED,
        result=Result.SUCCESS,
    )
    b = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.DISCOVERED,
        result=Result.SUCCESS,
    )
    assert a.extra is not b.extra


# ---------------------------------------------------------------------------
# ManifestRecord frozen / equality semantics
# ---------------------------------------------------------------------------


def test_manifest_record_is_frozen_against_required_field_mutation():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATE,
        result=Result.SUCCESS,
    )
    with pytest.raises(FrozenInstanceError):
        rec.timestamp = "2024-02-02T00:00:00.000Z"  # type: ignore[misc]


def test_manifest_record_is_frozen_against_optional_field_mutation():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATE,
        result=Result.SUCCESS,
    )
    with pytest.raises(FrozenInstanceError):
        rec.error = "boom"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        rec.size_bytes = 42  # type: ignore[misc]


def test_two_equal_manifest_records_compare_equal():
    a = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATE,
        result=Result.SUCCESS,
        source="s3-prod",
        target="gcs-archive",
        key="photos/IMG_0001.jpg",
        content_hash="a" * 64,
        size_bytes=1024,
        error=None,
        extra={"reason": "absent-on-target"},
    )
    b = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATE,
        result=Result.SUCCESS,
        source="s3-prod",
        target="gcs-archive",
        key="photos/IMG_0001.jpg",
        content_hash="a" * 64,
        size_bytes=1024,
        error=None,
        extra={"reason": "absent-on-target"},
    )
    assert a == b


def test_manifest_records_with_different_action_compare_unequal():
    base_kwargs = dict(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        result=Result.SUCCESS,
    )
    a = ManifestRecord(action=Action.REPLICATE, **base_kwargs)
    b = ManifestRecord(action=Action.OVERWRITE, **base_kwargs)
    assert a != b


# ---------------------------------------------------------------------------
# error field accepts None or str
# ---------------------------------------------------------------------------


def test_error_field_accepts_none():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATE,
        result=Result.SUCCESS,
        error=None,
    )
    assert rec.error is None


def test_error_field_accepts_string():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.REPLICATION_ERROR,
        result=Result.ERROR,
        error="size mismatch: expected 1024, observed 1023",
    )
    assert rec.error == "size mismatch: expected 1024, observed 1023"


# ---------------------------------------------------------------------------
# Construction by keyword and by mixing positional + keyword
# ---------------------------------------------------------------------------


def test_manifest_record_construction_by_keyword():
    rec = ManifestRecord(
        timestamp="2024-01-01T00:00:00.000Z",
        run_id="abcdef12",
        action=Action.DISCOVERED,
        result=Result.SUCCESS,
    )
    assert rec.action is Action.DISCOVERED
    assert rec.result is Result.SUCCESS


def test_manifest_record_construction_positional_required_fields():
    rec = ManifestRecord(
        "2024-01-01T00:00:00.000Z",
        "abcdef12",
        Action.DISCOVERED,
        Result.SUCCESS,
    )
    assert rec.timestamp == "2024-01-01T00:00:00.000Z"
    assert rec.run_id == "abcdef12"
    assert rec.action is Action.DISCOVERED
    assert rec.result is Result.SUCCESS
