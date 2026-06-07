"""Unit tests for `mcps.errors`.

Validates: Requirements 1.5, 3.6, 5.10, 6.5, 9.6, 10.8, 14.6, 16.5, 17.7-17.9,
18.6 (covered by the design's exit-code table and exception hierarchy).
"""

from __future__ import annotations

import pytest

from mcps.errors import (
    CatalogParseError,
    ColdStartListingFailed,
    ConfigError,
    CredentialError,
    ExitCode,
    LastCopyProtectionViolation,
    LegacyConfigDetected,
    LockConflict,
    ManifestWriteError,
    McpsError,
    NonTransientError,
    ReadOnlySourceError,
    ReplicationVerifyMismatch,
    RetriesExhausted,
)


# ---------------------------------------------------------------------------
# ExitCode integer values
# ---------------------------------------------------------------------------


def test_exit_code_table_matches_design():
    """Every exit code constant maps to the integer documented in design.md."""
    assert ExitCode.OK == 0
    assert ExitCode.RUN_HAD_ERRORS == 2
    assert ExitCode.CONFIG_INVALID == 64
    assert ExitCode.CATALOG_INVALID == 65
    assert ExitCode.LEGACY_CONFIG == 66
    assert ExitCode.MANIFEST_UNAVAILABLE == 67
    assert ExitCode.CREDENTIAL_FAILED == 71
    assert ExitCode.CONFLICT_FAILURE == 72
    assert ExitCode.LOCK_CONFLICT == 73
    assert ExitCode.INTERACTIVE_REQUIRED == 74
    assert ExitCode.DRIVE_ACCESS_FAILED == 75
    assert ExitCode.FIRST_PASS_REVIEW_REQUIRED == 76
    assert ExitCode.COLD_START_LISTING_FAILED == 77
    assert ExitCode.INCONSISTENCY_DETECTED == 78


def test_exit_code_has_exactly_fourteen_members():
    """The design table lists 14 distinct exit codes; nothing more, nothing less."""
    expected_values = {0, 2, 64, 65, 66, 67, 71, 72, 73, 74, 75, 76, 77, 78}
    assert {int(code) for code in ExitCode} == expected_values
    assert len(list(ExitCode)) == 14


def test_exit_code_is_intenum():
    """ExitCode values must compare equal to plain integers (used by sys.exit)."""
    assert int(ExitCode.LOCK_CONFLICT) == 73
    assert ExitCode.LOCK_CONFLICT == 73


# ---------------------------------------------------------------------------
# Base class behaviour
# ---------------------------------------------------------------------------


def test_mcps_error_is_exception_subclass():
    assert issubclass(McpsError, Exception)


def test_mcps_error_default_exit_code_is_run_had_errors():
    """Per design: the per-object loop default is RUN_HAD_ERRORS."""
    err = McpsError("boom")
    assert err.exit_code == ExitCode.RUN_HAD_ERRORS
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


@pytest.mark.parametrize(
    "exc_cls",
    [
        ConfigError,
        LegacyConfigDetected,
        CredentialError,
        CatalogParseError,
        LockConflict,
        RetriesExhausted,
        NonTransientError,
        ReplicationVerifyMismatch,
        ReadOnlySourceError,
        ManifestWriteError,
        LastCopyProtectionViolation,
        ColdStartListingFailed,
    ],
)
def test_every_subclass_descends_from_mcps_error(exc_cls):
    assert issubclass(exc_cls, McpsError)


# ---------------------------------------------------------------------------
# Per-subclass field signatures and exit-code mapping
# ---------------------------------------------------------------------------


def test_config_error_fields_and_exit_code():
    err = ConfigError(path="/etc/mcps.toml", line=12, field="replication.fail_on_inconsistency")
    assert err.path == "/etc/mcps.toml"
    assert err.line == 12
    assert err.field == "replication.fail_on_inconsistency"
    assert err.to_exit_code() == ExitCode.CONFIG_INVALID
    assert err.exit_code == 64


def test_config_error_optional_fields_default_to_none():
    err = ConfigError(path="/etc/mcps.toml")
    assert err.line is None
    assert err.field is None


def test_legacy_config_detected_fields_and_exit_code():
    err = LegacyConfigDetected(path="/work/config.ini")
    assert err.path == "/work/config.ini"
    assert err.to_exit_code() == ExitCode.LEGACY_CONFIG
    assert err.exit_code == 66


def test_credential_error_fields_and_exit_code():
    err = CredentialError(provider="aws", sources_tried=["env", "profile", "instance-role"])
    assert err.provider == "aws"
    # sources_tried is normalised to a tuple so it is hashable / immutable.
    assert err.sources_tried == ("env", "profile", "instance-role")
    assert err.to_exit_code() == ExitCode.CREDENTIAL_FAILED
    assert err.exit_code == 71


def test_catalog_parse_error_fields_and_exit_code():
    err = CatalogParseError(path="/var/mcps/catalog.jsonl", line=42)
    assert err.path == "/var/mcps/catalog.jsonl"
    assert err.line == 42
    assert err.to_exit_code() == ExitCode.CATALOG_INVALID
    assert err.exit_code == 65


def test_catalog_parse_error_line_optional():
    err = CatalogParseError(path="/var/mcps/catalog.jsonl")
    assert err.line is None


def test_lock_conflict_fields_and_exit_code():
    err = LockConflict(holder_pid=12345)
    assert err.holder_pid == 12345
    assert err.to_exit_code() == ExitCode.LOCK_CONFLICT
    assert err.exit_code == 73


def test_retries_exhausted_fields_and_exit_code():
    cause = RuntimeError("503 Service Unavailable")
    err = RetriesExhausted(operation="s3.list_objects_v2", last=cause, attempts=6)
    assert err.operation == "s3.list_objects_v2"
    assert err.last is cause
    assert err.attempts == 6
    # Per-object error → defaults to RUN_HAD_ERRORS.
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


def test_retries_exhausted_supports_keyword_only_subset():
    """Decorator-style usage: RetriesExhausted(last=e, attempts=attempt)."""
    cause = ValueError("nope")
    err = RetriesExhausted(last=cause, attempts=3)
    assert err.operation is None
    assert err.last is cause
    assert err.attempts == 3


def test_non_transient_error_fields_and_exit_code():
    err = NonTransientError(status=403, body="AccessDenied")
    assert err.status == 403
    assert err.body == "AccessDenied"
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


def test_replication_verify_mismatch_fields_and_exit_code():
    err = ReplicationVerifyMismatch(
        src="s3-prod",
        dst="gcs-archive",
        key="photos/2024/01/IMG_0001.jpg",
        expected="a" * 64,
        observed="b" * 64,
    )
    assert err.src == "s3-prod"
    assert err.dst == "gcs-archive"
    assert err.key == "photos/2024/01/IMG_0001.jpg"
    assert err.expected == "a" * 64
    assert err.observed == "b" * 64
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


def test_read_only_source_error_fields_and_exit_code():
    err = ReadOnlySourceError(adapter="google_drive", op="write_bytes")
    assert err.adapter == "google_drive"
    assert err.op == "write_bytes"
    # Per design: no specific exit code; surfaces as a per-record error.
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


def test_manifest_write_error_fields_and_exit_code():
    cause = OSError("disk full")
    err = ManifestWriteError(path="/var/mcps/manifest.jsonl", cause=cause)
    assert err.path == "/var/mcps/manifest.jsonl"
    assert err.cause is cause
    assert err.to_exit_code() == ExitCode.MANIFEST_UNAVAILABLE
    assert err.exit_code == 67


def test_manifest_write_error_cause_optional():
    err = ManifestWriteError(path="/var/mcps/manifest.jsonl")
    assert err.cause is None


def test_last_copy_protection_violation_fields_and_exit_code():
    err = LastCopyProtectionViolation(content_hash="c" * 64, source="s3-prod")
    assert err.content_hash == "c" * 64
    assert err.source == "s3-prod"
    assert err.to_exit_code() == ExitCode.RUN_HAD_ERRORS


def test_cold_start_listing_failed_fields_and_exit_code():
    cause = TimeoutError("listing timed out")
    err = ColdStartListingFailed(
        source_name="s3-prod",
        source_kind="s3",
        cause=cause,
    )
    assert err.source_name == "s3-prod"
    assert err.source_kind == "s3"
    assert err.cause is cause
    assert err.to_exit_code() == ExitCode.COLD_START_LISTING_FAILED
    assert err.exit_code == 77


def test_cold_start_listing_failed_cause_optional():
    err = ColdStartListingFailed(source_name="gcs-archive", source_kind="gcs")
    assert err.cause is None


# ---------------------------------------------------------------------------
# Exit-code mapping table (single source of truth for the CLI)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc, expected",
    [
        (ConfigError(path="/c"), ExitCode.CONFIG_INVALID),
        (LegacyConfigDetected(path="/c.ini"), ExitCode.LEGACY_CONFIG),
        (CredentialError(provider="aws", sources_tried=()), ExitCode.CREDENTIAL_FAILED),
        (CatalogParseError(path="/cat"), ExitCode.CATALOG_INVALID),
        (LockConflict(holder_pid=1), ExitCode.LOCK_CONFLICT),
        (RetriesExhausted(operation="op", last=None, attempts=1), ExitCode.RUN_HAD_ERRORS),
        (NonTransientError(status=400), ExitCode.RUN_HAD_ERRORS),
        (
            ReplicationVerifyMismatch(
                src="a", dst="b", key="k", expected="x" * 64, observed="y" * 64
            ),
            ExitCode.RUN_HAD_ERRORS,
        ),
        (ReadOnlySourceError(adapter="drive", op="delete"), ExitCode.RUN_HAD_ERRORS),
        (ManifestWriteError(path="/m"), ExitCode.MANIFEST_UNAVAILABLE),
        (
            LastCopyProtectionViolation(content_hash="z" * 64, source="s3-prod"),
            ExitCode.RUN_HAD_ERRORS,
        ),
        (
            ColdStartListingFailed(source_name="s3-prod", source_kind="s3"),
            ExitCode.COLD_START_LISTING_FAILED,
        ),
    ],
)
def test_to_exit_code_returns_documented_value(exc, expected):
    assert exc.to_exit_code() == expected


# ---------------------------------------------------------------------------
# Errors are raisable and catchable through the McpsError base class
# ---------------------------------------------------------------------------


def test_subclasses_raise_and_catch_via_base():
    """The CLI catches `McpsError` once and dispatches via to_exit_code()."""
    with pytest.raises(McpsError) as info:
        raise LockConflict(holder_pid=999)
    assert info.value.to_exit_code() == ExitCode.LOCK_CONFLICT
