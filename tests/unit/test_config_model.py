"""Unit tests for `mcps.config.model`.

Covers boundary validation per field, enum-style rejection, default-value
round-trip, and the `Config.lookup_source` / `Config.replicated_sources`
helpers documented in design.md.

Validates: Requirements 8.6, 8.7, 9.1, 9.4, 12.6, 16.1, 17.4, 19.3.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from mcps.config.model import (
    Config,
    DuplicatesConfig,
    PhotosConfig,
    ReplicationConfig,
    RetriesConfig,
    RuntimeConfig,
    SourceConfig,
)
from mcps.errors import ConfigError, ExitCode


# ---------------------------------------------------------------------------
# Helpers — terse builders that produce valid sub-configs by default. Tests
# that exercise validation pass overrides via kwargs.
# ---------------------------------------------------------------------------


def make_s3_source(**overrides) -> SourceConfig:
    defaults = dict(name="s3-prod", kind="s3", bucket="prod-bucket", region="us-east-1")
    defaults.update(overrides)
    return SourceConfig(**defaults)


def make_gcs_source(**overrides) -> SourceConfig:
    defaults = dict(name="gcs-archive", kind="gcs", bucket="archive-bucket")
    defaults.update(overrides)
    return SourceConfig(**defaults)


def make_drive_source(**overrides) -> SourceConfig:
    defaults = dict(
        name="drive-import",
        kind="google_drive",
        drive_root_folder_id="0BABCDEF",
    )
    defaults.update(overrides)
    return SourceConfig(**defaults)


def make_config(
    *,
    sources: tuple[SourceConfig, ...] | None = None,
    replication: ReplicationConfig | None = None,
    duplicates: DuplicatesConfig | None = None,
    photos: PhotosConfig | None = None,
    retries: RetriesConfig | None = None,
    runtime: RuntimeConfig | None = None,
) -> Config:
    return Config(
        sources=sources
        if sources is not None
        else (make_s3_source(), make_gcs_source(), make_drive_source()),
        replication=replication if replication is not None else ReplicationConfig(),
        duplicates=duplicates if duplicates is not None else DuplicatesConfig(),
        photos=photos if photos is not None else PhotosConfig(),
        retries=retries if retries is not None else RetriesConfig(),
        runtime=runtime if runtime is not None else RuntimeConfig(),
    )


# ---------------------------------------------------------------------------
# SourceConfig
# ---------------------------------------------------------------------------


class TestSourceConfig:
    def test_s3_source_with_required_fields_succeeds(self):
        src = SourceConfig(name="s3-prod", kind="s3", bucket="prod-bucket")
        assert src.name == "s3-prod"
        assert src.kind == "s3"
        assert src.bucket == "prod-bucket"
        # Optional fields default to None.
        assert src.prefix is None
        assert src.region is None
        assert src.drive_root_folder_id is None

    def test_gcs_source_with_required_fields_succeeds(self):
        src = SourceConfig(name="gcs-archive", kind="gcs", bucket="archive-bucket")
        assert src.kind == "gcs"
        assert src.bucket == "archive-bucket"

    def test_google_drive_source_with_root_folder_id_succeeds(self):
        src = SourceConfig(
            name="drive-import", kind="google_drive", drive_root_folder_id="0BABCDEF"
        )
        assert src.kind == "google_drive"
        assert src.drive_root_folder_id == "0BABCDEF"
        # Bucket and region remain None for drive sources.
        assert src.bucket is None
        assert src.region is None

    def test_unknown_kind_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="weird", kind="azure")  # type: ignore[arg-type]
        assert info.value.field == "kind"
        assert info.value.to_exit_code() == ExitCode.CONFIG_INVALID

    def test_empty_name_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="", kind="s3", bucket="prod-bucket")
        assert info.value.field == "name"

    def test_s3_source_without_bucket_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="s3-prod", kind="s3")
        assert info.value.field == "bucket"

    def test_s3_source_with_empty_bucket_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="s3-prod", kind="s3", bucket="")
        assert info.value.field == "bucket"

    def test_gcs_source_without_bucket_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="gcs-archive", kind="gcs")
        assert info.value.field == "bucket"

    def test_google_drive_source_without_root_folder_id_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="drive-import", kind="google_drive")
        assert info.value.field == "drive_root_folder_id"

    def test_google_drive_source_with_empty_root_folder_id_is_rejected(self):
        with pytest.raises(ConfigError) as info:
            SourceConfig(
                name="drive-import", kind="google_drive", drive_root_folder_id=""
            )
        assert info.value.field == "drive_root_folder_id"

    def test_source_config_is_frozen(self):
        src = make_s3_source()
        with pytest.raises(FrozenInstanceError):
            src.bucket = "different"  # type: ignore[misc]

    def test_config_error_uses_empty_path_at_model_layer(self):
        """The model layer never knows the file path; the parser fills it in."""
        with pytest.raises(ConfigError) as info:
            SourceConfig(name="s3-prod", kind="s3")
        assert info.value.path == ""


# ---------------------------------------------------------------------------
# ReplicationConfig
# ---------------------------------------------------------------------------


class TestReplicationConfig:
    def test_defaults_match_design(self):
        cfg = ReplicationConfig()
        assert cfg.pairs == ()
        assert cfg.on_key_conflict == "skip"
        assert cfg.fail_on_conflict is False
        assert cfg.delete_propagation == "none"
        assert cfg.tombstone_retention_days == 30
        assert cfg.fail_on_inconsistency is False

    @pytest.mark.parametrize("value", ["skip", "rename", "overwrite"])
    def test_on_key_conflict_accepts_documented_values(self, value):
        cfg = ReplicationConfig(on_key_conflict=value)
        assert cfg.on_key_conflict == value

    @pytest.mark.parametrize("value", ["", "merge", "SKIP", "delete", None, 1])
    def test_on_key_conflict_rejects_anything_else(self, value):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(on_key_conflict=value)  # type: ignore[arg-type]
        assert info.value.field == "on_key_conflict"

    @pytest.mark.parametrize("value", ["none", "soft", "hard"])
    def test_delete_propagation_accepts_documented_values(self, value):
        cfg = ReplicationConfig(delete_propagation=value)
        assert cfg.delete_propagation == value

    @pytest.mark.parametrize("value", ["", "yes", "tombstone", "HARD", None])
    def test_delete_propagation_rejects_anything_else(self, value):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(delete_propagation=value)  # type: ignore[arg-type]
        assert info.value.field == "delete_propagation"

    @pytest.mark.parametrize("value", [1, 30, 365, 3650])
    def test_tombstone_retention_days_accepts_in_range(self, value):
        cfg = ReplicationConfig(tombstone_retention_days=value)
        assert cfg.tombstone_retention_days == value

    @pytest.mark.parametrize("value", [-1, 0, 3651, 100_000])
    def test_tombstone_retention_days_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(tombstone_retention_days=value)
        assert info.value.field == "tombstone_retention_days"

    def test_tombstone_retention_days_rejects_non_integer(self):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(tombstone_retention_days="30")  # type: ignore[arg-type]
        assert info.value.field == "tombstone_retention_days"

    def test_tombstone_retention_days_rejects_bool(self):
        # `bool` is a subclass of `int` in Python; reject it explicitly so a
        # YAML/TOML `true` is never silently accepted as 1.
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(tombstone_retention_days=True)  # type: ignore[arg-type]
        assert info.value.field == "tombstone_retention_days"

    def test_fail_on_conflict_must_be_bool(self):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(fail_on_conflict="true")  # type: ignore[arg-type]
        assert info.value.field == "fail_on_conflict"

    def test_fail_on_inconsistency_must_be_bool(self):
        with pytest.raises(ConfigError) as info:
            ReplicationConfig(fail_on_inconsistency="yes")  # type: ignore[arg-type]
        assert info.value.field == "fail_on_inconsistency"

    def test_fail_on_inconsistency_can_be_set_true(self):
        cfg = ReplicationConfig(fail_on_inconsistency=True)
        assert cfg.fail_on_inconsistency is True


# ---------------------------------------------------------------------------
# DuplicatesConfig
# ---------------------------------------------------------------------------


class TestDuplicatesConfig:
    def test_defaults_match_design(self):
        cfg = DuplicatesConfig()
        assert cfg.canonical_source_priority == ()
        assert cfg.quarantine_retention_days == 30

    @pytest.mark.parametrize("value", [1, 30, 365, 3650])
    def test_quarantine_retention_days_accepts_in_range(self, value):
        cfg = DuplicatesConfig(quarantine_retention_days=value)
        assert cfg.quarantine_retention_days == value

    @pytest.mark.parametrize("value", [-1, 0, 3651, 100_000])
    def test_quarantine_retention_days_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            DuplicatesConfig(quarantine_retention_days=value)
        assert info.value.field == "quarantine_retention_days"

    def test_quarantine_retention_days_rejects_bool(self):
        with pytest.raises(ConfigError) as info:
            DuplicatesConfig(quarantine_retention_days=True)  # type: ignore[arg-type]
        assert info.value.field == "quarantine_retention_days"


# ---------------------------------------------------------------------------
# PhotosConfig
# ---------------------------------------------------------------------------


class TestPhotosConfig:
    def test_defaults_to_none(self):
        cfg = PhotosConfig()
        assert cfg.drive_source is None
        assert cfg.drive_destination is None

    def test_accepts_named_sources(self):
        cfg = PhotosConfig(drive_source="drive-import", drive_destination="s3-prod")
        assert cfg.drive_source == "drive-import"
        assert cfg.drive_destination == "s3-prod"


# ---------------------------------------------------------------------------
# RetriesConfig
# ---------------------------------------------------------------------------


class TestRetriesConfig:
    def test_defaults_match_design(self):
        cfg = RetriesConfig()
        assert cfg.max_retries == 5
        assert cfg.initial_backoff_ms == 500
        assert cfg.max_backoff_ms == 30000
        assert cfg.request_timeout_ms == 30000

    # --- max_retries 1..10 ---------------------------------------------------

    @pytest.mark.parametrize("value", [1, 5, 10])
    def test_max_retries_accepts_in_range(self, value):
        assert RetriesConfig(max_retries=value).max_retries == value

    @pytest.mark.parametrize("value", [-1, 0, 11, 1_000])
    def test_max_retries_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            RetriesConfig(max_retries=value)
        assert info.value.field == "max_retries"

    # --- initial_backoff_ms 100..10_000 -------------------------------------

    @pytest.mark.parametrize("value", [100, 500, 10_000])
    def test_initial_backoff_ms_accepts_in_range(self, value):
        assert RetriesConfig(initial_backoff_ms=value).initial_backoff_ms == value

    @pytest.mark.parametrize("value", [0, 99, 10_001, 1_000_000])
    def test_initial_backoff_ms_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            RetriesConfig(initial_backoff_ms=value)
        assert info.value.field == "initial_backoff_ms"

    # --- max_backoff_ms 1_000..300_000 --------------------------------------

    @pytest.mark.parametrize("value", [1_000, 30_000, 300_000])
    def test_max_backoff_ms_accepts_in_range(self, value):
        assert RetriesConfig(max_backoff_ms=value).max_backoff_ms == value

    @pytest.mark.parametrize("value", [0, 999, 300_001, 10_000_000])
    def test_max_backoff_ms_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            RetriesConfig(max_backoff_ms=value)
        assert info.value.field == "max_backoff_ms"

    # --- request_timeout_ms 1_000..120_000 ----------------------------------

    @pytest.mark.parametrize("value", [1_000, 30_000, 120_000])
    def test_request_timeout_ms_accepts_in_range(self, value):
        assert RetriesConfig(request_timeout_ms=value).request_timeout_ms == value

    @pytest.mark.parametrize("value", [0, 999, 120_001, 10_000_000])
    def test_request_timeout_ms_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            RetriesConfig(request_timeout_ms=value)
        assert info.value.field == "request_timeout_ms"

    def test_retries_rejects_bool_for_int_field(self):
        with pytest.raises(ConfigError) as info:
            RetriesConfig(max_retries=True)  # type: ignore[arg-type]
        assert info.value.field == "max_retries"


# ---------------------------------------------------------------------------
# RuntimeConfig
# ---------------------------------------------------------------------------


class TestRuntimeConfig:
    def test_defaults_match_design(self):
        cfg = RuntimeConfig()
        assert cfg.catalog_path == "./mcps.catalog.jsonl"
        assert cfg.manifest_dir == "./manifests"
        assert cfg.max_concurrent_transfers == 4
        assert cfg.lock_path is None

    @pytest.mark.parametrize("value", [1, 4, 32, 64])
    def test_max_concurrent_transfers_accepts_in_range(self, value):
        assert (
            RuntimeConfig(max_concurrent_transfers=value).max_concurrent_transfers
            == value
        )

    @pytest.mark.parametrize("value", [-1, 0, 65, 10_000])
    def test_max_concurrent_transfers_rejects_out_of_range(self, value):
        with pytest.raises(ConfigError) as info:
            RuntimeConfig(max_concurrent_transfers=value)
        assert info.value.field == "max_concurrent_transfers"

    def test_max_concurrent_transfers_rejects_bool(self):
        with pytest.raises(ConfigError) as info:
            RuntimeConfig(max_concurrent_transfers=True)  # type: ignore[arg-type]
        assert info.value.field == "max_concurrent_transfers"

    def test_lock_path_can_be_explicit_string(self):
        cfg = RuntimeConfig(lock_path="/var/run/mcps.lock")
        assert cfg.lock_path == "/var/run/mcps.lock"


# ---------------------------------------------------------------------------
# Config (top-level helpers)
# ---------------------------------------------------------------------------


class TestConfig:
    def test_construction_with_defaults_succeeds(self):
        cfg = make_config()
        assert isinstance(cfg.replication, ReplicationConfig)
        assert isinstance(cfg.duplicates, DuplicatesConfig)
        assert isinstance(cfg.photos, PhotosConfig)
        assert isinstance(cfg.retries, RetriesConfig)
        assert isinstance(cfg.runtime, RuntimeConfig)

    def test_lookup_source_returns_named_source(self):
        s3 = make_s3_source()
        gcs = make_gcs_source()
        drive = make_drive_source()
        cfg = make_config(sources=(s3, gcs, drive))

        assert cfg.lookup_source("s3-prod") is s3
        assert cfg.lookup_source("gcs-archive") is gcs
        assert cfg.lookup_source("drive-import") is drive

    def test_lookup_source_returns_none_for_unknown_name(self):
        cfg = make_config()
        assert cfg.lookup_source("does-not-exist") is None

    def test_lookup_source_returns_none_on_empty_sources(self):
        cfg = make_config(sources=())
        assert cfg.lookup_source("s3-prod") is None

    def test_replicated_sources_excludes_google_drive(self):
        s3 = make_s3_source()
        gcs = make_gcs_source()
        drive = make_drive_source()
        cfg = make_config(sources=(s3, gcs, drive))

        result = cfg.replicated_sources()
        assert isinstance(result, tuple)
        assert result == (s3, gcs)
        # Drive source is excluded.
        assert drive not in result

    def test_replicated_sources_preserves_input_order(self):
        gcs = make_gcs_source()
        drive = make_drive_source()
        s3 = make_s3_source()
        cfg = make_config(sources=(drive, gcs, s3))

        # drive is filtered out; gcs and s3 keep their relative order.
        assert cfg.replicated_sources() == (gcs, s3)

    def test_replicated_sources_empty_when_only_drive_configured(self):
        cfg = make_config(sources=(make_drive_source(),))
        assert cfg.replicated_sources() == ()

    def test_replicated_sources_empty_when_no_sources_configured(self):
        cfg = make_config(sources=())
        assert cfg.replicated_sources() == ()

    def test_config_is_frozen(self):
        cfg = make_config()
        with pytest.raises(FrozenInstanceError):
            cfg.sources = ()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Round-trip of documented defaults across the whole Config tree
# ---------------------------------------------------------------------------


def test_documented_defaults_round_trip():
    """Constructing every section with no args yields the documented defaults."""
    rep = ReplicationConfig()
    dup = DuplicatesConfig()
    pho = PhotosConfig()
    ret = RetriesConfig()
    run = RuntimeConfig()

    assert (rep.pairs, rep.on_key_conflict, rep.fail_on_conflict) == ((), "skip", False)
    assert (rep.delete_propagation, rep.tombstone_retention_days) == ("none", 30)
    assert rep.fail_on_inconsistency is False

    assert (dup.canonical_source_priority, dup.quarantine_retention_days) == ((), 30)

    assert (pho.drive_source, pho.drive_destination) == (None, None)

    assert (ret.max_retries, ret.initial_backoff_ms) == (5, 500)
    assert (ret.max_backoff_ms, ret.request_timeout_ms) == (30000, 30000)

    assert run.catalog_path == "./mcps.catalog.jsonl"
    assert run.manifest_dir == "./manifests"
    assert run.max_concurrent_transfers == 4
    assert run.lock_path is None
