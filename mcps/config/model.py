"""Configuration data model for MultiCloud_Photo_Sync.

Frozen dataclasses for every section of the on-disk configuration plus the
top-level `Config`. Range and enum-style validation is performed in
`__post_init__` and surfaces as `mcps.errors.ConfigError` with the offending
field name. The model layer leaves `path=""` on every `ConfigError`; the
`Config_Parser` (task 11) is responsible for filling in the path and line
number when raising from a parse context.

Validates: Requirements 8.6, 8.7, 9.1, 9.4, 12.6, 16.1, 17.4, 19.3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from mcps.errors import ConfigError


# ---------------------------------------------------------------------------
# Allowed enum values, kept as module-level constants so tests and the parser
# can import them without re-deriving the literal sets.
# ---------------------------------------------------------------------------

SOURCE_KINDS: tuple[str, ...] = ("s3", "gcs", "google_drive")
REPLICATED_KINDS: frozenset[str] = frozenset({"s3", "gcs"})
ON_KEY_CONFLICT_VALUES: tuple[str, ...] = ("skip", "rename", "overwrite")
DELETE_PROPAGATION_VALUES: tuple[str, ...] = ("none", "soft", "hard")


def _check_int_range(value: int, *, field_name: str, low: int, high: int) -> None:
    """Raise ConfigError if `value` is not an int in the inclusive range [low, high].

    `bool` is rejected explicitly because `isinstance(True, int)` is True in
    Python and we never want a boolean silently accepted as a numeric setting.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(path="", field=field_name)
    if value < low or value > high:
        raise ConfigError(path="", field=field_name)


# ---------------------------------------------------------------------------
# SourceConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceConfig:
    """One configured Source — an S3 bucket, a GCS bucket, or a Drive folder."""

    name: str
    kind: Literal["s3", "gcs", "google_drive"]
    # s3 / gcs:
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    region: Optional[str] = None  # s3 only
    # google_drive:
    drive_root_folder_id: Optional[str] = None

    def __post_init__(self) -> None:
        # `name` must be a non-empty string.
        if not isinstance(self.name, str) or not self.name:
            raise ConfigError(path="", field="name")

        # `kind` must be one of the documented literals.
        if self.kind not in SOURCE_KINDS:
            raise ConfigError(path="", field="kind")

        if self.kind in REPLICATED_KINDS:
            # s3 and gcs both require a bucket; drive_root_folder_id is unused.
            if not isinstance(self.bucket, str) or not self.bucket:
                raise ConfigError(path="", field="bucket")
        elif self.kind == "google_drive":
            # Drive needs a root folder id; bucket/region/prefix are unused.
            if (
                not isinstance(self.drive_root_folder_id, str)
                or not self.drive_root_folder_id
            ):
                raise ConfigError(path="", field="drive_root_folder_id")


# ---------------------------------------------------------------------------
# ReplicationConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplicationConfig:
    """Replication policy: ordered pairs, conflict policy, deletion policy."""

    pairs: tuple[tuple[str, str], ...] = ()
    on_key_conflict: Literal["skip", "rename", "overwrite"] = "skip"
    fail_on_conflict: bool = False
    delete_propagation: Literal["none", "soft", "hard"] = "none"
    tombstone_retention_days: int = 30  # 1..3650
    fail_on_inconsistency: bool = False  # req 19.3

    def __post_init__(self) -> None:
        if self.on_key_conflict not in ON_KEY_CONFLICT_VALUES:
            raise ConfigError(path="", field="on_key_conflict")
        if self.delete_propagation not in DELETE_PROPAGATION_VALUES:
            raise ConfigError(path="", field="delete_propagation")
        if not isinstance(self.fail_on_conflict, bool):
            raise ConfigError(path="", field="fail_on_conflict")
        if not isinstance(self.fail_on_inconsistency, bool):
            raise ConfigError(path="", field="fail_on_inconsistency")
        _check_int_range(
            self.tombstone_retention_days,
            field_name="tombstone_retention_days",
            low=1,
            high=3650,
        )


# ---------------------------------------------------------------------------
# DuplicatesConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DuplicatesConfig:
    """Duplicate-resolution policy: canonical priority and quarantine retention."""

    canonical_source_priority: tuple[str, ...] = ()
    quarantine_retention_days: int = 30  # 1..3650

    def __post_init__(self) -> None:
        _check_int_range(
            self.quarantine_retention_days,
            field_name="quarantine_retention_days",
            low=1,
            high=3650,
        )


# ---------------------------------------------------------------------------
# PhotosConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PhotosConfig:
    """Drive pull-only configuration.

    The section is named 'photos' for forward compatibility with a future
    Google Photos backend; the current implementation reads from a Drive folder
    (see requirements introduction).
    """

    drive_source: Optional[str] = None       # name of a SourceConfig kind=google_drive
    drive_destination: Optional[str] = None  # name of a SourceConfig kind=s3 or gcs


# ---------------------------------------------------------------------------
# RetriesConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetriesConfig:
    """Retry decorator parameters; ranges enforced per design.md."""

    max_retries: int = 5            # 1..10
    initial_backoff_ms: int = 500   # 100..10000
    max_backoff_ms: int = 30000     # 1000..300000
    request_timeout_ms: int = 30000  # 1000..120000

    def __post_init__(self) -> None:
        _check_int_range(self.max_retries, field_name="max_retries", low=1, high=10)
        _check_int_range(
            self.initial_backoff_ms,
            field_name="initial_backoff_ms",
            low=100,
            high=10000,
        )
        _check_int_range(
            self.max_backoff_ms,
            field_name="max_backoff_ms",
            low=1000,
            high=300000,
        )
        _check_int_range(
            self.request_timeout_ms,
            field_name="request_timeout_ms",
            low=1000,
            high=120000,
        )


# ---------------------------------------------------------------------------
# RuntimeConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuntimeConfig:
    """Process-level runtime knobs: paths and concurrency."""

    catalog_path: str = "./mcps.catalog.jsonl"
    manifest_dir: str = "./manifests"
    max_concurrent_transfers: int = 4  # 1..64
    lock_path: Optional[str] = None    # defaults to <catalog_path>.lock at parser layer

    def __post_init__(self) -> None:
        _check_int_range(
            self.max_concurrent_transfers,
            field_name="max_concurrent_transfers",
            low=1,
            high=64,
        )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    """Top-level configuration aggregating all six sections."""

    sources: tuple[SourceConfig, ...]
    replication: ReplicationConfig
    duplicates: DuplicatesConfig
    photos: PhotosConfig
    retries: RetriesConfig
    runtime: RuntimeConfig

    def lookup_source(self, name: str) -> Optional[SourceConfig]:
        """Return the SourceConfig with the given name, or None if absent."""
        for source in self.sources:
            if source.name == name:
                return source
        return None

    def replicated_sources(self) -> tuple[SourceConfig, ...]:
        """Return the tuple of Sources whose kind participates in replication.

        Replicated_Sources are exactly the `s3` and `gcs` sources; `google_drive`
        is a Pull_Only_Source and is excluded.
        """
        return tuple(s for s in self.sources if s.kind in REPLICATED_KINDS)


__all__ = [
    "SOURCE_KINDS",
    "REPLICATED_KINDS",
    "ON_KEY_CONFLICT_VALUES",
    "DELETE_PROPAGATION_VALUES",
    "SourceConfig",
    "ReplicationConfig",
    "DuplicatesConfig",
    "PhotosConfig",
    "RetriesConfig",
    "RuntimeConfig",
    "Config",
]
