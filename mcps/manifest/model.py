"""Data models for the per-run JSONL Manifest: `Action`, `Result`, and
`ManifestRecord`.

Every Sync_Run appends one `ManifestRecord` per planned or executed decision
to the on-disk Manifest file (UTF-8 LF-terminated JSONL). The streaming
parser/printer/writer pair lives in `mcps.manifest.{parser,printer,writer}`
(task 7); this module owns only the in-memory shape and the enum string
mappings used for serialisation.

Design references (`design.md` "Manifest_Record" section):

* `Action` and `Result` subclass `(str, Enum)` so that
  ``json.dumps(Action.REPLICATE)`` renders as the bare string ``"replicate"``
  without a custom encoder. This is required for JSONL round-trip
  (req 14.2, 15.1).
* `ManifestRecord` is a frozen dataclass: every Sync_Run treats records as
  immutable values, which makes property tests (task 7) trivial and
  guarantees the JSONL writer never observes a partially-mutated record.
* The four required fields ``timestamp``, ``run_id``, ``action``, ``result``
  are positional/keyword without defaults; every other field defaults to
  ``None`` (or an empty mapping for ``extra``) so unrelated event types do
  not have to populate fields they do not own.

Validates: Requirements 14.2, 15.1.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional


class Action(str, Enum):
    """Every Manifest action emitted across the design.

    The string values are the on-disk JSON values; they are stable and
    operators may match them with downstream tooling, so the spelling is
    fixed by design.md and must not be altered without a spec update.
    """

    DISCOVERED = "discovered"
    REPLICATE = "replicate"
    REPLICATE_SKIP = "replicate-skip-existing"
    LOOP_SKIP = "loop-skip"
    SOURCE_TAGGED = "source-tagged"
    HASH_RECOMPUTED = "hash-recomputed"
    KEY_CONFLICT = "key-conflict"
    OVERWRITE = "overwrite"
    RENAME = "rename"
    QUARANTINE = "quarantine"
    PHYSICAL_DELETE = "physical-delete"
    LAST_COPY_GUARD = "last-copy-protection"
    TOMBSTONE = "tombstone"
    DRIVE_SKIP_UNSUP = "drive-skip-unsupported"
    DRIVE_SKIP_NDOC = "drive-skip-native-doc"
    DRIVE_SKIP_EXIST = "drive-skip-existing"
    DRIVE_IMPORT_OK = "drive-import-success"
    DRIVE_DOWNLOAD_E = "drive-download-error"
    DRIVE_WARN_TIME = "drive-warning-missing-created-time"
    LIST_ERROR = "list-error"
    HASH_ERROR = "hash-error"
    RETRIES_EXHAUSTED = "retries-exhausted"
    REPLICATION_ERROR = "replication-error"
    SUMMARY = "summary"


class Result(str, Enum):
    """Outcome classification for a single Manifest entry.

    ``PLANNED`` is reserved for `--dry-run` invocations where the action
    describes work that *would* run under `--apply`. ``ERROR`` is the only
    value for which a non-empty ``ManifestRecord.error`` field is expected.
    """

    SUCCESS = "success"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"
    DELETED = "deleted"
    PLANNED = "planned"
    ERROR = "error"


@dataclass(frozen=True)
class ManifestRecord:
    """One line of the on-disk Manifest, in memory.

    Required positional/keyword fields:

    * ``timestamp``: ISO-8601 UTC with millisecond precision and trailing Z.
    * ``run_id``: UUIDv4 hex (>= 8 chars) shared across every record in the
      same Sync_Run.
    * ``action``: the `Action` enum value classifying this record.
    * ``result``: the `Result` enum value classifying the outcome.

    Optional fields default to ``None`` (or an empty mapping for ``extra``):

    * ``source``: originating Source name (``None`` is valid for SUMMARY).
    * ``target``: destination Source name for replicate/drive-import flows.
    * ``key``: provider key the action operated on.
    * ``content_hash``: 64-char lowercase hex SHA-256 (when applicable).
    * ``size_bytes``: byte size of the Object the action operated on.
    * ``error``: free-form error string. Set if and only if
      ``result == Result.ERROR``.
    * ``extra``: action-specific structured payload (e.g. expected/observed
      hashes for ``REPLICATION_ERROR``). Defaults to an empty dict via
      ``default_factory`` so instances never share a mutable default; the
      surrounding dataclass is frozen so the mapping itself is not reassigned
      after construction.
    """

    timestamp: str
    run_id: str
    action: Action
    result: Result
    source: Optional[str] = None
    target: Optional[str] = None
    key: Optional[str] = None
    content_hash: Optional[str] = None
    size_bytes: Optional[int] = None
    error: Optional[str] = None
    extra: Mapping[str, str] = field(default_factory=dict)


__all__ = ["Action", "Result", "ManifestRecord"]
