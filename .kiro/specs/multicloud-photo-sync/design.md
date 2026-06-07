# Design Document

## Overview

`MultiCloud_Photo_Sync` (mcps) is a refactor of the existing `uploader.py` / `delete.py` scripts into a single Python 3 CLI that performs deduplicated, bidirectional replication between Amazon S3 and Google Cloud Storage, plus pull-only import from a Google Drive folder. A single invocation acquires a writer lock, lists every configured Source in parallel, computes Content_Hashes (preferring cached values from the on-disk Catalog and from `mcps-content-sha256` object metadata), detects cross-source duplicates, optionally quarantines non-canonical copies, replicates missing Content_Hashes to peer Replicated_Sources with metadata-based loop prevention, imports new Drive items into a configured destination, persists the updated Catalog, and emits a per-run JSONL Manifest.

The design replaces the current weak points of `uploader.py`/`delete.py`:

| Current behaviour | New behaviour |
|---|---|
| AWS keys hardcoded in `config.ini` | Credential resolution chain via env vars / profiles / ADC; legacy `config.ini` is detected at startup and refuses to run |
| Drive-only one-way → S3 | Bidirectional S3 ↔ GCS plus Drive pull-only into a configured destination |
| `head_object` per-key check | Catalog of Content_Hashes; identity is SHA-256 of bytes, not key |
| Best-effort retry inside an `except` block | Single retry utility with classified transient/non-transient errors and `Retry-After` honoring |
| Unstructured `logging` to `logfile.log` | Structured JSON logs to stderr + JSONL Manifest with `[REDACTED]` enforcement |
| No concurrency control | Bounded `ThreadPoolExecutor` plus single-writer `flock` with stale-PID reclaim |
| No dry-run | Mandatory `--dry-run` planning mode; writes are gated by `--apply` plus interactive or `--auto-approve` confirmation for destructive actions |

The design favours a small set of pure data structures (`ObjectRecord`, `Catalog`, `Manifest_Record`, `Config`) with round-trip parser/printer pairs so the bulk of the logic is testable with Hypothesis property-based tests, while side-effecting code (network, fs) is isolated behind the `SourceAdapter` interface and can be mocked.

## Architecture

### Component diagram

```mermaid
flowchart TB
  CLI[CLI Entrypoint<br/>mcps.cli.main]

  subgraph Configuration
    CP[Config_Parser]
    CR[Credential_Manager]
  end

  subgraph Concurrency
    LM[Lock_Manager<br/>fcntl + PID]
    EX[Bounded ThreadPoolExecutor<br/>max_concurrent_transfers]
  end

  subgraph SourceAdapters
    direction LR
    S3A[S3SourceAdapter<br/>boto3]
    GCSA[GCSSourceAdapter<br/>google-cloud-storage]
    DRA[GoogleDriveSourceAdapter<br/>google-api-python-client]
  end

  subgraph Persistence
    CAT[Catalog<br/>JSONL on disk]
    CATP[Catalog_Parser /<br/>Catalog_Printer]
  end

  subgraph CoreLogic
    DD[Duplicate_Detector]
    DR[Duplicate_Resolver]
    REP[Replicator]
    DI[Drive_Importer]
  end

  subgraph Observability
    MAN[Manifest_Writer<br/>JSONL append]
    LOG[Structured_Logger<br/>JSON to stderr]
    RED[Redactor]
  end

  RT[Retry_Utility]

  CLI --> CP
  CLI --> CR
  CLI --> LM
  CLI --> CATP
  CATP --> CAT
  CLI --> SourceAdapters
  CR --> SourceAdapters
  SourceAdapters --> RT
  CLI --> DD
  DD --> DR
  CLI --> REP
  CLI --> DI
  REP --> EX
  DI --> EX
  EX --> SourceAdapters
  REP --> MAN
  DR --> MAN
  DI --> MAN
  REP --> LOG
  CLI --> LOG
  MAN --> RED
  LOG --> RED
```

### Reconciliation and inconsistency components (added for requirements 18, 19)

The component diagram above is preserved as-is for the steady-state Sync_Run. The diagram below shows where the two new components plug into that flow without altering any existing arrow:

```mermaid
flowchart TB
  subgraph SourceAdapters
    direction LR
    S3A[S3SourceAdapter]
    GCSA[GCSSourceAdapter]
    DRA[GoogleDriveSourceAdapter]
  end

  CAT[Catalog<br/>JSONL on disk]

  DD[Duplicate_Detector]

  RR[Reconciliation_Reporter<br/>mcps/reconciliation.py<br/>Cold_Start only]
  ID[Inconsistency_Detector<br/>mcps/reconciliation.py<br/>post-replication]

  REP[Replicator]
  DI[Drive_Importer]

  STDOUT[(stdout)]
  RFILE[(reconciliation-&lt;ts&gt;-&lt;run-id&gt;.txt<br/>under manifest_dir)]
  MAN[Manifest_Writer]
  LOG[Structured_Logger]

  SourceAdapters -->|ObjectRecord stream| RR
  CAT -->|catalog snapshot at run start| RR
  DD -->|duplicate groups| RR
  DI -.would-import count.-> RR
  RR --> STDOUT
  RR --> RFILE

  REP -->|post-replication state + replication-error entries| ID
  CAT -->|catalog snapshot at run start| ID
  SourceAdapters -->|ObjectRecord stream| ID
  ID --> MAN
  ID --> LOG
```

The `Reconciliation_Reporter` is invoked by `cli.main` only when the in-memory Catalog at run start is empty (Cold_Start, req 18.1). The `Inconsistency_Detector` is invoked at the end of every Sync_Run, immediately after the Replicator completes, regardless of Cold_Start (req 19.1, 19.2).

### Module layout (Python package `mcps/`)

```
mcps/
  __init__.py
  cli.py                   # argparse entry, exit-code mapping, --dry-run / --apply / --auto-approve
  config/
    parser.py              # Config_Parser  (TOML + YAML)
    printer.py             # Config_Printer (TOML or YAML, matching loaded format)
    model.py               # @dataclass Config and subsections
  credentials.py           # Credential_Manager (AWS chain, GCP ADC, Drive SA file)
  catalog/
    model.py               # ObjectRecord, Catalog
    parser.py              # Catalog_Parser
    printer.py             # Catalog_Printer (deterministic JSONL)
  manifest/
    model.py               # Manifest_Record, Action enum, Result enum
    writer.py              # streaming JSONL append
    parser.py              # Manifest_Parser  (round-trip)
    printer.py             # Manifest_Printer (round-trip)
  sources/
    base.py                # SourceAdapter ABC + ListPage/ObjectMeta types
    s3.py                  # S3SourceAdapter
    gcs.py                 # GCSSourceAdapter
    drive.py               # GoogleDriveSourceAdapter (read-only)
  hashing.py               # streamed SHA-256 + S3 ETag heuristic
  duplicates/
    detector.py            # Duplicate_Detector
    resolver.py            # Duplicate_Resolver (canonical pick, quarantine, last-copy-protection)
  replication.py           # Replicator (loop check, conflict policy, post-write verification)
  drive_import.py          # Drive_Importer
  reconciliation.py        # Reconciliation_Reporter (Cold_Start) + Inconsistency_Detector (post-run)
  retry.py                 # @retry_transient decorator, Retry-After parsing
  concurrency.py           # bounded executor + Lock_Manager (flock + PID file)
  logging_setup.py         # JSON logger, [REDACTED] filter
  redaction.py             # Redactor: secret-pattern stripping
  errors.py                # exception hierarchy and exit codes
```

## Components and Interfaces

### `SourceAdapter` interface

All three providers implement a single abstract base class. The Replicator, Duplicate_Resolver, and Drive_Importer talk only through this interface, so unit tests can substitute an in-memory fake.

```python
# mcps/sources/base.py

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Mapping, Optional

@dataclass(frozen=True)
class ObjectMeta:
    """Provider-reported metadata about one object, before content_hash is resolved."""
    key: str
    size_bytes: int
    last_modified: str            # ISO-8601 UTC, second precision
    content_type: Optional[str]   # may be None for some providers
    user_metadata: Mapping[str, str]   # custom metadata (mcps-* keys live here)
    etag: Optional[str]           # S3 only; None elsewhere
    provider_hash: Optional[str]  # S3 ETag (if singlepart) or GCS crc32c (decoded);
                                  # always None for Drive

class SourceAdapter(ABC):
    """One configured source. Stateless w.r.t. the Catalog."""

    name: str            # logical source name (e.g. "s3-prod", "gcs-archive")
    kind: str            # one of "s3" | "gcs" | "google_drive"

    @abstractmethod
    def list_objects(self) -> Iterator[ObjectMeta]:
        """Stream all objects under the configured prefix/folder.
        Implementations MUST follow continuation tokens to exhaustion (req 2.5).
        Implementations SHOULD apply server-side filtering when cheap (e.g. Drive
        mimeType filter)."""

    @abstractmethod
    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Yield content chunks suitable for streaming SHA-256 (req 2.2-2.4)."""

    @abstractmethod
    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Atomic write. user_metadata MUST include the mcps-* keys
        (req 6.4, 7.4)."""

    @abstractmethod
    def get_metadata(self, key: str) -> ObjectMeta:
        """HEAD-equivalent. Used for post-write verification (req 6.5)
        and for duplicate detection of pre-existing destination objects."""

    @abstractmethod
    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Quarantine and tombstone tagging (req 5.7, 9.3).
        Implementations: S3 uses object tagging, GCS uses metadata update.
        GoogleDriveSourceAdapter MUST raise ReadOnlySourceError (req 10.8)."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Physical delete (req 5.9, 9.5).
        GoogleDriveSourceAdapter MUST raise ReadOnlySourceError (req 10.8)."""

    @property
    def supports_writes(self) -> bool:
        """False for GoogleDriveSourceAdapter, True for s3/gcs."""
        return True
```

### Provider-specific implementations

#### `S3SourceAdapter`

- `list_objects` paginates `list_objects_v2` and emits one `ObjectMeta` per object. `etag` is captured raw (with quotes stripped). `user_metadata` is left empty during listing — `mcps-*` are stored as object **tags**, not user-metadata, because they can be set without rewriting the object.
- For the **ETag-singlepart heuristic** (req 2.2): an S3 ETag corresponds to a single-part upload iff it is a 32-character lowercase hex digest with no `-N` suffix. In that case the ETag equals the MD5, but we still need SHA-256, so we do **not** treat the ETag as `content_hash`. Instead we use the `mcps-content-sha256` user-metadata when present (req 7.1) and otherwise fall back to streaming SHA-256.
  - The "ETag-singlepart heuristic" matters in one place: when both ETags match across two S3 objects with identical sizes and `mcps-content-sha256` is missing on both, we can still treat them as duplicates without re-hashing. This is an optimisation; the canonical identity remains SHA-256.
- `write_bytes` uses `boto3.client('s3').upload_fileobj` for parts >5 MiB and `put_object` otherwise; `Metadata=` carries `mcps-source` and `mcps-content-sha256`, and post-write `head_object` is used to verify size and the round-tripped `mcps-content-sha256` (req 6.5).
- `set_tag` uses `put_object_tagging`; `delete` uses `delete_object`.

#### `GCSSourceAdapter`

- `list_objects` uses `client.list_blobs(bucket, prefix=...)`. `provider_hash` is set to the decoded CRC32C (informational only — never used as Content_Hash per req 2.3).
- `read_bytes` uses `blob.open("rb")` for streaming, which performs server-side CRC32C verification (req 2.3).
- `write_bytes` uses `blob.upload_from_file(stream, content_type=...)` and writes `mcps-*` to `blob.metadata`.
- `set_tag` is implemented by patching `blob.metadata` (GCS does not have S3-style tags).
- `delete` uses `blob.delete()`.

#### `GoogleDriveSourceAdapter` (read-only)

- `list_objects` calls `drive_service.files().list(q="'{folder_id}' in parents and trashed=false", pageSize=1000)` and follows `nextPageToken` until exhaustion (req 10.1). Server-side filtering applies the `mimeType` allowlist (`image/*` or `video/*`) where supported; remaining filtering is applied client-side (req 10.2, 10.3).
- `read_bytes` uses `MediaIoBaseDownload(io.BytesIO(), drive_service.files().get_media(fileId=...))` and yields chunks for streaming SHA-256 (req 2.4).
- `write_bytes`, `set_tag`, `delete` raise `ReadOnlySourceError` (req 10.8). `supports_writes` is `False`.

### Streamed-SHA-256 path

```python
# mcps/hashing.py

import hashlib
from typing import Iterator

CHUNK_SIZE = 1024 * 1024  # 1 MiB

def stream_sha256(chunks: Iterator[bytes]) -> str:
    h = hashlib.sha256()
    for chunk in chunks:
        h.update(chunk)
    return h.hexdigest()

def s3_etag_is_singlepart(etag: str) -> bool:
    """A singlepart S3 ETag is exactly 32 lowercase hex chars with no -N suffix."""
    if not etag or "-" in etag:
        return False
    return len(etag) == 32 and all(c in "0123456789abcdef" for c in etag.lower())
```

`hashing.compute_content_hash(adapter, meta)` is the single entry point used by listing:

1. If `meta.user_metadata.get("mcps-content-sha256")` is present and is a 64-char lowercase hex string, return it (req 7.1).
2. Otherwise, look up `(source.name, key, size, last_modified)` in the in-memory Catalog; if all four match a prior `ObjectRecord`, reuse its `content_hash` (req 3.7).
3. Otherwise, stream `adapter.read_bytes(key)` and compute SHA-256 (req 2.2-2.4, 7.2).

### Duplicate_Detector

Pure function. Takes the in-memory Catalog (a `Mapping[content_hash, frozenset[ObjectRecord]]`) and returns a list of `DuplicateGroup` records, sorted by `content_hash` ascending so that output is order-independent (req 4.4). Object_Records with missing or invalid hash/size are diverted to a `skipped_records` list (req 4.6).

### Duplicate_Resolver

Stateful: needs the configured `canonical_source_priority`, the current run's `--auto-approve` flag, and a `tty.isatty()` check.

- `pick_canonical(group, priority)` applies the deterministic tie-break (req 5.1) and falls back when priority is missing/empty (req 5.2). Returns `(canonical: ObjectRecord, removable: list[ObjectRecord], priority_warning: bool)`.
- `quarantine(removable, adapters)` calls `adapter.set_tag(key, "mcps-quarantined-at", now_iso_utc_seconds())` for each member, after the **last-copy-protection** check (req 5.10, 5.11, 9.6, 9.7): for every `removable` Object_Record, the resolver asserts that **at least one non-quarantined Object_Record with the same `content_hash` survives in any Source after this batch is applied**.
- `physically_delete_expired(catalog, adapters, now)` enumerates Object_Records whose `mcps-quarantined-at` tag plus `quarantine_retention_days` is in the past, re-runs last-copy-protection, and calls `adapter.delete(key)` (req 5.9).

### Replicator

Bounded by the shared `ThreadPoolExecutor`. For each ordered pair `(src, dst)` of Replicated_Sources and each `content_hash` present in `src` but absent in `dst`:

1. Pick the `src` Object_Record whose `mcps-source != dst.name` (req 7.3); if all candidates have `mcps-source == dst.name`, record `loop-skip` and continue.
2. If `dst` already has the `key`, branch on `on_key_conflict` (req 8): `skip` / `rename` (suffix `.<first 8 hex of content_hash>`) / `overwrite`.
3. Open `src.read_bytes(key)`, pass to `dst.write_bytes(key, chunks, size, content_type, mcps_metadata)` where `mcps_metadata = {mcps-source: src.name, mcps-content-sha256: hash, mcps-replicated-at: now_iso_utc_seconds()}` (req 6.4).
4. **Post-write verification** (req 6.5): `dst.get_metadata(key)`, compare `size_bytes` and `user_metadata["mcps-content-sha256"]`. On mismatch, `dst.delete(key)` and emit a `replication-error` Manifest record.
5. On success, append a `replicate` Manifest record with `result=success`.

A `_select_canonical_source(content_hash)` helper picks the source side using the same canonical priority used by the Duplicate_Resolver, so that "replicate the canonical copy" is well defined (req 6.2).

### Drive_Importer

Pull-only. For each Drive file passing the mimeType filter (req 10.2-10.3):

1. Stream-hash the bytes (req 10.4).
2. If the hash exists in any Replicated_Source according to the in-memory Catalog, emit `drive-skip-existing` (req 10.5).
3. Otherwise, build the destination key `google-drive/<YYYY>/<MM>/<file-id>__<sanitised-name>` (req 10.6) and call `drive_destination.write_bytes(...)` with the streamed bytes piped from a fresh `read_bytes` call. The mcps-* metadata is set with `mcps-source = "<drive-source-name>"` so subsequent runs treat the destination copy as canonical and never try to "replicate it back" to Drive (which is read-only anyway).

### Reconciliation_Reporter (Cold_Start only)

`mcps.reconciliation.Reconciliation_Reporter` is a pure function over the empty Catalog snapshot at run start and the `ObjectRecord` stream produced by listing every configured Source. It is invoked by `cli.main` only when `len(catalog.by_hash) == 0` at lock-acquire time (Cold_Start, req 18.1). The Reporter's contract:

```python
# mcps/reconciliation.py

@dataclass(frozen=True)
class ReconciliationReport:
    run_id: str
    started_at: str                                  # ISO-8601 UTC seconds
    per_source: Mapping[str, "PerSourceCounts"]      # name -> counts
    cross_source_diff: "CrossSourceDiff"             # S3-only / GCS-only / Drive-only / exactly-two / all-three
    same_source_dup_groups: Mapping[str, int]        # source name -> count of dup groups confined to that source
    cross_source_dup_groups: int                     # count of dup groups spanning >= 2 sources
    drive_would_import: int                          # count of Drive files whose hash is absent from every Replicated_Source
    estimated_bytes_to_hash: int                     # sum of size_bytes over records that needed streaming hash

@dataclass(frozen=True)
class PerSourceCounts:
    object_count: int
    total_bytes: int
    distinct_content_hashes: int

@dataclass(frozen=True)
class CrossSourceDiff:
    s3_only: int
    gcs_only: int
    drive_only: int
    exactly_two: int
    all_three: int

class Reconciliation_Reporter:
    def build(
        self,
        *,
        catalog_at_start: Catalog,                   # MUST be empty for Cold_Start
        object_records: Iterable[ObjectRecord],
        duplicate_groups: Sequence[DuplicateGroup],  # output of Duplicate_Detector
        drive_would_import_count: int,               # output of Drive_Importer.plan()
        bytes_to_hash_estimator: Callable[[ObjectRecord], bool],  # True iff record needed streaming
        run_id: str,
        started_at: str,
    ) -> ReconciliationReport:
        ...

    def emit(
        self,
        report: ReconciliationReport,
        *,
        stdout: TextIO,
        manifest_dir: str,
    ) -> None:
        """Writes a structured human-readable summary to stdout AND writes the
        same content to <manifest_dir>/reconciliation-<UTC-timestamp>-<run-id>.txt
        (req 18.2). The on-disk file uses the same YYYYMMDDTHHMMSSZ format as
        the Manifest filename in req 14.1."""
```

The Reporter is **deterministic given input** — `build(...)` walks the `object_records` iterable into per-source dicts, then computes the per-source distinct-hash counts, the cross-source diff (using set operations over the per-source-kind hash sets), and the duplicate-group classifications. No field of the report depends on the order in which `object_records` are yielded.

The `cross_source_diff` collapses the per-Source view to per-Source-kind: all `s3` Sources contribute to a single `s3` hash set, all `gcs` Sources to `gcs`, all `google_drive` Sources to `drive`. This matches req 18.1's (b) directly: "Content_Hashes present in S3 only, in GCS only, in Drive only, in exactly two of the three Source kinds, and in all three Source kinds".

`bytes_to_hash_estimator(record)` returns `True` for any `ObjectRecord` whose `content_hash` had to be computed by streaming during this Cold_Start (i.e. it was neither reused from a Catalog cache hit per req 3.7 nor read from a valid `mcps-content-sha256` user-metadata value per req 7.1). Because Cold_Start has an empty Catalog, the cache-hit term is vacuously false, and every record without a valid `mcps-content-sha256` ends up streamed; the pre-populated S3 / Drive folders described in the requirements introduction therefore contribute their full byte size to `estimated_bytes_to_hash` (req 18.5).

### Inconsistency_Detector (post-replication, every run)

`mcps.reconciliation.Inconsistency_Detector` runs at the end of every Sync_Run, immediately after the Replicator completes and immediately before the final SUMMARY Manifest record is written. It is invoked regardless of Cold_Start.

```python
# mcps/reconciliation.py

@dataclass(frozen=True)
class DivergentHash:
    content_hash: str
    present_in: tuple[str, ...]      # Replicated_Source names where the hash is present
    absent_from: tuple[str, ...]     # Replicated_Source names where the hash is absent
    canonical_key: str               # key of the canonical ObjectRecord per Requirement 5.1

@dataclass(frozen=True)
class InconsistencyReport:
    new_records_per_source: Mapping[str, int]       # source -> count new vs catalog_at_start
    removed_records_per_source: Mapping[str, int]   # source -> count present at start, absent now
    divergent_hashes_count: int
    divergent_hashes: tuple[DivergentHash, ...]

class Inconsistency_Detector:
    def analyse(
        self,
        *,
        catalog_at_start: Catalog,
        object_records_observed: Iterable[ObjectRecord],
        replicated_source_names: frozenset[str],
        replication_error_hashes: frozenset[str],   # content_hashes with replication-error in the Manifest
        canonical_for: Callable[[str], ObjectRecord],  # picks canonical per Requirement 5.1
    ) -> InconsistencyReport:
        ...

    def emit(
        self,
        report: InconsistencyReport,
        *,
        manifest_writer: Manifest_Writer,
        logger: Structured_Logger,
        fail_on_inconsistency: bool,
    ) -> int:
        """Emits the per-source counts in the SUMMARY log record (req 19.1) and
        appends a WARN log record per divergent hash (req 19.2). Returns 0 if
        no divergent hashes were observed; returns 1 if divergent hashes were
        observed AND fail_on_inconsistency is True. The CLI folds this return
        value into its exit-code computation."""
```

The `divergent_hashes` set is computed as: the set of Content_Hashes present in some Replicated_Source-listed Object_Record but absent from at least one other Replicated_Source, **minus** the set of Content_Hashes for which a `replication-error` entry exists in the Manifest (req 19.2). Pull_Only_Sources (`google_drive`) are excluded from this analysis — they are read-only and divergence between Drive and a Replicated_Source is not a system fault.

`new_records_per_source` and `removed_records_per_source` are the symmetric-difference partitions of `(catalog_at_start.records_for_source(name), records_observed_for_source(name))` keyed by `(source, key)` (req 19.1).

### Retry utility

Single decorator that all SourceAdapters route their network calls through.

```python
# mcps/retry.py

class TransientError(Exception): ...
class NonTransientError(Exception): ...

TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
NON_TRANSIENT_HTTP = {400, 401, 403, 404}  # 404 only treated transient if expected absence

def classify_http(status: int, *, expect_404_as_absent: bool) -> str:
    if status in TRANSIENT_HTTP:
        return "transient"
    if status == 404 and expect_404_as_absent:
        return "absent"
    if status in NON_TRANSIENT_HTTP:
        return "non_transient"
    if 200 <= status < 300:
        return "ok"
    return "non_transient"

def retry_transient(
    *, max_retries: int, initial_backoff_ms: int, max_backoff_ms: int,
    request_timeout_ms: int, sleep=time.sleep, now=time.monotonic,
):
    """Decorator. Caller raises TransientError(status, retry_after_seconds=None)
    or NonTransientError(...) and the decorator handles backoff."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            backoff_ms = initial_backoff_ms
            while True:
                try:
                    return fn(*args, **kwargs)
                except NonTransientError:
                    raise
                except TransientError as e:
                    attempt += 1
                    if attempt > max_retries:
                        raise RetriesExhausted(last=e, attempts=attempt) from e
                    wait_ms = max(backoff_ms, (e.retry_after_seconds or 0) * 1000)
                    wait_ms = min(wait_ms, max_backoff_ms)        # req 12.4
                    sleep(wait_ms / 1000.0)
                    backoff_ms = min(backoff_ms * 2, max_backoff_ms)
        return wrapper
    return deco
```

`Retry-After` honoring (req 12.3): when the underlying client returns a 429 with a `Retry-After` header, the adapter reads it (numeric seconds or HTTP date) and embeds it in `TransientError(retry_after_seconds=...)`. The decorator picks `max(retry_after, computed_backoff)`, capped at `max_backoff_ms`.

### Concurrency model

- **Bounded transfers** (req 16.1, 16.2): `concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_transfers)` is created once per Sync_Run. Replicator and Drive_Importer submit one future per per-object operation (read+verify+write). The pool is the single chokepoint, so the simultaneous-in-flight count never exceeds `max_concurrent_transfers`. Threads are appropriate here because all three SDKs (boto3, google-cloud-storage, google-api-python-client) release the GIL during socket I/O.
- **Single-writer lock** (req 16.3-16.6): the Lock_Manager acquires `fcntl.flock(fd, LOCK_EX | LOCK_NB)` on a sibling file `<catalog_path>.lock`. On success it writes the current PID + ISO-8601 start timestamp + run_id and registers an `atexit` handler to release. On `BlockingIOError` it reads the existing PID and sends signal 0 to it: if `os.kill(pid, 0)` raises `ProcessLookupError`, the lock is stale → unlink and reclaim with a fresh `flock` attempt; otherwise exit with `LOCK_CONFLICT` (exit code 73) within 5 seconds and **do not** modify the catalog (req 16.5).
- POSIX-only by design. On Windows we would use `msvcrt.locking`, but this tool ships for cron / systemd, so Linux/macOS only is acceptable.

## Data Models

### `ObjectRecord`

```python
# mcps/catalog/model.py

from dataclasses import dataclass, field
from typing import Optional

@dataclass(frozen=True, order=True)
class ObjectRecord:
    """One object in one Source, identified by SHA-256."""
    source: str            # logical source name (e.g. "s3-prod")
    key: str               # provider key, byte-for-byte
    content_hash: str      # 64-char lowercase hex SHA-256
    size_bytes: int        # >= 0
    last_seen_at: str      # ISO-8601 UTC, second precision, trailing Z
    last_modified: str     # provider-reported, ISO-8601 UTC
    content_type: Optional[str]
    quarantined_at: Optional[str] = None  # set by Duplicate_Resolver
    tombstoned_at: Optional[str] = None   # set under delete_propagation=soft
    mcps_source_meta: Optional[str] = None  # value of mcps-source on the live object
```

`ObjectRecord` is hashable, ordered (so deterministic JSONL output is feasible), and **immutable** — every state change produces a new value. This makes property tests trivial: the Catalog is a pure value.

### `Catalog`

On-disk format: **JSONL with one `ObjectRecord` per line**, sorted by `(content_hash, source, key)` ascending. JSONL was chosen over a single JSON document because:

1. It scales: appending or rewriting line-by-line never holds the whole catalog in memory beyond a streaming pass.
2. It is byte-identical for equal inputs when keys within each JSON object are emitted in a fixed order — necessary for the round-trip property (req 3.3, 3.4).

In memory the Catalog is the equivalence class:

```python
@dataclass
class Catalog:
    by_hash: dict[str, frozenset[ObjectRecord]]  # content_hash -> records

    def all_records(self) -> Iterator[ObjectRecord]: ...
    def upsert(self, rec: ObjectRecord) -> "Catalog": ...
    def remove(self, source: str, key: str) -> "Catalog": ...
```

`Catalog_Printer` writes records sorted by `(content_hash, source, key)` with `json.dumps(asdict(rec), sort_keys=True, separators=(",",":"), ensure_ascii=False)` per line, terminated by `\n`. The file is written via `tempfile.NamedTemporaryFile` in the same directory followed by `os.replace` for atomic swap (req 3.1).

`Catalog_Parser` reads the file in one streaming pass, asserts that each line is a valid `ObjectRecord`, and rebuilds `by_hash`. Any parse failure aborts before any mutation (req 3.6).

### `Manifest_Record`

```python
# mcps/manifest/model.py

from enum import Enum

class Action(str, Enum):
    DISCOVERED       = "discovered"
    REPLICATE        = "replicate"
    REPLICATE_SKIP   = "replicate-skip-existing"
    LOOP_SKIP        = "loop-skip"
    SOURCE_TAGGED    = "source-tagged"
    HASH_RECOMPUTED  = "hash-recomputed"
    KEY_CONFLICT     = "key-conflict"
    OVERWRITE        = "overwrite"
    RENAME           = "rename"
    QUARANTINE       = "quarantine"
    PHYSICAL_DELETE  = "physical-delete"
    LAST_COPY_GUARD  = "last-copy-protection"
    TOMBSTONE        = "tombstone"
    DRIVE_SKIP_UNSUP = "drive-skip-unsupported"
    DRIVE_SKIP_NDOC  = "drive-skip-native-doc"
    DRIVE_SKIP_EXIST = "drive-skip-existing"
    DRIVE_IMPORT_OK  = "drive-import-success"
    DRIVE_DOWNLOAD_E = "drive-download-error"
    DRIVE_WARN_TIME  = "drive-warning-missing-created-time"
    LIST_ERROR       = "list-error"
    HASH_ERROR       = "hash-error"
    RETRIES_EXHAUSTED= "retries-exhausted"
    REPLICATION_ERROR= "replication-error"
    SUMMARY          = "summary"

class Result(str, Enum):
    SUCCESS     = "success"
    SKIPPED     = "skipped"
    QUARANTINED = "quarantined"
    DELETED     = "deleted"
    PLANNED     = "planned"   # dry-run
    ERROR       = "error"

@dataclass(frozen=True)
class ManifestRecord:
    timestamp: str           # ISO-8601 UTC ms
    run_id: str              # UUIDv4 hex (>=8 chars)
    action: Action
    result: Result
    source: Optional[str]    # may be None for SUMMARY
    target: Optional[str]    # for replicate/drive-import
    key: Optional[str]
    content_hash: Optional[str]
    size_bytes: Optional[int]
    error: Optional[str]     # set iff result == ERROR
    extra: Mapping[str, str] # action-specific payload (e.g. expected/observed hash)
```

JSONL on disk, one record per line, UTF-8 LF-terminated (req 14.2, 15.2). All secret-shaped fields run through the `Redactor` before being serialised (req 14.4).

### `ReconciliationReport` and `InconsistencyReport`

These two dataclasses live in `mcps/reconciliation.py` (interfaces sketched in the Reconciliation_Reporter / Inconsistency_Detector sections above) and are part of the data model surface because both are produced from pure functions over `Catalog` / `ObjectRecord` and are amenable to property-based testing. Restated here for completeness:

```python
# mcps/reconciliation.py

@dataclass(frozen=True)
class PerSourceCounts:
    object_count: int
    total_bytes: int
    distinct_content_hashes: int

@dataclass(frozen=True)
class CrossSourceDiff:
    s3_only: int          # Content_Hashes present only in s3 sources
    gcs_only: int
    drive_only: int
    exactly_two: int      # Content_Hashes present in exactly two of {s3, gcs, drive}
    all_three: int        # Content_Hashes present in s3 AND gcs AND drive

@dataclass(frozen=True)
class ReconciliationReport:
    run_id: str
    started_at: str                                  # ISO-8601 UTC seconds
    per_source: Mapping[str, PerSourceCounts]        # source name -> counts
    cross_source_diff: CrossSourceDiff
    same_source_dup_groups: Mapping[str, int]        # source name -> count of dup groups confined to that source
    cross_source_dup_groups: int
    drive_would_import: int
    estimated_bytes_to_hash: int

@dataclass(frozen=True)
class DivergentHash:
    content_hash: str
    present_in: tuple[str, ...]      # Replicated_Source names where the hash is present
    absent_from: tuple[str, ...]     # Replicated_Source names where the hash is absent
    canonical_key: str               # key of the canonical ObjectRecord per Requirement 5.1

@dataclass(frozen=True)
class InconsistencyReport:
    new_records_per_source: Mapping[str, int]       # source -> count new vs catalog_at_start
    removed_records_per_source: Mapping[str, int]   # source -> count present at start, absent now
    divergent_hashes_count: int
    divergent_hashes: tuple[DivergentHash, ...]
```

`ReconciliationReport` has no on-disk parser/printer round-trip requirement — it is rendered as a structured human-readable summary by `Reconciliation_Reporter.emit(...)` and that text file is not consumed back into the system. `InconsistencyReport` is similarly write-only, surfacing through the SUMMARY log record (req 19.1) and per-divergence WARN log records (req 19.2).

### `Config`

TOML or YAML on disk. Both formats parse into the same dataclass; `Config_Printer` re-emits in the format that was originally loaded (defaulting to YAML on a fresh write).

```python
# mcps/config/model.py

@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: Literal["s3", "gcs", "google_drive"]
    # s3/gcs:
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    region: Optional[str] = None  # s3 only
    # google_drive:
    drive_root_folder_id: Optional[str] = None

@dataclass(frozen=True)
class ReplicationConfig:
    pairs: tuple[tuple[str, str], ...]  # ordered (src, dst) replicated_source names
    on_key_conflict: Literal["skip", "rename", "overwrite"] = "skip"
    fail_on_conflict: bool = False
    delete_propagation: Literal["none", "soft", "hard"] = "none"
    tombstone_retention_days: int = 30  # 1..3650
    fail_on_inconsistency: bool = False  # req 19.3

@dataclass(frozen=True)
class DuplicatesConfig:
    canonical_source_priority: tuple[str, ...] = ()
    quarantine_retention_days: int = 30  # 1..3650

@dataclass(frozen=True)
class PhotosConfig:
    """Drive pull-only (the section is named 'photos' for forward compatibility
    with Photos API support, even though the current implementation reads from
    a Drive folder, per requirements introduction)."""
    drive_source: Optional[str] = None        # name of a SourceConfig kind=google_drive
    drive_destination: Optional[str] = None   # name of a SourceConfig kind=s3 or gcs

@dataclass(frozen=True)
class RetriesConfig:
    max_retries: int = 5                # 1..10
    initial_backoff_ms: int = 500       # 100..10000
    max_backoff_ms: int = 30000         # 1000..300000
    request_timeout_ms: int = 30000     # 1000..120000

@dataclass(frozen=True)
class RuntimeConfig:
    catalog_path: str = "./mcps.catalog.jsonl"
    manifest_dir: str = "./manifests"
    max_concurrent_transfers: int = 4   # 1..64
    lock_path: Optional[str] = None     # defaults to <catalog_path>.lock

@dataclass(frozen=True)
class Config:
    sources: tuple[SourceConfig, ...]
    replication: ReplicationConfig
    duplicates: DuplicatesConfig
    photos: PhotosConfig
    retries: RetriesConfig
    runtime: RuntimeConfig
```

`Config_Parser` validates:

- exactly the six top-level sections (req 17.4),
- no unknown keys at any level (req 17.7),
- all required fields present (req 17.8) with line-numbered errors,
- type and range checks (req 17.9),
- every `replication.pairs` reference resolves to a `kind in {s3, gcs}` Source,
- `photos.drive_source` resolves to `kind=google_drive` and `photos.drive_destination` resolves to `kind in {s3, gcs}`.

`Config_Printer` emits keys in the order declared in the dataclass and uses `tomli_w.dump` for TOML / `yaml.safe_dump(sort_keys=False, default_flow_style=False)` for YAML, both of which are deterministic for a given dataclass.

## Sequence Diagrams

### Full Sync_Run

```mermaid
sequenceDiagram
  autonumber
  participant U as User/cron
  participant CLI
  participant Cfg as Config_Parser
  participant Cred as Credential_Manager
  participant Lock as Lock_Manager
  participant Cat as Catalog_Parser
  participant Pool as ThreadPool
  participant Adp as SourceAdapters
  participant Det as Duplicate_Detector
  participant Res as Duplicate_Resolver
  participant Rep as Replicator
  participant DI as Drive_Importer
  participant Man as Manifest_Writer

  U->>CLI: mcps --config mcps.config.yaml [--dry-run|--apply]
  CLI->>Cfg: parse(config_path)
  Cfg-->>CLI: Config
  CLI->>Cred: resolve(aws,gcp,drive)
  Cred-->>CLI: credentials (or abort: exit 71)
  CLI->>Lock: acquire(<catalog>.lock)
  alt held by live PID
    Lock-->>CLI: LOCK_CONFLICT (exit 73)
  else stale or free
    Lock-->>CLI: ok
  end
  CLI->>Cat: load(catalog_path)
  alt parse error
    Cat-->>CLI: error (exit 65)  // do not modify catalog
  else ok or missing
    Cat-->>CLI: in-memory Catalog
  end
  CLI->>Pool: create(max_concurrent_transfers)
  par list every source in parallel
    Pool->>Adp: S3 list_objects + hash
    Pool->>Adp: GCS list_objects + hash
    Pool->>Adp: Drive list_objects + hash
  end
  Adp-->>CLI: ObjectRecord stream
  CLI->>Det: detect(catalog)
  Det-->>CLI: duplicate_groups
  CLI->>Res: resolve(groups, mode)
  Res->>Man: removal plan (planned|quarantined|deleted)
  CLI->>Rep: replicate(catalog)
  Rep->>Pool: per-object replicate task
  Pool->>Adp: read+write+verify
  Rep->>Man: replicate / loop-skip / key-conflict / replication-error
  CLI->>DI: import(drive_source -> drive_destination)
  DI->>Pool: per-file import task
  DI->>Man: drive-import-success / drive-skip-* / drive-download-error
  CLI->>Cat: write(catalog) atomically
  CLI->>Lock: release
  CLI->>Man: SUMMARY
  CLI-->>U: exit 0 (or non-zero per error policy)
```

### Cold_Start Sync_Run

This diagram is **additive**: it covers the first run against a pre-populated S3 bucket and Drive folder where the on-disk Catalog is missing or empty (Cold_Start, req 18). It does not replace the Full Sync_Run diagram above; that diagram describes every subsequent steady-state run.

```mermaid
sequenceDiagram
  autonumber
  participant U as User/cron
  participant CLI
  participant Lock as Lock_Manager
  participant Cat as Catalog_Parser
  participant Pool as ThreadPool
  participant Adp as SourceAdapters
  participant Det as Duplicate_Detector
  participant RR as Reconciliation_Reporter
  participant Res as Duplicate_Resolver
  participant Rep as Replicator
  participant DI as Drive_Importer
  participant ID as Inconsistency_Detector
  participant Man as Manifest_Writer
  participant Out as stdout
  participant FS as <manifest_dir>

  U->>CLI: mcps --apply [--first-pass-confirmed]
  CLI->>Lock: acquire(<catalog>.lock)
  Lock-->>CLI: ok
  CLI->>Cat: load(catalog_path)
  alt file missing or empty
    Cat-->>CLI: empty Catalog (Cold_Start = true)
  else parseable but empty file
    Cat-->>CLI: empty Catalog (Cold_Start = true)
  end
  CLI->>Pool: create(max_concurrent_transfers)
  par list every source in parallel — every record streamed because no mcps-content-sha256
    Pool->>Adp: S3 list_objects + stream-hash
    Pool->>Adp: GCS list_objects + stream-hash
    Pool->>Adp: Drive list_objects + stream-hash
  end
  alt any source's listing fails after max retries
    Adp-->>CLI: ListingFailed(source)
    CLI->>Man: list-error / retries-exhausted
    CLI-->>U: exit COLD_START_LISTING_FAILED (77)
    Note over CLI: req 18.6: no Reconciliation_Report is produced
  else all sources listed
    Adp-->>CLI: ObjectRecord stream
  end
  CLI->>Det: detect(catalog of just-listed records)
  Det-->>CLI: duplicate_groups (same-source + cross-source)
  CLI->>DI: plan() — count Drive files whose hash is absent from every Replicated_Source
  DI-->>CLI: drive_would_import_count
  CLI->>RR: build(catalog_at_start=empty, records, dup_groups, drive_would_import_count)
  RR-->>CLI: ReconciliationReport
  CLI->>Out: write structured human-readable summary
  CLI->>FS: write manifest_dir/reconciliation-<UTC-timestamp>-<run-id>.txt
  Note over CLI,FS: req 18.2: stdout AND on-disk copy

  alt --apply WITHOUT --first-pass-confirmed
    Note over CLI: req 18.3: non-destructive only
    CLI->>Rep: replicate to absent destinations only<br/>(no overwrite, no key-conflict overwrite)
    Rep->>Adp: write_bytes for absent Content_Hashes
    Rep->>Man: replicate / replicate-skip-existing / loop-skip / key-conflict (skipped)
    CLI->>DI: import only files whose hash is absent everywhere
    DI->>Adp: write_bytes for new Drive items
    DI->>Man: drive-import-success / drive-skip-existing
    Note over Res: SKIPPED on this run (req 18.3): no quarantine set_tag, no physical delete
    CLI->>ID: analyse(post-replication state)
    ID->>Man: per-source new/removed counts
    ID->>Man: WARN per divergent hash
    CLI->>Cat: write(catalog) atomically
    CLI->>Lock: release
    CLI->>Man: SUMMARY
    CLI-->>U: exit FIRST_PASS_REVIEW_REQUIRED (76)
    Note over CLI,U: operator inspects reconciliation-*.txt before re-running
  else --apply WITH --first-pass-confirmed
    Note over CLI: req 18.4: full destructive path subject to 5.5/5.6/9.6/9.7
    CLI->>Res: resolve(groups, mode=apply)
    Res->>Adp: set_tag(mcps-quarantined-at, ...) per removable record<br/>after last-copy-protection check
    Res->>Adp: delete(key) for expired-quarantine records
    Res->>Man: quarantine / physical-delete / last-copy-protection
    CLI->>Rep: replicate(catalog) — full path including overwrite
    Rep->>Adp: read+write+verify
    Rep->>Man: replicate / overwrite / rename / replication-error
    CLI->>DI: import drive_source -> drive_destination
    DI->>Adp: write_bytes
    DI->>Man: drive-import-success / drive-skip-*
    CLI->>ID: analyse(post-replication state)
    ID->>Man: per-source new/removed counts
    ID->>Man: WARN per divergent hash
    CLI->>Cat: write(catalog) atomically
    CLI->>Lock: release
    CLI->>Man: SUMMARY
    CLI-->>U: exit 0 (or non-zero per error policy, or 78 if fail_on_inconsistency)
  end
```

The branch on `--first-pass-confirmed` is the only place in the system where `--apply` does not unconditionally enable destructive actions. The `Replicator` is configured for this branch with a single boolean `destructive_writes_allowed` that gates the `overwrite` arm of `on_key_conflict` and forces `on_key_conflict=skip` semantics for the `overwrite` policy when `False`. The `Duplicate_Resolver` is short-circuited entirely on the unconfirmed branch — it still writes a planned-only Manifest entry per removable record so the operator can review the planned destructive actions in the same Manifest used by `--dry-run`, but it issues no `set_tag` or `delete` call.

### Single-object replication with loop check and post-write verification

```mermaid
sequenceDiagram
  autonumber
  participant Rep as Replicator
  participant Src as src.SourceAdapter
  participant Dst as dst.SourceAdapter
  participant Man as Manifest_Writer

  Rep->>Src: get_metadata(key)
  Src-->>Rep: meta(user_metadata.mcps-source, mcps-content-sha256)
  alt meta.mcps-source == dst.name
    Rep->>Man: loop-skip
  else key already exists in dst
    Rep->>Dst: get_metadata(key)
    Dst-->>Rep: dst_meta
    alt dst_meta.content_hash == src.content_hash
      Rep->>Man: replicate-skip-existing
    else on_key_conflict=skip
      Rep->>Man: key-conflict (skipped)
    else on_key_conflict=rename
      Rep->>Dst: write_bytes(key.<hash8>, ...)
      Rep->>Dst: get_metadata(renamed)
      Dst-->>Rep: verify
      Rep->>Man: rename
    else on_key_conflict=overwrite
      Rep->>Dst: write_bytes(key, ...)
      Rep->>Dst: get_metadata(key)
      Dst-->>Rep: verify
      Rep->>Man: overwrite
    end
  else clean write
    Rep->>Src: read_bytes(key)
    Src-->>Rep: stream chunks
    Rep->>Dst: write_bytes(key, chunks, mcps-* metadata)
    Rep->>Dst: get_metadata(key)
    Dst-->>Rep: verify size + mcps-content-sha256
    alt mismatch
      Rep->>Dst: delete(key)
      Rep->>Man: replication-error
    else match
      Rep->>Man: replicate (success)
    end
  end
```

### Duplicate quarantine flow with interactive confirmation

```mermaid
sequenceDiagram
  autonumber
  participant CLI
  participant Det as Duplicate_Detector
  participant Res as Duplicate_Resolver
  participant TTY as stdin
  participant Adp as SourceAdapters
  participant Man as Manifest_Writer

  CLI->>Det: detect(catalog)
  Det-->>CLI: duplicate_groups
  CLI->>Res: pick canonicals + plan removals
  Res-->>CLI: removal_plan(N items, B bytes)
  alt --dry-run
    Res->>Man: quarantine(planned) per item
  else --apply
    alt --auto-approve OR not isatty
      alt isatty AND not --auto-approve
        Res->>TTY: prompt "Quarantine N items (B bytes)? [y/N]"
        TTY-->>Res: response
        alt response != yes
          Res->>Man: aborted
          CLI-->>CLI: exit non-zero
        end
      end
      loop each removable record
        Res->>Res: assert last-copy-protection survives
        alt would violate last-copy-protection
          Res->>Man: last-copy-protection
        else
          Res->>Adp: set_tag(key, mcps-quarantined-at, now)
          alt success
            Res->>Man: quarantine (quarantined)
          else failure
            Res->>Man: quarantine (error)
          end
        end
      end
    else not interactive AND not auto-approve
      Res->>Man: aborted: interactive confirmation required
      CLI-->>CLI: exit non-zero
    end
  end
```

## Retry strategy implementation

`mcps.retry` exposes a single `retry_transient` decorator (sketched above). Adapter methods convert provider exceptions to `TransientError` / `NonTransientError` at the boundary:

```python
# in S3SourceAdapter
@retry_transient(...)
def _head_object(self, key):
    try:
        return self._s3.head_object(Bucket=self.bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        status = e.response["ResponseMetadata"]["HTTPStatusCode"]
        retry_after = e.response.get("ResponseMetadata", {}) \
                       .get("HTTPHeaders", {}).get("retry-after")
        if status in TRANSIENT_HTTP:
            raise TransientError(status=status,
                                 retry_after_seconds=_parse_retry_after(retry_after)) from e
        raise NonTransientError(status=status) from e
    except (botocore.exceptions.ConnectTimeoutError,
            botocore.exceptions.ReadTimeoutError) as e:
        raise TransientError(status=None) from e
```

GCS and Drive use the same shape. The adapter is the only place that knows provider-specific error codes; all other components observe one of `{TransientError -> RetriesExhausted, NonTransientError, ok}`.

The decorator's `sleep` and `now` parameters are injectable so Hypothesis property tests can drive the retry loop with deterministic clocks (req 12.1-12.6).

## Concurrency Model (detail)

```python
# mcps/concurrency.py

import fcntl, os, signal, errno, contextlib, json, time

class LockConflict(Exception):
    def __init__(self, holder_pid: int):
        self.holder_pid = holder_pid

@contextlib.contextmanager
def writer_lock(path: str, run_id: str, timeout_s: float = 5.0):
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, json.dumps({
                "pid": os.getpid(),
                "run_id": run_id,
                "started_at": _now_iso_seconds(),
            }).encode("utf-8") + b"\n")
            os.fsync(fd)
            try:
                yield fd
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(path)
            return
        except BlockingIOError:
            holder = _read_holder(path)  # may raise -> stale, retry once
            if holder and not _pid_alive(holder):
                # stale: clear it
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(path)
                continue
            if time.monotonic() > deadline:
                raise LockConflict(holder_pid=holder or -1)
            time.sleep(0.1)

def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
```

The bounded executor lives next to the lock helper:

```python
def make_executor(max_concurrent_transfers: int) -> ThreadPoolExecutor:
    return ThreadPoolExecutor(
        max_workers=max_concurrent_transfers,
        thread_name_prefix="mcps-xfer",
    )
```

The Replicator and Drive_Importer wrap each per-object operation in `submit(...)` and gather results with `as_completed(...)`. Manifest writes are funnelled through a single-thread `Lock` to keep JSONL line atomicity.


## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

The 19 requirements collapse into the 17 universally-quantified properties below after the prework reflection (see Acceptance Criteria Testing Prework). Each property is implemented with one Hypothesis test running ≥100 examples. Where requirements are concrete examples, edge cases, or smoke checks (e.g. "exit with non-zero code if --config points at a missing file"), they are not promoted to properties and are tested as example-based unit tests in the Testing Strategy section.

### Property 1: Catalog round-trip

*For any* in-memory `Catalog` `c` constructed from a generator over valid `ObjectRecord` values, `Catalog_Parser(Catalog_Printer(c)) == c`, and `Catalog_Printer(Catalog_Printer(c) parsed) == Catalog_Printer(c)` byte-identically.

**Validates: Requirements 3.3, 3.4**

### Property 2: Manifest round-trip

*For any* sequence of `ManifestRecord` values `m` of length 0 to 10,000, `Manifest_Parser(Manifest_Printer(m))` equals `m` element-wise across every field, and the printer output is UTF-8 LF-terminated JSONL with no BOM.

**Validates: Requirements 15.2, 15.3, 15.4**

### Property 3: Config round-trip

*For any* valid `Config` value `cfg`, both `Config_Parser(Config_Printer(cfg, "yaml"))` and `Config_Parser(Config_Printer(cfg, "toml"))` equal `cfg` field-by-field across all six top-level sections — including `replication.fail_on_inconsistency` (req 19.3) — and any `Config` whose values fall outside the declared ranges is rejected with a descriptive error.

**Validates: Requirements 8.6, 8.7, 9.1, 9.4, 12.6, 16.1, 17.4, 17.6, 17.7, 17.8, 17.9, 19.3**

### Property 4: Order-independence of run decisions

*For any* set of `ObjectRecord` values `R` and *for any* permutation `pi` of the order in which Sources are listed, the resulting Catalog, the duplicate-group set emitted by `Duplicate_Detector`, and the replication plan emitted by `Replicator` are equal under set/dict equality regardless of `pi`.

**Validates: Requirements 4.4, 11.3**

### Property 5: Canonical-survives invariant (last-copy-protection)

*For any* run configuration (any `delete_propagation` in `{none, soft, hard}`, any `quarantine_retention_days`, any `tombstone_retention_days`) and *for any* Catalog `c` whose Content_Hash set is `H`, after running Duplicate_Resolver and Replicator deletion logic to completion, every `h ∈ H` still has at least one non-quarantined, non-tombstoned `ObjectRecord` in some Source.

**Validates: Requirements 5.10, 5.11, 9.6, 9.7**

### Property 6: Replication eventual-consistency

*For any* pair of Replicated_Sources `(A, B)` with arbitrary `ObjectRecord` populations, after a successful Sync_Run with `delete_propagation=none` and no key conflicts, the set of Content_Hashes present in `A` equals the set of Content_Hashes present in `B`, modulo Content_Hashes whose copy operations recorded a `replication-error` Manifest entry.

**Validates: Requirements 6.1, 6.2, 6.6, 6.7, 7.4, 11.5**

### Property 7: Idempotence — second run is a no-op

*For any* Catalog `c` produced by a successful Sync_Run with input set `S`, a subsequent Sync_Run with the same `S` and no external state changes performs zero `write_bytes`, zero `delete`, zero `set_tag` calls and emits a Manifest whose only non-`discovered` action is one `summary` record with all action counters at zero except `discovered`.

**Validates: Requirements 7.5, 11.1**

### Property 8: Loop-free behaviour

*For any* `ObjectRecord` `r` whose `mcps-source` user-metadata equals the target Replicated_Source name, the Replicator performs no write to that target for `r` and emits exactly one `loop-skip` Manifest entry; *for any* `r` with missing `mcps-source`, the Replicator either skips it (if the destination has it) or writes it with `mcps-source` set to its originating Source name.

**Validates: Requirements 7.1, 7.3, 7.4**

### Property 9: Streamed SHA-256 identity over `SourceAdapter`

*For any* `SourceAdapter` instance backed by a fake provider with a mapping `{key: bytes}`, *for any* `key` in that mapping, the `content_hash` of the `ObjectRecord` produced by listing equals `sha256(bytes).hexdigest()`. The same equality holds across paginated listings (no record is dropped or duplicated by `(source, key)`), and a valid `mcps-content-sha256` user-metadata value short-circuits the streaming computation while an invalid one triggers a `hash-recomputed` Manifest entry whose final `content_hash` equals the streamed value.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2**

### Property 10: Conflict-resolution table

*For any* triple `(existing_dst_state, incoming_src_record, on_key_conflict)`, the resulting state of the destination Source matches the deterministic table:

| existing | incoming hash | policy | dst[key] after | dst[key.<hash8>] after | conflict counted? |
|---|---|---|---|---|---|
| absent | any | * | incoming | absent | no |
| same hash | same hash | * | unchanged | absent | no |
| different hash | different | skip | unchanged | absent | yes |
| different hash | different | rename | unchanged | incoming | yes |
| different hash | different | overwrite | incoming | absent | yes |

and the run's exit code is non-zero iff `fail_on_conflict=true` and the "conflict counted?" column is `yes` for at least one record.

**Validates: Requirements 6.7, 8.1, 8.2, 8.3, 8.4, 8.5**

### Property 11: Retry bounds and `Retry-After` honoring

*For any* sequence of `n` `TransientError` outcomes followed by an `ok` outcome, with retry parameters `(max_retries, initial_backoff_ms, max_backoff_ms)` and *for any* `retry_after_seconds` value `r ≥ 0` accompanying any of those errors, the `retry_transient` decorator either succeeds (when `n ≤ max_retries`) or raises `RetriesExhausted` (when `n > max_retries`), and every observed sleep `s` satisfies `max(computed_backoff, r) ≥ s ≥ 0` and `s ≤ max_backoff_ms / 1000`. Non-transient errors are never retried.

**Validates: Requirements 2.6, 12.1, 12.2, 12.3, 12.4, 12.5**

### Property 12: Redaction — no secret material in any output

*For any* run input that contains credential-shaped substrings (an AWS access key id matching `AKIA[A-Z0-9]{16}`, an AWS secret access key, a `-----BEGIN PRIVATE KEY-----...-----END PRIVATE KEY-----` block, an OAuth refresh token, or a presigned-URL `X-Amz-Signature=` parameter), the produced Manifest file content, the JSON log byte stream emitted to stderr, and any error message do not contain that substring; any field that originally held the secret is replaced with the literal string `[REDACTED]`.

**Validates: Requirements 1.6, 14.4**

### Property 13: Drive_Importer pull-only contract

*For any* set of Drive files generated with arbitrary `mimeType`, `createdTime`, `name`, and bytes, after `Drive_Importer.run(...)` completes:

1. No `write_bytes`, `set_tag`, or `delete` call is made to the `GoogleDriveSourceAdapter`;
2. Every imported destination key matches `^google-drive/(\d{4}|unknown-year)/(\d{2}|unknown-month)/[^/]+__[A-Za-z0-9._-]+$`;
3. Files whose `mimeType` does not start with `image/` or `video/` produce no destination write;
4. Files whose `mimeType` starts with `application/vnd.google-apps.` produce no destination write.

**Validates: Requirements 10.2, 10.3, 10.6, 10.7, 10.8**

### Property 14: Bounded concurrency

*For any* workload submitted to the bounded executor with `max_concurrent_transfers = N`, the maximum observed in-flight count of `read_bytes`/`write_bytes`/`set_tag`/`delete` calls (measured by an instrumented adapter that increments a counter on entry and decrements on exit) is ≤ `N` for every observed instant.

**Validates: Requirements 16.2**

### Property 15: Reconciliation_Report completeness and determinism

*For any* Cold_Start input — that is, *for any* multi-source `ObjectRecord` population `R` (spread across Sources of kinds `s3`, `gcs`, and `google_drive`), *for any* set of duplicate groups `G = Duplicate_Detector(R)`, and *for any* listing-order permutation `pi` over `R` — the `ReconciliationReport` produced by `Reconciliation_Reporter.build(catalog_at_start=empty, records=pi(R), duplicate_groups=G, drive_would_import_count=k, ...)` is equal under field-by-field equality to the report produced under any other listing-order permutation. Furthermore, every field of the report equals its brute-force closed-form value over `R`:

- `per_source[name].object_count` = `|{r ∈ R : r.source == name}|`
- `per_source[name].total_bytes` = `Σ {r.size_bytes : r ∈ R, r.source == name}`
- `per_source[name].distinct_content_hashes` = `|{r.content_hash : r ∈ R, r.source == name}|`
- `cross_source_diff.{s3_only, gcs_only, drive_only, exactly_two, all_three}` = the partitioning of `{r.content_hash : r ∈ R}` over the three source-kind hash sets
- `same_source_dup_groups[name]` = `|{g ∈ G : g.label == "same-source" ∧ g.source == name}|`
- `cross_source_dup_groups` = `|{g ∈ G : g.label == "cross-source"}|`
- `drive_would_import` = `k`
- `estimated_bytes_to_hash` = `Σ {r.size_bytes : r ∈ R, r needed streaming-hash}`

**Validates: Requirements 18.1, 18.5**

### Property 16: First-pass safety

*For any* Cold_Start `Sync_Run` invoked with `--apply` but without `--first-pass-confirmed`, *for any* multi-source workload `R` (including workloads with same-source duplicate groups, cross-source duplicate groups, and expired-quarantine records), and *for any* configuration `(on_key_conflict, delete_propagation, quarantine_retention_days, tombstone_retention_days)`:

- the count of `set_tag(key, "mcps-quarantined-at", ...)` calls observed on every adapter is exactly `0`;
- the count of `delete(key)` calls observed on every adapter is exactly `0`;
- the count of `write_bytes(key, ...)` calls observed against any destination key whose pre-existing destination Content_Hash differs from the incoming Content_Hash (i.e. the `overwrite` arm of `on_key_conflict`) is exactly `0`;
- the count of `write_bytes(key, ...)` calls against destinations where the incoming Content_Hash is **absent** from the destination is unconstrained (req 18.3 explicitly permits non-destructive replication writes);
- the count of `Drive_Importer` `write_bytes(...)` calls to the configured `drive_destination` for files whose Content_Hash is absent from every Replicated_Source is unconstrained (req 18.3 explicitly permits Drive_Importer-to-absent uploads);
- the run's exit code equals `FIRST_PASS_REVIEW_REQUIRED`.

**Validates: Requirements 18.3, 18.7**

### Property 17: Inconsistency_Detector soundness

*For any* post-replication state — that is, *for any* `Catalog` snapshot at run start `C0`, *for any* `ObjectRecord` set `R_observed` listed during the run, *for any* set of replicated-source names `RS`, and *for any* set of replication-error Content_Hashes `E` recorded in the Manifest — the WARN log entries emitted by `Inconsistency_Detector.emit(...)` are in one-to-one correspondence with the set:

`{ h : ∃ s1, s2 ∈ RS, ∃ r1, r2 ∈ R_observed where r1.source == s1 ∧ r1.content_hash == h ∧ ¬∃ r2 ∈ R_observed where r2.source == s2 ∧ r2.content_hash == h ∧ h ∉ E }`

(restated: WARN entries equal the set of Content_Hashes that are present in some Replicated_Source, absent from another Replicated_Source, and have no `replication-error` Manifest entry).

Furthermore, *for any* `Catalog` `C0` and `R_observed`, the SUMMARY log fields `new_records_per_source[name]` and `removed_records_per_source[name]` equal:

- `new_records_per_source[name]` = `|{ (r.source, r.key) : r ∈ R_observed, r.source == name } \ { (rec.source, rec.key) : rec ∈ C0.records_for_source(name) }|`
- `removed_records_per_source[name]` = `|{ (rec.source, rec.key) : rec ∈ C0.records_for_source(name) } \ { (r.source, r.key) : r ∈ R_observed, r.source == name }|`

The run's exit code is non-zero iff `divergent_hashes_count > 0` and `replication.fail_on_inconsistency == true`.

**Validates: Requirements 19.1, 19.2, 19.3**

## Error Handling

The package exposes a single exception hierarchy in `mcps/errors.py`:

```
McpsError
 ├─ ConfigError(path, line, field)         # 17.7-17.9, 8.7
 ├─ LegacyConfigDetected(path)             # 1.5
 ├─ CredentialError(provider, sources_tried)  # 1.3, 1.4
 ├─ CatalogParseError(path, line)          # 3.6
 ├─ LockConflict(holder_pid)               # 16.5
 ├─ RetriesExhausted(operation, last)      # 2.6, 12.5
 ├─ NonTransientError(status, body)        # 12.2
 ├─ ReplicationVerifyMismatch(src, dst, key, expected, observed)  # 6.5
 ├─ ReadOnlySourceError(adapter, op)       # 10.8
 ├─ ManifestWriteError(path, cause)        # 14.6
 ├─ LastCopyProtectionViolation(content_hash, source)  # 5.10, 9.6
 └─ ColdStartListingFailed(source_name, source_kind, cause)  # 18.6
```

Mapping rules:

- Anything inside the per-object loop (Replicator, Drive_Importer, Duplicate_Resolver) catches `McpsError` for that single object, emits a Manifest record with `result=error` and the specific `error` field, and continues. The run exits non-zero at the end if any such record was emitted (`exit code 2`, `RUN_HAD_ERRORS`).
- `ConfigError`, `LegacyConfigDetected`, `CredentialError`, `CatalogParseError`, `LockConflict`, and `ManifestWriteError` are fatal and abort before any provider mutation.
- `RetriesExhausted` for **listing** is fatal for that Source only — the run continues with other Sources (req 2.7); for **per-object** operations it is a per-record error (req 12.5).
- Manifest writes failing mid-run is special: we cannot record an error in the manifest when the manifest itself is broken, so we log to stderr and exit non-zero, leaving the partial manifest in place for forensics (req 14.6).
- `ColdStartListingFailed` is fatal **only on a Cold_Start** (req 18.6): on a Cold_Start, the Reconciliation_Report cannot be produced from a partial listing because cross-source diff counts would be misleading, so any single Source listing exhausting its retries aborts the run with exit code `COLD_START_LISTING_FAILED` (77). This exit code is distinct from `LOCK_CONFLICT` (73), `FIRST_PASS_REVIEW_REQUIRED` (76), and the per-Source-skip-and-continue behaviour of req 2.7 which only applies on non-Cold_Start runs. On a non-Cold_Start run, listing failure of a single Source still follows req 2.7 (skip the Source, continue, record `list-error` in the Manifest).

## Testing Strategy

The test suite is split into three tiers, mapped to a Python `pytest` layout under `tests/`:

```
tests/
  unit/                     # fast, no I/O, hypothesis-heavy
    test_catalog_roundtrip.py
    test_manifest_roundtrip.py
    test_config_roundtrip.py
    test_duplicate_detector.py
    test_duplicate_resolver.py
    test_replicator.py
    test_drive_importer.py
    test_retry.py
    test_redaction.py
    test_concurrency.py
    test_hashing.py
  integration/              # uses moto for S3, fakes for GCS+Drive
    test_s3_adapter.py
    test_gcs_adapter.py
    test_drive_adapter.py
    test_full_run_dry.py
    test_full_run_apply.py
    test_lock_stale_pid.py
  smoke/                    # single-execution checks
    test_cli_help.py
    test_legacy_config_detected.py
```

### Property-based tests (Hypothesis)

The Python property-based testing library is **Hypothesis** (already the de facto standard; not implemented from scratch). Each of the 14 properties above is implemented as one `@given(...)` test with `settings(max_examples=200, deadline=None)`. Each test carries a comment in this exact format so that the design document and the implementation stay in lock-step:

```python
# Feature: multicloud-photo-sync, Property 5: Canonical-survives invariant (last-copy-protection)
@given(catalog=catalogs(), config=run_configs())
@settings(max_examples=200, deadline=None)
def test_canonical_survives_invariant(catalog, config):
    ...
```

Generators (`hypothesis.strategies`) are written for:

- `object_records()` — ASCII keys, byte sizes 0..10⁹, content_hash sampled from a small pool so duplicates are likely.
- `catalogs()` — `dict[content_hash, frozenset[ObjectRecord]]` of size 0..200.
- `manifest_records()` — sampled `Action` and `Result`, well-formed timestamps.
- `configs()` — every field within its declared range; one of `{toml, yaml}` for the format.
- `fake_adapter(records)` — an in-memory `SourceAdapter` whose `read_bytes` / `write_bytes` operate on a `dict[key, bytes]` and whose mutation methods record call sites for assertions.
- `fault_injectors()` — `n` of the next `m` calls raise `TransientError` / `NonTransientError` for retry tests.

Properties 6, 7, 8, 13, and 14 use `fake_adapter` in place of real S3/GCS/Drive clients, so they remain cheap enough for 200 examples × multiple properties to run in seconds.

### Example-based unit tests

Used for requirements classified `EXAMPLE` or `EDGE_CASE` in the prework that did not consolidate into a property:

- 1.3, 1.4: credential resolution failure modes
- 3.5: missing/empty catalog initialises empty
- 5.5, 5.6: interactive vs non-interactive confirmation
- 10.10: Drive service-account auth failures
- 13.1: `--dry-run` flag activates dry-run mode
- 14.6, 14.7: manifest write/dir failures
- 15.5: manifest I/O failure
- 17.2: default config path resolution
- 1.5: legacy `config.ini` detection
- 16.5, 16.6: lock conflict and stale-PID reclaim

### Integration tests

`moto` for S3 (in-process AWS fake), `google-cloud-storage` testbench for GCS where available (else hand-rolled fake), and a hand-rolled fake `discovery.build('drive', 'v3', ...)` returning canned `files().list` responses. These run a full CLI Sync_Run end-to-end and verify the cross-component interactions (lock acquisition, manifest filename, exit codes).

### Smoke tests

Single-execution checks for the CLI surface: `mcps --help` exits 0; running `mcps` against a workspace containing a plaintext-credential `config.ini` aborts with a specific exit code and message (req 1.5).

## Security and Secret Handling

### `[REDACTED]` enforcement

The `mcps.redaction.Redactor` is a single object plumbed into both the `Manifest_Writer` and the JSON `logging.Formatter`. Every record produced by either component passes through `Redactor.scrub(record)` before serialization. `scrub` walks the dict, and for each string value runs a chain of regexes that target known secret shapes:

| Pattern | Description |
|---|---|
| `AKIA[0-9A-Z]{16}` | AWS Access Key Id |
| `(?<=aws_secret_access_key[=:\s]+)[A-Za-z0-9/+=]{40}` | AWS secret access key (in surrounding context) |
| `(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----` | PEM private key block |
| `ya29\.[A-Za-z0-9_\-]+` | Google OAuth access token |
| `1//[A-Za-z0-9_\-]+` | Google refresh token |
| `(?<=X-Amz-Signature=)[A-Fa-f0-9]+` | S3 presigned-URL signature |
| `Bearer\s+[A-Za-z0-9_\-\.=]+` | Bearer token in Authorization headers |

Any match is replaced with `[REDACTED]`. Field names known to carry secrets (`aws_secret_access_key`, `private_key`, `private_key_id`, `client_secret`, `refresh_token`, `access_token`, `Authorization`) have their entire value replaced regardless of regex match. Property 12 (Redaction) tests this end-to-end with Hypothesis-generated payloads.

### Credential resolution chain

`mcps.credentials.resolve()` returns a small dataclass per provider:

- **AWS**: tries env (`AWS_ACCESS_KEY_ID`+`AWS_SECRET_ACCESS_KEY`+optional `AWS_SESSION_TOKEN`) → named profile (`AWS_PROFILE`) → instance/container role via `boto3.Session().get_credentials()`. Returns the first complete set or raises `CredentialError(provider="aws", sources_tried=[...])`.
- **GCP**: tries `GOOGLE_APPLICATION_CREDENTIALS` (path must exist and contain a valid JSON service account file) → `google.auth.default()` for ADC. The same scope set (`storage` and `drive.readonly`) is requested.
- **Drive**: piggybacks on `GOOGLE_APPLICATION_CREDENTIALS` with the `drive.readonly` scope only.

The chain runs with a 10s wall-clock guard via `concurrent.futures.ThreadPoolExecutor.submit(...).result(timeout=10)` (req 1.3).

### Legacy `config.ini` detection

At the very start of `cli.main`, before any other I/O, `mcps.cli.detect_legacy_config(os.getcwd())` checks for a `config.ini` file under the working directory. If present, the file is opened in **read-only** mode and parsed with the stdlib `configparser` *only enough to check section/key names* — values are never read or logged. If the file contains a `[aws_credentials]` section with either `aws_access_key_id` or `aws_secret_access_key`, the CLI raises `LegacyConfigDetected` and exits with the `LEGACY_CONFIG` exit code (66). The error message names the file path and points the operator at the migration plan (no value is included).

## CLI Surface

`mcps` is a single `argparse`-based CLI. Subcommands were considered (`mcps sync`, `mcps doctor`, `mcps replay`) but rejected for v1 in favour of flags only — there is exactly one operational verb per invocation: run a Sync_Run.

```
mcps [--config PATH]
     [--dry-run | --apply]
     [--auto-approve]
     [--first-pass-confirmed]
     [--log-level {DEBUG,INFO,WARN,ERROR}]
     [--run-id ID]
     [--catalog PATH]      # overrides runtime.catalog_path
     [--manifest-dir PATH] # overrides runtime.manifest_dir
     [--lock-path PATH]    # overrides runtime.lock_path
```

Mode selection rules:

- Exactly one of `--dry-run` / `--apply` must be supplied; if neither, `--dry-run` is implied with a stderr warning.
- `--auto-approve` is only meaningful with `--apply`. It bypasses the interactive quarantine prompt (req 5.5, 5.6).
- `--first-pass-confirmed` is only meaningful with `--apply` on a Cold_Start Sync_Run (i.e. when the in-memory Catalog is empty at run start, req 18.7). On a Cold_Start `--apply` run **without** this flag, destructive actions are suppressed and the run exits `FIRST_PASS_REVIEW_REQUIRED` after producing the Reconciliation_Report (req 18.3). On a Cold_Start `--apply` run **with** this flag, destructive actions proceed subject to req 5.5/5.6/9.6/9.7. On a non-Cold_Start Sync_Run, supplying `--first-pass-confirmed` emits a `WARN`-level log record and is treated as a no-op (req 18.7).
- All other settings come from `--config` (default `./mcps.config.yaml`).

### Exit codes

| Code | Constant | Meaning |
|---:|---|---|
| 0 | `OK` | All actions succeeded; no error Manifest entries |
| 2 | `RUN_HAD_ERRORS` | Sync_Run completed but ≥1 per-object error was recorded |
| 64 | `CONFIG_INVALID` | `Config_Parser` rejected the configuration (req 17.7-17.9, 8.7, 9.1, 12.6, 16.1) |
| 65 | `CATALOG_INVALID` | Catalog file present but unparseable (req 3.6) |
| 66 | `LEGACY_CONFIG` | Legacy `config.ini` with plaintext keys detected (req 1.5) |
| 67 | `MANIFEST_UNAVAILABLE` | Manifest dir missing/non-writable (req 14.7) or write failure mid-run (req 14.6) |
| 71 | `CREDENTIAL_FAILED` | No credential source produced a complete set, or 401/invalid creds (req 1.3, 1.4) |
| 72 | `CONFLICT_FAILURE` | `fail_on_conflict=true` and ≥1 `key-conflict` recorded (req 8.4) |
| 73 | `LOCK_CONFLICT` | Another live process holds the writer lock (req 16.5) |
| 74 | `INTERACTIVE_REQUIRED` | `--apply` without `--auto-approve` and stdin is not a TTY (req 5.6) |
| 75 | `DRIVE_ACCESS_FAILED` | Drive credential file unreadable or root folder inaccessible (req 10.10) |
| 76 | `FIRST_PASS_REVIEW_REQUIRED` | Cold_Start `--apply` run completed Reconciliation_Report and non-destructive replication, awaiting `--first-pass-confirmed` to authorise destructive actions (req 18.3) |
| 77 | `COLD_START_LISTING_FAILED` | Cold_Start Sync_Run aborted because at least one Source's listing exhausted its retries; no Reconciliation_Report produced (req 18.6) |
| 78 | `INCONSISTENCY_DETECTED` | `replication.fail_on_inconsistency=true` and the Inconsistency_Detector observed ≥1 divergent Content_Hash with no replication-error (req 19.2) |

The codes match the BSD `sysexits.h` convention (64 = USAGE/CONFIG family, 71-75 = service-related) so that systemd / cron post-processing can distinguish operational from data errors.

## Migration Plan

The existing `uploader.py` / `delete.py` / `config.ini` setup is replaced wholesale, but the migration must be done in a specific order to (a) get the leaked AWS keys out of the working tree and (b) keep an emergency rollback path. The recommended order is:

1. **Rotate the leaked AWS credentials immediately**, before any other change.
   The AWS access key in `config.ini` (`AKIAYQ4K35M7H3INY75N`) has been visible on disk and likely committed in git history; assume it is compromised. In the AWS console, mark it as inactive, create a new access key for an IAM user scoped to only `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:GetObjectTagging`, `s3:PutObjectTagging` on the relevant bucket(s), and once the new key is wired in, delete the old one. **Do not skip this step** — moving the file out of the repo is not enough; the secret has already been on disk in plaintext.

2. **Move `credentials.json` (the Drive service account file) out of the repo**.
   - Choose a location outside the working tree, e.g. `~/.config/mcps/drive-service-account.json` with mode `0600`.
   - Set `GOOGLE_APPLICATION_CREDENTIALS=~/.config/mcps/drive-service-account.json` in the operator's shell profile or systemd unit.
   - Confirm the service account has been granted Viewer access to the `drive_root_folder_id` (run `mcps` once after step 5; it will fail fast with `DRIVE_ACCESS_FAILED` if not).

3. **Add `.gitignore` entries** to prevent recurrence:
   ```
   # mcps secrets and runtime artefacts
   config.ini
   credentials.json
   *.service-account.json
   mcps.catalog.jsonl
   mcps.catalog.jsonl.lock
   manifests/
   logfile.log
   ```
   Audit git history for any committed copy of `config.ini` or `credentials.json`. If found, plan a separate history-rewrite (BFG / `git filter-repo`) — not part of this change set, but flagged for the operator.

4. **Generate the new `mcps.config.yaml`** at the repo root. Minimum viable file mirroring the current behaviour (Drive folder `Marta` → S3 bucket `pickbackup`):

   ```yaml
   sources:
     - name: s3-pickbackup
       kind: s3
       bucket: pickbackup
       region: us-east-1
     - name: drive-marta
       kind: google_drive
       drive_root_folder_id: <FOLDER_ID_OF_Marta>
   replication:
     pairs: []
     on_key_conflict: skip
     fail_on_conflict: false
     delete_propagation: none
     tombstone_retention_days: 30
   duplicates:
     canonical_source_priority: ["s3-pickbackup"]
     quarantine_retention_days: 30
   photos:
     drive_source: drive-marta
     drive_destination: s3-pickbackup
   retries:
     max_retries: 5
     initial_backoff_ms: 500
     max_backoff_ms: 30000
     request_timeout_ms: 30000
   runtime:
     catalog_path: ./mcps.catalog.jsonl
     manifest_dir: ./manifests
     max_concurrent_transfers: 4
   ```

   Note that `replication.pairs` is empty until a second writable Source (e.g. a GCS bucket) is configured; the existing one-way Drive-to-S3 flow runs purely through `Drive_Importer`, with no S3↔GCS replication, so the replication phase is a no-op until the operator opts in.

5. **Install the package** (e.g. `pip install -e .` from the new package root) and run a first **dry run**:

   ```
   mcps --config mcps.config.yaml --dry-run
   ```

   Expected outcome: the legacy `config.ini` detection at the very start of `main()` will refuse to run and exit with code 66 until step 6 completes.

6. **Delete `config.ini`, `delete.py`, `uploader.py`, `delete_list.txt`, and `logfile.log`** from the working tree. The new tool replaces all of their behaviours:
   - `uploader.py` → `Drive_Importer` (one-way pull from Drive into the configured destination, with hash-based dedupe instead of `head_object`).
   - `delete.py` + `delete_list.txt` → `Duplicate_Resolver` (quarantine-based deletion gated by canonical-survives, with no plaintext list of object keys to maintain).
   - `logfile.log` → JSONL manifest in `runtime.manifest_dir` plus structured logs to stderr.

7. **Re-run `mcps --dry-run`** and review the planned actions. Then run `mcps --apply` to execute, but note that on this very first apply against the pre-populated buckets and Drive folder the Catalog is empty (Cold_Start, req 18). The Cold_Start apply cycle is a **two-step** review-then-confirm process:

   a. First `mcps --apply` (no `--first-pass-confirmed`):
      - Lists every Object in every Source, stream-hashing all of them since no `mcps-content-sha256` user-metadata exists yet (req 18 introduction).
      - Performs `Drive_Importer` uploads from Drive into `s3-pickbackup` for every Drive Content_Hash absent from S3 (req 18.3 explicitly permits Drive_Importer-to-absent uploads under unconfirmed Cold_Start `--apply`).
      - Performs `Replicator` writes to absent destinations only, with no `overwrite` arm and no Quarantine `set_tag` and no physical delete (req 18.3).
      - Writes the Reconciliation_Report to stdout and to `<manifest_dir>/reconciliation-<UTC-timestamp>-<run-id>.txt` (req 18.2).
      - Exits with code 76 (`FIRST_PASS_REVIEW_REQUIRED`).

   b. Operator inspects the on-disk `reconciliation-*.txt` and the Manifest. Once satisfied that the per-Source counts, cross-source diffs, and same-source / cross-source duplicate group counts are what they expect:

      ```
      mcps --apply --first-pass-confirmed                # interactive
      mcps --apply --first-pass-confirmed --auto-approve # under cron/systemd, non-interactive
      ```

   The confirmed run proceeds through the full Apply path, gated only by the existing safety rules (req 5.5/5.6 for interactive confirmation of duplicate quarantine, req 9.6/9.7 for last-copy-protection). Because the Catalog written by the unconfirmed run in step (a) is non-empty, no subsequent run is a Cold_Start, and `--first-pass-confirmed` becomes a no-op (req 18.7).

8. **Schedule** under cron or systemd. Example systemd timer unit:

   ```ini
   # /etc/systemd/system/mcps.service
   [Service]
   Type=oneshot
   Environment=AWS_PROFILE=mcps
   Environment=GOOGLE_APPLICATION_CREDENTIALS=/home/mcps/.config/mcps/drive-service-account.json
   ExecStart=/usr/local/bin/mcps --config /etc/mcps/mcps.config.yaml --apply --auto-approve
   ```

   ```ini
   # /etc/systemd/system/mcps.timer
   [Timer]
   OnCalendar=hourly
   Persistent=true

   [Install]
   WantedBy=timers.target
   ```

   Exit code 73 (`LOCK_CONFLICT`) tells systemd that another invocation is in progress and the timer should retry next tick. Exit code 2 (`RUN_HAD_ERRORS`) tells the operator to inspect the latest manifest in `runtime.manifest_dir`.

After step 8 the legacy files are gone, the AWS credentials live in `~/.aws` or instance metadata, the Drive credentials live in a path referenced by `GOOGLE_APPLICATION_CREDENTIALS`, and the repo working tree contains no provider secrets.
