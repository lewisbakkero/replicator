# Implementation Plan

## Overview

This plan implements the `mcps` (MultiCloud_Photo_Sync) Python package end to end: data models with round-trip parsers, the `SourceAdapter` ABC and three provider adapters (S3 via boto3, GCS via google-cloud-storage, Google Drive read-only via google-api-python-client), the retry decorator, the bounded executor and writer-lock, the deduplication / replication / Drive-import pipeline, the Reconciliation_Reporter and Inconsistency_Detector, and the CLI. Tasks are ordered so each task only depends on tasks above it. Every leaf task references the requirements it implements. Tasks paired with a correctness property in `design.md` include a sub-task that writes the Hypothesis property test using the exact comment format specified there.

## Notes

- Property-based tests use Hypothesis with `@settings(max_examples=200, deadline=None)` and the comment header format `# Feature: multicloud-photo-sync, Property N: <title>` so design and implementation stay in lock-step.
- All side-effecting code lives behind `SourceAdapter`; tests substitute a `FakeSourceAdapter` for fast property runs and use `moto` (S3) and in-process fakes (GCS, Drive) for integration runs.
- Migration tasks (41-43) MUST run after the first successful Cold_Start two-step Apply (task 37). Rotating the leaked AWS key (task 41) is the first migration step and must happen before any production run.
- Wave 1 contains the foundation (scaffold + errors). Subsequent waves each unblock a roughly self-contained slice (data models, adapters, core logic, CLI, integration tests, migration).

## Tasks

- [x] 1. Scaffold the `mcps/` package and dev tooling
  - Create the `mcps/` package directory with empty `__init__.py` files for `mcps/`, `mcps/config/`, `mcps/catalog/`, `mcps/manifest/`, `mcps/sources/`, `mcps/duplicates/`.
  - Create `pyproject.toml` declaring runtime deps `boto3`, `google-cloud-storage`, `google-api-python-client`, `google-auth`, `pyyaml`, `tomli`, `tomli-w`, and dev deps `pytest`, `hypothesis`, `moto[s3]`, `pytest-cov`.
  - Configure entry point `mcps = mcps.cli:main`.
  - Add `tests/` skeleton with `unit/`, `integration/`, `smoke/` subdirectories and a shared `tests/conftest.py`.
  - Add a `Makefile` (or `tox.ini`) target for `lint`, `test`, `test-property` so subsequent tasks can run them.
  - _Requirements: foundational; no specific acceptance criterion_

- [x] 2. Define the exception hierarchy in `mcps/errors.py`
  - Implement the `McpsError` base class and every subclass listed in design.md's Error Handling section: `ConfigError`, `LegacyConfigDetected`, `CredentialError`, `CatalogParseError`, `LockConflict`, `RetriesExhausted`, `NonTransientError`, `ReplicationVerifyMismatch`, `ReadOnlySourceError`, `ManifestWriteError`, `LastCopyProtectionViolation`, `ColdStartListingFailed`.
  - Each exception carries the structured fields named in the design (e.g. `LockConflict.holder_pid`, `ColdStartListingFailed(source_name, source_kind, cause)`).
  - Define an `ExitCode` IntEnum with every entry in the design's exit-code table (0, 2, 64, 65, 66, 67, 71, 72, 73, 74, 75, 76, 77, 78).
  - Unit tests: confirm exit codes match the design table and exceptions carry the documented fields.
  - _Requirements: 1.5, 3.6, 5.10, 6.5, 9.6, 10.8, 14.6, 16.5, 17.7-17.9, 18.6_

- [x] 3. Implement the `ObjectRecord` and `Catalog` data models
  - Create `mcps/catalog/model.py` with the frozen `ObjectRecord` dataclass exactly as specified in design.md (fields: source, key, content_hash, size_bytes, last_seen_at, last_modified, content_type, quarantined_at, tombstoned_at, mcps_source_meta).
  - Implement the `Catalog` class with `by_hash: dict[str, frozenset[ObjectRecord]]`, plus `all_records()`, `upsert(rec)`, `remove(source, key)`, `records_for_source(name)`.
  - Unit tests covering equality, hashability, ordering, and the upsert/remove invariants.
  - _Requirements: 3.2, 11.3, 11.5_

- [x] 4. Implement `Catalog_Parser` and `Catalog_Printer` with round-trip
  - Create `mcps/catalog/parser.py` with a streaming JSONL parser that builds a `Catalog` and raises `CatalogParseError(path, line)` on the first malformed line without mutating any input file.
  - Create `mcps/catalog/printer.py` writing records sorted by `(content_hash, source, key)` using `json.dumps(asdict(rec), sort_keys=True, separators=(",",":"), ensure_ascii=False)` per line, with atomic-replace via `tempfile.NamedTemporaryFile` + `os.replace`.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_catalog_roundtrip.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 1: Catalog round-trip`
    - Strategy `catalogs()` of size 0..200; assert `Catalog_Parser(Catalog_Printer(c)) == c` and printer output is byte-deterministic.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 5. Implement Catalog cache-hit logic
  - Add `Catalog.cache_lookup(source, key, size, last_modified) -> Optional[str]` returning the cached `content_hash` only when all four fields match, otherwise `None`.
  - Unit tests covering size-changed, mtime-changed, and exact-match cases.
  - _Requirements: 3.7, 3.8_

- [x] 6. Implement the `Manifest_Record` model and enums
  - Create `mcps/manifest/model.py` with `Action` and `Result` string enums covering every value in design.md's enum tables (DISCOVERED, REPLICATE, REPLICATE_SKIP, LOOP_SKIP, SOURCE_TAGGED, HASH_RECOMPUTED, KEY_CONFLICT, OVERWRITE, RENAME, QUARANTINE, PHYSICAL_DELETE, LAST_COPY_GUARD, TOMBSTONE, DRIVE_SKIP_*, DRIVE_IMPORT_OK, DRIVE_DOWNLOAD_E, DRIVE_WARN_TIME, LIST_ERROR, HASH_ERROR, RETRIES_EXHAUSTED, REPLICATION_ERROR, SUMMARY).
  - Implement the frozen `ManifestRecord` dataclass with the documented field set.
  - Unit tests for enum coverage and required-field validation.
  - _Requirements: 14.2, 15.1_

- [x] 7. Implement `Manifest_Parser`, `Manifest_Printer`, and `Manifest_Writer`
  - Create `mcps/manifest/parser.py` returning a `(records, errors)` tuple for partial parses and `mcps/manifest/printer.py` emitting JSONL with LF terminators and UTF-8 (no BOM).
  - Create `mcps/manifest/writer.py`: streaming append, single-thread `Lock` for line atomicity, raises `ManifestWriteError` on I/O failure.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_manifest_roundtrip.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 2: Manifest round-trip`
    - Strategy `manifest_records()` of length 0..10_000; assert printer-then-parser yields element-wise equality and output is LF-terminated UTF-8 without BOM.
  - Example-based unit tests for parse-failure line numbering and I/O failure cases.
  - _Requirements: 14.1, 14.2, 14.6, 14.7, 15.1, 15.2, 15.3, 15.4, 15.5_

- [x] 8. Implement the `Redactor` in `mcps/redaction.py`
  - Implement the regex chain documented in design.md's "Security and Secret Handling" section (AWS Access Key Id, AWS secret in surrounding context, PEM blocks, `ya29.*`, `1//*`, `X-Amz-Signature=`, `Bearer …`).
  - Implement field-name allowlist replacement (`aws_secret_access_key`, `private_key`, `private_key_id`, `client_secret`, `refresh_token`, `access_token`, `Authorization`).
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_redaction.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 12: Redaction — no secret material in any output`
    - Strategy: payloads injected with credential-shaped substrings; assert `[REDACTED]` replaces every match and the secret substring never appears in serialised output.
  - _Requirements: 1.6, 14.4_

- [x] 9. Implement structured JSON logging in `mcps/logging_setup.py`
  - Configure a `logging.Formatter` that emits one JSON object per record to stderr with fields `timestamp`, `level`, `run_id`, `event`, `message` (level enum DEBUG/INFO/WARN/ERROR).
  - Plumb `Redactor.scrub` into the formatter so every log line is redacted before serialization.
  - Provide `bind_run_id(logger, run_id)` so every record carries the current `run_id`.
  - Unit tests for level filtering, JSON shape, and redaction integration.
  - _Requirements: 14.3, 14.4, 14.5_

- [x] 10. Implement the `Config` model in `mcps/config/model.py`
  - Frozen dataclasses for `SourceConfig`, `ReplicationConfig` (including `fail_on_inconsistency: bool = False`), `DuplicatesConfig`, `PhotosConfig`, `RetriesConfig`, `RuntimeConfig`, `Config`.
  - Apply the validation ranges documented in design.md (e.g. `max_retries 1..10`, `max_concurrent_transfers 1..64`, `tombstone_retention_days 1..3650`).
  - Provide `Config.lookup_source(name)` and `Config.replicated_sources()` helpers.
  - Unit tests for boundary validation per field.
  - _Requirements: 8.6, 8.7, 9.1, 9.4, 12.6, 16.1, 17.4, 19.3_

- [x] 11. Implement `Config_Parser` and `Config_Printer`
  - Create `mcps/config/parser.py` that loads a TOML or YAML file (extensions `.toml`, `.yaml`, `.yml`; max 1 MiB), enforces the six top-level sections, rejects unknown keys with line numbers, validates source kinds (`s3`/`gcs`/`google_drive`), and raises `ConfigError` with a dotted-path field name on missing or out-of-range values.
  - Create `mcps/config/printer.py` emitting deterministic output via `tomli_w.dump` for TOML and `yaml.safe_dump(sort_keys=False, default_flow_style=False)` for YAML, preserving the loaded format (defaulting to YAML).
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_config_roundtrip.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 3: Config round-trip`
    - Strategy `configs()` over both `toml` and `yaml`; assert field-by-field equality across all six sections including `fail_on_inconsistency`.
  - Example-based tests for default-path resolution, unknown-key rejection with line numbers, missing-required-field with dotted paths, file-too-large rejection.
  - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 17.6, 17.7, 17.8, 17.9_

- [x] 12. Implement legacy `config.ini` detection in `mcps/cli.py` (helper)
  - Add `mcps.cli.detect_legacy_config(cwd)` that opens `config.ini` read-only, parses only section/key names with `configparser` (never reads values), and raises `LegacyConfigDetected(path)` if `[aws_credentials]` contains `aws_access_key_id` or `aws_secret_access_key`.
  - Smoke test: planted `config.ini` with plaintext keys → exit code 66; planted file without those keys → no error.
  - _Requirements: 1.5_

- [x] 13. Implement `Credential_Manager` in `mcps/credentials.py`
  - AWS chain: env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`) → `AWS_PROFILE` → instance/container role via `boto3.Session().get_credentials()`. Returns the first complete set or raises `CredentialError`.
  - GCP chain: `GOOGLE_APPLICATION_CREDENTIALS` → `google.auth.default()` for ADC.
  - Drive: same SA file, `drive.readonly` scope only.
  - Wrap the chain in a 10-second wall-clock guard via `ThreadPoolExecutor.submit(...).result(timeout=10)`.
  - On a 401 / invalid-credentials error during a Sync_Run, raise `CredentialError(provider, sources_tried)`.
  - Example-based unit tests for each failure mode.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 10.10_

- [x] 14. Implement the retry decorator in `mcps/retry.py`
  - Define `TransientError(status, retry_after_seconds=None)`, `NonTransientError(status, body)`, and `RetriesExhausted(operation, last, attempts)`.
  - Implement `classify_http(status, *, expect_404_as_absent)` returning one of `transient`, `non_transient`, `absent`, `ok`.
  - Implement `retry_transient(*, max_retries, initial_backoff_ms, max_backoff_ms, request_timeout_ms, sleep, now)` with injectable clocks. Honor `Retry-After` by selecting `max(computed_backoff, retry_after)` capped at `max_backoff_ms`.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_retry.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 11: Retry bounds and Retry-After honoring`
    - Strategy: sequences of `n` `TransientError` outcomes followed by `ok`, with `retry_after_seconds` ≥ 0 on any of them. Assert success when `n ≤ max_retries`, `RetriesExhausted` otherwise; assert every observed sleep `s` satisfies `0 ≤ s ≤ max_backoff_ms / 1000` and `s ≥ max(computed_backoff, retry_after)`. Non-transient errors are never retried.
  - _Requirements: 2.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

- [x] 15. Implement streaming SHA-256 in `mcps/hashing.py`
  - Implement `stream_sha256(chunks)` and `s3_etag_is_singlepart(etag)` (32 hex chars, no `-N`).
  - Implement `compute_content_hash(adapter, meta, catalog)` with the three-step priority documented in design.md: `mcps-content-sha256` user-metadata (validated 64-char lowercase hex) → Catalog cache hit on `(source, key, size, last_modified)` → streamed SHA-256.
  - Unit tests covering each priority branch and the validation of `mcps-content-sha256` shape.
  - _Requirements: 2.2, 2.3, 2.4, 3.7, 7.1, 7.2_

- [x] 16. Implement the `SourceAdapter` ABC in `mcps/sources/base.py`
  - Define `ObjectMeta` frozen dataclass and the `SourceAdapter` ABC exactly as in design.md (`list_objects`, `read_bytes`, `write_bytes`, `get_metadata`, `set_tag`, `delete`, `supports_writes`).
  - Define `ReadOnlySourceError` semantics: raise from `write_bytes`, `set_tag`, `delete` for read-only adapters.
  - Provide an in-memory `FakeSourceAdapter(records: dict[key, bytes], *, kind, name, supports_writes)` for tests, with call-site recording for assertions.
  - Unit tests covering the ABC contract via the fake.
  - _Requirements: 2.1, 10.8_

- [x] 17. Implement `S3SourceAdapter` in `mcps/sources/s3.py`
  - Use `boto3.client('s3')`. `list_objects` paginates `list_objects_v2` and emits `ObjectMeta` with `etag` (quotes stripped) and `provider_hash` set to the ETag when `s3_etag_is_singlepart` is true.
  - `read_bytes` streams via `get_object`'s `Body` in 1 MiB chunks.
  - `write_bytes` uses `put_object` for ≤5 MiB and `upload_fileobj` (multipart) above; sets `Metadata=` with `mcps-source` and `mcps-content-sha256`.
  - `get_metadata` uses `head_object` (and `get_object_tagging` to surface `mcps-quarantined-at` / `mcps-tombstoned-at` tags).
  - `set_tag` uses `put_object_tagging` (preserves prior tags).
  - `delete` uses `delete_object`.
  - Wrap every call with the `retry_transient` decorator, mapping `botocore.exceptions.ClientError` and timeouts into `TransientError` / `NonTransientError`.
  - _Requirements: 2.2, 2.5, 2.6, 5.7, 6.4, 6.5, 9.3, 9.5_

- [x] 18. Implement `GCSSourceAdapter` in `mcps/sources/gcs.py`
  - Use `google.cloud.storage.Client`. `list_objects` via `list_blobs(bucket, prefix=...)`; set `provider_hash` to the decoded CRC32C (informational only).
  - `read_bytes` uses `blob.open("rb")` (server-side CRC32C verification).
  - `write_bytes` uses `blob.upload_from_file` setting `metadata` for `mcps-*`.
  - `get_metadata` uses `blob.reload()`.
  - `set_tag` patches `blob.metadata` (GCS has no S3-style tags).
  - `delete` uses `blob.delete()`.
  - Wrap calls with `retry_transient`, mapping `google.api_core.exceptions` to transient/non-transient.
  - _Requirements: 2.3, 2.5, 2.6, 5.7, 6.4, 6.5, 9.3, 9.5_

- [x] 19. Implement `GoogleDriveSourceAdapter` in `mcps/sources/drive.py` (read-only)
  - Use `googleapiclient.discovery.build('drive', 'v3', ...)` with the service-account credential and `drive.readonly` scope.
  - `list_objects` paginates `files().list(q="'<root>' in parents and trashed=false", pageSize=1000, fields=...)` recursively for subfolders. Apply server-side mimeType filter where supported.
  - `read_bytes` streams via `MediaIoBaseDownload(io.BytesIO(), files().get_media(fileId=...))`.
  - `write_bytes`, `set_tag`, `delete` raise `ReadOnlySourceError`.
  - At construction time, call `files().get(fileId=drive_root_folder_id, fields="id")` and convert any error to `DriveAccessFailed` (exit 75).
  - Wrap calls with `retry_transient`.
  - _Requirements: 2.4, 2.5, 2.6, 10.1, 10.8, 10.10_

- [x] 20. Implement the `Duplicate_Detector` in `mcps/duplicates/detector.py`
  - Pure function over a `Catalog`. Group by `content_hash`, emit `DuplicateGroup(content_hash, members, label, total_size_bytes)` for groups of size ≥ 2 with all members having identical `(content_hash, size_bytes)`.
  - Label groups as `cross-source` if members span ≥ 2 distinct Sources, else `same-source`.
  - Divert records with missing/invalid hash or `size_bytes < 0` to a `skipped_records` list.
  - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6_

- [x] 21. Implement order-independence test for `Duplicate_Detector` and replication plan
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_order_independence.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 4: Order-independence of run decisions`
    - Strategy: a multi-source `ObjectRecord` set `R` and a permutation `pi`. Assert the resulting Catalog, the duplicate-group set, and the replication plan are equal under set/dict equality regardless of `pi`.
  - _Requirements: 4.4, 11.3_

- [x] 22. Implement the `Duplicate_Resolver` in `mcps/duplicates/resolver.py`
  - `pick_canonical(group, priority)` applies the deterministic tie-break (priority → earliest `last_seen_at` at ms precision → lexicographically smallest `key` UTF-8). Returns `(canonical, removable, priority_warning)`.
  - `quarantine(removable, adapters, manifest, *, dry_run, auto_approve, isatty)` runs interactive confirmation when needed, calls `set_tag(key, "mcps-quarantined-at", now_iso_seconds())` per removable, and applies last-copy-protection before each tag.
  - `physically_delete_expired(catalog, adapters, now, retention_days, manifest)` deletes expired-quarantine records under last-copy-protection.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_canonical_survives.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 5: Canonical-survives invariant (last-copy-protection)`
    - Strategy: any Catalog `c` and run config; assert every `Content_Hash` in `c` retains at least one non-quarantined, non-tombstoned record after the resolver and Replicator deletion logic complete.
  - Example-based tests for: missing/empty `canonical_source_priority` fallback (5.2), interactive prompt yes/no (5.5), non-interactive abort (5.6), set_tag failure (5.8), expired-quarantine delete (5.9), last-copy-protection skip (5.10).
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11, 9.6, 9.7_

- [x] 23. Implement the `Replicator` in `mcps/replication.py`
  - For each ordered `(src, dst)` Replicated_Source pair, compute the per-pair Content_Hash diff and produce a replication plan keyed on Content_Hash.
  - Per-object pipeline: loop check (skip if `src.mcps-source == dst.name`) → key-conflict policy (`skip`/`rename`/`overwrite` with `destructive_writes_allowed` gating) → stream copy with mcps-* metadata → post-write `get_metadata` verification → on mismatch `dst.delete(key)` + `replication-error`.
  - Source-tag missing-`mcps-source` records (`source-tagged` Manifest entry).
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_replication_eventual_consistency.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 6: Replication eventual-consistency`
    - Strategy: any pair `(A, B)` with arbitrary populations; assert post-run hash sets match, modulo `replication-error` hashes.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_loop_free.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 8: Loop-free behaviour`
    - Strategy: records with `mcps-source == dst.name` produce exactly one `loop-skip` and zero writes; records with missing `mcps-source` get tagged with originating Source name.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_conflict_table.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 10: Conflict-resolution table`
    - Strategy: cover every row of the conflict-resolution table from design.md; assert resulting destination state and exit-code rule for `fail_on_conflict`.
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.1, 7.2, 7.3, 7.4, 7.5, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [x] 24. Implement deletion-handling logic in the Replicator
  - Wire `delete_propagation` (`none`/`soft`/`hard`) into the Replicator: under `soft`, add `mcps-tombstoned-at` to peer Replicated_Sources for records present in the catalog at start but absent in the current run; under `hard`, physically delete tombstoned records older than `tombstone_retention_days`.
  - Apply last-copy-protection unconditionally (defence in depth, even under `delete_propagation=none`).
  - Treat unreachable Sources as "not absent" — never tombstone based on a failed listing.
  - Example-based tests for each propagation mode and the unreachable-Source guard.
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_

- [x] 25. Implement the `Drive_Importer` in `mcps/drive_import.py`
  - Pull-only flow: list filtered Drive files (mimeType `image/*` or `video/*`, exclude `application/vnd.google-apps.*`) → stream-hash → if hash exists in any Replicated_Source emit `drive-skip-existing` → else build destination key `google-drive/<YYYY|unknown-year>/<MM|unknown-month>/<file-id>__<sanitised-name>` (sanitise `[^A-Za-z0-9._-]` → `_`) and write to `drive_destination` with `mcps-source = <drive-source-name>` so subsequent runs treat the destination as canonical.
  - On `createdTime` parse failure emit `drive-warning-missing-created-time` and use `unknown-year/unknown-month`.
  - Provide a `plan()` method returning the would-import count for the Reconciliation_Reporter.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_drive_importer.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 13: Drive_Importer pull-only contract`
    - Strategy: arbitrary Drive files; assert no write/set_tag/delete against the Drive adapter, every destination key matches the regex from design.md, and disallowed mime types produce no destination write.
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10_

- [x] 26. Implement the bounded executor and writer-lock in `mcps/concurrency.py`
  - `make_executor(max_concurrent_transfers)` returns a `ThreadPoolExecutor` with thread name prefix `mcps-xfer`.
  - `writer_lock(path, run_id, timeout_s=5.0)` context manager using `fcntl.flock(LOCK_EX | LOCK_NB)`, writes `{pid, run_id, started_at}` JSON line, fsyncs, registers an `atexit` release; on `BlockingIOError` reads holder PID and reclaims if `os.kill(pid, 0)` raises `ProcessLookupError`.
  - On unrecoverable conflict raises `LockConflict(holder_pid)`.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_bounded_concurrency.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 14: Bounded concurrency`
    - Strategy: instrumented adapter increments/decrements an in-flight counter; assert the observed peak ≤ `max_concurrent_transfers` for any workload.
  - Example-based test: planted lock file with a non-running PID is reclaimed; planted lock file with the current process's PID exits with `LOCK_CONFLICT`.
  - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7_

- [x] 27. Implement the `Reconciliation_Reporter` in `mcps/reconciliation.py`
  - Implement `PerSourceCounts`, `CrossSourceDiff`, `ReconciliationReport` frozen dataclasses.
  - `Reconciliation_Reporter.build(...)` is a pure function: per-Source counts, per-Source-kind cross-source diff (s3_only / gcs_only / drive_only / exactly_two / all_three), same-source duplicate group counts partitioned by Source, cross-source duplicate group count, drive_would_import, estimated_bytes_to_hash (sum of `size_bytes` for records that needed streaming hashing).
  - `Reconciliation_Reporter.emit(report, *, stdout, manifest_dir)` writes a structured human-readable summary to stdout AND `<manifest_dir>/reconciliation-<UTC-timestamp>-<run-id>.txt`.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_reconciliation_report.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 15: Reconciliation_Report completeness and determinism`
    - Strategy: any multi-source `R` and any listing-order permutation; assert each report field equals its closed-form value over `R` and is permutation-invariant.
  - _Requirements: 18.1, 18.2, 18.5_

- [x] 28. Implement the `Inconsistency_Detector` in `mcps/reconciliation.py`
  - Implement `DivergentHash` and `InconsistencyReport` dataclasses.
  - `Inconsistency_Detector.analyse(...)` computes per-Source new/removed counts (symmetric difference of `(source, key)` pairs) and the divergent-hashes set (present-in-some Replicated_Source AND absent-in-some Replicated_Source AND not in `replication_error_hashes`). Pull_Only_Sources excluded from divergence analysis.
  - `Inconsistency_Detector.emit(...)` extends the SUMMARY log record with the per-Source counts, appends one WARN per divergent hash, and returns 1 when `fail_on_inconsistency=true` and divergences > 0.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_inconsistency_detector.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 17: Inconsistency_Detector soundness`
    - Strategy: any `(C0, R_observed, RS, E)` quadruple; assert WARN entries are in one-to-one correspondence with the divergent set and per-Source counts equal the symmetric-difference cardinalities.
  - _Requirements: 19.1, 19.2, 19.3_

- [x] 29. Implement idempotence sub-property tests
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_idempotence.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 7: Idempotence — second run is a no-op`
    - Strategy: produce a Catalog from a run with input `S`, then re-run with the same `S`; assert zero `write_bytes`, `delete`, `set_tag` calls and Manifest contains exactly one SUMMARY plus zero non-DISCOVERED actions.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_streamed_sha256_identity.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 9: Streamed SHA-256 identity over SourceAdapter`
    - Strategy: arbitrary `{key: bytes}` populations behind a fake adapter; assert per-record `content_hash` equals `sha256(bytes).hexdigest()` across paginated listings, with `mcps-content-sha256` shortcut and `hash-recomputed` fallback.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 7.1, 7.2, 7.5, 11.1, 11.2_

- [x] 30. Implement the CLI in `mcps/cli.py`
  - Argparse: `--config`, `--dry-run`, `--apply`, `--auto-approve`, `--first-pass-confirmed`, `--log-level`, `--run-id`, `--catalog`, `--manifest-dir`, `--lock-path`. Mutually exclusive: `--dry-run` vs `--apply`; default to `--dry-run` with stderr warning if neither.
  - Startup order: `detect_legacy_config(cwd)` → `Config_Parser` → `Credential_Manager.resolve()` → `writer_lock` → `Catalog_Parser.load()` → `Cold_Start = (catalog.is_empty)` decision.
  - On Cold_Start without `--first-pass-confirmed`: gate Replicator with `destructive_writes_allowed=False`, suppress `Duplicate_Resolver.quarantine` and `physically_delete_expired`, force `on_key_conflict=skip` semantics for the `overwrite` branch, exit 76 (`FIRST_PASS_REVIEW_REQUIRED`).
  - On Cold_Start with `--first-pass-confirmed`: full Apply path subject to req 5.5/5.6/9.6/9.7.
  - On non-Cold_Start with `--first-pass-confirmed`: emit a WARN log and treat as no-op (req 18.7).
  - Map every `McpsError` subclass to its exit code per the design table.
  - Run the `Inconsistency_Detector` after the Replicator on every run; honor `fail_on_inconsistency` for exit code 78.
  - Smoke test: `mcps --help` exits 0.
  - Sub-task — write the Hypothesis property test:
    - File: `tests/unit/test_first_pass_safety.py`
    - Comment: `# Feature: multicloud-photo-sync, Property 16: First-pass safety`
    - Strategy: any Cold_Start workload with `--apply` and no `--first-pass-confirmed`; assert zero `set_tag(mcps-quarantined-at, …)`, zero `delete`, zero overwriting `write_bytes` calls; non-destructive replicate-to-absent and Drive_Importer-to-absent unconstrained; exit code equals `FIRST_PASS_REVIEW_REQUIRED`.
  - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 18.3, 18.4, 18.7_

- [x] 31. Integration test: full Sync_Run in `--dry-run` against fake adapters
  - File: `tests/integration/test_full_run_dry.py`
  - Compose `S3SourceAdapter` (via `moto`), `GCSSourceAdapter` (in-process fake), `GoogleDriveSourceAdapter` (canned `files().list` fake) with seeded data.
  - Run `mcps.cli.main` with `--dry-run`; assert: zero mutating API calls observed; Manifest contains every planned action with `result=planned`; exit code 0 when no errors recorded.
  - _Requirements: 13.1, 13.2, 13.3, 13.5, 14.5_

- [x] 32. Integration test: full Sync_Run in `--apply --first-pass-confirmed --auto-approve`
  - File: `tests/integration/test_full_run_apply.py`
  - Seed S3 with two duplicates, GCS with one Object missing in S3, Drive with two new files. Run with `--apply --first-pass-confirmed --auto-approve`.
  - Assert: replication writes to absent destinations, Drive imports under the documented key shape, one Object quarantined under `mcps-quarantined-at`, no last-copy violation, Manifest summary counts add up, exit code 0.
  - _Requirements: 5.7, 6.2, 10.6, 14.5, 18.4_

- [x] 33. Integration test: lock-conflict and stale-PID reclaim
  - File: `tests/integration/test_lock_stale_pid.py`
  - Plant a lock file with the current process's PID, attempt a Sync_Run, assert exit code 73 within 5 seconds. Then plant a lock file with a PID known to be unused (`os.kill(pid, 0)` raises `ProcessLookupError`), attempt a Sync_Run, assert the run reclaims the lock and proceeds.
  - _Requirements: 16.5, 16.6_

- [x] 34. Integration test: S3 adapter via `moto`
  - File: `tests/integration/test_s3_adapter.py`
  - Cover list (with continuation tokens), read (streaming), write (with `Metadata=`), `put_object_tagging` for `mcps-quarantined-at`, `delete_object`. Verify post-write `head_object` round-trip.
  - _Requirements: 2.2, 2.5, 5.7, 6.4, 6.5_

- [x] 35. Integration test: GCS adapter via in-process fake
  - File: `tests/integration/test_gcs_adapter.py`
  - Cover list with prefix, streaming read with CRC32C verification, upload with metadata, metadata patch (tagging), and delete.
  - _Requirements: 2.3, 2.5, 5.7, 6.4, 6.5_

- [x] 36. Integration test: Drive adapter via canned fake
  - File: `tests/integration/test_drive_adapter.py`
  - Stub `discovery.build('drive', 'v3', ...)` returning canned `files().list` and `files().get_media` responses across pagination, mimeType filtering, native-doc skip, and 5xx-then-success retries. Assert `write_bytes`/`set_tag`/`delete` raise `ReadOnlySourceError`. Assert startup fails with exit 75 when the root folder id is not accessible.
  - _Requirements: 10.1, 10.2, 10.3, 10.8, 10.10_

- [x] 37. Integration test: Cold_Start two-step apply flow
  - File: `tests/integration/test_cold_start_two_step.py`
  - With no on-disk catalog, seed S3 with three Objects and Drive with two Objects (one of which already exists in S3 by hash). Run `mcps --apply --auto-approve` (no `--first-pass-confirmed`):
    - Assert exit code 76 (`FIRST_PASS_REVIEW_REQUIRED`).
    - Assert `<manifest_dir>/reconciliation-*.txt` was written and stdout contains the structured report.
    - Assert the Drive Object missing from S3 was uploaded to S3 (replicate-to-absent permitted).
    - Assert no quarantine `set_tag` was issued and no overwrite occurred.
  - Then run `mcps --apply --first-pass-confirmed --auto-approve`:
    - Assert destructive actions complete (e.g. duplicate quarantine), Manifest summary is non-empty, exit code 0.
  - _Requirements: 18.1, 18.2, 18.3, 18.4_

- [x] 38. Integration test: Cold_Start listing failure aborts the report
  - File: `tests/integration/test_cold_start_listing_failure.py`
  - Make the GCS adapter fail listing after exhausting retries on a Cold_Start run. Assert exit code 77 (`COLD_START_LISTING_FAILED`) and that no `reconciliation-*.txt` file was written.
  - _Requirements: 18.6_

- [x] 39. Integration test: Inconsistency detection with `fail_on_inconsistency=true`
  - File: `tests/integration/test_inconsistency_detection.py`
  - Seed a divergent state: S3 has Object H1, GCS has Object H2, both Replicated_Sources are configured. Run with `replication.fail_on_inconsistency=true`. Assert one WARN log per divergent hash, summary counts include divergence, exit code 78 (`INCONSISTENCY_DETECTED`).
  - _Requirements: 19.1, 19.2, 19.3_

- [x] 40. Smoke test: legacy `config.ini` rejection
  - File: `tests/smoke/test_legacy_config_detected.py`
  - Plant `config.ini` containing `[aws_credentials]\naws_access_key_id = AKIA…`. Run `mcps --dry-run`. Assert exit code 66 and a stderr message naming the file path. Assert no value from the file is logged.
  - _Requirements: 1.5_

- [x] 41. Migration step — rotate the leaked AWS credentials
  - Document the steps in `MIGRATION.md` and add an executable check (`mcps doctor --check-iam` or a one-shot script) that confirms the leaked key (`AKIAYQ4K35M7H3INY75N`) is no longer the active key on the AWS account by calling `iam:list-access-keys` for the bound user and asserting the key is `Inactive` or absent.
  - _Requirements: design migration plan step 1; supports req 1.5_

- [x] 42. Migration step — relocate Drive credentials and add `.gitignore`
  - Move `credentials.json` to `~/.config/mcps/drive-service-account.json` with mode `0600` and document `GOOGLE_APPLICATION_CREDENTIALS` setup.
  - Add `.gitignore` entries: `config.ini`, `credentials.json`, `*.service-account.json`, `mcps.catalog.jsonl`, `mcps.catalog.jsonl.lock`, `manifests/`, `logfile.log`.
  - _Requirements: design migration plan steps 2-3; supports req 1.6_

- [x] 43. Migration step — generate `mcps.config.yaml` and remove legacy files
  - Create the documented `mcps.config.yaml` template at the repo root with the `s3-pickbackup` and `drive-marta` sources, empty `replication.pairs`, and the runtime defaults from design.md.
  - After the first successful Cold_Start two-step Apply cycle, delete `config.ini`, `delete.py`, `uploader.py`, `delete_list.txt`, `logfile.log`.
  - _Requirements: design migration plan steps 4, 6, 7_

## Task Dependency Graph

The following waves group tasks that can be executed concurrently. Each wave only depends on tasks from earlier waves.

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"] },
    { "wave": 2, "tasks": ["2", "3", "6", "8", "10"] },
    { "wave": 3, "tasks": ["4", "5", "7", "9", "11", "12", "14"] },
    { "wave": 4, "tasks": ["13", "15", "16", "20"] },
    { "wave": 5, "tasks": ["17", "18", "19", "21", "26"] },
    { "wave": 6, "tasks": ["22", "27"] },
    { "wave": 7, "tasks": ["23", "25", "28"] },
    { "wave": 8, "tasks": ["24", "29"] },
    { "wave": 9, "tasks": ["30"] },
    { "wave": 10, "tasks": ["31", "32", "33", "34", "35", "36", "38", "39", "40"] },
    { "wave": 11, "tasks": ["37"] },
    { "wave": 12, "tasks": ["41", "42"] },
    { "wave": 13, "tasks": ["43"] }
  ]
}
```

```mermaid
flowchart TD
  T1[1. Scaffold package] --> T2[2. errors.py]
  T1 --> T3[3. ObjectRecord/Catalog model]
  T3 --> T4[4. Catalog parser/printer + P1]
  T4 --> T5[5. Catalog cache-hit]
  T1 --> T6[6. Manifest model]
  T6 --> T7[7. Manifest parser/printer/writer + P2]
  T1 --> T8[8. Redactor + P12]
  T8 --> T9[9. Logging setup]
  T1 --> T10[10. Config model]
  T10 --> T11[11. Config parser/printer + P3]
  T2 --> T12[12. Legacy config.ini detection]
  T11 --> T13[13. Credential_Manager]
  T2 --> T14[14. Retry decorator + P11]
  T5 --> T15[15. Hashing]
  T2 --> T16[16. SourceAdapter ABC + fake]
  T15 --> T16
  T14 --> T17[17. S3SourceAdapter]
  T16 --> T17
  T13 --> T17
  T14 --> T18[18. GCSSourceAdapter]
  T16 --> T18
  T13 --> T18
  T14 --> T19[19. GoogleDriveSourceAdapter]
  T16 --> T19
  T13 --> T19
  T4 --> T20[20. Duplicate_Detector]
  T20 --> T21[21. Order-independence + P4]
  T20 --> T22[22. Duplicate_Resolver + P5]
  T17 --> T22
  T18 --> T22
  T7 --> T22
  T17 --> T23[23. Replicator + P6, P8, P10]
  T18 --> T23
  T19 --> T23
  T22 --> T23
  T7 --> T23
  T23 --> T24[24. Deletion handling]
  T19 --> T25[25. Drive_Importer + P13]
  T17 --> T25
  T18 --> T25
  T7 --> T25
  T1 --> T26[26. Concurrency + P14]
  T20 --> T27[27. Reconciliation_Reporter + P15]
  T25 --> T27
  T20 --> T28[28. Inconsistency_Detector + P17]
  T9 --> T28
  T22 --> T29[29. Idempotence + Streamed-SHA256 P7, P9]
  T23 --> T29
  T25 --> T29
  T11 --> T30[30. CLI + P16]
  T12 --> T30
  T13 --> T30
  T26 --> T30
  T27 --> T30
  T28 --> T30
  T22 --> T30
  T23 --> T30
  T24 --> T30
  T25 --> T30
  T30 --> T31[31. IT: dry-run]
  T30 --> T32[32. IT: apply]
  T26 --> T33[33. IT: lock stale-PID]
  T17 --> T34[34. IT: S3 adapter via moto]
  T18 --> T35[35. IT: GCS adapter via fake]
  T19 --> T36[36. IT: Drive adapter via fake]
  T30 --> T37[37. IT: Cold_Start two-step]
  T30 --> T38[38. IT: Cold_Start listing failure]
  T30 --> T39[39. IT: Inconsistency fail_on_inconsistency]
  T12 --> T40[40. Smoke: legacy config.ini]
  T32 --> T41[41. Migration: rotate AWS keys]
  T32 --> T42[42. Migration: relocate Drive creds + gitignore]
  T37 --> T43[43. Migration: generate mcps.config.yaml + remove legacy files]
```
