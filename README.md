# mcps — MultiCloud Photo Sync

`mcps` is a deduplicated, replicating photo-and-video sync engine that operates across:

- **Amazon S3** (read + write)
- **Google Cloud Storage** (read + write)
- **Google Drive** (pull-only — read from a configured folder, never written to)

It identifies duplicates by SHA-256 content hash (not by key or filename), replicates across writable Sources to converge their content sets, imports new files from a Drive folder under a documented destination key shape, and quarantines duplicates with last-copy protection so a misconfigured run cannot orphan a content hash.

This is a successor to the `uploader.py` / `delete.py` / `config.ini` setup that previously lived at the repo root. Those files are scheduled for deletion at the end of the migration plan in [`MIGRATION.md`](MIGRATION.md).

---

## Table of contents

- [Quickstart](#quickstart)
- [Installation](#installation)
- [Credentials](#credentials)
- [Configuration](#configuration)
- [Running mcps](#running-mcps)
  - [Cold_Start two-step apply flow](#cold_start-two-step-apply-flow)
- [Exit codes](#exit-codes)
- [`mcps doctor`](#mcps-doctor)
- [On-disk artefacts](#on-disk-artefacts)
- [How it works (architecture)](#how-it-works-architecture)
- [Development](#development)
- [Migration from the legacy scripts](#migration-from-the-legacy-scripts)
- [Project layout](#project-layout)

---

## Quickstart

```bash
# 1. Install (editable mode keeps the package in sync with source edits).
pip install -e ".[dev]"

# 2. Wire credentials (no values in this repo — see Credentials section).
export AWS_PROFILE=mcps                                # or env vars / instance role
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/mcps/drive-service-account.json"

# 3. Edit mcps.config.yaml — at minimum, replace <drive-folder-id> with
#    the id of your Drive folder.
$EDITOR mcps.config.yaml

# 4. Plan only — never modifies any provider state.
mcps --config mcps.config.yaml --dry-run

# 5. Cold_Start review pass — emits the reconciliation report and exits 76.
mcps --config mcps.config.yaml --apply --auto-approve

# 6. Inspect ./manifests/reconciliation-*.txt; when satisfied, confirm:
mcps --config mcps.config.yaml --apply --first-pass-confirmed --auto-approve
```

After step 6 finishes cleanly with exit code 0, the system is in steady state and subsequent runs (cron / systemd) need only `--apply --auto-approve`.

---

## Installation

`mcps` is a standalone Python 3.10+ package. From the repo root:

```bash
pip install -e ".[dev]"
```

This installs `mcps` as a console script and pulls in the dev extras (`pytest`, `hypothesis`, `moto`, `pytest-cov`). For production-only installs drop the `[dev]` extras.

The `mcps` console script is registered via `pyproject.toml`. `python -m mcps` also works and is what the smoke tests exercise.

---

## Credentials

`mcps` deliberately does **not** read credentials from any config file. It uses each provider's standard credential chain so secrets stay out of the working tree.

### AWS

Resolved via `boto3.Session()` in this order:

1. Environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / optional `AWS_SESSION_TOKEN`).
2. Named profile (`AWS_PROFILE`, default `~/.aws/credentials` and `~/.aws/config`).
3. EC2 / ECS / IRSA instance role (when running on AWS compute).

Verify your wiring before the first run:

```bash
aws sts get-caller-identity
```

The IAM identity must allow:

- `s3:ListBucket` on the bucket(s) referenced in `mcps.config.yaml`,
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:GetObjectTagging`, `s3:PutObjectTagging` on every object under each bucket,
- (optionally, for `mcps doctor --check-iam`) `iam:GetUser` and `iam:ListAccessKeys` on the bound user.

A least-privilege policy is documented in [`MIGRATION.md`](MIGRATION.md).

### Google Cloud Storage

Resolved via `google.auth.default()`:

1. The path in `GOOGLE_APPLICATION_CREDENTIALS` (a service-account JSON file).
2. ADC (Application Default Credentials) configured via `gcloud auth application-default login`.

The bound principal needs `storage.objects.{get,list,create,update,delete}` on each configured bucket.

### Google Drive

Same chain as GCS, but the resolver pins the `https://www.googleapis.com/auth/drive.readonly` scope. The service account must be **shared as Viewer** on the configured Drive folder (Drive ignores the GCP-side IAM grant; sharing on the folder itself is what authorises access).

`mcps` exits with `DRIVE_ACCESS_FAILED` (75) at start-up if the configured `drive_root_folder_id` is not reachable.

> [!NOTE]
> The recommended location for the service-account file is
> `~/.config/mcps/drive-service-account.json` with mode `0600`. The
> migration plan (Step 2) walks you through relocating the legacy
> in-repo `credentials.json` and exporting `GOOGLE_APPLICATION_CREDENTIALS`.

---

## Configuration

`mcps` reads a single TOML or YAML file (default: `./mcps.config.yaml`). The committed template at the repo root looks like this:

```yaml
sources:
  - name: s3-pickbackup
    kind: s3
    bucket: pickbackup
    region: us-east-1

  - name: drive-marta
    kind: google_drive
    drive_root_folder_id: <drive-folder-id>      # REPLACE THIS

replication:
  pairs: []                                       # add S3 ↔ GCS pairs once you have a second writable Source
  on_key_conflict: skip
  fail_on_conflict: false
  delete_propagation: none
  tombstone_retention_days: 30
  fail_on_inconsistency: false

duplicates:
  canonical_source_priority:
    - s3-pickbackup
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

### Section reference

| Section | Field | Meaning |
| --- | --- | --- |
| `sources[]` | `name` | Logical id used in `replication.pairs`, `canonical_source_priority`, etc. |
| `sources[]` | `kind` | One of `s3`, `gcs`, `google_drive`. |
| `sources[]` | `bucket` | (s3, gcs) Bucket name. |
| `sources[]` | `region` | (s3) AWS region; defaults follow the boto3 chain. |
| `sources[]` | `prefix` | (optional) Restrict listing to a key prefix. |
| `sources[]` | `drive_root_folder_id` | (google_drive) Folder id from the Drive URL. |
| `replication.pairs` | `[[a, b], ...]` | Ordered pairs of source names to replicate from `a` → `b`. List `[a, b]` and `[b, a]` to make replication bidirectional. |
| `replication.on_key_conflict` | `skip` / `rename` / `overwrite` | What to do when the destination already has the same key but a different content hash. |
| `replication.fail_on_conflict` | `bool` | When `true`, any unresolved key conflict ends the run with a non-zero exit code. |
| `replication.delete_propagation` | `none` / `soft` / `hard` | Whether and how absent records propagate as deletes. |
| `replication.tombstone_retention_days` | `int` | How long soft-deleted markers are retained before becoming eligible for hard delete. |
| `replication.fail_on_inconsistency` | `bool` | When `true`, divergent hashes detected at end-of-run produce exit code 78. |
| `duplicates.canonical_source_priority` | `[name, ...]` | Tie-break ordering for the canonical pick within a duplicate group. |
| `duplicates.quarantine_retention_days` | `int` | How long quarantined records are retained before being eligible for physical delete. |
| `photos.drive_source` | `name` | Drive Source for the importer. |
| `photos.drive_destination` | `name` | Replicated_Source where Drive imports land. |
| `retries.*` | | Retry-decorator parameters (see `mcps/retry.py`). |
| `runtime.catalog_path` | path | Where the JSONL Catalog lives. |
| `runtime.manifest_dir` | path | Directory for per-run JSONL Manifests + Cold_Start reconciliation reports. |
| `runtime.max_concurrent_transfers` | `int` | Bounded executor parallelism for replication / Drive import. |
| `runtime.lock_path` | path | (optional) Override the writer-lock file path. Defaults to `<catalog_path>.lock`. |

The schema is validated by `mcps/config/model.py`. Unknown top-level keys, unknown per-section keys, missing required sections, and out-of-range numeric fields are all rejected at parse time with a precise error and a best-effort line number.

---

## Running mcps

```text
mcps [--config PATH] [--dry-run | --apply]
     [--auto-approve] [--first-pass-confirmed]
     [--log-level {DEBUG,INFO,WARN,ERROR}]
     [--run-id RUN_ID] [--catalog PATH] [--manifest-dir PATH]
     [--lock-path PATH]
```

### Modes

| Flag | Effect |
| --- | --- |
| `--dry-run` | Plan only. Lists every Source, computes hashes, and writes a Manifest with `result=PLANNED` entries. Never calls `write_bytes`, `delete`, or `set_tag`. **Default** when neither `--dry-run` nor `--apply` is supplied (a stderr warning is emitted). |
| `--apply` | Execute planned actions against the configured Sources. Required for the destructive arm. |
| `--auto-approve` | Skip the interactive duplicate-quarantine prompt. Required for non-interactive runs (cron / systemd). |
| `--first-pass-confirmed` | Authorises destructive actions on a Cold_Start `--apply` run after the operator has reviewed the reconciliation report. No-op on non-Cold_Start runs (a WARN log is emitted). |
| `--log-level` | One of `DEBUG`, `INFO`, `WARN`, `ERROR`. Default `INFO`. |
| `--run-id` | Override the auto-generated UUID4 run id. Useful for correlating with external audit systems. |
| `--catalog` / `--manifest-dir` / `--lock-path` | Override the matching `runtime.*` config field. |

### Cold_Start two-step apply flow

When `mcps` starts a run with no on-disk Catalog (or an empty one), the run is **Cold_Start**. The Replicated_Sources may contain pre-existing data with no `mcps-*` metadata, so a naive `--apply` could quarantine, delete, or overwrite content the operator has not yet reviewed.

To prevent that, `--apply` on a Cold_Start run is split into two passes:

```bash
# Pass 1 — review pass. NO quarantines, NO physical deletes, NO overwrites.
# Replicate-to-absent (writes to a destination that has no record for a hash)
# and Drive-import-to-absent are permitted because they are non-destructive.
# Exits with code 76 (FIRST_PASS_REVIEW_REQUIRED) and emits:
#   - <manifest_dir>/reconciliation-<UTC>-<run-id>.txt (also printed to stdout)
#   - <manifest_dir>/manifest-<UTC>-<run-id>.jsonl
mcps --config mcps.config.yaml --apply --auto-approve

# Pass 2 — confirmation pass. Full apply path with all the safety rules
# (last-copy protection, canonical-source priority, retry budgets).
# Exits 0 on success.
mcps --config mcps.config.yaml --apply --first-pass-confirmed --auto-approve
```

The reconciliation report contains:

- per-Source object counts, total bytes, distinct content-hash counts,
- a cross-source diff (s3-only / gcs-only / drive-only / exactly-two / all-three),
- same-source and cross-source duplicate-group counts,
- the Drive_Importer's would-import count,
- an estimated-bytes-to-hash figure for cost planning.

After Pass 2 succeeds once, subsequent runs are not Cold_Start (the Catalog is now populated) and `--first-pass-confirmed` becomes a no-op.

---

## Exit codes

`mcps` follows the BSD `sysexits.h` convention so cron / systemd post-processing can branch on the failure mode.

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | `OK` | Run completed successfully. |
| 2 | `RUN_HAD_ERRORS` | At least one per-record error was logged to the Manifest (the run did not abort). |
| 64 | `CONFIG_INVALID` | Configuration file failed schema validation. |
| 65 | `CATALOG_INVALID` | The on-disk Catalog file is present but unparseable. |
| 66 | `LEGACY_CONFIG` | A plaintext-credential `config.ini` was detected. Required reading: [`MIGRATION.md`](MIGRATION.md). |
| 67 | `MANIFEST_UNAVAILABLE` | The Manifest directory is missing or write failed. |
| 71 | `CREDENTIAL_FAILED` | Could not resolve provider credentials. |
| 72 | `CONFLICT_FAILURE` | An unresolved key conflict occurred (`replication.fail_on_conflict=true`). |
| 73 | `LOCK_CONFLICT` | Another live `mcps` process holds the writer lock. |
| 74 | `INTERACTIVE_REQUIRED` | `--apply` without `--auto-approve` and stdin is not a TTY. |
| 75 | `DRIVE_ACCESS_FAILED` | The configured `drive_root_folder_id` is not accessible to the service account. |
| 76 | `FIRST_PASS_REVIEW_REQUIRED` | Cold_Start `--apply` review pass; report is ready, run again with `--first-pass-confirmed` once you have inspected it. |
| 77 | `COLD_START_LISTING_FAILED` | A Cold_Start run aborted because a Source listing exhausted retries; no reconciliation report was produced. |
| 78 | `INCONSISTENCY_DETECTED` | `replication.fail_on_inconsistency=true` and one or more divergent hashes were observed after replication. |

These values are part of the operator-facing contract and will not be reordered.

---

## `mcps doctor`

A small diagnostic surface separate from the main Sync_Run.

### `mcps doctor --check-iam`

Confirms that the legacy AWS access key id `AKIAYQ4K35M7H3INY75N` (which was checked into the old `config.ini` and must be assumed compromised) is no longer the active key on the bound IAM user. Calls `iam:GetUser` + `iam:ListAccessKeys` and asserts the key is either absent (deleted) or has `Status == "Inactive"`.

```bash
mcps doctor --check-iam
# PASS: leaked AWS access key AKIAYQ4K35M7H3INY75N is absent from IAM user mcps-prod (deleted).
# (exit 0)
```

Exit codes:

- `0` — leaked key is absent or inactive.
- `1` — leaked key is still active (or has an unrecognised status).
- `71` — credential resolution or the IAM call failed.

This is the verification step at the end of [`MIGRATION.md`](MIGRATION.md) Step 1.

---

## On-disk artefacts

A run produces three kinds of files (none of which should ever be committed; all are covered by `.gitignore`):

- **Catalog** at `runtime.catalog_path` (default `./mcps.catalog.jsonl`). One JSONL line per `(source, key)` pair, atomically rewritten at the end of every Sync_Run. Carries each record's content hash, last-seen timestamp, and quarantine / tombstone markers.
- **Lock file** alongside the catalog (default `mcps.catalog.jsonl.lock`). Holds an `fcntl.flock(LOCK_EX)` and records the holder's PID + run id. Includes stale-PID reclaim if a previous holder crashed.
- **Manifests** under `runtime.manifest_dir` (default `./manifests`):
  - `manifest-<UTC>-<run-id>.jsonl` — every per-record action with structured fields (action, result, key, content_hash, error, etc.) plus a final `SUMMARY` entry.
  - `reconciliation-<UTC>-<run-id>.txt` — the human-readable Cold_Start report, written only on Cold_Start runs.

Filenames embed the run id so multiple runs (e.g. a failed Cold_Start review pass plus its confirmation pass) coexist cleanly.

---

## How it works (architecture)

The full design lives in [`.kiro/specs/multicloud-photo-sync/design.md`](.kiro/specs/multicloud-photo-sync/design.md). At a glance, every Sync_Run runs the following pipeline inside a writer-lock context:

1. **Legacy-config guard.** `mcps/cli.py::detect_legacy_config` refuses to start while `config.ini` with `[aws_credentials]` is present (exit 66).
2. **Config + credentials.** Parse `mcps.config.yaml` and resolve AWS / GCP / Drive credentials via the provider chains.
3. **Lock acquisition.** `fcntl.flock(LOCK_EX | LOCK_NB)` with a 5-second deadline, stale-PID reclaim, and a typed `LockConflict` (exit 73) if a live process is holding it.
4. **Catalog load.** Empty Catalog ⇒ Cold_Start; otherwise the cache-lookup optimisation can short-circuit unchanged objects.
5. **Listing.** Each `SourceAdapter` paginates its provider, computes content hashes via the priority chain `mcps-content-sha256` user-metadata → Catalog cache hit → streamed SHA-256 fallback, and produces `ObjectRecord`s.
6. **Reconciliation_Reporter** (Cold_Start only). Builds and emits the human-readable report; exits 76 if `--apply` was supplied without `--first-pass-confirmed`.
7. **Duplicate_Resolver.** Detects same- and cross-source duplicate groups, picks a canonical record per group (priority → earliest `last_seen_at` → smallest key), quarantines non-canonical records via `set_tag("mcps-quarantined-at", <iso>)`, and physically deletes records past `quarantine_retention_days`. Last-copy protection refuses any quarantine that would orphan a content hash.
8. **Replicator.** Computes per-pair plans from the Catalog, copies absent content hashes, applies `on_key_conflict` policy for collisions, verifies post-write via HEAD, propagates deletes per `delete_propagation`, and emits structured Manifest entries for every action.
9. **Drive_Importer.** One-way Drive → `photos.drive_destination`. Filters by mimeType (image/* and video/*, skipping native Google Docs), builds destination keys of the shape `google-drive/<YYYY>/<MM>/<file-id>__<sanitised-name>`, and short-circuits when the content hash already exists on a Replicated_Source.
10. **Inconsistency_Detector.** Re-lists every Replicated_Source post-replication, reports per-Source new/removed counts, and flags any content hash that is present in some Replicated_Source and absent from another (excluding hashes that recorded a `REPLICATION_ERROR`). Honours `fail_on_inconsistency` for exit 78.
11. **SUMMARY.** A final Manifest entry with structured run-level counters; the Catalog is atomically rewritten before the lock is released.

Every step is exercised by tests at three tiers — unit, integration (real adapters against `moto` and in-process fakes), and smoke — plus 17 Hypothesis property tests with at least 200 examples each. Total: 664 tests.

---

## Development

```bash
make install              # pip install -e ".[dev]"
make lint                 # byte-compile the package as a smoke check
make test                 # run the full pytest suite
make test-unit            # tests/unit/
make test-integration     # tests/integration/
make test-smoke           # tests/smoke/
make test-property        # only the @pytest.mark.property tests
make clean                # remove build/, dist/, .pytest_cache/, .hypothesis/, etc.
```

Markers (configured in `pyproject.toml`):

- `@pytest.mark.property` — Hypothesis property tests.
- `@pytest.mark.integration` — exercise real adapters (S3 via `moto`, GCS / Drive via in-process fakes).
- `@pytest.mark.smoke` — minimal CLI surface checks.

Property tests use `@settings(max_examples=200, deadline=None)` and are stable on developer hardware (~90 s for the full suite).

### Repository conventions

- All new code is type-annotated and passes `python -m compileall mcps`.
- Tests are under `tests/unit/`, `tests/integration/`, `tests/smoke/`. Mirror the source-file structure: `mcps/foo.py` ⇄ `tests/unit/test_foo*.py` (multiple test files per source module are fine).
- Property tests start with the comment header `# Feature: multicloud-photo-sync, Property N: <title>`.

---

## Migration from the legacy scripts

The repo previously shipped `uploader.py`, `delete.py`, `delete_list.txt`, `logfile.log`, `config.ini`, and an in-repo `credentials.json`. They are scheduled for deletion at the end of the migration plan documented in [`MIGRATION.md`](MIGRATION.md).

Operator checklist (full version with commands and pre-conditions in `MIGRATION.md`):

1. **Rotate** the leaked AWS access key `AKIAYQ4K35M7H3INY75N`. Verify with `mcps doctor --check-iam`.
2. **Relocate** `credentials.json` to `~/.config/mcps/drive-service-account.json` mode `0600`; export `GOOGLE_APPLICATION_CREDENTIALS`.
3. **Add** the `.gitignore` entries (already in place at the repo root).
4. **Edit** `mcps.config.yaml` — at minimum, replace the `<drive-folder-id>` placeholder.
5. **Install** the package and run a dry-run. The legacy guard will reject the run with exit 66 until step 6a; that is the guard doing its job.
6. **Run the Cold_Start two-step Apply cycle** (Step 7 in `MIGRATION.md`). Pass 1 (review) → inspect `manifests/reconciliation-*.txt` → Pass 2 (`--first-pass-confirmed`).
7. **Delete** the legacy files (`config.ini` first, then `delete.py` / `uploader.py` / `delete_list.txt` / `logfile.log` / in-repo `credentials.json`) per Step 6 in `MIGRATION.md`. Backup tarball outside the working tree before `rm -f`.
8. **Schedule** under cron or systemd.

---

## Project layout

```
mcps/
├── __init__.py
├── __main__.py            # `python -m mcps` forwarder
├── cli.py                 # argparse, run() pipeline, main()
├── doctor.py              # `mcps doctor --check-iam`
├── catalog/               # ObjectRecord + Catalog + parser/printer
├── config/                # Config schema + parser/printer (YAML + TOML)
├── credentials.py         # Credential_Manager (AWS / GCP / Drive)
├── concurrency.py         # writer_lock + bounded executor
├── duplicates/            # Duplicate_Detector + Duplicate_Resolver
├── drive_import.py        # DriveImporter
├── errors.py              # ExitCode enum + McpsError hierarchy
├── hashing.py             # streaming SHA-256 + content-hash priority chain
├── logging_setup.py       # JsonFormatter + bind_run_id
├── manifest/              # Action / Result / ManifestRecord + writer
├── reconciliation.py      # Reconciliation_Reporter + Inconsistency_Detector
├── redaction.py           # Redactor
├── replication.py         # Replicator (with deletion handling)
├── retry.py               # retry_transient decorator
└── sources/               # SourceAdapter ABC + s3 / gcs / drive / fake
tests/
├── unit/                  # ~620 tests; includes 17 property tests
├── integration/           # 25 tests; moto + in-process fakes
└── smoke/                 # 4 tests; subprocess + helper-level
.kiro/specs/multicloud-photo-sync/
├── requirements.md        # 19 EARS-format requirements
├── design.md              # architecture + 17 properties + exit codes
└── tasks.md               # 43 tasks (all completed)
mcps.config.yaml           # operator-facing config template
MIGRATION.md               # operator playbook (steps 1-8 + checklist)
Makefile                   # install / lint / test targets
pyproject.toml
```
