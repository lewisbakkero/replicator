# Requirements Document

## Introduction

This feature refactors the existing one-way Google Drive → Amazon S3 backup tool (`uploader.py`, `delete.py`) into a multi-cloud photo and video synchronisation system. The new system shall:

1. Detect and resolve duplicate objects across all configured cloud locations (cross-bucket and cross-provider).
2. Replicate objects bidirectionally between Google Cloud Platform (GCP) and Amazon Web Services (AWS) object stores.
3. Pull-only sync from Google Photos, copying only items that do not already exist in any configured destination.

It also addresses non-functional concerns of the current code: hard-coded plaintext credentials, lack of idempotency guarantees beyond `head_object`, no dry-run mode, no central catalog of known objects, weak retry strategy, and no structured observability.

### Initial Assumptions (to confirm during review)

The following defaults are baked into the requirements below. If any are wrong, raise it during requirements review and the document will be revised.

- **Object identity** for deduplication is the SHA-256 of the byte content. Filenames and paths are *not* part of identity.
- **GCP_Object_Store** in this document refers to **Google Cloud Storage (GCS) buckets**. The existing Google Drive folder access is treated as a legacy migration source and is replaced by GCS for ongoing replication. (If Drive must remain a first-class peer, this is a known deviation to revisit.)
- **Google Drive** is treated as a read-only source. The original request was to sync from Google Photos, but as of March 31, 2025 the Google Photos Library API is restricted to app-created content only and does not support service accounts. The user has elected to instead sync from a Google Drive folder (e.g. a folder kept in sync with the device camera, or a folder containing exported Takeout media). This avoids the Photos API entirely and lets the existing service account credential type continue to work.
- **Duplicate removal** is opt-in per run and never performed without either explicit `--apply` confirmation or non-interactive `--auto-approve` flag plus a quarantine retention period.
- **Deletion propagation across providers is disabled by default** — a delete in one provider does not delete the corresponding object in the other. Explicit opt-in is required.
- **Cold-start preconditions**: On the first Sync_Run, the on-disk Catalog file does not exist (or is empty) and the in-memory Catalog is therefore empty, while BOTH the configured AWS S3 bucket(s) and the configured Google Drive folder are already pre-populated with objects that carry NO `mcps-*` metadata (no `mcps-source`, no `mcps-content-sha256`, no `mcps-quarantined-at`, no `mcps-tombstoned-at`). The configured GCS bucket(s) may be empty or pre-populated under the same assumption.
- **First Sync_Run is a reconciliation pass**: Because no Object carries cached hash metadata and the Catalog is empty, the first Sync_Run must list every Object in every Source, stream-hash every Object (none can be skipped via the Requirement 7.1 `mcps-content-sha256` shortcut), and produce a Reconciliation_Report describing per-Source state and cross-Source inconsistencies before any destructive action is taken. Destructive actions (Quarantine tagging, physical delete, `on_key_conflict=overwrite`) require an explicit operator opt-in (`--first-pass-confirmed`) on a Cold_Start run; non-destructive replication writes and Drive_Importer uploads still proceed under `--apply` so that absent destinations can be filled in. See Requirement 18.

## Glossary

- **MultiCloud_Photo_Sync**: The top-level system orchestrating discovery, deduplication, replication, and import.
- **Source**: A configured location holding objects. Each Source has a kind (`s3`, `gcs`, `google_drive`) and a name.
- **Replicated_Source**: A Source of kind `s3` or `gcs` that participates in bidirectional replication.
- **Pull_Only_Source**: A Source of kind `google_drive` that participates only as a read source. The configured `drive_destination` is the Replicated_Source into which Drive_Importer writes new items.
- **Object**: An immutable byte payload (photo or video) stored in a Source, addressed by a provider-specific key.
- **Content_Hash**: The lowercase hexadecimal SHA-256 of the full byte content of an Object.
- **Object_Record**: A record `{source, key, content_hash, size_bytes, last_seen_at, content_type}` describing one Object in one Source.
- **Catalog**: A persistent local index mapping `Content_Hash → set of Object_Record`. Used by the Duplicate_Detector and Replicator to decide work without re-downloading.
- **Manifest**: A per-run, append-only record of intended and executed actions (uploads, deletes, skips, errors).
- **Replicator**: The component that copies missing Objects between Replicated_Sources.
- **Duplicate_Detector**: The component that groups Object_Records by Content_Hash and identifies duplicates.
- **Duplicate_Resolver**: The component that decides which copy of a duplicate group is canonical and which are removable.
- **Drive_Importer**: The component that pulls new items from a Pull_Only_Source (Google Drive folder) into a configured destination Source.
- **Credential_Manager**: The component that loads provider credentials from a non-plaintext source (env vars, AWS Secrets Manager, GCP Secret Manager, or instance metadata).
- **Catalog_Parser**: The component that parses the on-disk Catalog file format.
- **Catalog_Printer**: The component that serialises the in-memory Catalog into the on-disk file format.
- **Manifest_Parser**: The component that parses the on-disk Manifest JSONL file format.
- **Manifest_Printer**: The component that serialises in-memory Manifest_Records into the JSONL file format.
- **Config_Parser**: The component that parses the on-disk configuration file (TOML or YAML).
- **Config_Printer**: The component that serialises an in-memory Config object into the on-disk configuration format.
- **Dry_Run_Mode**: An execution mode in which no Object is uploaded, deleted, or modified in any Source; only the Manifest is produced.
- **Apply_Mode**: An execution mode in which planned actions are executed against Sources.
- **Quarantine**: A logical state in which a duplicate is marked for deletion but retained for a configured grace period before physical removal.
- **Sync_Run**: One end-to-end execution of MultiCloud_Photo_Sync.
- **Cold_Start**: A Sync_Run in which the in-memory Catalog is empty (no prior Catalog file existed at the configured `catalog_path`, or the prior file existed but contained zero Object_Records).
- **Reconciliation_Report**: A human-readable summary produced on Cold_Start Sync_Runs describing the state of every Source (per-Source counts of Objects, bytes, and distinct Content_Hashes) and the inconsistencies between Sources (per-pair and three-way Content_Hash diffs, same-source duplicate groups, cross-source duplicate groups, planned Drive_Importer imports).
- **First_Pass_Confirmed**: A CLI flag (`--first-pass-confirmed`) that operators must supply alongside `--apply` on a Cold_Start Sync_Run to authorise destructive actions (Quarantine tagging, physical delete, `on_key_conflict=overwrite`). Without this flag, a Cold_Start `--apply` run produces only the Reconciliation_Report and non-destructive replication writes.

## Requirements

### Requirement 1: Secret-Free Configuration and Credential Loading

**User Story:** As an operator, I want provider credentials loaded from a secure source rather than a plaintext file, so that AWS access keys and Google service account material are not committed alongside source code.

#### Acceptance Criteria

1. WHEN a Sync_Run starts, THE Credential_Manager SHALL resolve AWS credentials by checking sources in the following priority order and using the first source that yields a complete credential set: (a) the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN` environment variables, (b) a named AWS profile identified by the `AWS_PROFILE` environment variable, (c) AWS instance or container role credentials provided by the host environment.
2. WHEN a Sync_Run starts, THE Credential_Manager SHALL resolve GCP credentials by checking sources in the following priority order and using the first source that yields a complete credential set: (a) the service account key file located at the path in the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, (b) Application Default Credentials provided by the host environment.
3. IF no source in the AWS priority order or no source in the GCP priority order yields a complete credential set within 10 seconds of the Sync_Run start, THEN THE MultiCloud_Photo_Sync SHALL abort the Sync_Run with a non-zero exit code and a structured error record naming the failing provider and identifying which sources were attempted.
4. IF a credential source returns a 401 or invalid-credentials error during a Sync_Run, THEN THE MultiCloud_Photo_Sync SHALL abort the Sync_Run with a non-zero exit code and a structured error record naming the failing provider and credential source, AND THE non-zero exit code SHALL be returned regardless of whether other parts of the Sync_Run completed successfully before the abort.
5. IF a configuration file containing the keys `aws_access_key_id` or `aws_secret_access_key` in plaintext is present at the legacy path `config.ini` when a Sync_Run is invoked, THEN THE MultiCloud_Photo_Sync SHALL refuse to start, exit with a non-zero exit code, and emit an error record directing the operator to migrate to environment variables, named profiles, or instance/container roles, without reading any value from that file.
6. THE MultiCloud_Photo_Sync SHALL NOT write any provider credential value, including access key identifiers, secret access keys, session tokens, service account private keys, or refresh tokens, to the Manifest, the Catalog, the log output, any error record, or any file produced by the Sync_Run.

### Requirement 2: Source Discovery and Listing

**User Story:** As an operator, I want each configured Source to be enumerated into Object_Records, so that downstream components have a uniform view across S3, GCS, and Google Photos.

#### Acceptance Criteria

1. WHEN a Sync_Run begins, THE MultiCloud_Photo_Sync SHALL list all Objects in every configured Source within a per-Source listing timeout of 600 seconds and SHALL produce exactly one Object_Record per Object containing source_id, object_key, size_bytes, last_modified timestamp, and content_hash.
2. WHERE a Source is of kind `s3`, THE MultiCloud_Photo_Sync SHALL populate `content_hash` from the S3 object's `ETag` only IF the ETag corresponds to a single-part upload; otherwise THE MultiCloud_Photo_Sync SHALL compute the SHA-256 from a streaming download without persisting the full object to local disk.
3. WHERE a Source is of kind `gcs`, THE MultiCloud_Photo_Sync SHALL populate `content_hash` by computing SHA-256 from the GCS object's CRC32C-verified streamed content, and the GCS-provided MD5 SHALL NOT be used as Content_Hash.
4. WHERE a Source is of kind `google_drive`, THE MultiCloud_Photo_Sync SHALL populate `content_hash` by streaming the file's content via the Drive `files.get_media` endpoint and computing SHA-256 over the streamed bytes, without persisting the full file to local disk.
5. WHEN listing returns a paginated response, THE MultiCloud_Photo_Sync SHALL follow continuation tokens until exhaustion and SHALL include every page's Objects in the resulting Object_Records with no duplicate Object_Records for the same (source_id, object_key) pair.
6. IF a single list or download request to a Source fails with a transient error, THEN THE MultiCloud_Photo_Sync SHALL retry the request up to 5 times using exponential backoff before treating the Source listing as failed.
7. IF listing of a Source fails after the configured maximum retries, THEN THE MultiCloud_Photo_Sync SHALL skip that Source for the current Sync_Run, record a per-Source error entry in the Manifest indicating the Source identifier and failure reason, preserve any Object_Records already produced for other Sources, and continue listing the remaining Sources.
8. IF an Object's content_hash cannot be computed after retries, THEN THE MultiCloud_Photo_Sync SHALL exclude that Object from the resulting Object_Records and record a per-Object error entry in the Manifest indicating the Source identifier, object_key, and failure reason.

### Requirement 3: Persistent Catalog with Round-Trip Serialisation

**User Story:** As an operator, I want the system to persist what it has seen across runs in a single catalog file, so that runs are incremental and do not require re-hashing every Object every time.

#### Acceptance Criteria

1. WHEN a Sync_Run completes successfully, THE MultiCloud_Photo_Sync SHALL write the in-memory Catalog to a single local file at the path specified in the configuration, replacing any prior contents atomically so the prior file is not left in a partially-written state.
2. WHEN the Catalog_Parser is invoked on a syntactically valid Catalog file, THE Catalog_Parser SHALL produce an in-memory mapping where each key is a Content_Hash string and each value is a set of one or more Object_Records, with no duplicate Object_Records within a set.
3. WHEN the Catalog_Printer is invoked on an in-memory Catalog, THE Catalog_Printer SHALL serialise it into the Catalog file format and SHALL produce deterministic output such that two invocations on equal inputs yield byte-identical output.
4. FOR ALL valid in-memory Catalogs C, WHEN the Catalog_Printer serialises C and the Catalog_Parser then parses that output, THE resulting in-memory Catalog SHALL equal C, where equality is defined as the same set of Content_Hash keys and, for each key, the same set of Object_Records compared by all Object_Record fields.
5. IF the Catalog file does not exist or cannot be opened for reading at the start of a Sync_Run, THEN THE MultiCloud_Photo_Sync SHALL initialise an empty in-memory Catalog, log a message indicating that no prior Catalog was loaded, and continue the Sync_Run.
6. IF the Catalog file is present but the Catalog_Parser reports a parse failure, THEN THE MultiCloud_Photo_Sync SHALL abort the Sync_Run before any provider listing or hashing work begins, exit with a non-zero exit code, emit an error message identifying the Catalog file path and the parse failure, and SHALL NOT modify, truncate, or overwrite the existing Catalog file.
7. WHEN a previously catalogued Object is re-listed by a provider and both its provider-reported size in bytes and its provider-reported modification timestamp are bit-for-bit identical to the values stored in the Catalog for that Object, THE MultiCloud_Photo_Sync SHALL reuse the cached Content_Hash from the Catalog and SHALL NOT download the Object content for re-hashing during that Sync_Run.
8. IF a previously catalogued Object is re-listed and either its provider-reported size or its provider-reported modification timestamp differs from the value stored in the Catalog, THEN THE MultiCloud_Photo_Sync SHALL treat the cached Content_Hash as stale, re-hash the Object from its current content, and replace the prior Object_Record in the Catalog with one reflecting the new size, modification timestamp, and Content_Hash.

### Requirement 4: Cross-Source Duplicate Detection

**User Story:** As an operator, I want to know which Objects are duplicates of each other across all my buckets and providers, so that I can remove redundant copies.

#### Acceptance Criteria

1. THE Duplicate_Detector SHALL group all Object_Records in the Catalog by Content_Hash and SHALL emit one duplicate group for each Content_Hash whose group contains two or more Object_Records, where each duplicate group includes the Content_Hash, the count of members (minimum 2), and for each member the Source identifier, bucket identifier, object key, and `size_bytes`.
2. THE Duplicate_Detector SHALL classify two Object_Records as duplicates only IF their Content_Hash values are byte-for-byte equal AND their `size_bytes` values are equal as non-negative integers, and SHALL exclude from any duplicate group any Object_Record whose Content_Hash is missing, empty, or whose `size_bytes` is missing or negative.
3. THE Duplicate_Detector SHALL include duplicate groups whose members span two or more distinct Sources and SHALL include duplicate groups whose members all belong to the same Source, and SHALL label each duplicate group as either `cross-source` (members span 2 or more Sources) or `same-source` (all members in 1 Source).
4. WHEN the same Content_Hash appears in the Catalog and again during a Sync_Run, THE Duplicate_Detector SHALL produce a set of duplicate groups that is identical, in group membership and in per-group member set, to the set produced for the same Catalog state regardless of the order in which Sources were listed or the order in which Object_Records were ingested.
5. IF the Catalog contains zero Object_Records or no Content_Hash is shared by two or more Object_Records, THEN THE Duplicate_Detector SHALL emit an empty set of duplicate groups and SHALL return a status indicating that no duplicates were found, without raising an error.
6. IF an Object_Record cannot be evaluated for duplicate detection because its Content_Hash or `size_bytes` is unavailable, THEN THE Duplicate_Detector SHALL exclude that Object_Record from all duplicate groups, SHALL record it in a skipped-records report identifying the Source, bucket identifier, and object key, and SHALL continue processing remaining Object_Records.

### Requirement 5: Duplicate Resolution and Safe Removal

**User Story:** As an operator, I want one canonical copy of each Object retained and the others removed only with my approval, so that I cannot accidentally lose all copies of a photo.

#### Acceptance Criteria

1. FOR each duplicate group, THE Duplicate_Resolver SHALL designate exactly one Object_Record as canonical using a deterministic tie-break applied in order: (a) Source listed first in the configured `canonical_source_priority`, (b) earliest `last_seen_at` timestamp at millisecond precision, (c) lexicographically smallest `key` byte-by-byte using UTF-8 encoding.
2. IF the configured `canonical_source_priority` is missing, empty, or references a Source not present in the duplicate group, THEN THE Duplicate_Resolver SHALL skip rule (a) and proceed to rule (b), and SHALL record a warning in the Manifest indicating which groups fell back.
3. THE Duplicate_Resolver SHALL emit a removal plan to the Manifest listing every non-canonical Object_Record in every duplicate group, including for each entry the Source identifier, `key`, `Content_Hash`, byte size, and the `key` of the designated canonical Object_Record.
4. WHILE the Sync_Run is in Dry_Run_Mode, THE Duplicate_Resolver SHALL write the removal plan to the Manifest and SHALL NOT modify, tag, or delete any Object in any Source.
5. IF Apply_Mode is selected without the `--auto-approve` flag and standard input is a terminal, THEN THE Duplicate_Resolver SHALL prompt the operator with the count of Object_Records to be removed and the total bytes to be removed, and SHALL NOT modify, tag, or delete any Object until the operator enters an affirmative confirmation response.
6. IF Apply_Mode is selected without the `--auto-approve` flag and standard input is not a terminal, THEN THE Duplicate_Resolver SHALL abort the Sync_Run without modifying any Object and SHALL emit an error indicating that interactive confirmation is required.
7. WHEN a non-canonical Object is approved for removal, THE Duplicate_Resolver SHALL move it to Quarantine by tagging the Object with `mcps-quarantined-at=<ISO-8601 UTC timestamp with second precision>` rather than physically deleting it, and SHALL preserve the Object's content and `key` unchanged.
8. IF tagging an Object for Quarantine fails, THEN THE Duplicate_Resolver SHALL leave the Object unchanged in its Source, SHALL record the failure in the Manifest with the Source identifier and `key`, and SHALL continue processing remaining Object_Records in the removal plan.
9. WHEN a Quarantined Object's `mcps-quarantined-at` tag is older than the configured retention period (default 30 days, configurable from 1 to 3650 days), THE Duplicate_Resolver SHALL physically delete the Object from its Source.
10. BEFORE physically deleting any Quarantined Object, THE Duplicate_Resolver SHALL verify that at least one non-quarantined Object_Record with the same `Content_Hash` exists in some Source, and IF no such Object_Record exists, THEN THE Duplicate_Resolver SHALL skip deletion of that Object and record a canonical-survives violation in the Manifest.
11. THE Duplicate_Resolver SHALL guarantee that for every `Content_Hash` present at the start of removal, at least one Object_Record with that `Content_Hash` remains non-quarantined in some Source after removal completes (canonical-survives property).

### Requirement 6: Bidirectional Replication Between AWS and GCP

**User Story:** As an operator, I want Objects that exist in one Replicated_Source but not the other to be copied across, so that AWS and GCP buckets converge to the same set of Content_Hashes.

#### Acceptance Criteria

1. WHEN a Sync_Run runs replication, THE Replicator SHALL identify, for each pair of configured Replicated_Sources, the set of Content_Hashes present in one Source and absent from the other, comparing on lowercase hex-encoded SHA-256 strings of length 64.
2. FOR each Content_Hash absent from a target Replicated_Source, THE Replicator SHALL copy exactly one Object_Record (the canonical one per Requirement 5) from a source Replicated_Source to the target Replicated_Source, and SHALL skip the copy and record a replication-error in the Manifest IF the source Object_Record cannot be read after 3 attempts.
3. WHEN the Replicator writes an Object to a target Replicated_Source, THE Replicator SHALL set the destination key to the source Object_Record's `key` byte-for-byte, preserving directory-style prefixes and case, and SHALL NOT modify, normalize, or re-encode the key.
4. WHEN the Replicator writes an Object to a target Replicated_Source, THE Replicator SHALL attach metadata entries `mcps-source=<source-name>`, `mcps-content-sha256=<lowercase-hex-64>`, and `mcps-replicated-at=<ISO-8601 UTC timestamp with second precision and trailing Z>`.
5. WHEN replication of a single Object completes, THE Replicator SHALL re-fetch the destination Object's reported size in bytes and SHALL verify that the destination Content_Hash equals the source Content_Hash by byte-for-byte string comparison; IF the sizes differ OR the Content_Hashes differ, THEN THE Replicator SHALL delete the partial destination Object and SHALL record a replication-error entry in the Manifest containing the source name, target name, key, expected Content_Hash, and observed Content_Hash.
6. AFTER replication completes for a Sync_Run, THE Replicator SHALL ensure that for every Content_Hash present in any Replicated_Source at the start of the run, that Content_Hash is present in every Replicated_Source upon Sync_Run completion, in the absence of concurrent removals and excluding Content_Hashes whose copy operations recorded a replication-error in the Manifest.
7. IF a destination Object with the same key already exists in the target Replicated_Source at the time of write, THEN THE Replicator SHALL skip the copy, leave the existing destination Object unchanged, and record a skipped-existing entry in the Manifest indicating the source name, target name, key, and source Content_Hash.

### Requirement 7: Replication Loop Prevention

**User Story:** As an operator, I want replicated Objects not to bounce back and forth between providers, so that one upload does not cause a second redundant upload on the next run.

#### Acceptance Criteria

1. WHEN listing Objects in a Replicated_Source during a Sync_Run, THE MultiCloud_Photo_Sync SHALL read the `mcps-content-sha256` metadata value if present and SHALL use that value as the Object's Content_Hash without recomputing the SHA-256.
2. IF the `mcps-content-sha256` metadata is absent, malformed (not a 64-character lowercase hexadecimal string), or fails integrity validation, THEN THE MultiCloud_Photo_Sync SHALL recompute the Content_Hash from the Object's bytes, record a `hash-recomputed` entry in the Manifest identifying the Object, and use the recomputed value.
3. WHEN the Replicator considers copying an Object whose `mcps-source` metadata equals the target Replicated_Source's name, THE Replicator SHALL skip the copy operation, leave the target Replicated_Source unchanged, and record a `loop-skip` entry in the Manifest containing the Object identifier, the source name, and the target name.
4. IF an Object's `mcps-source` metadata is missing or empty when evaluating a copy to a Replicated_Source, THEN THE Replicator SHALL treat the Object as eligible for replication, set `mcps-source` on the copied Object to the originating Replicated_Source's name, and record a `source-tagged` entry in the Manifest.
5. FOR ALL Sync_Runs after the first, given an unchanged set of Replicated_Sources and an unchanged set of Objects (identical Object identifiers, Content_Hash values, and `mcps-source` metadata), THE Replicator SHALL perform zero copy operations and SHALL record a `no-op` entry in the Manifest for the Sync_Run.

### Requirement 8: Conflict Resolution on Key Collision

**User Story:** As an operator, I want predictable behaviour when two Objects have the same key in different Replicated_Sources but different content, so that replication does not silently overwrite one with the other.

#### Acceptance Criteria

1. IF the Replicator is about to write a Content_Hash to a target Replicated_Source at a key where a different Content_Hash already exists AND `on_key_conflict` is `skip`, THEN THE Replicator SHALL skip the write, leave the existing Object at that key unchanged, and record a `key-conflict` entry in the Manifest naming the colliding key, the existing Content_Hash, and the incoming Content_Hash.
2. IF the Replicator is about to write a Content_Hash to a target Replicated_Source at a key where a different Content_Hash already exists AND `on_key_conflict` is `overwrite`, THEN THE Replicator SHALL replace the existing Object with the incoming Object and record an `overwrite` entry in the Manifest naming the colliding key, the previous Content_Hash, and the new Content_Hash.
3. WHERE `on_key_conflict` is `rename`, WHEN the Replicator is about to write a Content_Hash to a target Replicated_Source at a key where a different Content_Hash already exists, THE Replicator SHALL write the new Object to a key suffixed with `.<short-content-hash>` derived from the first 8 hex characters of the Content_Hash, leave the existing Object at the original key unchanged, and record a `rename` entry in the Manifest naming the original key, the renamed key, and the Content_Hash.
4. IF a Sync_Run recorded one or more `key-conflict` entries in the Manifest AND the configuration option `fail_on_conflict` is `true`, THEN THE MultiCloud_Photo_Sync SHALL exit with a non-zero exit code at the end of the Sync_Run.
5. WHEN no `key-conflict` entries are recorded in the Manifest during a Sync_Run, THE MultiCloud_Photo_Sync SHALL exit with a zero exit code at the end of the Sync_Run regardless of the value of `fail_on_conflict`.
6. THE MultiCloud_Photo_Sync SHALL provide a configuration option `on_key_conflict` accepting exactly the values `skip`, `rename`, or `overwrite`, with a default value of `skip`, and SHALL provide a configuration option `fail_on_conflict` accepting exactly the boolean values `true` or `false`, with a default value of `false`.
7. IF the value supplied for `on_key_conflict` is not one of `skip`, `rename`, or `overwrite`, or the value supplied for `fail_on_conflict` is not a boolean, THEN THE MultiCloud_Photo_Sync SHALL reject the configuration at startup, exit with a non-zero exit code, and produce an error message indicating the invalid option name and the accepted values.

### Requirement 9: Deletion Handling

**User Story:** As an operator, I want deletes in one Replicated_Source to not silently destroy the only remaining copy elsewhere, so that I do not lose data because of a misconfigured delete.

#### Acceptance Criteria

1. THE MultiCloud_Photo_Sync SHALL provide a configuration option `delete_propagation` accepting exactly one of the values `none`, `soft`, or `hard`, with a default value of `none`, and SHALL reject any other value at startup with an error indicating an invalid `delete_propagation` value.
2. WHERE `delete_propagation` is `none`, THE Replicator SHALL NOT delete, tombstone, or otherwise modify any Object in any Replicated_Source as a result of an Object being absent from another Replicated_Source during a Sync_Run.
3. WHERE `delete_propagation` is `soft`, WHEN an Object_Record exists in the Catalog from a previous Sync_Run but its Object is absent from its originating Replicated_Source in the current Sync_Run, THE Replicator SHALL add a tombstone metadata entry `mcps-tombstoned-at` set to the current timestamp in ISO-8601 UTC format on the corresponding Object in every other Replicated_Source, SHALL NOT delete the Object bytes, and SHALL record the tombstone action in the Manifest.
4. THE MultiCloud_Photo_Sync SHALL provide a configuration option `tombstone_retention_days` accepting an integer between 1 and 3650 inclusive, with a default value of 30, and SHALL reject any other value at startup with an error indicating an invalid `tombstone_retention_days` value.
5. WHERE `delete_propagation` is `hard`, WHEN the elapsed time since an Object's `mcps-tombstoned-at` timestamp is greater than or equal to `tombstone_retention_days`, THE Replicator SHALL physically delete the tombstoned Object from its Replicated_Source and SHALL record the deletion in the Manifest.
6. IF a deletion or tombstone operation would result in zero non-tombstoned Object_Records sharing the same Content_Hash across all Replicated_Sources, THEN THE Replicator SHALL refuse the operation, SHALL leave the affected Object and its metadata unchanged in every Replicated_Source, and SHALL record a `last-copy-protection` entry in the Manifest identifying the affected Content_Hash and the Replicated_Source where the operation was refused.
7. THE Replicator SHALL apply the last-copy-protection rule defined in criterion 6 under every value of `delete_propagation`, including `none`, as a defence-in-depth safeguard.

### Requirement 10: Google Drive Pull-Only Sync

**User Story:** As an operator, I want photos and videos in a configured Google Drive folder that are not already present in any of my buckets to be copied into a chosen destination, so that my Drive-synced media is backed up without re-uploading items I already have. (This requirement replaces the original Google Photos plan because the Photos Library API no longer supports general-library access or service accounts as of March 2025.)

#### Acceptance Criteria

1. WHEN a Google Drive pull-only sync run starts, THE Drive_Importer SHALL list files recursively under the configured Drive folder identified by `drive_root_folder_id` using the Drive `files.list` endpoint with `q="'<parent-id>' in parents and trashed=false"`, paginating via `nextPageToken` until exhaustion, with up to 5 retries per page request using exponential backoff between 1 and 30 seconds.
2. THE Drive_Importer SHALL only consider files whose Drive `mimeType` begins with `image/` or `video/`, and SHALL skip every other file by recording a `drive-skip-unsupported` entry in the Manifest for each skipped item containing the file id, name, and observed `mimeType`.
3. THE Drive_Importer SHALL skip files whose `mimeType` begins with `application/vnd.google-apps.` (Google-native documents), recording a `drive-skip-native-doc` entry in the Manifest.
4. WHEN evaluating whether a Google Drive file exists in any other Source, THE Drive_Importer SHALL compute Content_Hash by streaming the file bytes via `files.get_media` and SHA-256-hashing them, treating two items as equal if and only if their lowercase hexadecimal SHA-256 digests are identical.
5. WHEN the Content_Hash of a Drive file matches the Content_Hash of any Object_Record in any configured Replicated_Source, THE Drive_Importer SHALL skip the import, SHALL NOT upload the bytes to the `drive_destination` Source, and SHALL record a `drive-skip-existing` entry in the Manifest containing the Drive file id and the matching Content_Hash.
6. WHEN the Content_Hash of a Drive file is absent from every configured Replicated_Source, THE Drive_Importer SHALL upload the file bytes to the configured `drive_destination` Source at a key of the form `google-drive/<created-year>/<created-month>/<file-id>__<sanitised-name>`, where `<created-year>` is the 4-digit year and `<created-month>` is the 2-digit zero-padded month derived from the Drive file's `createdTime`, `<file-id>` is the Drive file id, and `<sanitised-name>` is the original filename with any character outside `[A-Za-z0-9._-]` replaced with `_`. THE Drive_Importer SHALL record a `drive-import-success` entry in the Manifest upon successful upload.
7. IF a Drive file's `createdTime` is missing or cannot be parsed, THEN THE Drive_Importer SHALL substitute `unknown-year/unknown-month` in the destination key and SHALL record a `drive-warning-missing-created-time` entry in the Manifest.
8. THE Drive_Importer SHALL NOT issue any delete, update, rename, or metadata-write operation against Google Drive during a pull-only sync run.
9. IF a Drive file's bytes cannot be downloaded after 5 retry attempts using exponential backoff between 1 and 30 seconds, THEN THE Drive_Importer SHALL record a `drive-download-error` entry in the Manifest containing the Drive file id and the terminal failure reason, SHALL NOT upload partial bytes to the `drive_destination` Source, and SHALL continue processing remaining files.
10. THE Drive_Importer SHALL authenticate to Google Drive using a service account credential file referenced via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, with the `https://www.googleapis.com/auth/drive.readonly` scope, and SHALL fail fast at startup with a non-zero exit code IF the credential file is unreadable or the configured `drive_root_folder_id` is not accessible to the service account.

### Requirement 11: Idempotency of Sync_Run

**User Story:** As an operator, I want re-running the tool with no underlying changes to be a no-op, so that scheduled runs do not generate spurious work or churn.

#### Acceptance Criteria

1. WHEN a Sync_Run is executed against Sources whose Content_Hash and Source identity values are byte-for-byte identical to those observed at the end of the immediately preceding Sync_Run, THE MultiCloud_Photo_Sync SHALL perform zero upload operations, zero copy operations, zero delete operations, and zero tag-modification operations during the second Sync_Run.
2. WHEN a Sync_Run is interrupted before completion and a subsequent Sync_Run is executed against Sources whose Content_Hash and Source identity values are unchanged from the interrupted run, THE MultiCloud_Photo_Sync SHALL produce a Catalog whose set of Object_Records is equal (by Content_Hash and Source identity) to the set produced by a single uninterrupted Sync_Run over the same Sources.
3. THE MultiCloud_Photo_Sync SHALL determine all replication and import decisions exclusively from Content_Hash and Source identity, independent of the order in which Sources are listed or enumerated.
4. IF a Sync_Run cannot compute Content_Hash for a Source object within 30 seconds per object, THEN THE MultiCloud_Photo_Sync SHALL skip that object for the current Sync_Run, record a skip indication identifying the object and the reason in the Sync_Run result, and preserve any existing Object_Record for that object in the Catalog unchanged.
5. IF two or more Sources present objects with identical Content_Hash values during the same Sync_Run, THEN THE MultiCloud_Photo_Sync SHALL create exactly one Object_Record keyed by that Content_Hash and SHALL associate every contributing Source identity with that single Object_Record.

### Requirement 12: Retries with Bounded Backoff

**User Story:** As an operator, I want transient provider errors to be retried automatically and permanent errors to fail fast, so that runs are reliable without spinning forever on a real failure.

#### Acceptance Criteria

1. WHEN a provider operation fails with a transient error (HTTP 408, 429, 500, 502, 503, 504, or a connection timeout where no response is received within `request_timeout_ms` between 1000 and 120000 milliseconds), THE MultiCloud_Photo_Sync SHALL retry the operation up to a configured maximum of `max_retries` attempts (integer between 1 and 10, default 5), with exponential backoff starting at `initial_backoff_ms` (integer between 100 and 10000 milliseconds, default 500) and doubling each subsequent attempt.
2. WHEN a provider operation fails with a non-transient error (HTTP 400, 401, 403, or 404 where 404 is not the expected absence signal for the requested Object), THE MultiCloud_Photo_Sync SHALL NOT retry the operation and SHALL record an error entry in the Manifest for the affected Object indicating the failure reason and the originating HTTP status, while preserving any prior Manifest state for that Object.
3. WHEN a provider operation returns HTTP 429 with a `Retry-After` value, THE MultiCloud_Photo_Sync SHALL wait for the greater of the `Retry-After` value and the computed exponential backoff, capped at `max_backoff_ms`, before the next retry attempt.
4. WHILE retrying a provider operation, THE MultiCloud_Photo_Sync SHALL cap each individual backoff interval at `max_backoff_ms` (integer between 1000 and 300000 milliseconds, default 30000).
5. WHEN `max_retries` attempts have all failed for an Object, THE MultiCloud_Photo_Sync SHALL record a `retries-exhausted` entry for that Object in the Manifest including the last observed error reason and the total attempt count, and SHALL continue processing with the next Object without terminating the run.
6. IF a configured retry parameter (`max_retries`, `initial_backoff_ms`, or `max_backoff_ms`) is missing, non-numeric, or outside its allowed range, THEN THE MultiCloud_Photo_Sync SHALL reject the configuration at startup with an error message identifying the invalid parameter and SHALL NOT begin processing.

### Requirement 13: Dry-Run Mode

**User Story:** As an operator, I want to preview exactly what a run would do without changing anything, so that I can review removals and replications before they happen.

#### Acceptance Criteria

1. WHERE `--dry-run` is supplied on the command line, THE MultiCloud_Photo_Sync SHALL execute the entire Sync_Run in Dry_Run_Mode and SHALL log a message at Sync_Run start indicating that Dry_Run_Mode is active.
2. WHILE in Dry_Run_Mode, THE MultiCloud_Photo_Sync SHALL NOT issue any `PUT`, `COPY`, `DELETE`, or tag-modifying API call against any Source, and SHALL NOT modify any local file, manifest of record, or persisted state other than the Dry_Run_Mode Manifest and log output.
3. WHILE in Dry_Run_Mode, THE MultiCloud_Photo_Sync SHALL produce a Manifest containing every action that Apply_Mode would have taken, where each Manifest entry includes the action type (one of: replicate, remove, tag-update, skip), the Source identifier, the target Source identifier (if applicable), the object identifier, and a status field set to "planned" or "error".
4. IF any error is encountered while computing a planned action during Dry_Run_Mode, THEN THE MultiCloud_Photo_Sync SHALL record a Manifest entry for that action with status "error" and an error indication describing the failure cause, and SHALL continue processing remaining planned actions without aborting the Sync_Run.
5. WHEN a Dry_Run_Mode Sync_Run completes with zero Manifest entries having status "error", THE MultiCloud_Photo_Sync SHALL exit with status code 0.
6. IF one or more Manifest entries have status "error" when a Dry_Run_Mode Sync_Run completes, THEN THE MultiCloud_Photo_Sync SHALL exit with a non-zero status code in the range 1 to 255 and SHALL emit a summary message indicating the count of error entries.

### Requirement 14: Manifest and Structured Logging

**User Story:** As an operator, I want every action and error in a run captured in a machine-readable manifest and a structured log, so that I can audit, alert, and debug.

#### Acceptance Criteria

1. WHEN a Sync_Run starts, THE MultiCloud_Photo_Sync SHALL create one Manifest file in the configured manifest directory named `manifest-<UTC-timestamp>-<run-id>.jsonl`, where `<UTC-timestamp>` is in ISO-8601 format `YYYYMMDDTHHMMSSZ` and `<run-id>` is a unique identifier of at least 8 characters.
2. WHEN any per-object action occurs during a Sync_Run, THE MultiCloud_Photo_Sync SHALL append exactly one JSON-encoded record per line to the Manifest file, each record containing fields `timestamp` (ISO-8601 UTC), `run_id`, `action`, `source`, `key`, `content_hash`, `result` (one of `success`, `skipped`, `quarantined`, `deleted`, `error`), and an optional `error` field populated only when `result` is `error`.
3. WHEN any log event occurs during a Sync_Run, THE MultiCloud_Photo_Sync SHALL emit one JSON-encoded record per line to standard error containing fields `timestamp` (ISO-8601 UTC), `level` (one of `DEBUG`, `INFO`, `WARN`, `ERROR`), `run_id`, `event`, and `message`.
4. THE MultiCloud_Photo_Sync SHALL exclude credential values, signed URLs, access tokens, and secrets from every Manifest record and every log record, replacing any such field value with the literal string `[REDACTED]`.
5. WHEN a Sync_Run completes, THE MultiCloud_Photo_Sync SHALL emit a final summary log record at level `INFO` containing non-negative integer counts for `discovered`, `replicated`, `skipped`, `quarantined`, `deleted`, and `errored`, where the sum of `replicated`, `skipped`, `quarantined`, `deleted`, and `errored` equals `discovered`.
6. IF writing a record to the Manifest file fails, THEN THE MultiCloud_Photo_Sync SHALL emit an `ERROR` level log record indicating the manifest write failure and SHALL terminate the Sync_Run with a non-zero exit status without deleting the partially written Manifest file.
7. IF the configured manifest directory does not exist or is not writable at Sync_Run start, THEN THE MultiCloud_Photo_Sync SHALL emit an `ERROR` level log record indicating the manifest directory is unavailable and SHALL terminate the Sync_Run with a non-zero exit status before performing any replication action.

### Requirement 15: Manifest Parser and Round-Trip

**User Story:** As an operator, I want manifests to be reliably parseable and re-emittable so that downstream tooling can ingest them without ambiguity.

#### Acceptance Criteria

1. WHEN the Manifest_Parser is invoked on a Manifest file no larger than 1 GiB and containing no more than 10,000,000 lines, THE Manifest_Parser SHALL parse the file into an ordered sequence of in-memory Manifest_Record values preserving the original line order.
2. WHEN the Manifest_Printer is invoked on a sequence of up to 10,000,000 Manifest_Record values, THE Manifest_Printer SHALL serialise the sequence into a Manifest file in JSONL format with exactly one Manifest_Record per line, lines terminated by a single LF (0x0A) character, and UTF-8 encoding without a byte order mark.
3. FOR ALL valid sequences of Manifest_Record values of length 0 to 10,000,000, WHEN the output of the Manifest_Printer is provided as input to the Manifest_Parser, THE Manifest_Parser SHALL produce a sequence equal in length and element-wise equal in every field to the input sequence.
4. IF a Manifest line fails to parse due to invalid JSONL syntax, missing required fields, or field values outside their defined domains, THEN THE Manifest_Parser SHALL return a structured error containing the 1-based line number of the failing line and a machine-readable error category identifying the failure cause, SHALL NOT include the failing line in the returned sequence, and SHALL NOT silently drop or skip the line.
5. IF the Manifest file cannot be opened or read due to I/O failure, THEN THE Manifest_Parser SHALL return a structured error indicating the I/O failure cause and SHALL NOT return a partial sequence as a successful result.

### Requirement 16: Scheduling and Concurrency

**User Story:** As an operator, I want to run the tool on a schedule with bounded concurrency, so that it can keep up with large libraries without overwhelming provider rate limits or my local machine.

#### Acceptance Criteria

1. THE MultiCloud_Photo_Sync SHALL accept a configuration option `max_concurrent_transfers` that accepts integer values from 1 to 64 inclusive, with a default value of 4, and SHALL reject values outside this range with a non-zero exit code and an error message identifying the invalid value.
2. WHILE replication is in progress, THE MultiCloud_Photo_Sync SHALL NOT have more than `max_concurrent_transfers` upload, download, or copy operations in flight at any time, where an operation is considered in flight from the moment its transfer request is issued until a terminal success or failure response is received.
3. WHEN THE MultiCloud_Photo_Sync starts a Sync_Run, THE MultiCloud_Photo_Sync SHALL attempt to acquire a single-writer lock file at the configured catalog path before performing any upload, download, or copy operation.
4. WHEN THE MultiCloud_Photo_Sync successfully acquires the lock file, THE MultiCloud_Photo_Sync SHALL record the current process ID in the lock file, continue with the Sync_Run, and release the lock file on Sync_Run completion regardless of success or failure.
5. IF the lock file is already held by another process at acquisition time, THEN THE MultiCloud_Photo_Sync SHALL exit within 5 seconds with a non-zero `LOCK_CONFLICT` exit code and an error message identifying the conflicting process ID, SHALL NOT modify any catalog or transfer state, and SHALL NOT return this exit code when the current process itself successfully acquires the lock.
6. IF the lock file exists but the recorded process ID is not running, THEN THE MultiCloud_Photo_Sync SHALL treat the lock as stale, reclaim it, and proceed with the Sync_Run.
7. WHEN running on a schedule, THE MultiCloud_Photo_Sync SHALL be invokable as a single non-interactive CLI command that exits with code 0 on success and a non-zero code on failure, suitable for use as a cron entry or a systemd timer unit.

### Requirement 17: Configuration File Parser

**User Story:** As an operator, I want one declarative configuration file that drives Sources, replication options, and credential references, so that I do not have to remember command-line flags between runs.

#### Acceptance Criteria

1. WHEN MultiCloud_Photo_Sync is invoked with `--config <path>`, THE MultiCloud_Photo_Sync SHALL load configuration from the file at `<path>`, accepting only files with a `.toml`, `.yaml`, or `.yml` extension and a maximum file size of 1 MiB.
2. WHEN MultiCloud_Photo_Sync is invoked without `--config <path>`, THE MultiCloud_Photo_Sync SHALL load configuration from `./mcps.config.yaml` resolved relative to the current working directory.
3. IF the configuration file resolved from `--config <path>` or the default `./mcps.config.yaml` does not exist, is not readable, or exceeds 1 MiB, THEN THE MultiCloud_Photo_Sync SHALL terminate without loading any partial configuration and SHALL emit a descriptive error naming the resolved file path and the failure reason.
4. WHEN a configuration file is successfully read, THE Config_Parser SHALL parse it into an in-memory Config object containing exactly the top-level sections `sources`, `replication`, `duplicates`, `photos`, `retries`, and `runtime`, and SHALL reject any file whose top-level structure deviates from this set.
5. WHEN invoked with an in-memory Config object, THE Config_Printer SHALL serialise it back into the on-disk configuration format (TOML or YAML, matching the format of the loaded file or defaulting to YAML when none was loaded) using UTF-8 encoding.
6. FOR ALL valid Config objects, parsing the output of the Config_Printer SHALL produce a Config object that is field-by-field equal to the input across all six top-level sections (round-trip property).
7. IF the configuration file contains an unknown top-level key, an unknown key inside any of the six top-level sections, or a Source `kind` value other than `s3`, `gcs`, or `google_drive`, THEN THE Config_Parser SHALL return a descriptive error naming the offending key or value and the 1-based line number within the file, and SHALL NOT return a partial Config object.
8. IF the configuration file omits a required field within any of the six top-level sections, THEN THE Config_Parser SHALL return a descriptive error naming each missing field with its dotted path (for example, `sources[0].kind`), and SHALL NOT return a partial Config object.
9. IF a required field is present but its value violates its declared type or value range, THEN THE Config_Parser SHALL return a descriptive error naming the offending field path, the observed value, and the expected type or range.

### Requirement 18: First-Pass Reconciliation Report

**User Story:** As an operator running the tool for the first time against pre-populated S3 buckets and a pre-populated Drive folder, I want a single human-readable report of what exists where and what is inconsistent, so that I can review the planned actions before approving any destructive change.

#### Acceptance Criteria

1. WHEN the in-memory Catalog is empty at Sync_Run start (Cold_Start), THE MultiCloud_Photo_Sync SHALL emit a Reconciliation_Report at the end of source listing and duplicate detection that includes (a) per-Source counts of total Objects, total bytes, and total distinct Content_Hashes, (b) cross-source diff counts of Content_Hashes present in S3 only, in GCS only, in Drive only, in exactly two of the three Source kinds, and in all three Source kinds, (c) the count of same-source duplicate groups partitioned by Source, (d) the count of cross-source duplicate groups, and (e) the count of Drive files that would be imported by the Drive_Importer.
2. WHEN a Reconciliation_Report is produced, THE MultiCloud_Photo_Sync SHALL write the Reconciliation_Report to standard output as a structured human-readable summary AND SHALL write a copy of the same Reconciliation_Report to a file at `<manifest_dir>/reconciliation-<UTC-timestamp>-<run-id>.txt`, where `<manifest_dir>` is the configured `runtime.manifest_dir`, `<UTC-timestamp>` follows the same `YYYYMMDDTHHMMSSZ` format defined in Requirement 14.1, and `<run-id>` is the same run identifier used for the Manifest file in Requirement 14.1.
3. IF the in-memory Catalog is empty at Sync_Run start AND `--apply` was supplied without `--first-pass-confirmed`, THEN THE MultiCloud_Photo_Sync SHALL refuse to perform any destructive action during the Sync_Run (no `set_tag` for Quarantine per Requirement 5.7, no physical delete per Requirements 5.9 and 9.5, no overwrite under `on_key_conflict=overwrite` per Requirement 8.2), SHALL still perform replication writes to Replicated_Sources where the Content_Hash is absent (Requirement 6.2) and Drive_Importer uploads to absent destinations (Requirement 10.6), and SHALL exit at the end of the Sync_Run with a non-zero `FIRST_PASS_REVIEW_REQUIRED` exit code after the Reconciliation_Report has been produced.
4. WHEN `--first-pass-confirmed` is supplied alongside `--apply` on a Cold_Start Sync_Run, THE MultiCloud_Photo_Sync SHALL proceed with destructive actions normally subject to all other safety rules already defined, including the last-copy-protection rule of Requirements 9.6 and 9.7, the interactive confirmation rule of Requirement 5.5, and the non-interactive abort rule of Requirement 5.6.
5. THE Reconciliation_Report SHALL include the estimated total bytes that will be downloaded for hashing during the Cold_Start Sync_Run, computed as the sum of `size_bytes` over every Object_Record whose Content_Hash had to be computed by streaming the Object's bytes (i.e. the Content_Hash was neither reused from a Catalog cache entry per Requirement 3.7 nor read from a valid `mcps-content-sha256` user-metadata value per Requirement 7.1).
6. IF a Source's listing fails after the maximum retries defined in Requirement 2.6 during a Cold_Start Sync_Run, THEN THE MultiCloud_Photo_Sync SHALL refuse to produce a Reconciliation_Report, SHALL emit an error record identifying the failing Source by name and kind, and SHALL exit with a non-zero exit code that is distinct from `FIRST_PASS_REVIEW_REQUIRED` and from `LOCK_CONFLICT` (Requirement 16.5).
7. THE MultiCloud_Photo_Sync SHALL accept a CLI flag `--first-pass-confirmed` whose presence is meaningful only when both `--apply` is supplied and the in-memory Catalog is empty at Sync_Run start, and SHALL emit a WARN-level log record IF `--first-pass-confirmed` is supplied on a non-Cold_Start Sync_Run, treating the flag as a no-op in that case.

### Requirement 19: Inconsistency Detection on Subsequent Runs

**User Story:** As an operator running the tool repeatedly, I want any drift between Replicated_Sources to be surfaced even when no new files have been added, so that I learn about externally-induced inconsistencies (manual deletions, manual uploads, bucket-side replication failures) without needing to re-bootstrap.

#### Acceptance Criteria

1. WHEN a Sync_Run completes, THE MultiCloud_Photo_Sync SHALL include in the summary log record defined in Requirement 14.5 (a) per-Source counts of Object_Records that are new in the current Sync_Run relative to the Catalog at Sync_Run start, (b) per-Source counts of Object_Records present in the Catalog at Sync_Run start but absent from the Source after listing in the current Sync_Run, and (c) the count of Content_Hashes whose member sets diverge between Replicated_Sources after replication completes.
2. IF after replication completes there exists a Content_Hash that is present in one Replicated_Source but absent from another Replicated_Source AND the Manifest contains no `replication-error` entry (Requirement 6.5) for that Content_Hash, THEN THE MultiCloud_Photo_Sync SHALL append a `WARN`-level log record naming the affected Content_Hash, both Replicated_Source names, and the canonical Object_Record's `key` per Requirement 5.1, AND IF the configuration option `fail_on_inconsistency` is `true`, THEN THE MultiCloud_Photo_Sync SHALL exit with a non-zero exit code at the end of the Sync_Run.
3. THE MultiCloud_Photo_Sync SHALL provide a configuration option `fail_on_inconsistency` accepting exactly the boolean values `true` or `false`, with a default value of `false`, and SHALL reject any other value at startup with a non-zero exit code and an error message identifying the invalid `fail_on_inconsistency` value.
