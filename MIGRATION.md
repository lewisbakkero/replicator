# Migration Plan

This document is the operator-facing companion to the design's "Migration
Plan" section (see `.kiro/specs/multicloud-photo-sync/design.md`). The
existing `uploader.py` / `delete.py` / `config.ini` setup is replaced
wholesale by the new `mcps` package, but the migration must be performed
in a specific order to (a) get the leaked AWS keys out of the working
tree and (b) keep an emergency rollback path open.

The recommended order is:

1. **Rotate the leaked AWS access key.** _(this document)_
2. Move the Google Drive service-account file out of the repo and set
   `GOOGLE_APPLICATION_CREDENTIALS`.
3. Add `.gitignore` entries to prevent recurrence.
4. Generate the new `mcps.config.yaml`.
5. Install the package and run a first dry run.
6. Delete the legacy files.
7. Re-run with `--apply` (Cold_Start two-step review-then-confirm).
8. Schedule under cron or systemd.

This file documents **step 1** in operational detail. Steps 2-8 are
covered by separate migration tasks in the spec's `tasks.md`.

> [!CAUTION]
> Do not skip step 1. The leaked secret has been on disk in plaintext;
> moving the file out of the repo is **not** sufficient. Rotate the key
> first, then come back for the rest of the migration.

---

## Step 1 — Rotate the leaked AWS credentials

### Background

The legacy `config.ini` shipped with this repository contains a hard-coded
AWS access key with id:

```
AKIAYQ4K35M7H3INY75N
```

This key has been visible on disk in the working tree, was likely
committed at some point in git history, and must be assumed compromised.
Until it is rotated and deactivated, the key remains a security exposure
regardless of any other migration progress. The corresponding secret
access key is **not** reproduced anywhere in this document or in the
codebase migration tooling.

### What you will do

1. Identify which IAM user owns the leaked key.
2. Create a new access key for that IAM user (or, preferably, for a new,
   least-privilege IAM user scoped to only the S3 actions `mcps`
   requires).
3. Wire the **new** key into your local AWS credentials chain (env vars,
   `~/.aws/credentials` profile, or instance role) and confirm `mcps`
   can authenticate with it.
4. Mark the **old** key (`AKIAYQ4K35M7H3INY75N`) as `Inactive`.
5. After at least one successful `mcps --apply` run with the new key,
   delete the old key.
6. Run `mcps doctor --check-iam` to confirm the leaked key is no longer
   active.

The intermediate `Inactive` step lets you roll back to the old key
within the AWS-side retention window if the new key turns out to be
misconfigured. Once you have proven the new key works, **delete the
old key** so it cannot be reactivated by anyone holding a copy of this
repository.

### Recommended IAM policy for the new key

The new IAM user / role only needs S3 actions on the configured
bucket(s). Attach an inline policy along these lines (replace
`<bucket>` with your actual bucket name; add additional resource
ARNs if you sync more than one bucket):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "McpsBucketLevel",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::<bucket>"
    },
    {
      "Sid": "McpsObjectLevel",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObjectTagging",
        "s3:PutObjectTagging"
      ],
      "Resource": "arn:aws:s3:::<bucket>/*"
    }
  ]
}
```

`mcps` does not need `s3:CreateBucket`, `iam:*` (other than the read-only
`iam:ListAccessKeys` and `iam:GetUser` used by `mcps doctor --check-iam`,
which can be granted on a separate, operator-only credential), or any
KMS action beyond the bucket's default encryption configuration.

---

### Procedure A — AWS Console (recommended for one-off rotation)

1. Sign in to the AWS Management Console for the account that owns
   `AKIAYQ4K35M7H3INY75N`.
2. Open **IAM → Users**, find the user that owns the leaked key (the
   key id is visible on each user's "Security credentials" tab; you
   can also use Procedure B's CLI lookup if you have admin
   credentials).
3. On the user's **Security credentials** tab, locate the access key
   row whose id matches `AKIAYQ4K35M7H3INY75N`.
4. Click **Create access key** at the top of the access keys section
   to mint a new key for the same user. Save the new access key id
   and secret access key into a password manager. **Do not paste the
   secret into this repository, into `config.ini`, or into any file
   under the working tree.**
5. Update your local AWS credentials chain to use the new key:
   - Preferred: add a named profile in `~/.aws/credentials`
     (e.g. `[mcps]`) and set `AWS_PROFILE=mcps` in your shell.
   - Alternatively: export `AWS_ACCESS_KEY_ID` /
     `AWS_SECRET_ACCESS_KEY` in the shell that runs `mcps`.
6. Run a single `mcps --dry-run` to confirm the new key authenticates.
7. Back in the console, click the leaked key's **Actions → Make
   inactive**.
8. Run `mcps --dry-run` again to confirm the new key is still
   working (i.e. `mcps` is not silently picking up the old key).
9. After you have proven the new key works under at least one
   successful `mcps --apply` run, click the leaked key's **Actions
   → Delete**.

### Procedure B — AWS CLI (for scripted rotation)

These commands assume your shell is currently authenticated as an IAM
admin with permission to create / deactivate / delete access keys for
the user that owns `AKIAYQ4K35M7H3INY75N`. Replace `<USER>` with the
IAM username (use the lookup command if you do not know it).

```bash
# 0. Look up which user owns the leaked key.
aws iam list-users --query "Users[].UserName" --output text \
  | tr '\t' '\n' \
  | while read -r u; do
      aws iam list-access-keys --user-name "$u" \
        --query "AccessKeyMetadata[?AccessKeyId=='AKIAYQ4K35M7H3INY75N'].UserName" \
        --output text
    done

# 1. Create the new key. Stash the JSON output in your password manager;
#    do NOT redirect it into a file inside this repository.
aws iam create-access-key --user-name <USER>

# 2. Wire the new key into ~/.aws/credentials or your shell, then
#    confirm mcps authenticates:
mcps --dry-run

# 3. Deactivate the leaked key.
aws iam update-access-key \
  --user-name <USER> \
  --access-key-id AKIAYQ4K35M7H3INY75N \
  --status Inactive

# 4. Verify with the bundled doctor check (see below).
mcps doctor --check-iam

# 5. After at least one successful --apply run with the new key,
#    delete the old key permanently.
aws iam delete-access-key \
  --user-name <USER> \
  --access-key-id AKIAYQ4K35M7H3INY75N
```

---

## Verifying the rotation

Run the bundled doctor check:

```bash
mcps doctor --check-iam
```

It resolves AWS credentials through the same chain `mcps` itself uses
(env vars → named profile → instance / container role), calls
`iam:GetUser` + `iam:ListAccessKeys`, and asserts that
`AKIAYQ4K35M7H3INY75N` is either:

- **absent** from the bound IAM user's access-key list (deleted), or
- **present with `Status == "Inactive"`** (deactivated, awaiting
  deletion).

The check exits with code `0` on success and a non-zero exit code on
failure. The active-key path emits a `FAIL:` line to stderr naming the
IAM user and pointing back to this document.

For `mcps doctor --check-iam` to work, the credential it authenticates
with must have `iam:GetUser` and `iam:ListAccessKeys` permission for
itself (the default `iam:GetUser`/`iam:ListAccessKeys` on resource
`arn:aws:iam::*:user/${aws:username}` is sufficient — see the
[AWS docs on self-managed IAM permissions][iam-self]). If your
operational `mcps` credential is intentionally scoped narrower than
that, run `mcps doctor --check-iam` from a separate operator profile
that does have the IAM read permissions.

[iam-self]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_understanding_self_managed.html

## After rotation

Continue with steps 2-8 of the migration plan documented in
`.kiro/specs/multicloud-photo-sync/design.md` ("Migration Plan"):

- Move `credentials.json` (the Drive service-account file) out of the
  repo and set `GOOGLE_APPLICATION_CREDENTIALS`.
- Add `.gitignore` entries.
- Generate `mcps.config.yaml`.
- Install the package and run `mcps --dry-run`.
- Delete `config.ini`, `delete.py`, `uploader.py`, `delete_list.txt`,
  `logfile.log`.
- Run the Cold_Start two-step `--apply` cycle.
- Schedule under cron or systemd.

> [!NOTE]
> Removing `config.ini` from the working tree only removes the secret
> from the *current* commit. If `config.ini` was ever committed to git
> history, plan a separate history-rewrite (BFG or `git filter-repo`)
> as a follow-up — it is out of scope for this migration document but
> the leaked key remains a risk until that history is rewritten and
> force-pushed (or until every consumer of the repository's history
> has been notified to re-clone).


---

## Step 2 — Move the Drive service-account credentials out of the repo

### Background

The legacy working tree shipped a Google service-account file as
`credentials.json` at the repository root. That file holds a long-lived
RSA private key with `drive.readonly` access to the operator's Drive
folder; like the leaked AWS access key it must be assumed exposed to
anyone who has ever cloned this repository. Even after `mcps` itself is
in place, leaving the file on the working tree means a future
`git add .` or a wide-cast Docker build context will silently re-leak
it.

The new location is `$HOME/.config/mcps/drive-service-account.json`
with mode `0600`, owned by the operator account that runs `mcps`. The
parent directory `~/.config/mcps/` is created with mode `0700` for
the same reason. Both modes are enforced by the operator (the tool
does not chmod files for you).

### What the resolver does

`Credential_Manager.resolve_drive()` (see `mcps/credentials.py`) runs
the GCP credential chain pinned to the `drive.readonly` scope:

1. The path in `GOOGLE_APPLICATION_CREDENTIALS`, if set and the file
   exists, is loaded as a service-account file.
2. Otherwise `google.auth.default()` is consulted (Application Default
   Credentials).

There is no implicit fallback to a path in the working tree; if
`GOOGLE_APPLICATION_CREDENTIALS` is unset and ADC is not configured,
`mcps` aborts the Sync_Run with `CredentialError` per Requirement 1.3.
The recommended setup is therefore: relocate the file once and export
the environment variable in the shell profile (or a systemd
`Environment=` line) that runs `mcps`.

### Procedure

```bash
# 1. Create the per-user config directory with restricted permissions.
install -d -m 0700 "$HOME/.config/mcps"

# 2. Copy (do NOT move) the existing credentials file to the new
#    location, then lock it down to owner-only read/write. Copying
#    rather than moving keeps the legacy uploader.py / delete.py
#    runnable as a rollback path until task 43 deletes the originals.
cp ./credentials.json "$HOME/.config/mcps/drive-service-account.json"
chmod 600 "$HOME/.config/mcps/drive-service-account.json"

# 3. Verify mode and ownership.
ls -l "$HOME/.config/mcps/drive-service-account.json"
# expected: -rw-------  1 <you> <group>  ...  drive-service-account.json

# 4. Export the credential path in your shell profile (zsh / bash):
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/mcps/drive-service-account.json"' \
  >> "$HOME/.zshrc"
# or, for bash:
# echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/mcps/drive-service-account.json"' \
#   >> "$HOME/.bashrc"

# 5. Reload the shell (or `source` the file) and confirm:
echo "$GOOGLE_APPLICATION_CREDENTIALS"
test -r "$GOOGLE_APPLICATION_CREDENTIALS" && echo "readable"
```

For systemd-managed runs, set the variable in the unit instead of the
shell profile:

```ini
# /etc/systemd/system/mcps.service
[Service]
Environment=GOOGLE_APPLICATION_CREDENTIALS=/home/<operator>/.config/mcps/drive-service-account.json
```

### Verification

Once `mcps` is installed (step 5 of the migration plan) you can confirm
the resolver picks up the relocated file with a dry run:

```bash
mcps --config mcps.config.yaml --dry-run
```

A successful Drive listing in the dry-run output (or the absence of a
`CredentialError` naming the `drive` provider) indicates the new path
is in effect. If `mcps` aborts with a `CredentialError` whose
`sources_tried` includes `service_account_file` and
`application_default`, re-check that `GOOGLE_APPLICATION_CREDENTIALS`
is exported in the shell that ran the command (a common failure mode
is exporting it only in the interactive shell while running `mcps`
under cron with an empty environment).

> [!NOTE]
> The original `./credentials.json` in the working tree is **not**
> deleted by this step. It stays in place until task 43 removes the
> legacy files (`config.ini`, `credentials.json`, `delete.py`,
> `uploader.py`, `delete_list.txt`, `logfile.log`) after the first
> successful Cold_Start two-step Apply cycle. The `.gitignore`
> entries added in step 3 keep that lingering copy from being
> committed in the meantime.

---

## Step 3 — Add `.gitignore` entries

The relocated credential file is safe at `$HOME/.config/mcps/`, but
the working tree still contains (or will accumulate) several files
that must never be committed: the legacy plaintext `config.ini` with
the leaked AWS key, the lingering `credentials.json` until task 43
deletes it, and the runtime artefacts `mcps` itself produces (the
JSONL Catalog, its lock file, the Manifests directory, and the
legacy `logfile.log`).

A `.gitignore` at the repo root covers all of those:

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

Rationale per entry:

| Pattern                       | Why it is ignored                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `config.ini`                  | Holds the leaked AWS access key id (`AKIAYQ4K35M7H3INY75N`) and secret. Refused by `mcps` at startup per req 1.5.            |
| `credentials.json`            | The legacy in-repo Drive service-account file. Removed in task 43; ignored until then so it is not re-added accidentally.   |
| `*.service-account.json`      | Catches any future GCP service-account file dropped under the working tree (e.g. while testing a second account).            |
| `mcps.catalog.jsonl`          | The on-disk Catalog (req 3). Per-host runtime state, not source.                                                            |
| `mcps.catalog.jsonl.lock`     | The `fcntl` lock file held during Catalog write (design "Catalog persistence"). Pure runtime artefact.                      |
| `manifests/`                  | Per-run Manifest JSONL files (design "Manifest"). Pure runtime artefact and may be large.                                   |
| `logfile.log`                 | The legacy unstructured log produced by `uploader.py` / `delete.py`. Replaced by structured stderr logs and the Manifest.    |

The `.gitignore` shipped with this change also includes the standard
Python project exclusions (`__pycache__/`, `*.py[cod]`, `build/`,
`dist/`, `*.egg-info/`, `.pytest_cache/`, `.hypothesis/`,
`.mypy_cache/`, `.ruff_cache/`, `.coverage`, `htmlcov/`, `.venv/`,
`venv/`, `.env`, `.DS_Store`) so the repo stays clean once the
package is installed in editable mode (step 5 of the migration
plan).

### Audit existing history

`.gitignore` only prevents *future* commits. If `config.ini` or
`credentials.json` have ever been committed, the secrets remain in the
git history regardless of any ignore rule:

```bash
git -P log --all --oneline -- config.ini credentials.json | head -100
```

If either path has commits, plan a separate history-rewrite (BFG or
`git filter-repo`) as a follow-up. That history rewrite is out of
scope for this migration document, but the leaked AWS key remains a
risk until either (a) the rotated key from step 1 is deleted (not
just `Inactive`) and the old key is confirmed unused via CloudTrail,
or (b) the history is rewritten and every consumer of the repository
re-clones.


---

## Step 4 — Generate `mcps.config.yaml`

### Background

`mcps` reads its configuration from a single TOML or YAML file (default
path: `./mcps.config.yaml` — see `mcps/config/parser.py`,
`default_config_path`). The schema is documented in design.md ("Data
Models" → `Config`) and validated by `mcps/config/model.py`; unknown
top-level keys, unknown per-section keys, missing required sections,
and out-of-range numeric fields are all rejected at parse time with
`ConfigError` and a best-effort line number.

The minimum viable template for the existing one-way Drive→S3 flow has
two sources (the legacy `pickbackup` S3 bucket and the legacy `Marta`
Drive folder), an empty `replication.pairs` list (no S3↔GCS replication
configured yet), and the runtime defaults from design.md. The actual
template generated by this step is committed at the repo root as
`mcps.config.yaml`; the operator only has to fill in the Drive folder id
before the first run.

### What is in the generated template

The committed `mcps.config.yaml` mirrors the template in design.md
("Migration Plan", step 4). Concretely:

| Section | Field | Value | Source / rationale |
| --- | --- | --- | --- |
| `sources[0]` | `name` | `s3-pickbackup` | Same logical name as design.md's worked example. |
| `sources[0]` | `kind` | `s3` | Replicated_Source (writable). |
| `sources[0]` | `bucket` | `pickbackup` | Recovered from legacy `uploader.py` (`s3_bucket = 'pickbackup'`). Bucket names are not secret. |
| `sources[0]` | `region` | `us-east-1` | Legacy code constructed `boto3.client('s3', ...)` with no explicit region; boto3's default resolution lands on `us-east-1` absent env / profile overrides. Edit if the bucket actually lives elsewhere. |
| `sources[1]` | `name` | `drive-marta` | Same logical name as design.md's worked example. |
| `sources[1]` | `kind` | `google_drive` | Pull_Only_Source (read-only, never written to). |
| `sources[1]` | `drive_root_folder_id` | `<drive-folder-id>` | **Placeholder.** Legacy `uploader.py` looked the folder up by name (`'Marta'`), so the id is not recoverable from the working tree. Operator must edit. |
| `replication` | `pairs` | `[]` | One-way Drive→S3 today; no S3↔GCS replication. Operator adds pairs after the first-pass review (step 7) once a second writable Source exists. |
| `replication` | `on_key_conflict` | `skip` | design.md default. |
| `replication` | `fail_on_conflict` | `false` | design.md default. |
| `replication` | `delete_propagation` | `none` | design.md default. |
| `replication` | `tombstone_retention_days` | `30` | design.md default. |
| `replication` | `fail_on_inconsistency` | `false` | design.md default (req 19.3). |
| `duplicates` | `canonical_source_priority` | `[s3-pickbackup]` | `s3-pickbackup` is the only writable Source today, so it wins ties. Re-rank when a second Replicated_Source is added. |
| `duplicates` | `quarantine_retention_days` | `30` | design.md default. |
| `photos` | `drive_source` | `drive-marta` | Names the Drive Source for `Drive_Importer`. |
| `photos` | `drive_destination` | `s3-pickbackup` | Drive items land in the same S3 bucket the legacy `uploader.py` wrote to. |
| `retries` | `max_retries` | `5` | design.md default. |
| `retries` | `initial_backoff_ms` | `500` | design.md default. |
| `retries` | `max_backoff_ms` | `30000` | design.md default. |
| `retries` | `request_timeout_ms` | `30000` | design.md default. |
| `runtime` | `catalog_path` | `./mcps.catalog.jsonl` | design.md default. The `.gitignore` from step 3 keeps this file out of git. |
| `runtime` | `manifest_dir` | `./manifests` | design.md default. The `.gitignore` from step 3 keeps this directory out of git. |
| `runtime` | `max_concurrent_transfers` | `4` | design.md default. |

The template intentionally contains **no credentials**. AWS credentials
come from the standard AWS credential chain (env vars / named profile /
instance role); the Drive service-account file is located via
`GOOGLE_APPLICATION_CREDENTIALS` (set in step 2). If you find yourself
about to paste an access key into this YAML file, stop and re-read step 1.

### Procedure

```bash
# 1. Confirm the template is in place at the repo root.
ls -l mcps.config.yaml

# 2. Edit the Drive folder id in place. The placeholder is
#    `<drive-folder-id>`; replace it with the real id (the long
#    alphanumeric string from the Drive folder's URL,
#    e.g. https://drive.google.com/drive/folders/<id>).
$EDITOR mcps.config.yaml

# 3. Confirm the template still parses cleanly. This validates schema,
#    section presence, range checks, and enum values; it does NOT
#    contact AWS or Google.
python -c "from mcps.config.parser import parse_config_file; \
  cfg, fmt = parse_config_file('mcps.config.yaml'); \
  print('OK', fmt, [s.name for s in cfg.sources])"

# 4. (Optional, after step 5) Confirm `mcps` itself can load it.
mcps --config ./mcps.config.yaml --dry-run
# Until the legacy config.ini is removed in step 6a, this exits with
# code 66 (LegacyConfigDetected) before the new YAML is even read —
# that is expected and is, in itself, a successful smoke test of the
# legacy guard from req 1.5.
```

### Verification

The committed template was validated by round-tripping it through
`mcps.config.parser.parse_config_file`. The placeholder
`<drive-folder-id>` is itself a valid (non-empty) string under the
schema, so the file parses; only the live Drive call (in
`GoogleDriveSourceAdapter.__init__`) will reject it with
`DRIVE_ACCESS_FAILED` (exit 75). That failure mode is the intended
forcing function: parsing succeeds, dry-run reaches the Drive listing
phase, and the operator gets a precise pointer to the field they
forgot to fill in.

> [!NOTE]
> The committed `mcps.config.yaml` references the legacy bucket
> (`pickbackup`, region `us-east-1`) recovered from `uploader.py`. If
> your S3 bucket is elsewhere — different name, different region, or
> a sub-prefix — edit `sources[0].bucket`, `sources[0].region`, and
> optionally `sources[0].prefix` before the first run. None of these
> values are secrets.

---

## Step 5 — Install the package and run a first dry-run

This step is covered by tasks 38-39 of the spec's `tasks.md`: install
the package in editable mode (`pip install -e .` from the repo root)
and run `mcps --config mcps.config.yaml --dry-run`. The dry-run is
expected to **fail with exit code 66** (`LegacyConfigDetected`) until
step 6a removes the plaintext `config.ini`. That refusal is the legacy
guard from requirement 1.5 doing its job — it is a successful smoke
test, not a regression.

Before continuing, confirm the credentials wired in steps 1 and 2 are
discoverable from the shell that will run `mcps`:

```bash
aws sts get-caller-identity                # must print the new IAM identity
echo "$GOOGLE_APPLICATION_CREDENTIALS"     # must print the path from step 2
test -r "$GOOGLE_APPLICATION_CREDENTIALS" && echo readable
```

---

## Step 6 — Remove legacy files

### Background

The legacy files fall into two groups with different deletion timing:

* **`config.ini`** must be removed (or its `[aws_credentials]` section
  scrubbed) **before** the Cold_Start two-step Apply cycle in step 7.
  The legacy guard `mcps.cli.detect_legacy_config` refuses to start any
  Sync_Run while a `config.ini` containing
  `[aws_credentials]` / `aws_access_key_id` is present in the working
  tree (req 1.5, exit code 66). Step 7 is therefore impossible to run
  until `config.ini` is gone.
* **`delete.py`, `uploader.py`, `delete_list.txt`, `logfile.log`**, and
  the in-repo copy of **`credentials.json`** are removed **after** step
  7's confirmed Apply run. They do not block `mcps` from running, and
  keeping them on disk during the first Apply cycle preserves the
  emergency rollback path (worst case: restore credentials and re-run
  `uploader.py` against the legacy bucket).

This split is a refinement of design.md's "Migration Plan" item 6
(which lumps the deletions together). The split exists because design.md
predates the legacy guard wiring, while in practice the guard makes the
"delete everything in step 6, then run step 7" ordering impossible
without first quietly editing the legacy file the guard is meant to
protect against.

In aggregate, the files going away are:

| File | Replaced by | Why it goes |
| --- | --- | --- |
| `config.ini` | AWS credential chain (env / profile / role); `mcps.config.yaml` for non-secret bucket name and region | Holds the leaked AWS access key (`AKIAYQ4K35M7H3INY75N`). Refused at startup by the legacy guard (req 1.5). Deletion removes the secret from the working tree; combine with the history-rewrite follow-up flagged in step 1 if it was ever committed. |
| `delete.py` | `Duplicate_Resolver` (quarantine + last-copy-protected delete) | `delete.py` reads a plaintext `delete_list.txt` of S3 keys and unconditionally deletes them. The new tool selects deletions from cross-source duplicate groups, requires interactive or `--auto-approve` confirmation (req 5.5/5.6), and refuses to delete the last copy of a `content_hash` (req 9.6/9.7). |
| `uploader.py` | `Drive_Importer` (one-way Drive→destination, hash-based dedupe) | Replaced one-for-one. The new importer uses SHA-256 of bytes for dedupe instead of `head_object` on the destination key, follows the GCP credential chain instead of an in-repo service-account file, and writes structured Manifest entries instead of `logfile.log`. |
| `delete_list.txt` | The Manifest's `quarantine` and `physical-delete` records | An unstructured plaintext list of object keys. Replaced by structured per-run JSONL Manifests in `runtime.manifest_dir` (req 14). |
| `logfile.log` | Structured JSON logs to stderr + JSONL Manifest | Replaced by structured logs in `mcps/logging_setup.py` and the per-run Manifest. The legacy log was unstructured and lacked redaction. |

`credentials.json` (the Drive service-account file) was relocated to
`~/.config/mcps/drive-service-account.json` in step 2; the in-repo
copy is removed in step 6b since the relocation only copied (not
moved) the file to keep the legacy `uploader.py` runnable as an
emergency rollback path.

### Step 6a — Remove `config.ini` (before Cold_Start Apply)

#### Pre-conditions

1. Step 1 is complete: the leaked AWS key is `Inactive` (or deleted),
   the new key is wired into the AWS credential chain, and
   `aws sts get-caller-identity` reports the new IAM identity.
2. `mcps --config mcps.config.yaml --dry-run` has been run at least
   once and exited with code 66 (`LegacyConfigDetected`) — i.e. the
   legacy guard fired as expected, confirming `mcps` itself is
   installed and reachable.

#### Procedure

```bash
# 1. Take a dated backup of config.ini OUTSIDE the working tree so a
#    future `git add .` cannot accidentally re-introduce the secret.
ts=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$HOME/.local/state/mcps/legacy-backups"
cp config.ini "$HOME/.local/state/mcps/legacy-backups/config-${ts}.ini"
chmod 600 "$HOME/.local/state/mcps/legacy-backups/config-${ts}.ini"

# 2. Delete config.ini from the working tree.
rm -f config.ini

# 3. Confirm the legacy guard now passes through.
mcps --config mcps.config.yaml --dry-run
# Expected: NOT exit 66 any more. The dry-run proceeds to credential
# resolution, Source listing, and (for Drive) the access check.
# Other failures (CredentialError, DRIVE_ACCESS_FAILED) point to
# specific config / credential gaps; address those before step 7.
```

After step 6a, step 7 (Cold_Start two-step Apply cycle) can run.
`delete.py`, `uploader.py`, `delete_list.txt`, `logfile.log`, and the
in-repo `credentials.json` remain on disk; their deletion is deferred
to step 6b after step 7 completes.

### Step 6b — Remove the remaining legacy files (after Cold_Start Apply)

#### Pre-conditions

Do **not** run step 6b until all of the following are true:

1. Step 6a is complete (`config.ini` removed).
2. Step 7 (Cold_Start two-step Apply cycle) has completed. The first
   `mcps --apply` exited with code 76 (`FIRST_PASS_REVIEW_REQUIRED`),
   you reviewed `<manifest_dir>/reconciliation-*.txt`, and the
   confirmation run `mcps --apply --first-pass-confirmed` exited
   cleanly.
3. The on-disk `mcps.catalog.jsonl` is non-empty (so subsequent runs
   are not Cold_Starts and the legacy `uploader.py` would re-process
   everything if re-run as a rollback path).
4. The new IAM key from step 1 has been **deleted** (not just
   `Inactive`) and `mcps doctor --check-iam` reports the leaked key
   as absent. Until then, leaving the in-repo `credentials.json`
   around is no worse than the AWS-side state of the world; once the
   leaked AWS key is gone, the Drive credential becomes the riskiest
   on-disk artefact and should not linger.

If any of those is not yet true, stop here and complete the missing
pre-condition first.

#### Procedure

```bash
# 1. Re-run the doctor check; abort if it does not pass.
mcps doctor --check-iam || { echo "leaked key still active; do NOT proceed"; exit 1; }

# 2. Take a dated backup of the legacy files in case the operator
#    later wants to confirm what was removed. The tarball lives
#    OUTSIDE the working tree so a future `git add .` cannot
#    accidentally re-introduce the secret.
ts=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$HOME/.local/state/mcps/legacy-backups"
tar -czf "$HOME/.local/state/mcps/legacy-backups/legacy-${ts}.tar.gz" \
  delete.py uploader.py delete_list.txt logfile.log credentials.json \
  2>/dev/null || true
chmod 600 "$HOME/.local/state/mcps/legacy-backups/legacy-${ts}.tar.gz"

# 3. Delete the remaining legacy files from the working tree.
#    config.ini was removed in step 6a; this command targets the
#    rest.
rm -f delete.py uploader.py delete_list.txt logfile.log

# 4. Delete the in-repo copy of the Drive service-account file. The
#    relocated copy at $HOME/.config/mcps/drive-service-account.json
#    (from step 2) is what mcps actually reads.
rm -f credentials.json

# 5. Confirm the working tree is clean of legacy files.
ls -1 config.ini delete.py uploader.py delete_list.txt logfile.log credentials.json 2>/dev/null \
  && { echo "still present!"; exit 1; } \
  || echo "all legacy files removed"

# 6. Stage and commit the deletions. The .gitignore from step 3
#    means git would not otherwise volunteer them.
git add -u config.ini delete.py uploader.py delete_list.txt logfile.log credentials.json
git -P diff --cached --stat
git commit -m "chore: remove legacy uploader/delete scripts and config.ini

Replaced wholesale by the mcps package per the migration plan in
.kiro/specs/multicloud-photo-sync/design.md. The first Cold_Start
two-step Apply cycle has completed and mcps doctor --check-iam
reports the leaked AWS key as absent / inactive."
```

> [!CAUTION]
> `rm -f` is not undoable. The step 6a / 6b backup tarballs are the
> only recovery path. Verify the tarball exists and is readable
> (`tar -tzf "$HOME/.local/state/mcps/legacy-backups/legacy-${ts}.tar.gz"`)
> *before* running the `rm` commands.

### Verification

```bash
# mcps still parses its config and runs through dry-run cleanly.
mcps --config mcps.config.yaml --dry-run

# The legacy guard at startup (req 1.5) is now a no-op because the
# file is gone; mcps proceeds straight to credential resolution and
# Source listing.
```

If `mcps --dry-run` fails after the legacy files are removed, the
expected failure modes are:

- `CredentialError` for AWS or Drive — re-check steps 1 and 2.
- `DRIVE_ACCESS_FAILED` (exit 75) — the `drive_root_folder_id` in
  `mcps.config.yaml` is wrong or the service account lacks Viewer
  access; re-check step 4 and the share settings on the Drive folder.

There is no path back to `uploader.py` after this step short of
restoring from the step 2 backup tarball.

---

## Step 7 — Cold_Start two-step Apply cycle

This step is the operator-facing companion of design.md's "Migration
Plan", item 7. The first `mcps --apply` after step 5 lands on an
empty Catalog (Cold_Start, req 18.1). To prevent a stampede of
quarantines, deletes, or destination overwrites against the
pre-populated S3 bucket and Drive folder, the Cold_Start path is
deliberately split into two:

```bash
# (a) Unconfirmed first pass: lists, hashes, imports Drive items into
#     absent S3 keys, replicates Replicated_Source content to absent
#     destinations only. NO quarantines, NO physical deletes, NO
#     overwrites. Exits with code 76 (FIRST_PASS_REVIEW_REQUIRED).
mcps --config mcps.config.yaml --apply

# (b) Operator review. The reconciliation report lives at:
ls -1 ./manifests/reconciliation-*.txt
# Inspect the per-Source counts, cross-source diff, and duplicate
# group classifications. Compare against expectations.

# (c) Confirmed run: full Apply path with all the safety rules
#     (req 5.5/5.6 for duplicate quarantine, req 9.6/9.7 for
#     last-copy-protection).
mcps --config mcps.config.yaml --apply --first-pass-confirmed
```

After (c) completes successfully, proceed to step 6b (remove the
remaining legacy files) and step 8 (Schedule under cron / systemd;
see design.md).

---

## Operator checklist

Tick these off in order. Skipping a step is rarely safe; the steps
are ordered to keep an emergency rollback path open and to remove
the leaked AWS key from active use as early as possible.

- [ ] **Step 1.** Rotate the leaked AWS access key. New key wired
      in, old key marked `Inactive`, eventual deletion scheduled
      after step 7 completes.
- [ ] **Step 2.** Move `credentials.json` to
      `~/.config/mcps/drive-service-account.json` (mode `0600`),
      export `GOOGLE_APPLICATION_CREDENTIALS`.
- [ ] **Step 3.** Add `.gitignore` entries; audit git history for
      committed copies of `config.ini` / `credentials.json`.
- [ ] **Step 4.** Generate `mcps.config.yaml` (committed by this
      task) and replace the `<drive-folder-id>` placeholder with
      the real Drive folder id.
- [ ] **Step 5.** `pip install -e .` and run
      `mcps --config mcps.config.yaml --dry-run`. Expect exit code
      66 (legacy `config.ini` still present); that is the legacy
      guard from req 1.5 firing correctly.
- [ ] **Step 6a.** Remove `config.ini` (with backup outside the
      working tree). Re-run `mcps --config mcps.config.yaml --dry-run`
      and confirm the legacy guard no longer fires (exit code is
      not 66).
- [ ] **Step 7.** Cold_Start two-step Apply cycle:
  - [ ] First `mcps --apply` (no `--first-pass-confirmed`) exits
        with code 76 and writes `reconciliation-*.txt`.
  - [ ] Operator inspects the report and is satisfied with the
        per-Source counts, cross-source diff, and duplicate group
        classifications.
  - [ ] `mcps --apply --first-pass-confirmed` exits cleanly.
- [ ] **Post-step-7.** Delete the rotated-out IAM access key (not
      just `Inactive`); confirm with `mcps doctor --check-iam`.
- [ ] **Step 6b.** Now run the legacy-file deletion procedure for the
      remaining files: `rm -f delete.py uploader.py delete_list.txt
      logfile.log credentials.json`, with the dated backup tarball
      outside the working tree. Commit the deletions.
- [ ] **Step 8.** Schedule under cron or systemd. See design.md
      "Migration Plan" for the systemd timer template.
