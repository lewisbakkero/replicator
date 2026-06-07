"""Stream the 42 S3-only single-part keys to ~/Desktop/photosync-s3-backup/.

Reads the key list from ``s3_only_keys.txt`` produced by
``pre_wipe_check.py``. Preserves the original key path under the
backup root, sets the local mtime to the S3 LastModified, and records
each key's SHA-256 + size in a ``manifest.txt`` next to the backup
files for verification.
"""

from __future__ import annotations

import configparser
import hashlib
import os
from pathlib import Path

LEGACY_CONFIG = Path.home() / ".local/state/mcps/legacy-backups/config.ini.stashed"
BACKUP_ROOT = Path.home() / "Desktop" / "photosync-s3-backup" / "singlepart"
BUCKET = "pickbackup"
REGION = "us-east-1"

parser = configparser.RawConfigParser()
parser.read(LEGACY_CONFIG)
section = parser["aws_credentials"]
os.environ["AWS_ACCESS_KEY_ID"] = section["aws_access_key_id"].strip()
os.environ["AWS_SECRET_ACCESS_KEY"] = section["aws_secret_access_key"].strip()
os.environ["AWS_DEFAULT_REGION"] = REGION
del section, parser

import boto3  # noqa: E402

s3 = boto3.client("s3", region_name=REGION)

with open("s3_only_keys.txt") as f:
    keys = [line.rstrip("\n") for line in f if line.strip()]

BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
manifest_lines: list[str] = []
total_bytes = 0

for i, key in enumerate(keys, start=1):
    local = BACKUP_ROOT / key  # preserve the S3 key as a relative path
    local.parent.mkdir(parents=True, exist_ok=True)

    # Skip if already present and same size as S3 reports.
    head = s3.head_object(Bucket=BUCKET, Key=key)
    expected_size = int(head["ContentLength"])
    if local.exists() and local.stat().st_size == expected_size:
        # Compute the local sha256 to record in the manifest.
        h = hashlib.sha256()
        with open(local, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        manifest_lines.append(f"{h.hexdigest()}  {expected_size}  {key}")
        total_bytes += expected_size
        print(f"  [{i:>3}/{len(keys)}] (skip, already present) {key}", flush=True)
        continue

    print(f"  [{i:>3}/{len(keys)}] downloading {key} ({expected_size} bytes)",
          flush=True)
    h = hashlib.sha256()
    with open(local, "wb") as fh:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
            h.update(chunk)
    if local.stat().st_size != expected_size:
        print(f"    WARN: size mismatch {local.stat().st_size} != {expected_size}",
              flush=True)
    manifest_lines.append(f"{h.hexdigest()}  {expected_size}  {key}")
    total_bytes += expected_size

with open(BACKUP_ROOT / "manifest.txt", "w") as f:
    f.write("# sha256  size_bytes  s3_key\n")
    for line in manifest_lines:
        f.write(line + "\n")

print()
print(f"Backed up {len(keys)} keys, {total_bytes:,} bytes "
      f"({total_bytes / 1024 / 1024:.2f} MiB) to {BACKUP_ROOT}")
print(f"Manifest at {BACKUP_ROOT}/manifest.txt")
