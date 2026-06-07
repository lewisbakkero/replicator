"""Stream every S3 multipart-ETag object to ~/Desktop/photosync-s3-backup/multipart/.

These are the 735 S3 objects whose ETag is multipart (and therefore
not directly comparable to a Drive md5Checksum). We back them up
locally before any wipe so that, regardless of how many turn out to
be Drive-only, none are irrecoverable.

Runs in parallel with ``check_multipart_drive.py``.
"""

from __future__ import annotations

import collections
import configparser
import hashlib
import json
import os
import sys
import time
from pathlib import Path

LEGACY_CONFIG = Path.home() / ".local/state/mcps/legacy-backups/config.ini.stashed"
BACKUP_ROOT = Path.home() / "Desktop" / "photosync-s3-backup" / "multipart"
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

# Build catalog lookup so we can record SHA-256 (already known) per key.
key_to_sha = {}
key_to_size = {}
with open("mcps.catalog.jsonl") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        key_to_sha[rec["key"]] = rec["content_hash"]
        key_to_size[rec["key"]] = rec["size_bytes"]

S3_HEX = set("0123456789abcdef")
multipart_keys: list[tuple[str, int]] = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET):
    for obj in page.get("Contents", []) or []:
        key = obj["Key"]
        size = int(obj["Size"])
        etag = (obj.get("ETag") or "").strip('"')
        is_singlepart = (
            len(etag) == 32
            and all(c in S3_HEX for c in etag)
        )
        if not is_singlepart:
            multipart_keys.append((key, size))

print(f"{len(multipart_keys)} multipart-ETag keys to back up", flush=True)
total_bytes = sum(s for _, s in multipart_keys)
print(f"total bytes: {total_bytes / 1024 / 1024 / 1024:.2f} GiB", flush=True)
BACKUP_ROOT.mkdir(parents=True, exist_ok=True)

manifest_lines: list[str] = []
streamed_bytes = 0
start = time.monotonic()

for i, (key, size) in enumerate(multipart_keys, start=1):
    local = BACKUP_ROOT / key
    local.parent.mkdir(parents=True, exist_ok=True)

    if local.exists() and local.stat().st_size == size:
        # Already there. Re-record manifest entry from local hash.
        h = hashlib.sha256()
        with open(local, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        manifest_lines.append(f"{h.hexdigest()}  {size}  {key}")
        streamed_bytes += size
        if i % 25 == 0:
            elapsed = time.monotonic() - start
            print(
                f"  [{i:>4}/{len(multipart_keys)}] (skipped existing) "
                f"cum={streamed_bytes / 1024 / 1024 / 1024:.2f} GiB "
                f"elapsed={elapsed:.0f}s",
                flush=True,
            )
        continue

    h = hashlib.sha256()
    with open(local, "wb") as fh:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
        while True:
            chunk = body.read(4 * 1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
            h.update(chunk)
    streamed_bytes += size
    manifest_lines.append(f"{h.hexdigest()}  {size}  {key}")

    if i % 5 == 0 or i == len(multipart_keys):
        elapsed = time.monotonic() - start
        rate = streamed_bytes / elapsed / 1024 / 1024 if elapsed else 0
        print(
            f"  [{i:>4}/{len(multipart_keys)}] cum={streamed_bytes / 1024 / 1024 / 1024:.2f} GiB "
            f"elapsed={elapsed:.0f}s rate={rate:.1f} MiB/s",
            flush=True,
        )

with open(BACKUP_ROOT / "manifest.txt", "w") as f:
    f.write("# sha256  size_bytes  s3_key\n")
    for line in manifest_lines:
        f.write(line + "\n")

elapsed = time.monotonic() - start
print()
print(f"Backed up {len(multipart_keys)} keys "
      f"({streamed_bytes / 1024 / 1024 / 1024:.2f} GiB) to {BACKUP_ROOT}")
print(f"Took {elapsed / 60:.1f} min, average rate "
      f"{streamed_bytes / elapsed / 1024 / 1024:.1f} MiB/s")
