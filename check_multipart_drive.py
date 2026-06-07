"""Resolve which multipart S3 objects are also in Drive.

For every Drive file whose size matches a multipart S3 object's size,
stream it through SHA-256 once and check whether that SHA-256 is in
the S3 multipart-SHA-256 set. Any S3 SHA-256 that the candidate scan
reproduces is "in Drive too"; the rest are S3-only.

Writes the result to ``multipart_check_result.json`` so the wipe
script can read it.
"""

from __future__ import annotations

import collections
import configparser
import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path

LEGACY_CONFIG = Path.home() / ".local/state/mcps/legacy-backups/config.ini.stashed"
DRIVE_CREDS = Path("credentials.json")
DRIVE_FOLDER_ID = "1zHhkML0CGM4yA4iQQRtmjwoSILTFUu2L"
BUCKET = "pickbackup"
REGION = "us-east-1"

parser = configparser.RawConfigParser()
parser.read(LEGACY_CONFIG)
section = parser["aws_credentials"]
os.environ["AWS_ACCESS_KEY_ID"] = section["aws_access_key_id"].strip()
os.environ["AWS_SECRET_ACCESS_KEY"] = section["aws_secret_access_key"].strip()
os.environ["AWS_DEFAULT_REGION"] = REGION
del section, parser
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(DRIVE_CREDS.resolve())

import boto3  # noqa: E402
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from googleapiclient.http import MediaIoBaseDownload  # noqa: E402

# Load multipart S3 SHA-256 set and key-to-sha map.
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

s3 = boto3.client("s3", region_name=REGION)
S3_HEX = set("0123456789abcdef")
multipart_keys: list[tuple[str, int]] = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET):
    for obj in page.get("Contents", []) or []:
        etag = (obj.get("ETag") or "").strip('"')
        is_sp = len(etag) == 32 and all(c in S3_HEX for c in etag)
        if not is_sp:
            multipart_keys.append((obj["Key"], int(obj["Size"])))

multipart_sha_set: set[str] = set()
multipart_sizes: set[int] = set()
sha_to_keys: dict[str, list[str]] = collections.defaultdict(list)
sha_to_size: dict[str, int] = {}
for key, size in multipart_keys:
    sha = key_to_sha.get(key)
    if sha:
        multipart_sha_set.add(sha)
        sha_to_keys[sha].append(key)
        sha_to_size[sha] = size
        multipart_sizes.add(size)

print(f"S3 multipart hashes: {len(multipart_sha_set)}", flush=True)
print(f"Distinct sizes:      {len(multipart_sizes)}", flush=True)

# Drive: list every file, keep size-matched candidates.
creds = service_account.Credentials.from_service_account_file(
    str(DRIVE_CREDS),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

candidates: list[dict] = []
stack = [(DRIVE_FOLDER_ID, "Marta")]
while stack:
    fid, prefix = stack.pop()
    page_token = None
    while True:
        kwargs = dict(
            q=f"'{fid}' in parents and trashed=false",
            pageSize=1000,
            fields="nextPageToken,files(id,name,mimeType,size,md5Checksum,parents)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        if page_token:
            kwargs["pageToken"] = page_token
        resp = drive.files().list(**kwargs).execute()
        for f in resp.get("files", []):
            f["path"] = f"{prefix}/{f['name']}"
            if f["mimeType"] == "application/vnd.google-apps.folder":
                stack.append((f["id"], f["path"]))
                continue
            sz = int(f.get("size", 0) or 0)
            if sz in multipart_sizes:
                candidates.append(f)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

print(f"Drive size-matched candidates: {len(candidates)}", flush=True)

# Stream each candidate through SHA-256.
drive_sha_set: set[str] = set()
streamed_bytes = 0
start = time.monotonic()

# Cache file in case we get interrupted.
cache_path = Path("drive_streamed_shas.txt")
if cache_path.exists():
    for line in cache_path.read_text().splitlines():
        if line.strip():
            drive_sha_set.add(line.strip())

cache_fh = open(cache_path, "a")

for i, c in enumerate(candidates, start=1):
    fid = c["id"]
    sz = int(c.get("size", 0) or 0)
    if sz == 0:
        continue
    h = hashlib.sha256()
    request = drive.files().get_media(fileId=fid, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request, chunksize=4 * 1024 * 1024)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    h.update(buf.getvalue())
    sha = h.hexdigest()
    drive_sha_set.add(sha)
    cache_fh.write(sha + "\n")
    cache_fh.flush()
    streamed_bytes += sz
    if i % 25 == 0 or i == len(candidates):
        elapsed = time.monotonic() - start
        rate = streamed_bytes / elapsed / 1024 / 1024 if elapsed else 0
        eta = (len(candidates) - i) * (elapsed / i) if i else 0
        print(
            f"  [{i:>4}/{len(candidates)}] cum={streamed_bytes / 1024 / 1024 / 1024:.2f} GiB "
            f"elapsed={elapsed:.0f}s rate={rate:.1f} MiB/s ETA={eta / 60:.0f} min",
            flush=True,
        )

cache_fh.close()

# Final result.
in_both = multipart_sha_set & drive_sha_set
s3_only = multipart_sha_set - drive_sha_set
s3_only_keys = []
s3_only_bytes = 0
for sha in s3_only:
    keys = sha_to_keys[sha]
    size = sha_to_size[sha]
    s3_only_bytes += size
    s3_only_keys.append({"sha256": sha, "size": size, "keys": keys})

result = {
    "multipart_sha_total": len(multipart_sha_set),
    "in_both_count": len(in_both),
    "s3_only_count": len(s3_only),
    "s3_only_bytes": s3_only_bytes,
    "s3_only": sorted(s3_only_keys, key=lambda r: -r["size"]),
}
with open("multipart_check_result.json", "w") as f:
    json.dump(result, f, indent=2)

print()
print(f"Result: {len(in_both)} in both, {len(s3_only)} S3-only "
      f"({s3_only_bytes / 1024 / 1024 / 1024:.2f} GiB)")
print(f"Saved to multipart_check_result.json")
