"""Pre-wipe sanity checks.

Before running a destructive `aws s3 rm`, confirm:

* Whether the bucket has versioning enabled (which would make the
  delete recoverable as a delete-marker rollback).
* The exact list of S3-only single-part keys we're about to lose.
* Their total size.

Does NOT modify anything in S3.
"""

from __future__ import annotations

import collections
import configparser
import os
from pathlib import Path

LEGACY_CONFIG = Path.home() / ".local/state/mcps/legacy-backups/config.ini.stashed"
DRIVE_CREDS = Path("credentials.json")
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


def _human(n: int) -> str:
    f = float(n)
    for u in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if f < 1024 or u == "TiB":
            return f"{f:.2f} {u}"
        f /= 1024


s3 = boto3.client("s3", region_name=REGION)

# 1. Versioning state.
versioning = s3.get_bucket_versioning(Bucket=BUCKET)
status = versioning.get("Status", "<not set>")
mfa_delete = versioning.get("MFADelete", "<not set>")
print(f"Bucket versioning: Status={status}, MFADelete={mfa_delete}")
if status == "Enabled":
    print("  ✓ wipe is RECOVERABLE — delete creates a delete-marker; the")
    print("    objects can be restored by removing the delete-marker.")
elif status == "Suspended":
    print("  ✗ versioning suspended — wipe is NOT recoverable.")
else:
    print("  ✗ versioning never enabled — wipe is PERMANENT.")
print()

# 2. Which exact keys are S3-only single-part?
print("Listing Drive md5 set ...")
creds = service_account.Credentials.from_service_account_file(
    str(DRIVE_CREDS),
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

drive_md5s: set[str] = set()
stack = [("1zHhkML0CGM4yA4iQQRtmjwoSILTFUu2L", "Marta")]
while stack:
    fid, prefix = stack.pop()
    page_token = None
    while True:
        kwargs = dict(
            q=f"'{fid}' in parents and trashed=false",
            pageSize=1000,
            fields="nextPageToken,files(id,name,mimeType,md5Checksum)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        if page_token:
            kwargs["pageToken"] = page_token
        resp = drive.files().list(**kwargs).execute()
        for f in resp.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                stack.append((f["id"], prefix))
            elif "md5Checksum" in f:
                drive_md5s.add(f["md5Checksum"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

print(f"  {len(drive_md5s)} distinct Drive md5 hashes")
print()

# 3. Find S3 single-part objects whose md5 is not in Drive.
print("Finding S3-only single-part keys ...")
S3_HEX = set("0123456789abcdef")
paginator = s3.get_paginator("list_objects_v2")
md5_to_keys: dict[str, list[tuple[str, int]]] = collections.defaultdict(list)
for page in paginator.paginate(Bucket=BUCKET):
    for obj in page.get("Contents", []) or []:
        etag = (obj.get("ETag") or "").strip('"')
        if len(etag) == 32 and all(c in S3_HEX for c in etag):
            md5_to_keys[etag].append((obj["Key"], int(obj["Size"])))

s3_only_md5s = [m for m in md5_to_keys if m not in drive_md5s]
print(f"  {len(s3_only_md5s)} S3-only md5 hashes")

total_keys = 0
total_bytes = 0
all_keys_to_back_up: list[tuple[str, int]] = []
for m in s3_only_md5s:
    for key, size in md5_to_keys[m]:
        all_keys_to_back_up.append((key, size))
        total_keys += 1
        total_bytes += size

print(f"  {total_keys} S3-only single-part KEYS (one per copy)")
print(f"  {_human(total_bytes)} of S3-only content total")
print()

print("FULL LIST of S3-only single-part keys (would be lost in wipe):")
for key, size in sorted(all_keys_to_back_up):
    print(f"  {_human(size):>10s}  {key}")

# Save the list so the backup helper can use it.
with open("s3_only_keys.txt", "w") as f:
    for key, size in sorted(all_keys_to_back_up):
        f.write(f"{key}\n")
print()
print(f"Saved {total_keys} keys to s3_only_keys.txt for backup.")
