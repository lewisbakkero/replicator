"""Integration test: a full Sync_Run in `--apply --first-pass-confirmed --auto-approve`.

Validates: Requirements 5.7, 6.2, 10.6, 14.5, 18.4.

This test mirrors `tests/integration/test_full_run_dry.py`'s
adapter-injection pattern, but exercises the destructive Apply path:

* Cold_Start → ``--apply --first-pass-confirmed --auto-approve``.
* S3 is seeded with **two byte-identical duplicates** so the
  Duplicate_Resolver picks one canonical and quarantines the other
  via ``set_tag("mcps-quarantined-at", ...)`` (req 5.7).
* GCS is seeded with **one Object that does not exist in S3**, so the
  Replicator must copy that Content_Hash from GCS into S3 (req 6.2).
* Drive is seeded with **two image files** carrying ``image/jpeg``
  ``mimeType`` and a parseable ``createdTime``; the Drive_Importer
  must upload them to ``drive_destination`` (S3) under the
  ``google-drive/<YYYY>/<MM>/<file-id>__<sanitised-name>`` key shape
  documented in design.md and req 10.6.

The test asserts on the post-run state of every adapter, not on the
call log, and on the on-disk Manifest / SUMMARY record (req 14.5,
18.4).
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import os
from typing import Any, Dict, List, Mapping, Optional

import boto3  # type: ignore[import-not-found]
import pytest

# moto-related imports are lazy so a missing optional dep surfaces a
# skip rather than a collection error.
moto = pytest.importorskip("moto")
from moto import mock_aws  # type: ignore[import-not-found]  # noqa: E402

from mcps import cli  # noqa: E402
from mcps.config.model import SourceConfig  # noqa: E402
from mcps.errors import ExitCode  # noqa: E402
from mcps.manifest.model import Action, Result  # noqa: E402
from mcps.manifest.parser import parse_manifest_file  # noqa: E402
from mcps.sources.base import SourceAdapter  # noqa: E402
from mcps.sources.drive import GoogleDriveSourceAdapter  # noqa: E402
from mcps.sources.gcs import GCSSourceAdapter  # noqa: E402
from mcps.sources.s3 import S3SourceAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixed run-time values so assertions are stable
# ---------------------------------------------------------------------------

_AWS_REGION = "us-east-1"
_S3_BUCKET = "mcps-int-bucket-s3-apply"
_GCS_BUCKET = "mcps-int-bucket-gcs-apply"
_DRIVE_ROOT = "drive-root-folder-id-apply"

_RUN_ID = "integration00testapply01"  # >= 8 chars (req 14.1)
_FIXED_NOW = dt.datetime(2024, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


# Pre-computed payloads. The two S3 duplicates share bytes; the GCS
# object has unique bytes that should land in S3 by replication; each
# Drive file has unique bytes that should land in S3 by import.
_DUP_PAYLOAD = b"DUPLICATE-CONTENT-BYTES" * 8
_GCS_ONLY_PAYLOAD = b"GCS-ONLY-PAYLOAD-BYTES" * 8
_DRIVE_PAYLOAD_A = b"DRIVE-FILE-A-BYTES" * 8
_DRIVE_PAYLOAD_B = b"DRIVE-FILE-B-BYTES" * 8


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# In-process GCS fake (mirrors the dry-run integration test).
# ---------------------------------------------------------------------------


class _FakeBlob:
    """Minimal ``google.cloud.storage.Blob`` stand-in."""

    def __init__(
        self,
        *,
        name: str,
        data: bytes = b"",
        updated: Optional[dt.datetime] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Mapping[str, str]] = None,
        crc32c: Optional[str] = None,
        client: Optional["_FakeGcsClient"] = None,
    ) -> None:
        self.name = name
        self._data: bytes = data
        self.size: Optional[int] = len(data) if data else 0
        self.updated: Optional[dt.datetime] = updated
        self.content_type: Optional[str] = content_type
        self.metadata: Optional[Dict[str, str]] = (
            dict(metadata) if metadata is not None else None
        )
        self.crc32c: Optional[str] = crc32c
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def open(self, mode: str = "rb") -> io.BytesIO:
        self.calls.append(("open", {"mode": mode}))
        assert mode == "rb"
        return io.BytesIO(self._data)

    def upload_from_file(
        self,
        file_obj: Any,
        content_type: Optional[str] = None,
    ) -> None:
        self.calls.append(
            ("upload_from_file", {"content_type": content_type})
        )
        if hasattr(file_obj, "read"):
            self._data = file_obj.read()
        else:  # pragma: no cover - defensive
            self._data = bytes(file_obj)
        self.size = len(self._data)

    def reload(self) -> None:
        self.calls.append(("reload", {}))

    def patch(self) -> None:
        self.calls.append(("patch", {"metadata": dict(self.metadata or {})}))

    def delete(self) -> None:
        self.calls.append(("delete", {}))
        if self._client is not None:
            self._client._remove_blob(self.name)


class _FakeBucket:
    def __init__(self, name: str, client: "_FakeGcsClient") -> None:
        self.name = name
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def blob(self, key: str) -> _FakeBlob:
        self.calls.append(("blob", {"key": key}))
        return self._client._get_or_create_blob(self.name, key)


class _FakeGcsClient:
    """In-process ``google.cloud.storage.Client`` stand-in."""

    def __init__(
        self,
        *,
        blobs: Optional[Mapping[tuple[str, str], _FakeBlob]] = None,
    ) -> None:
        self._blobs: Dict[tuple[str, str], _FakeBlob] = dict(blobs or {})
        for blob in self._blobs.values():
            blob._client = self
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def _get_or_create_blob(self, bucket: str, key: str) -> _FakeBlob:
        existing = self._blobs.get((bucket, key))
        if existing is not None:
            return existing
        fresh = _FakeBlob(name=key, client=self)
        self._blobs[(bucket, key)] = fresh
        return fresh

    def _remove_blob(self, key: str) -> None:
        for (bucket, name), _blob in list(self._blobs.items()):
            if name == key:
                del self._blobs[(bucket, name)]

    def bucket(self, name: str) -> _FakeBucket:
        self.calls.append(("bucket", {"name": name}))
        return _FakeBucket(name, self)

    def list_blobs(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> List[_FakeBlob]:
        self.calls.append(("list_blobs", {"bucket": bucket, "prefix": prefix}))
        result: List[_FakeBlob] = []
        for (b, _key), blob in self._blobs.items():
            if b != bucket:
                continue
            if prefix and not blob.name.startswith(prefix):
                continue
            result.append(blob)
        result.sort(key=lambda x: x.name)
        return result


# ---------------------------------------------------------------------------
# In-process Drive fake (mirrors the dry-run integration test).
# ---------------------------------------------------------------------------


_FOLDER_MIME = "application/vnd.google-apps.folder"


class _FileRequest:
    def __init__(
        self,
        op: str,
        kwargs: Mapping[str, Any],
        responder: "_FilesResource",
    ) -> None:
        self.op = op
        self.kwargs = dict(kwargs)
        self._responder = responder

    def execute(self) -> Any:
        return self._responder._dispatch(self.op, self.kwargs)


class _MediaRequest:
    def __init__(self, file_id: str, responder: "_FilesResource") -> None:
        self.file_id = file_id
        self._responder = responder

    def fetch_bytes(self) -> bytes:
        return self._responder._fetch_bytes(self.file_id)


class _FilesResource:
    """In-process ``service.files()`` stand-in.

    The Drive_Importer issues ``read_bytes`` twice per file (once to
    hash and once to stream-write). The seeded ``list_responses_by_parent``
    is consumed once on the first list page; we re-prime it on each
    list call by returning a fresh copy so the importer's two-pass
    behaviour does not exhaust the pool.
    """

    def __init__(
        self,
        *,
        list_pages_by_parent: Optional[
            Dict[str, List[Dict[str, Any]]]
        ] = None,
        get_responses: Optional[Dict[str, Dict[str, Any]]] = None,
        media_responses: Optional[Dict[str, bytes]] = None,
    ) -> None:
        # Store the canonical pages so each call to list() restarts at
        # the first page; the Drive adapter paginates within a single
        # list_objects invocation, but the importer / cold-start
        # reporter / inconsistency-detector each call list_objects
        # independently.
        self.list_pages_by_parent: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in (list_pages_by_parent or {}).items()
        }
        self.get_responses: Dict[str, Dict[str, Any]] = dict(get_responses or {})
        self.media_responses: Dict[str, bytes] = dict(media_responses or {})
        self.calls: List[tuple[str, Dict[str, Any]]] = []
        # Per-(parent, q) cursor: the position of the next page to
        # serve. Reset to 0 whenever a new list_objects walk begins.
        # The Drive adapter signals "new walk" implicitly: a fresh
        # call with no pageToken means "start over". We detect it by
        # the absence of a ``pageToken`` kwarg.
        self._cursor: Dict[str, int] = {}

    def list(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("list", kwargs, self)

    def get(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("get", kwargs, self)

    def get_media(self, *, fileId: str) -> _MediaRequest:  # noqa: N803
        self.calls.append(("get_media", {"fileId": fileId}))
        return _MediaRequest(fileId, self)

    def _dispatch(self, op: str, kwargs: Dict[str, Any]) -> Any:
        self.calls.append((op, kwargs))
        if op == "get":
            file_id = kwargs.get("fileId")
            if file_id not in self.get_responses:
                raise KeyError(
                    f"no canned response for files().get(fileId={file_id!r})"
                )
            return self.get_responses[file_id]
        if op == "list":
            q = kwargs.get("q", "")
            parent_id: Optional[str] = None
            if "'" in q:
                parts = q.split("'")
                if len(parts) >= 2:
                    parent_id = parts[1]
            if parent_id is None or parent_id not in self.list_pages_by_parent:
                return {"files": []}
            pages = self.list_pages_by_parent[parent_id]
            page_token = kwargs.get("pageToken")
            if page_token is None:
                # New walk for this parent: serve page 0.
                self._cursor[parent_id] = 0
            idx = self._cursor.get(parent_id, 0)
            if idx >= len(pages):
                return {"files": []}
            page = pages[idx]
            self._cursor[parent_id] = idx + 1
            return page
        raise AssertionError(f"unexpected op: {op!r}")

    def _fetch_bytes(self, file_id: str) -> bytes:
        if file_id not in self.media_responses:
            raise KeyError(f"no canned media for fileId={file_id!r}")
        return self.media_responses[file_id]


class _FakeDriveService:
    def __init__(self, files: _FilesResource) -> None:
        self._files = files

    def files(self) -> _FilesResource:
        return self._files


# ---------------------------------------------------------------------------
# Helpers: drive list-response builders
# ---------------------------------------------------------------------------


def _drive_file(
    *,
    id: str,
    name: str,
    parent: str,
    mime_type: str = "image/jpeg",
    size: Optional[str] = "1024",
    created_time: str = "2024-01-15T08:30:00Z",
    modified_time: str = "2024-02-01T00:00:00Z",
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": id,
        "name": name,
        "mimeType": mime_type,
        "createdTime": created_time,
        "modifiedTime": modified_time,
        "parents": [parent],
    }
    if size is not None:
        payload["size"] = size
    return payload


# ---------------------------------------------------------------------------
# Stub credential manager so cli.run does not touch real provider chains.
# ---------------------------------------------------------------------------


class _StubCredentialManager:
    """Minimal stand-in for `mcps.credentials.Credential_Manager`."""

    def resolve_aws(self) -> Any:  # pragma: no cover - shape only
        return None

    def resolve_gcp(self) -> Any:  # pragma: no cover - shape only
        return None

    def resolve_drive(self) -> Any:  # pragma: no cover - shape only
        return None


# ---------------------------------------------------------------------------
# Config writer
# ---------------------------------------------------------------------------


def _write_config_yaml(
    *,
    config_path: str,
    catalog_path: str,
    manifest_dir: str,
    lock_path: str,
) -> None:
    """Write a minimal but complete YAML config wiring all three Sources."""
    content = f"""\
sources:
  - name: s3-prod
    kind: s3
    bucket: {_S3_BUCKET}
    region: {_AWS_REGION}
  - name: gcs-archive
    kind: gcs
    bucket: {_GCS_BUCKET}
  - name: drive-camera
    kind: google_drive
    drive_root_folder_id: {_DRIVE_ROOT}

replication:
  pairs:
    - [s3-prod, gcs-archive]
    - [gcs-archive, s3-prod]
  on_key_conflict: skip
  fail_on_conflict: false
  delete_propagation: none
  tombstone_retention_days: 30
  fail_on_inconsistency: false

duplicates:
  canonical_source_priority: [s3-prod, gcs-archive]
  quarantine_retention_days: 30

photos:
  drive_source: drive-camera
  drive_destination: s3-prod

retries:
  max_retries: 1
  initial_backoff_ms: 100
  max_backoff_ms: 1000
  request_timeout_ms: 1000

runtime:
  catalog_path: {catalog_path}
  manifest_dir: {manifest_dir}
  max_concurrent_transfers: 2
  lock_path: {lock_path}
"""
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def _seed_s3_bucket(s3_client: Any) -> Dict[str, bytes]:
    """Create the integration bucket and seed two byte-identical duplicates.

    ``photos/dup-A.jpg`` and ``photos/dup-B.jpg`` carry the same bytes
    (and therefore the same SHA-256), so the Duplicate_Resolver MUST
    quarantine exactly one of them — whichever loses the deterministic
    tie-break (priority → earliest last_seen_at → smallest key).
    Because both rows share the s3-prod priority, the tie-break falls
    through to the lexicographic key comparison: ``photos/dup-A.jpg``
    is canonical and ``photos/dup-B.jpg`` is quarantined.
    """
    s3_client.create_bucket(Bucket=_S3_BUCKET)
    payloads: Dict[str, bytes] = {
        "photos/dup-A.jpg": _DUP_PAYLOAD,
        "photos/dup-B.jpg": _DUP_PAYLOAD,
    }
    sha = _sha256_hex(_DUP_PAYLOAD)
    for key, body in payloads.items():
        s3_client.put_object(
            Bucket=_S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="image/jpeg",
            Metadata={
                "mcps-content-sha256": sha,
                "mcps-source": "s3-prod",
            },
        )
    return payloads


def _build_gcs_client_with_seeded_blobs() -> _FakeGcsClient:
    """Seed one GCS blob whose Content_Hash is absent from S3."""
    blobs = {
        (_GCS_BUCKET, "photos/img-gcs-only.jpg"): _FakeBlob(
            name="photos/img-gcs-only.jpg",
            data=_GCS_ONLY_PAYLOAD,
            updated=dt.datetime(2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={
                "mcps-content-sha256": _sha256_hex(_GCS_ONLY_PAYLOAD),
                "mcps-source": "gcs-archive",
            },
            crc32c="BBBBB2==",
        ),
    }
    return _FakeGcsClient(blobs=blobs)


def _build_drive_files_resource() -> _FilesResource:
    """Seed two Drive image files under the configured root folder.

    Each file has ``mimeType=image/jpeg`` and a parseable
    ``createdTime`` so the Drive_Importer builds a destination key
    of the form ``google-drive/2024/01/<file-id>__<sanitised-name>``
    (req 10.6).
    """
    file_a_id = "drive-file-A"
    file_b_id = "drive-file-B"

    list_pages_by_parent = {
        _DRIVE_ROOT: [
            {
                "files": [
                    _drive_file(
                        id=file_a_id,
                        name="vacation.jpg",
                        parent=_DRIVE_ROOT,
                        size=str(len(_DRIVE_PAYLOAD_A)),
                    ),
                    _drive_file(
                        id=file_b_id,
                        name="sunset.jpg",
                        parent=_DRIVE_ROOT,
                        size=str(len(_DRIVE_PAYLOAD_B)),
                    ),
                ],
            },
        ],
    }
    return _FilesResource(
        list_pages_by_parent=list_pages_by_parent,
        get_responses={_DRIVE_ROOT: {"id": _DRIVE_ROOT}},
        media_responses={
            file_a_id: _DRIVE_PAYLOAD_A,
            file_b_id: _DRIVE_PAYLOAD_B,
        },
    )


# ---------------------------------------------------------------------------
# MediaIoBaseDownload patch (mirrors the dry-run test)
# ---------------------------------------------------------------------------


def _patched_media_io_base_download() -> Any:
    """Return a class compatible with ``MediaIoBaseDownload``."""

    class _FakeMediaIoBaseDownload:
        def __init__(self, buffer: io.BytesIO, request: _MediaRequest) -> None:
            self._buffer = buffer
            self._request = request
            self._delivered = False

        def next_chunk(self) -> tuple[Any, bool]:
            if self._delivered:
                return None, True
            self._buffer.write(self._request.fetch_bytes())
            self._delivered = True
            return None, True

    return _FakeMediaIoBaseDownload


# ---------------------------------------------------------------------------
# The integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_apply_replicates_imports_and_quarantines_with_exit_zero(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end ``--apply --first-pass-confirmed --auto-approve``.

    Asserts on the post-run state of every adapter and on the
    resulting Manifest. Specifically:

    1. The GCS-only Content_Hash is now present in S3 with matching
       bytes (replication writes to absent destinations, req 6.2).
    2. Both Drive files are uploaded to S3 under the documented
       ``google-drive/<YYYY>/<MM>/<file-id>__<sanitised-name>`` key
       shape (req 10.6).
    3. Exactly one of the two S3 duplicates carries the
       ``mcps-quarantined-at`` tag (req 5.7); the canonical pick
       per the deterministic tie-break (priority → earliest
       last_seen_at → smallest key) keeps ``photos/dup-A.jpg`` live
       and quarantines ``photos/dup-B.jpg``.
    4. No last-copy-protection violation: the duplicate's
       Content_Hash still has at least one live (non-quarantined)
       record after the run (req 5.11 / 9.6 / 9.7).
    5. The SUMMARY Manifest record's ``extra`` carries non-zero
       counts (req 14.5) consistent with the run.
    6. Exit code is 0 (req 18.4: Cold_Start ``--apply`` with
       ``--first-pass-confirmed`` proceeds normally and exits OK
       when no errors are recorded).
    """
    # --- Patch the lazy MediaIoBaseDownload import on the Drive adapter.
    import googleapiclient.http  # type: ignore[import-not-found]

    monkeypatch.setattr(
        googleapiclient.http,
        "MediaIoBaseDownload",
        _patched_media_io_base_download(),
    )

    # --- Build the run-scoped temp paths.
    config_path = str(tmp_path / "mcps.config.yaml")
    catalog_path = str(tmp_path / "mcps.catalog.jsonl")
    manifest_dir = str(tmp_path / "manifests")
    lock_path = str(tmp_path / "mcps.lock")
    os.makedirs(manifest_dir, exist_ok=True)

    _write_config_yaml(
        config_path=config_path,
        catalog_path=catalog_path,
        manifest_dir=manifest_dir,
        lock_path=lock_path,
    )

    dup_sha = _sha256_hex(_DUP_PAYLOAD)
    gcs_only_sha = _sha256_hex(_GCS_ONLY_PAYLOAD)
    drive_sha_a = _sha256_hex(_DRIVE_PAYLOAD_A)
    drive_sha_b = _sha256_hex(_DRIVE_PAYLOAD_B)

    with mock_aws():
        s3_client = boto3.client("s3", region_name=_AWS_REGION)
        seeded_s3_payloads = _seed_s3_bucket(s3_client)
        seeded_s3_keys = sorted(seeded_s3_payloads.keys())

        gcs_client = _build_gcs_client_with_seeded_blobs()
        drive_files = _build_drive_files_resource()

        def _adapter_factory(src: SourceConfig) -> SourceAdapter:
            if src.kind == "s3":
                return S3SourceAdapter(
                    name=src.name,
                    bucket=src.bucket,
                    prefix=src.prefix,
                    region=src.region,
                    s3_client=s3_client,
                )
            if src.kind == "gcs":
                return GCSSourceAdapter(
                    name=src.name,
                    bucket=src.bucket,
                    prefix=src.prefix,
                    gcs_client=gcs_client,
                )
            if src.kind == "google_drive":
                return GoogleDriveSourceAdapter(
                    name=src.name,
                    drive_root_folder_id=src.drive_root_folder_id,
                    drive_service=_FakeDriveService(drive_files),
                )
            raise AssertionError(f"unknown kind {src.kind!r}")

        args = argparse.Namespace(
            config=config_path,
            dry_run=False,
            apply=True,
            auto_approve=True,
            first_pass_confirmed=True,
            log_level="ERROR",  # quiet stderr
            run_id=_RUN_ID,
            catalog=None,
            manifest_dir=None,
            lock_path=None,
        )

        exit_code = cli.run(
            args,
            cwd=str(tmp_path),
            adapter_factory=_adapter_factory,
            credential_manager=_StubCredentialManager(),
            now=lambda: _FIXED_NOW,
        )

        # ------------------------------------------------------------
        # Assertion 1: exit code is 0 (req 18.4 / req 14.5).
        # ------------------------------------------------------------
        assert exit_code == int(ExitCode.OK), (
            f"expected exit code OK ({int(ExitCode.OK)}), got {exit_code}; "
            f"stderr={capsys.readouterr().err!r}"
        )

        # ------------------------------------------------------------
        # Assertion 2: replication wrote the GCS-only Content_Hash to
        # S3 (req 6.2). The Replicator copies the canonical record's
        # `key` byte-for-byte (req 6.3), so the new S3 object lands
        # at ``photos/img-gcs-only.jpg``.
        # ------------------------------------------------------------
        gcs_only_key = "photos/img-gcs-only.jpg"
        response = s3_client.get_object(Bucket=_S3_BUCKET, Key=gcs_only_key)
        replicated_bytes = response["Body"].read()
        assert replicated_bytes == _GCS_ONLY_PAYLOAD, (
            "GCS-only Content_Hash was not replicated to S3 with matching "
            f"bytes; observed length={len(replicated_bytes)}"
        )
        head = s3_client.head_object(Bucket=_S3_BUCKET, Key=gcs_only_key)
        head_meta = head.get("Metadata", {}) or {}
        assert head_meta.get("mcps-content-sha256") == gcs_only_sha, (
            "Replicated S3 object missing mcps-content-sha256 metadata"
        )
        # The Replicator stamps mcps-source with the originating
        # Source's name (req 6.4) so subsequent runs do not bounce.
        assert head_meta.get("mcps-source") == "gcs-archive", (
            f"Replicated S3 object's mcps-source = {head_meta.get('mcps-source')!r}; "
            "expected 'gcs-archive'"
        )

        # ------------------------------------------------------------
        # Assertion 3: Drive_Importer wrote both Drive files to S3
        # under the documented key shape (req 10.6, design.md
        # "Drive_Importer").
        # ------------------------------------------------------------
        # createdTime "2024-01-15T08:30:00Z" → year=2024, month=01.
        expected_drive_key_a = (
            "google-drive/2024/01/drive-file-A__vacation.jpg"
        )
        expected_drive_key_b = (
            "google-drive/2024/01/drive-file-B__sunset.jpg"
        )
        for expected_key, payload, expected_hash in (
            (expected_drive_key_a, _DRIVE_PAYLOAD_A, drive_sha_a),
            (expected_drive_key_b, _DRIVE_PAYLOAD_B, drive_sha_b),
        ):
            obj = s3_client.get_object(Bucket=_S3_BUCKET, Key=expected_key)
            assert obj["Body"].read() == payload, (
                f"Drive import bytes mismatch at {expected_key!r}"
            )
            obj_head = s3_client.head_object(
                Bucket=_S3_BUCKET, Key=expected_key
            )
            obj_meta = obj_head.get("Metadata", {}) or {}
            # mcps-source on Drive imports is the Drive Source's name
            # so subsequent runs treat the destination copy as
            # canonical (design.md "Drive_Importer", req 10.6).
            assert obj_meta.get("mcps-source") == "drive-camera", (
                f"Drive-imported S3 object {expected_key!r} has "
                f"mcps-source={obj_meta.get('mcps-source')!r}; "
                "expected 'drive-camera'"
            )
            assert obj_meta.get("mcps-content-sha256") == expected_hash, (
                f"Drive-imported S3 object {expected_key!r} carries the "
                "wrong mcps-content-sha256 metadata"
            )

        # ------------------------------------------------------------
        # Assertion 4: exactly one of the two S3 duplicates is
        # quarantined under the ``mcps-quarantined-at`` tag (req 5.7).
        # The canonical tie-break (priority → earliest last_seen_at →
        # smallest key) keeps the lexicographically smaller key live;
        # both duplicates carry the same priority and last_seen_at,
        # so ``photos/dup-A.jpg`` survives and ``photos/dup-B.jpg``
        # is tagged.
        # ------------------------------------------------------------
        quarantine_status: Dict[str, Optional[str]] = {}
        for dup_key in seeded_s3_keys:
            tagging = s3_client.get_object_tagging(
                Bucket=_S3_BUCKET, Key=dup_key
            )
            tag_map = {
                tag["Key"]: tag["Value"]
                for tag in (tagging.get("TagSet") or [])
            }
            quarantine_status[dup_key] = tag_map.get("mcps-quarantined-at")

        quarantined_keys = [
            k for k, v in quarantine_status.items() if v
        ]
        assert len(quarantined_keys) == 1, (
            "expected exactly one S3 duplicate to be quarantined; "
            f"observed quarantine_status={quarantine_status!r}"
        )
        # Sanity: the quarantined value is a non-empty ISO-8601
        # second-precision timestamp (req 5.7 — value contract).
        quarantined_value = quarantine_status[quarantined_keys[0]]
        assert quarantined_value, "mcps-quarantined-at tag value is empty"
        assert quarantined_value.endswith("Z") and "T" in quarantined_value, (
            f"mcps-quarantined-at value {quarantined_value!r} does not "
            "look like ISO-8601 UTC seconds"
        )
        # The deterministic tie-break must NOT quarantine the
        # canonical record. Both duplicates have priority=s3-prod and
        # the same last_seen_at, so the smallest-key wins; verify
        # ``photos/dup-A.jpg`` remained live.
        assert quarantine_status["photos/dup-A.jpg"] is None, (
            "canonical record photos/dup-A.jpg was unexpectedly tagged "
            "for quarantine"
        )
        assert quarantine_status["photos/dup-B.jpg"] is not None, (
            "non-canonical record photos/dup-B.jpg was not tagged for "
            "quarantine"
        )

        # ------------------------------------------------------------
        # Assertion 5: no last-copy-protection violation. The
        # canonical record (live, non-quarantined) for the duplicate's
        # Content_Hash must remain present in some Source after the
        # run (req 5.11 / 9.6 / 9.7).
        # ------------------------------------------------------------
        # Re-list S3 and check at least one record with the duplicate
        # Content_Hash is NOT tagged with mcps-quarantined-at.
        live_dup_records: List[str] = []
        for dup_key in seeded_s3_keys:
            head_dup = s3_client.head_object(
                Bucket=_S3_BUCKET, Key=dup_key
            )
            head_dup_meta = head_dup.get("Metadata", {}) or {}
            if head_dup_meta.get("mcps-content-sha256") != dup_sha:
                continue
            if quarantine_status[dup_key] is None:
                live_dup_records.append(dup_key)
        assert live_dup_records, (
            "last-copy-protection violation: every record carrying the "
            "duplicate Content_Hash is quarantined"
        )

    # ----------------------------------------------------------------
    # Manifest assertions are made outside the moto context so the
    # only state we touch is the on-disk JSONL file.
    # ----------------------------------------------------------------
    manifest_files = sorted(
        f for f in os.listdir(manifest_dir) if f.endswith(".jsonl")
    )
    assert len(manifest_files) == 1, (
        f"expected exactly one manifest file under {manifest_dir!r}, "
        f"got {manifest_files!r}"
    )
    manifest_path = os.path.join(manifest_dir, manifest_files[0])
    records, errors = parse_manifest_file(manifest_path)
    assert errors == [], f"manifest parse errors: {errors!r}"

    # The manifest must end with exactly one SUMMARY record (req 14.5).
    summary_records = [r for r in records if r.action == Action.SUMMARY]
    assert len(summary_records) == 1, (
        f"expected exactly one SUMMARY record, got {len(summary_records)}"
    )
    summary = summary_records[0]
    assert summary.result == Result.SUCCESS

    # ------------------------------------------------------------
    # Assertion 6: SUMMARY counts (req 14.5) — discovered is the
    # total number of Object_Records observed across Sources, which
    # in this run is 5 (two S3 duplicates + one GCS object + two
    # Drive files). The flag fields confirm the apply path ran.
    # ------------------------------------------------------------
    discovered = int(summary.extra.get("discovered") or 0)
    assert discovered == 5, (
        f"summary.discovered={discovered}; expected 5 (2 S3 dups + 1 GCS "
        "+ 2 Drive)"
    )
    assert summary.extra.get("apply") == "true"
    assert summary.extra.get("dry_run") == "false"
    assert summary.extra.get("cold_start") == "true"
    assert summary.extra.get("first_pass_confirmed") == "true"

    # ------------------------------------------------------------
    # Counts derived from the action stream — these prove the run
    # actually performed the planned work, not that we just got
    # lucky on the post-run S3 state.
    # ------------------------------------------------------------
    quarantine_records = [
        r
        for r in records
        if r.action == Action.QUARANTINE and r.result == Result.QUARANTINED
    ]
    assert len(quarantine_records) == 1, (
        "expected exactly one successful QUARANTINE Manifest record, got "
        f"{len(quarantine_records)} (records={[r.key for r in quarantine_records]!r})"
    )

    replicate_records = [
        r
        for r in records
        if r.action == Action.REPLICATE and r.result == Result.SUCCESS
    ]
    # The Replicator runs over both ordered pairs; the only absent
    # Content_Hash in this scenario is the GCS-only one (S3 has no
    # hashes that GCS lacks because both S3 duplicates carry the same
    # bytes). So we expect exactly one successful REPLICATE entry.
    replicated_hashes = {r.content_hash for r in replicate_records}
    assert gcs_only_sha in replicated_hashes, (
        "expected the GCS-only Content_Hash to appear in a successful "
        f"REPLICATE Manifest entry; got {replicated_hashes!r}"
    )

    drive_import_records = [
        r
        for r in records
        if r.action == Action.DRIVE_IMPORT_OK and r.result == Result.SUCCESS
    ]
    assert len(drive_import_records) == 2, (
        "expected exactly two DRIVE_IMPORT_OK Manifest records, got "
        f"{len(drive_import_records)}"
    )
    drive_import_keys = {r.key for r in drive_import_records}
    assert drive_import_keys == {
        "google-drive/2024/01/drive-file-A__vacation.jpg",
        "google-drive/2024/01/drive-file-B__sunset.jpg",
    }, (
        "DRIVE_IMPORT_OK records used unexpected destination keys: "
        f"{drive_import_keys!r}"
    )

    # ------------------------------------------------------------
    # No LAST_COPY_GUARD entries: the canonical-survives invariant
    # must hold (req 5.11). A LAST_COPY_GUARD record would mean the
    # resolver refused a quarantine because doing it would orphan
    # the Content_Hash; in this seeded scenario both duplicates share
    # the same bytes and the same Source, so quarantining either
    # leaves the other live and no guard fires.
    # ------------------------------------------------------------
    guard_records = [
        r for r in records if r.action == Action.LAST_COPY_GUARD
    ]
    assert guard_records == [], (
        "unexpected LAST_COPY_GUARD entries: "
        f"{[(r.action.value, r.key) for r in guard_records]!r}"
    )

    # ------------------------------------------------------------
    # No error-result entries: a clean apply must record zero errors
    # (the exit-code assertion above also proves this, but we pin
    # the manifest content too).
    # ------------------------------------------------------------
    error_results = [r for r in records if r.result == Result.ERROR]
    assert error_results == [], (
        "apply manifest unexpectedly contains result=ERROR entries: "
        f"{[(r.action.value, r.error) for r in error_results]!r}"
    )

    # All records must carry the same run_id.
    for r in records:
        assert r.run_id == _RUN_ID, (
            f"record carries unexpected run_id {r.run_id!r}; "
            f"expected {_RUN_ID!r}"
        )
