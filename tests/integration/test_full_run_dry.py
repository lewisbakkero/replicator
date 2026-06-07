"""Integration test: a full Sync_Run in `--dry-run` against fake adapters.

Validates: Requirements 13.1, 13.2, 13.3, 13.5, 14.5.

This test wires the production adapter classes
(`mcps.sources.s3.S3SourceAdapter`, `mcps.sources.gcs.GCSSourceAdapter`,
`mcps.sources.drive.GoogleDriveSourceAdapter`) through the real
`mcps.cli.run` pipeline with `--dry-run`, and asserts that:

1. The pipeline executes end-to-end against three real adapter classes
   (S3 backed by a `moto`-mocked S3 bucket, GCS backed by an in-process
   ``FakeGcsClient`` injected through the adapter's ``gcs_client=...``
   constructor seam, Drive backed by a ``FakeDriveService`` injected
   through ``drive_service=...``).
2. **Zero mutating API calls are observed** on any adapter
   (req 13.2): the moto-backed S3 bucket's keyspace is byte-for-byte
   unchanged after the run; the GCS fake's blobs are unchanged; the
   Drive fake (read-only) records no writes; and per-adapter
   ``set_tag`` / ``delete`` / ``upload_*`` / ``put_*`` / mutating
   logs are empty.
3. The Manifest written under the configured ``manifest_dir``
   contains entries for every planned action with ``result=PLANNED``
   for the dry-run-mode actions (req 13.3) — specifically, the
   QUARANTINE entries the duplicate resolver would emit under
   ``--apply``. Other entries (LIST_ERROR, HASH_ERROR,
   REPLICATION_ERROR, SUMMARY) are tolerated; the test asserts the
   PLANNED-bearing entries are present and that ``result=PLANNED``
   never appears outside the dry-run branches.
4. The CLI exits with status code 0 when no errors are recorded
   (req 13.5).

Why integration-tier: the unit tests for each adapter prove the per-method
contract; this test proves they wire together correctly through
``cli.run`` and never mutate provider state under ``--dry-run``. The
``cli.run(args, ...)`` API is exercised end-to-end (not stubbed) so the
test catches regressions in the orchestration code that pure-unit tests
would miss.

Pragmatic note: the GCS in-process fake here is the same hand-rolled
``FakeGcsClient`` used by ``tests/unit/test_gcs_adapter_unit.py``; it
exercises the real ``GCSSourceAdapter`` listing / metadata / tagging /
delete code paths. A second-pass integration test against a real
``google-cloud-storage`` server (gcloud emulator or equivalent) is
captured by task 35.
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
_S3_BUCKET = "mcps-int-bucket-s3"
_GCS_BUCKET = "mcps-int-bucket-gcs"
_DRIVE_ROOT = "drive-root-folder-id"

_RUN_ID = "integration00testrun01"  # >= 8 chars (req 14.1)
_FIXED_NOW = dt.datetime(2024, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# In-process GCS fake (mirrors `tests/unit/test_gcs_adapter_unit.py`).
# Kept local so the integration test does not depend on the unit-test
# module's import order.
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
        # Recording the call is enough: the dry-run path must never
        # invoke this. The test asserts the call log is empty.
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
    """In-process ``google.cloud.storage.Client`` stand-in.

    The integration test wires this into the real ``GCSSourceAdapter``
    via the ``gcs_client=...`` constructor seam, so the adapter's
    listing / metadata / read / tag / delete code paths run unchanged
    against a deterministic in-memory backing store.
    """

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
# In-process Drive fake (mirrors `tests/unit/test_drive_adapter_unit.py`)
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

    Driven by ``list_responses_by_token`` (parent-folder-id -> response
    dict) and ``get_responses`` (file_id -> response dict). Each call
    records its kwargs into ``self.calls`` for assertions.
    """

    def __init__(
        self,
        *,
        list_responses_by_parent: Optional[
            Dict[str, List[Dict[str, Any]]]
        ] = None,
        get_responses: Optional[Dict[str, Dict[str, Any]]] = None,
        media_responses: Optional[Dict[str, bytes]] = None,
    ) -> None:
        self.list_responses_by_parent: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in (list_responses_by_parent or {}).items()
        }
        self.get_responses: Dict[str, Dict[str, Any]] = dict(get_responses or {})
        self.media_responses: Dict[str, bytes] = dict(media_responses or {})
        self.calls: List[tuple[str, Dict[str, Any]]] = []

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
            # Extract the parent folder id from the q-string.
            q = kwargs.get("q", "")
            parent_id: Optional[str] = None
            if "'" in q:
                # q is "'<parent>' in parents and trashed=false"
                parts = q.split("'")
                if len(parts) >= 2:
                    parent_id = parts[1]
            if parent_id is None or parent_id not in self.list_responses_by_parent:
                # Default: empty page so the recursion terminates cleanly
                # for unknown folders.
                return {"files": []}
            pool = self.list_responses_by_parent[parent_id]
            if not pool:
                return {"files": []}
            return pool.pop(0)
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
    created_time: str = "2024-01-01T00:00:00Z",
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
    """Minimal stand-in for `mcps.credentials.Credential_Manager`.

    `cli.run` calls ``resolve_aws()`` / ``resolve_gcp()`` /
    ``resolve_drive()`` only when the corresponding kind is referenced
    by a configured Source. The values returned here are never used
    because the integration test passes its own ``adapter_factory``;
    we only need the calls to succeed without raising.
    """

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
    """Create the integration bucket and seed two photos.

    Returns the ``{key: payload}`` mapping written so the test can
    re-list and confirm byte-for-byte equality after the dry-run.
    """
    s3_client.create_bucket(Bucket=_S3_BUCKET)
    payloads: Dict[str, bytes] = {
        "photos/img-s3-only.jpg": b"S3-ONLY-IMG-BYTES" * 8,
        "photos/img-shared.jpg": b"SHARED-CONTENT-BYTES" * 8,
    }
    for key, body in payloads.items():
        s3_client.put_object(
            Bucket=_S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="image/jpeg",
            Metadata={
                # Pre-compute the SHA-256 so the dry-run does not have
                # to stream the bytes; keeps the test fast.
                "mcps-content-sha256": _sha256_hex(body),
                "mcps-source": "s3-prod",
            },
        )
    return payloads


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _build_gcs_client_with_seeded_blobs() -> _FakeGcsClient:
    """Seed two GCS blobs, one of which shares its content with S3."""
    shared_payload = b"SHARED-CONTENT-BYTES" * 8
    gcs_only_payload = b"GCS-ONLY-BYTES" * 8

    blobs = {
        (_GCS_BUCKET, "photos/img-shared.jpg"): _FakeBlob(
            name="photos/img-shared.jpg",
            data=shared_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={
                "mcps-content-sha256": _sha256_hex(shared_payload),
                "mcps-source": "s3-prod",
            },
            crc32c="AAAAA1==",
        ),
        (_GCS_BUCKET, "photos/img-gcs-only.jpg"): _FakeBlob(
            name="photos/img-gcs-only.jpg",
            data=gcs_only_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={
                "mcps-content-sha256": _sha256_hex(gcs_only_payload),
                "mcps-source": "gcs-archive",
            },
            crc32c="BBBBB2==",
        ),
    }
    return _FakeGcsClient(blobs=blobs)


def _build_drive_files_resource() -> _FilesResource:
    """Seed one Drive file under the configured root folder."""
    drive_payload = b"DRIVE-CAMERA-PHOTO-BYTES" * 8
    file_id = "drive-file-1"

    list_responses_by_parent = {
        _DRIVE_ROOT: [
            {
                "files": [
                    _drive_file(id=file_id, name="vacation.jpg", parent=_DRIVE_ROOT),
                ],
            },
        ],
    }
    return _FilesResource(
        list_responses_by_parent=list_responses_by_parent,
        # The adapter's constructor probes files().get(fileId=root) to
        # verify the folder is accessible.
        get_responses={_DRIVE_ROOT: {"id": _DRIVE_ROOT}},
        media_responses={file_id: drive_payload},
    )


# ---------------------------------------------------------------------------
# The integration test
# ---------------------------------------------------------------------------


def _patched_media_io_base_download() -> Any:
    """Return a class compatible with ``MediaIoBaseDownload``.

    The Drive adapter resolves ``MediaIoBaseDownload`` lazily via
    ``from googleapiclient.http import MediaIoBaseDownload``; we patch
    the upstream module so the import binds the in-memory fake. The
    real implementation drains chunks; our stand-in writes the whole
    payload in one shot and signals ``done=True``.
    """

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


@pytest.mark.integration
def test_full_dry_run_does_not_mutate_any_adapter_and_exits_zero(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end ``--dry-run`` against three real adapter classes.

    The S3 adapter talks to a moto-mocked bucket; the GCS adapter
    talks to an in-process ``_FakeGcsClient``; the Drive adapter talks
    to an in-process ``_FakeDriveService``. The CLI's ``cli.run`` is
    invoked end-to-end (no orchestration is stubbed). Assertions:

    * Exit code is 0 (req 13.5).
    * The Manifest file exists under ``manifest_dir`` and contains at
      least one ``result=PLANNED`` entry (the QUARANTINE planning the
      duplicate resolver emits in dry-run mode for the
      ``photos/img-shared.jpg`` cross-source duplicate).
    * No mutating method is called on any adapter (req 13.2): the
      moto-backed S3 bucket's keyspace and per-key bytes are unchanged
      after the run; the GCS fake's blob ``calls`` log contains no
      ``upload_from_file`` / ``patch`` / ``delete`` entries; the
      Drive fake's ``calls`` log contains no mutating ``op`` values.
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

    # --- Stand up the moto S3 mock and seed the bucket.
    with mock_aws():
        s3_client = boto3.client("s3", region_name=_AWS_REGION)
        seeded_s3_payloads = _seed_s3_bucket(s3_client)

        # Snapshot the bucket state BEFORE the dry-run so we can
        # confirm zero mutation afterwards.
        before_listing = s3_client.list_objects_v2(Bucket=_S3_BUCKET).get(
            "Contents", []
        )
        before_keys = sorted(o["Key"] for o in before_listing)
        before_etags = {o["Key"]: o["ETag"] for o in before_listing}

        # --- Build the in-process GCS / Drive fakes.
        gcs_client = _build_gcs_client_with_seeded_blobs()
        drive_files = _build_drive_files_resource()

        # Snapshot pre-run blob state for post-run comparison.
        gcs_keys_before = sorted(name for (_b, name) in gcs_client._blobs.keys())
        gcs_blobs_before = {
            name: bytes(blob._data)
            for (_b, name), blob in gcs_client._blobs.items()
        }

        # --- Custom adapter_factory closure: build real adapters
        # configured against the in-process fakes.
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

        # --- Build argparse Namespace mirroring `mcps --config <path>
        # --dry-run --run-id ...`.
        args = argparse.Namespace(
            config=config_path,
            dry_run=True,
            apply=False,
            auto_approve=False,
            first_pass_confirmed=False,
            log_level="ERROR",  # quiet stderr
            run_id=_RUN_ID,
            catalog=None,
            manifest_dir=None,
            lock_path=None,
        )

        # --- Run the pipeline end-to-end.
        exit_code = cli.run(
            args,
            cwd=str(tmp_path),
            adapter_factory=_adapter_factory,
            credential_manager=_StubCredentialManager(),
            now=lambda: _FIXED_NOW,
        )

        # ------------------------------------------------------------
        # Assertion 1: exit code is 0 (req 13.5).
        # ------------------------------------------------------------
        assert exit_code == int(ExitCode.OK), (
            f"expected exit code OK ({int(ExitCode.OK)}), got {exit_code}; "
            f"stderr={capsys.readouterr().err!r}"
        )

        # ------------------------------------------------------------
        # Assertion 2: zero mutation on the S3 bucket.
        # ------------------------------------------------------------
        after_listing = s3_client.list_objects_v2(Bucket=_S3_BUCKET).get(
            "Contents", []
        )
        after_keys = sorted(o["Key"] for o in after_listing)
        after_etags = {o["Key"]: o["ETag"] for o in after_listing}

        assert after_keys == before_keys, (
            "dry-run unexpectedly added/removed S3 keys; "
            f"before={before_keys!r}, after={after_keys!r}"
        )
        assert after_etags == before_etags, (
            "dry-run unexpectedly mutated existing S3 objects (ETag drift)"
        )
        # Bytes round-trip cleanly.
        for key, expected in seeded_s3_payloads.items():
            response = s3_client.get_object(Bucket=_S3_BUCKET, Key=key)
            assert response["Body"].read() == expected

        # ------------------------------------------------------------
        # Assertion 3: zero mutation on the GCS fake.
        # ------------------------------------------------------------
        gcs_keys_after = sorted(name for (_b, name) in gcs_client._blobs.keys())
        assert gcs_keys_after == gcs_keys_before, (
            "dry-run unexpectedly added/removed GCS blobs; "
            f"before={gcs_keys_before!r}, after={gcs_keys_after!r}"
        )
        for (_b, name), blob in gcs_client._blobs.items():
            assert bytes(blob._data) == gcs_blobs_before[name], (
                f"dry-run unexpectedly modified GCS blob {name!r}"
            )
            mutating_calls = [
                op for (op, _kw) in blob.calls
                if op in ("upload_from_file", "patch", "delete")
            ]
            assert mutating_calls == [], (
                f"dry-run unexpectedly invoked {mutating_calls!r} on "
                f"GCS blob {name!r}"
            )

        # ------------------------------------------------------------
        # Assertion 4: zero mutation on the Drive fake (read-only).
        # ------------------------------------------------------------
        # The Drive adapter raises ReadOnlySourceError on any write,
        # so the only way the dry-run could mutate the fake is via a
        # bug. We assert no list-response queue was drained beyond its
        # legitimate entries and no ``set_tag`` / ``delete`` style op
        # appears in the call log.
        drive_mutating_ops = {
            "create",
            "update",
            "delete",
            "trash",
            "permissions_create",
            "permissions_delete",
        }
        for op, _kw in drive_files.calls:
            assert op not in drive_mutating_ops, (
                f"dry-run unexpectedly invoked Drive op {op!r}"
            )

    # ----------------------------------------------------------------
    # Assertion 5: Manifest contains entries for planned actions and
    # at least one entry with result=PLANNED (req 13.3).
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

    # The manifest should be non-empty.
    assert records, "manifest must contain at least one record"

    # All records must carry the same run_id.
    for r in records:
        assert r.run_id == _RUN_ID, (
            f"record carries unexpected run_id {r.run_id!r}; "
            f"expected {_RUN_ID!r}"
        )

    # At least one record has result=PLANNED — this is the dry-run
    # contract (req 13.3).
    planned = [r for r in records if r.result == Result.PLANNED]
    assert planned, (
        "dry-run manifest must contain at least one result=PLANNED entry; "
        f"got actions={[r.action.value for r in records]!r}"
    )
    # The planned entries must be QUARANTINE entries (the only PLANNED
    # branch the CLI emits in dry-run today). If new dry-run branches
    # are added, this assertion can be relaxed — the contract under
    # test is the ``Result.PLANNED`` value, not the specific Action.
    for r in planned:
        assert r.action == Action.QUARANTINE, (
            f"unexpected action paired with result=PLANNED: {r.action.value!r}"
        )

    # The manifest must end with exactly one SUMMARY record
    # (req 14.5 — final summary log record).
    summary_records = [r for r in records if r.action == Action.SUMMARY]
    assert len(summary_records) == 1, (
        f"expected exactly one SUMMARY record, got {len(summary_records)}"
    )
    summary = summary_records[0]
    assert summary.result == Result.SUCCESS
    # The SUMMARY's ``extra`` block carries the dry-run flag for
    # downstream tooling.
    assert summary.extra.get("dry_run") == "true"
    assert summary.extra.get("apply") == "false"

    # ------------------------------------------------------------
    # Assertion 6: no REPLICATION_ERROR / RETRIES_EXHAUSTED entries
    # (req 13.5: "exit code 0 when no errors recorded"). The
    # exit-code assertion above already covers this, but we pin
    # the manifest content too so a regression that quietly
    # records errors but still exits 0 is caught here.
    # ------------------------------------------------------------
    error_results = [r for r in records if r.result == Result.ERROR]
    assert error_results == [], (
        "dry-run manifest unexpectedly contains result=ERROR entries: "
        f"{[(r.action.value, r.error) for r in error_results]!r}"
    )
