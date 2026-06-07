"""Integration test: the Cold_Start two-step ``--apply`` flow.

Validates: Requirements 18.1, 18.2, 18.3, 18.4.

This test exercises the complete Cold_Start two-step apply contract
end-to-end through `mcps.cli.run`:

* **Phase 1** — Cold_Start ``--apply --auto-approve`` *without*
  ``--first-pass-confirmed``. Per req 18.3, the run must:

  - emit a Reconciliation_Report to stdout AND to
    ``<manifest_dir>/reconciliation-<UTC-timestamp>-<run-id>.txt``
    (req 18.1, 18.2);
  - perform non-destructive replication writes — the Drive_Importer
    upload of the Drive-only file to S3 is permitted because the
    target key is absent (req 18.3 explicit carve-out);
  - perform NO destructive action — zero ``set_tag`` of
    ``mcps-quarantined-at`` and no overwrite of an existing S3 key;
  - exit with code 76 (``FIRST_PASS_REVIEW_REQUIRED``).

* **Phase 2** — fresh Cold_Start ``--apply --first-pass-confirmed
  --auto-approve``. The catalog file written by phase 1 is deleted
  between runs so the second invocation is itself a Cold_Start;
  per req 18.4 destructive actions are now authorised. Phase 2 must:

  - quarantine the non-canonical S3 duplicate (one of
    ``photos/dup-A.jpg`` / ``photos/dup-B.jpg`` carries the
    ``mcps-quarantined-at`` tag);
  - emit a non-empty Manifest including a SUMMARY record;
  - exit with code 0.

The adapter-injection pattern mirrors `test_full_run_apply.py` and
`test_full_run_dry.py`: real `S3SourceAdapter` against a moto-mocked
S3, real `GCSSourceAdapter` against an in-process fake, real
`GoogleDriveSourceAdapter` against an in-process fake `files()`
resource. The unit-tier counterpart is
`tests/unit/test_first_pass_safety.py` (Property 16).
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
_S3_BUCKET = "mcps-int-bucket-cold-start-2step"
_GCS_BUCKET = "mcps-int-bucket-cold-start-2step-gcs"
_DRIVE_ROOT = "drive-root-folder-id-cs2step"

_RUN_ID_PHASE_1 = "coldstart00phase01phase01"
_RUN_ID_PHASE_2 = "coldstart00phase02phase02"
_FIXED_NOW = dt.datetime(2024, 6, 15, 12, 0, 0, tzinfo=dt.timezone.utc)


# Pre-computed payloads. ``_DUP_PAYLOAD`` is shared by ``dup-A`` and
# ``dup-B`` so they are byte-for-byte duplicates. ``_UNIQ_PAYLOAD`` is
# shared by S3's ``photos/uniq.jpg`` AND the Drive-side
# ``uniq-from-drive.jpg`` so the Drive_Importer treats the latter as
# already-present (drive-skip-existing per req 10.5). ``_DRIVE_NEW_PAYLOAD``
# is unique to Drive and must replicate to S3 in phase 1 because the
# replicate-to-absent path is permitted on Cold_Start unconfirmed apply.
_DUP_PAYLOAD = b"DUPLICATE-CONTENT-BYTES" * 8
_UNIQ_PAYLOAD = b"S3-AND-DRIVE-MATCH-PAYLOAD" * 8
_DRIVE_NEW_PAYLOAD = b"DRIVE-ONLY-NEW-VACATION-PAYLOAD" * 8


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# In-process GCS fake (mirrors `test_full_run_apply.py`).
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
    """In-process ``google.cloud.storage.Client`` stand-in.

    The cold-start two-step test seeds GCS with no blobs (the task
    scenario only specifies S3 and Drive seeds), but the GCS source
    is still a configured Replicated_Source so the adapter must be
    real and listable.
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
# In-process Drive fake (mirrors `test_full_run_apply.py`).
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

    The cold-start two-step test runs ``cli.run`` twice over the same
    seeded Drive state, and within a single run the importer issues
    ``read_bytes`` twice per file (once to hash and once to stream-
    write). Page-cursors reset whenever ``pageToken`` is absent so the
    adapter's repeated walks restart cleanly.
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
        self.list_pages_by_parent: Dict[str, List[Dict[str, Any]]] = {
            k: list(v) for k, v in (list_pages_by_parent or {}).items()
        }
        self.get_responses: Dict[str, Dict[str, Any]] = dict(get_responses or {})
        self.media_responses: Dict[str, bytes] = dict(media_responses or {})
        self.calls: List[tuple[str, Dict[str, Any]]] = []
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
    """Seed three S3 objects: a duplicate pair and one unique file.

    Cold_Start preconditions (requirements.md "Initial Assumptions")
    require pre-populated objects to carry NO ``mcps-*`` metadata. We
    honour that here by passing no ``Metadata=`` kwarg on
    ``put_object``; the listing path is therefore forced through the
    streaming SHA-256 fallback (req 7.2 / 18.5).

    Returns the ``{key: payload}`` mapping written so the test can
    re-list and confirm bytes after each phase.
    """
    s3_client.create_bucket(Bucket=_S3_BUCKET)
    payloads: Dict[str, bytes] = {
        "photos/dup-A.jpg": _DUP_PAYLOAD,
        "photos/dup-B.jpg": _DUP_PAYLOAD,
        "photos/uniq.jpg": _UNIQ_PAYLOAD,
    }
    for key, body in payloads.items():
        s3_client.put_object(
            Bucket=_S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="image/jpeg",
        )
    return payloads


def _build_empty_gcs_client() -> _FakeGcsClient:
    """Return a fake GCS client with no blobs.

    The task scenario seeds only S3 and Drive; GCS is an empty
    Replicated_Source. The Replicator has nothing to do for the
    ``s3-prod -> gcs-archive`` direction in phase 1 because the
    ``destructive_writes_allowed=False`` gate forces the
    ``on_key_conflict=skip`` semantics, and the ``gcs-archive ->
    s3-prod`` direction has no source content to copy.
    """
    return _FakeGcsClient(blobs={})


def _build_drive_files_resource() -> _FilesResource:
    """Seed two Drive files under the configured root folder.

    File ``A`` (``vacation.jpg``) carries bytes absent from S3 — the
    Drive_Importer must replicate it to ``s3-prod`` even on Cold_Start
    unconfirmed apply (req 18.3 carve-out for replicate-to-absent).

    File ``B`` (``uniq-from-drive.jpg``) carries bytes byte-for-byte
    identical to S3's ``photos/uniq.jpg``, so the importer's
    existence-check (req 10.5) emits ``DRIVE_SKIP_EXIST`` and the file
    is *not* re-imported.
    """
    file_a_id = "drive-vacation"
    file_b_id = "drive-uniq"

    list_pages_by_parent = {
        _DRIVE_ROOT: [
            {
                "files": [
                    _drive_file(
                        id=file_a_id,
                        name="vacation.jpg",
                        parent=_DRIVE_ROOT,
                        size=str(len(_DRIVE_NEW_PAYLOAD)),
                    ),
                    _drive_file(
                        id=file_b_id,
                        name="uniq-from-drive.jpg",
                        parent=_DRIVE_ROOT,
                        size=str(len(_UNIQ_PAYLOAD)),
                    ),
                ],
            },
        ],
    }
    return _FilesResource(
        list_pages_by_parent=list_pages_by_parent,
        get_responses={_DRIVE_ROOT: {"id": _DRIVE_ROOT}},
        media_responses={
            file_a_id: _DRIVE_NEW_PAYLOAD,
            file_b_id: _UNIQ_PAYLOAD,
        },
    )


# ---------------------------------------------------------------------------
# MediaIoBaseDownload patch (mirrors the dry-run / apply tests)
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


# The literal header text shown to the operator at the top of the
# Reconciliation_Report. Tracks the literal in
# ``mcps.reconciliation.Reconciliation_Reporter._render``; the test
# pins this string so a regression that breaks the operator-facing
# header trips the assertion below.
_REPORT_HEADER = "MultiCloud_Photo_Sync — Cold_Start Reconciliation Report"


@pytest.mark.integration
def test_cold_start_two_step_apply_flow(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end Cold_Start two-step ``--apply`` flow.

    Phase 1 (``--apply --auto-approve`` without ``--first-pass-confirmed``):

    * Asserts exit code 76 (``FIRST_PASS_REVIEW_REQUIRED``, req 18.3).
    * Asserts the ``reconciliation-*.txt`` file was written under
      ``manifest_dir`` and the structured report header is present
      both on stdout (capsys-captured) and on disk (req 18.1, 18.2).
    * Asserts the Drive-only file ``vacation.jpg`` was uploaded to
      S3 (replicate-to-absent permitted, req 18.3 carve-out).
    * Asserts zero ``mcps-quarantined-at`` tags on every S3 object.
    * Asserts no overwrite of any pre-existing S3 key (the duplicate
      pair and the unique file all retain their original bytes).

    Phase 2 (``--apply --first-pass-confirmed --auto-approve`` after
    deleting the catalog so the run is again Cold_Start):

    * Asserts the duplicate is quarantined — exactly one of
      ``photos/dup-A.jpg`` / ``photos/dup-B.jpg`` carries the
      ``mcps-quarantined-at`` tag (req 5.7 / 18.4).
    * Asserts the Manifest is non-empty and contains a SUMMARY record.
    * Asserts exit code 0 (req 18.4).
    """
    # Patch the lazy MediaIoBaseDownload import on the Drive adapter so
    # ``read_bytes`` resolves to our in-memory fake.
    import googleapiclient.http  # type: ignore[import-not-found]

    monkeypatch.setattr(
        googleapiclient.http,
        "MediaIoBaseDownload",
        _patched_media_io_base_download(),
    )

    # Build the run-scoped temp paths.
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

    drive_new_sha = _sha256_hex(_DRIVE_NEW_PAYLOAD)
    dup_sha = _sha256_hex(_DUP_PAYLOAD)
    uniq_sha = _sha256_hex(_UNIQ_PAYLOAD)

    with mock_aws():
        s3_client = boto3.client("s3", region_name=_AWS_REGION)
        seeded_s3_payloads = _seed_s3_bucket(s3_client)
        seeded_s3_keys = sorted(seeded_s3_payloads.keys())

        gcs_client = _build_empty_gcs_client()
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

        # ============================================================
        # PHASE 1: Cold_Start --apply WITHOUT --first-pass-confirmed
        # ============================================================
        phase_1_args = argparse.Namespace(
            config=config_path,
            dry_run=False,
            apply=True,
            auto_approve=True,
            first_pass_confirmed=False,
            log_level="ERROR",  # quiet stderr
            run_id=_RUN_ID_PHASE_1,
            catalog=None,
            manifest_dir=None,
            lock_path=None,
        )

        # Capture pre-run S3 state so we can prove no overwrite of
        # pre-existing keys happened during phase 1.
        before_etags = {
            o["Key"]: o["ETag"]
            for o in s3_client.list_objects_v2(Bucket=_S3_BUCKET).get(
                "Contents", []
            )
        }

        phase_1_exit = cli.run(
            phase_1_args,
            cwd=str(tmp_path),
            adapter_factory=_adapter_factory,
            credential_manager=_StubCredentialManager(),
            now=lambda: _FIXED_NOW,
        )
        phase_1_capture = capsys.readouterr()

        # ------------------------------------------------------------
        # Phase 1, Assertion 1: exit code 76 (req 18.3).
        # ------------------------------------------------------------
        assert phase_1_exit == int(ExitCode.FIRST_PASS_REVIEW_REQUIRED), (
            f"phase 1: expected exit code "
            f"{int(ExitCode.FIRST_PASS_REVIEW_REQUIRED)} "
            f"(FIRST_PASS_REVIEW_REQUIRED), got {phase_1_exit}; "
            f"stderr={phase_1_capture.err!r}"
        )

        # ------------------------------------------------------------
        # Phase 1, Assertion 2: reconciliation-*.txt written under
        # manifest_dir and stdout carries the structured header
        # (req 18.1, 18.2).
        # ------------------------------------------------------------
        recon_files = sorted(
            f for f in os.listdir(manifest_dir)
            if f.startswith("reconciliation-") and f.endswith(".txt")
        )
        assert len(recon_files) == 1, (
            f"phase 1: expected exactly one reconciliation-*.txt under "
            f"{manifest_dir!r}, got {recon_files!r}"
        )
        recon_path = os.path.join(manifest_dir, recon_files[0])
        # Filename must include the run_id so operators can correlate
        # the report with the per-run Manifest (req 18.2).
        assert _RUN_ID_PHASE_1 in recon_files[0], (
            f"phase 1: reconciliation file {recon_files[0]!r} does not "
            f"include the run_id {_RUN_ID_PHASE_1!r}"
        )
        with open(recon_path, "r", encoding="utf-8") as f:
            recon_text = f.read()
        assert _REPORT_HEADER in recon_text, (
            f"phase 1: reconciliation file is missing the structured "
            f"header; got: {recon_text[:200]!r}"
        )
        assert _REPORT_HEADER in phase_1_capture.out, (
            f"phase 1: stdout is missing the structured Reconciliation_"
            f"Report header; stdout starts with {phase_1_capture.out[:200]!r}"
        )

        # ------------------------------------------------------------
        # Phase 1, Assertion 3: the Drive-only ``vacation.jpg`` was
        # uploaded to S3 (replicate-to-absent permitted on Cold_Start
        # unconfirmed apply, req 18.3).
        # ------------------------------------------------------------
        # createdTime "2024-01-15T08:30:00Z" → year=2024, month=01.
        expected_drive_key = (
            "google-drive/2024/01/drive-vacation__vacation.jpg"
        )
        # The S3 head_object call raises if the key is absent; do it
        # explicitly so the AssertionError carries a clear message.
        try:
            head = s3_client.head_object(
                Bucket=_S3_BUCKET, Key=expected_drive_key
            )
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"phase 1: expected Drive_Importer to upload "
                f"{expected_drive_key!r} to S3 on Cold_Start unconfirmed "
                f"apply (req 18.3 carve-out), but head_object raised: {exc!r}"
            )
        head_meta = head.get("Metadata", {}) or {}
        assert head_meta.get("mcps-content-sha256") == drive_new_sha, (
            f"phase 1: Drive-imported S3 object {expected_drive_key!r} "
            f"is missing or has wrong mcps-content-sha256 metadata"
        )
        # The Drive-only payload is now retrievable from S3 with bytes
        # matching the source.
        body = s3_client.get_object(
            Bucket=_S3_BUCKET, Key=expected_drive_key
        )["Body"].read()
        assert body == _DRIVE_NEW_PAYLOAD, (
            "phase 1: Drive-imported S3 object bytes do not match the "
            "Drive-side payload"
        )

        # ------------------------------------------------------------
        # Phase 1, Assertion 4: zero mcps-quarantined-at tags on every
        # S3 object (req 18.3 — no Quarantine set_tag on unconfirmed
        # apply).
        # ------------------------------------------------------------
        post_phase_1_listing = s3_client.list_objects_v2(
            Bucket=_S3_BUCKET
        ).get("Contents", [])
        for entry in post_phase_1_listing:
            key = entry["Key"]
            tagging = s3_client.get_object_tagging(Bucket=_S3_BUCKET, Key=key)
            tag_map = {
                tag["Key"]: tag["Value"]
                for tag in (tagging.get("TagSet") or [])
            }
            assert "mcps-quarantined-at" not in tag_map, (
                f"phase 1: S3 object {key!r} carries the "
                f"``mcps-quarantined-at`` tag — Cold_Start unconfirmed "
                f"apply must not Quarantine (req 18.3); tags={tag_map!r}"
            )

        # ------------------------------------------------------------
        # Phase 1, Assertion 5: no overwrite of pre-existing S3 keys
        # (req 18.3 — no destructive overwrite). ETags on the seeded
        # keys must be byte-identical to the pre-run snapshot.
        # ------------------------------------------------------------
        after_etags = {
            o["Key"]: o["ETag"]
            for o in post_phase_1_listing
        }
        for seeded_key in seeded_s3_keys:
            assert before_etags.get(seeded_key) == after_etags.get(seeded_key), (
                f"phase 1: pre-existing S3 key {seeded_key!r} was "
                f"overwritten during Cold_Start unconfirmed apply "
                f"(before ETag={before_etags.get(seeded_key)!r}, "
                f"after ETag={after_etags.get(seeded_key)!r})"
            )
        # And the bytes round-trip cleanly for every seeded key.
        for key, expected in seeded_s3_payloads.items():
            obj = s3_client.get_object(Bucket=_S3_BUCKET, Key=key)
            assert obj["Body"].read() == expected, (
                f"phase 1: pre-existing S3 key {key!r} has been "
                f"modified — its bytes no longer match the seeded payload"
            )

        # ============================================================
        # PHASE 2: Cold_Start --apply --first-pass-confirmed --auto-approve
        # ============================================================
        # The catalog file written by phase 1 (atomic-replace at end
        # of run) makes phase 2 a non-Cold_Start unless we delete it.
        # The task description calls this out explicitly, and req 18.7
        # documents that ``--first-pass-confirmed`` is a no-op on
        # non-Cold_Start runs. We delete the catalog so the second
        # run is again Cold_Start and the flag therefore authorises
        # destructive actions per req 18.4.
        if os.path.isfile(catalog_path):
            os.unlink(catalog_path)
        # Snapshot: the catalog is gone; phase 2 begins as Cold_Start.
        assert not os.path.isfile(catalog_path), (
            "phase 2 setup: expected the catalog file to be removed "
            "before the second run"
        )

        phase_2_args = argparse.Namespace(
            config=config_path,
            dry_run=False,
            apply=True,
            auto_approve=True,
            first_pass_confirmed=True,
            log_level="ERROR",
            run_id=_RUN_ID_PHASE_2,
            catalog=None,
            manifest_dir=None,
            lock_path=None,
        )

        phase_2_exit = cli.run(
            phase_2_args,
            cwd=str(tmp_path),
            adapter_factory=_adapter_factory,
            credential_manager=_StubCredentialManager(),
            now=lambda: _FIXED_NOW,
        )
        phase_2_capture = capsys.readouterr()

        # ------------------------------------------------------------
        # Phase 2, Assertion 1: exit code 0 (req 18.4 — destructive
        # actions complete normally on confirmed Cold_Start apply).
        # ------------------------------------------------------------
        assert phase_2_exit == int(ExitCode.OK), (
            f"phase 2: expected exit code OK ({int(ExitCode.OK)}), "
            f"got {phase_2_exit}; stderr={phase_2_capture.err!r}"
        )

        # ------------------------------------------------------------
        # Phase 2, Assertion 2: the duplicate is quarantined —
        # exactly one of dup-A / dup-B carries the
        # ``mcps-quarantined-at`` tag (req 5.7, 18.4).
        # ------------------------------------------------------------
        quarantine_status: Dict[str, Optional[str]] = {}
        for dup_key in ("photos/dup-A.jpg", "photos/dup-B.jpg"):
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
            f"phase 2: expected exactly one of the S3 duplicates to "
            f"be quarantined, observed quarantine_status="
            f"{quarantine_status!r}"
        )
        # The quarantined value is a non-empty ISO-8601 second-precision
        # timestamp (req 5.7 value contract).
        quarantined_value = quarantine_status[quarantined_keys[0]]
        assert quarantined_value, (
            "phase 2: ``mcps-quarantined-at`` tag value is empty"
        )
        assert (
            quarantined_value.endswith("Z") and "T" in quarantined_value
        ), (
            f"phase 2: ``mcps-quarantined-at`` value "
            f"{quarantined_value!r} does not look like ISO-8601 UTC seconds"
        )
        # last-copy-protection sanity check: the duplicate Content_Hash
        # still has at least one live (non-quarantined) record.
        live_dup_records = [
            k for k, v in quarantine_status.items() if not v
        ]
        assert live_dup_records, (
            "phase 2: last-copy-protection violation — every record "
            "carrying the duplicate Content_Hash is quarantined"
        )

    # ----------------------------------------------------------------
    # Phase 2, Assertion 3: the Manifest written by phase 2 is
    # non-empty and contains a SUMMARY record (req 14.5 / 18.4).
    # ----------------------------------------------------------------
    # Each phase emits its own manifest file because the run_id is
    # part of the filename (req 14.1). Pick the one matching the
    # phase-2 run_id.
    all_manifest_files = sorted(
        f for f in os.listdir(manifest_dir)
        if f.startswith("manifest-") and f.endswith(".jsonl")
    )
    assert len(all_manifest_files) == 2, (
        f"expected exactly two manifest files (one per phase) under "
        f"{manifest_dir!r}, got {all_manifest_files!r}"
    )
    phase_2_manifest_files = [
        f for f in all_manifest_files if _RUN_ID_PHASE_2 in f
    ]
    assert len(phase_2_manifest_files) == 1, (
        f"expected exactly one phase-2 manifest, got {phase_2_manifest_files!r}"
    )
    phase_2_manifest_path = os.path.join(
        manifest_dir, phase_2_manifest_files[0]
    )
    records, errors = parse_manifest_file(phase_2_manifest_path)
    assert errors == [], (
        f"phase 2: manifest parse errors: {errors!r}"
    )
    assert records, (
        "phase 2: Manifest must be non-empty (req 18.4)"
    )
    summary_records = [r for r in records if r.action == Action.SUMMARY]
    assert len(summary_records) == 1, (
        f"phase 2: expected exactly one SUMMARY record, got "
        f"{len(summary_records)}"
    )
    summary = summary_records[0]
    assert summary.result == Result.SUCCESS, (
        f"phase 2: SUMMARY record has result={summary.result!r}; "
        f"expected SUCCESS"
    )
    assert summary.extra.get("apply") == "true"
    assert summary.extra.get("dry_run") == "false"
    assert summary.extra.get("cold_start") == "true"
    assert summary.extra.get("first_pass_confirmed") == "true"

    # The destructive arm fired — at least one QUARANTINE record with
    # ``Result.QUARANTINED`` is present (req 5.7 / 18.4).
    quarantine_records = [
        r
        for r in records
        if r.action == Action.QUARANTINE
        and r.result == Result.QUARANTINED
    ]
    assert quarantine_records, (
        "phase 2: expected at least one QUARANTINE Manifest record "
        "with result=QUARANTINED; got actions="
        f"{[r.action.value for r in records]!r}"
    )

    # All phase-2 records carry the phase-2 run_id.
    for r in records:
        assert r.run_id == _RUN_ID_PHASE_2, (
            f"phase 2: record carries unexpected run_id {r.run_id!r}; "
            f"expected {_RUN_ID_PHASE_2!r}"
        )

    # ----------------------------------------------------------------
    # Phase 1 manifest cross-check: the run that exited 76 must still
    # have written its manifest (the writer-lock context closes on
    # the way out per req 14.7), and that manifest must NOT contain
    # any QUARANTINE-result entries — the safety gate forbids them
    # (req 18.3). This is the manifest-level mirror of phase 1
    # Assertion 4 above.
    # ----------------------------------------------------------------
    phase_1_manifest_files = [
        f for f in all_manifest_files if _RUN_ID_PHASE_1 in f
    ]
    assert len(phase_1_manifest_files) == 1, (
        f"expected exactly one phase-1 manifest, got "
        f"{phase_1_manifest_files!r}"
    )
    phase_1_manifest_path = os.path.join(
        manifest_dir, phase_1_manifest_files[0]
    )
    phase_1_records, phase_1_errors = parse_manifest_file(
        phase_1_manifest_path
    )
    assert phase_1_errors == [], (
        f"phase 1: manifest parse errors: {phase_1_errors!r}"
    )
    quarantined_in_phase_1 = [
        r
        for r in phase_1_records
        if r.action == Action.QUARANTINE
        and r.result == Result.QUARANTINED
    ]
    assert quarantined_in_phase_1 == [], (
        f"phase 1: Cold_Start unconfirmed apply must not record any "
        f"successful QUARANTINE entries; got "
        f"{[(r.action.value, r.key) for r in quarantined_in_phase_1]!r}"
    )
