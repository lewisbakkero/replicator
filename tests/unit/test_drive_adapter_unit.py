"""Unit tests for `mcps.sources.drive.GoogleDriveSourceAdapter`.

These tests inject a hand-rolled `FakeDriveService` into the adapter via
the ``drive_service=...`` constructor seam, so the network is never
touched. The fake mirrors the request-builder shape of the real
``google-api-python-client``: ``service.files().list(...).execute()`` and
``service.files().get(...).execute()`` both return canned dicts; an
audit log records every method call so the tests can inspect the call
shape (paginate, recurse, fields=..., supportsAllDrives=True, etc.).

Coverage:

* Construction with a valid root folder succeeds; ``files().get`` is
  invoked with ``fileId=root`` and ``supportsAllDrives=True``.
* Construction with an invalid root folder raises ``DriveAccessFailed``
  with the offending folder id and the underlying cause attached.
* ``list_objects`` with one folder containing two files emits two
  ``ObjectMeta`` values whose ``key`` is the file id, ``content_type``
  is the mimeType, ``provider_hash`` is ``None``, and
  ``user_metadata`` carries ``drive_file_id`` / ``drive_path`` /
  ``createdTime``.
* ``list_objects`` recurses into subfolders and yields nested files
  with ``drive_path`` reflecting the relative path under the root.
* ``list_objects`` paginates: a list response carrying
  ``nextPageToken`` triggers a follow-up ``files().list`` call with
  ``pageToken=...``.
* ``read_bytes`` streams the bytes for a file id via
  ``MediaIoBaseDownload`` against ``files().get_media``.
* ``write_bytes`` / ``set_tag`` / ``delete`` raise
  ``ReadOnlySourceError`` (req 10.8) and ``supports_writes`` is
  ``False``.
* HttpError 503 → TransientError → retried; HttpError 403 →
  NonTransientError, never retried.

Validates: Requirements 2.4, 2.5, 2.6, 10.1, 10.8, 10.10.
"""

from __future__ import annotations

import io
from typing import Any, Dict, List, Mapping, Optional

import pytest

from mcps.config.model import RetriesConfig
from mcps.errors import (
    DriveAccessFailed,
    NonTransientError,
    ReadOnlySourceError,
    RetriesExhausted,
)
from mcps.sources.base import ObjectMeta
from mcps.sources.drive import GoogleDriveSourceAdapter


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Stand-in for ``httplib2.Response`` carrying an HTTP status."""

    def __init__(self, status: int) -> None:
        self.status = status
        self.reason = "fake"

    def __getitem__(self, key: str) -> str:
        return ""


def _make_http_error(status: int) -> Exception:
    """Construct a real ``googleapiclient.errors.HttpError`` with ``status``."""
    from googleapiclient.errors import (  # type: ignore[import-not-found]
        HttpError,
    )

    resp = _FakeResponse(status)
    return HttpError(resp, b'{"error":{"message":"boom"}}')


class _FileRequest:
    """Captures one ``files().get(...)`` / ``files().list(...)`` call.

    The real request-builder pattern goes
    ``service.files().list(...).execute()``: ``list(...)`` returns a
    request object whose ``execute()`` invokes the canned response.
    The fake mirrors that shape so the adapter can call into it
    unchanged.
    """

    def __init__(
        self,
        op: str,
        kwargs: Mapping[str, Any],
        responder: "FilesResource",
    ) -> None:
        self.op = op
        self.kwargs = dict(kwargs)
        self._responder = responder

    def execute(self) -> Any:
        return self._responder._dispatch(self.op, self.kwargs)


class _MediaRequest:
    """Stand-in for ``files().get_media(fileId=...)``.

    Holds the file id so ``MediaIoBaseDownload`` (or our fake of it)
    can resolve the bytes during ``next_chunk``.
    """

    def __init__(self, file_id: str, responder: "FilesResource") -> None:
        self.file_id = file_id
        self._responder = responder

    def fetch_bytes(self) -> bytes:
        return self._responder._fetch_bytes(self.file_id)


class FilesResource:
    """Stand-in for ``service.files()``.

    Driven by canned ``list_responses`` (a queue of dicts) and
    canned ``get_responses`` (a dict from file_id → response dict).
    Each call records its kwargs into ``self.calls`` for assertions.

    Per-op ``error_queue`` lets tests seed an exception sequence:
    each call pops the head; if the popped value is an exception, it
    is raised before the canned response is consulted.
    """

    def __init__(
        self,
        *,
        list_responses: Optional[List[Dict[str, Any]]] = None,
        list_responses_by_token: Optional[
            Dict[Optional[str], List[Dict[str, Any]]]
        ] = None,
        get_responses: Optional[Dict[str, Dict[str, Any]]] = None,
        media_responses: Optional[Dict[str, bytes]] = None,
    ) -> None:
        # ``list_responses`` is a flat queue consumed in call order;
        # ``list_responses_by_token`` lets a test address a specific
        # ``pageToken`` value (or ``None`` for the first page) when
        # multiple folders interleave. Both shapes are supported so
        # individual tests can use whichever is clearer.
        self.list_responses: List[Dict[str, Any]] = list(list_responses or [])
        self.list_responses_by_token: Dict[
            Optional[str], List[Dict[str, Any]]
        ] = {
            k: list(v) for k, v in (list_responses_by_token or {}).items()
        }
        self.get_responses: Dict[str, Dict[str, Any]] = dict(get_responses or {})
        self.media_responses: Dict[str, bytes] = dict(media_responses or {})

        self.calls: List[tuple[str, Dict[str, Any]]] = []
        self.error_queue: Dict[str, List[Any]] = {}

    def _maybe_raise(self, op: str) -> None:
        queue = self.error_queue.get(op)
        if not queue:
            return
        exc = queue.pop(0)
        if exc is None:
            return
        if isinstance(exc, type):
            raise exc("boom")
        raise exc

    # Real client API: ``service.files().list(**kwargs)`` returns a
    # request whose ``.execute()`` does the work. We mirror that
    # shape so the adapter can call us unchanged.
    def list(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("list", kwargs, self)

    def get(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("get", kwargs, self)

    def get_media(self, *, fileId: str) -> _MediaRequest:  # noqa: N803 - Drive API name
        self.calls.append(("get_media", {"fileId": fileId}))
        return _MediaRequest(fileId, self)

    # ------------------------------------------------------------------
    # Internal dispatch
    # ------------------------------------------------------------------

    def _dispatch(self, op: str, kwargs: Dict[str, Any]) -> Any:
        self.calls.append((op, kwargs))
        self._maybe_raise(op)

        if op == "get":
            file_id = kwargs.get("fileId")
            if file_id not in self.get_responses:
                raise KeyError(f"no canned response for files().get(fileId={file_id!r})")
            return self.get_responses[file_id]

        if op == "list":
            token = kwargs.get("pageToken")
            # Token-addressed queue takes priority over the flat queue.
            if self.list_responses_by_token:
                key = token  # may be None for the first page
                pool = self.list_responses_by_token.get(key)
                if pool is None and key is None:
                    pool = self.list_responses_by_token.get(None)
                if not pool:
                    raise KeyError(
                        f"no canned response for files().list(pageToken={token!r})"
                    )
                return pool.pop(0)
            if not self.list_responses:
                raise KeyError("no more canned responses for files().list")
            return self.list_responses.pop(0)

        raise AssertionError(f"unexpected op: {op!r}")

    def _fetch_bytes(self, file_id: str) -> bytes:
        if file_id not in self.media_responses:
            raise KeyError(f"no canned media for fileId={file_id!r}")
        return self.media_responses[file_id]


class FakeDriveService:
    """Stand-in for the object returned by
    ``googleapiclient.discovery.build('drive', 'v3', ...)``.

    Drive's API surface is uniform: every operation goes through
    ``service.files()``. We delegate to a single ``FilesResource``
    instance so tests can both seed responses and inspect calls
    through the same handle.
    """

    def __init__(self, files: FilesResource) -> None:
        self._files = files

    def files(self) -> FilesResource:
        return self._files


# ``MediaIoBaseDownload`` does the multi-chunk download dance against a
# ``HttpRequest``. Our adapter calls it under
# ``self._service.files().get_media(fileId=...)``; tests need to
# substitute it with a class that knows how to drain the
# ``_MediaRequest`` fake. We monkey-patch ``mcps.sources.drive``'s
# imported reference at test time.
class _FakeMediaIoBaseDownload:
    """Trivial in-memory MediaIoBaseDownload stand-in.

    On the first ``next_chunk`` it drains the request's bytes into the
    target buffer in one shot and reports ``done=True``. The adapter
    only relies on the ``done`` signal and never interrogates the
    ``status`` value, so this is sufficient.
    """

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


@pytest.fixture(autouse=True)
def _patch_media_downloader(monkeypatch: pytest.MonkeyPatch) -> None:
    """Substitute the in-memory MediaIoBaseDownload for every test.

    The adapter resolves ``MediaIoBaseDownload`` lazily inside
    ``read_bytes`` via ``from googleapiclient.http import
    MediaIoBaseDownload``; we patch the upstream module so the import
    binds the fake. This keeps the production import path intact
    while letting unit tests run without touching network code.
    """
    import googleapiclient.http  # type: ignore[import-not-found]

    monkeypatch.setattr(
        googleapiclient.http,
        "MediaIoBaseDownload",
        _FakeMediaIoBaseDownload,
    )


# Tight retry config so retry-driven tests finish in a few milliseconds
# rather than several seconds.
_FAST_RETRIES = RetriesConfig(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=1000,
    request_timeout_ms=1000,
)


_ROOT_ID = "root-folder-id"


def _file(
    *,
    id: str,
    name: str,
    mime_type: str = "image/jpeg",
    size: Optional[str] = "1024",
    created_time: str = "2024-01-01T00:00:00Z",
    modified_time: str = "2024-02-01T00:00:00Z",
) -> Dict[str, Any]:
    """Build a minimal ``files`` resource matching the adapter's projection."""
    payload: Dict[str, Any] = {
        "id": id,
        "name": name,
        "mimeType": mime_type,
        "createdTime": created_time,
        "modifiedTime": modified_time,
        "parents": [_ROOT_ID],
    }
    if size is not None:
        payload["size"] = size
    return payload


def _folder(*, id: str, name: str) -> Dict[str, Any]:
    """Build a folder ``files`` resource (mimeType=application/vnd.google-apps.folder)."""
    return _file(
        id=id,
        name=name,
        mime_type="application/vnd.google-apps.folder",
        size=None,
    )


def _make_adapter(
    files: FilesResource,
    *,
    root: str = _ROOT_ID,
    retries_config: Optional[RetriesConfig] = None,
    seed_root_get: bool = True,
) -> GoogleDriveSourceAdapter:
    """Build an adapter against a `FilesResource`, seeding the access check.

    The adapter's constructor issues ``files().get(fileId=root,
    fields="id")`` so we seed that response unless the caller wants to
    test the failure path explicitly.
    """
    if seed_root_get and root not in files.get_responses:
        files.get_responses[root] = {"id": root}
    return GoogleDriveSourceAdapter(
        name="drive-test",
        drive_root_folder_id=root,
        drive_service=FakeDriveService(files),
        retries_config=retries_config or _FAST_RETRIES,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_construction_with_valid_root_succeeds_and_calls_files_get() -> None:
    files = FilesResource(get_responses={_ROOT_ID: {"id": _ROOT_ID}})
    adapter = _make_adapter(files, seed_root_get=False)

    assert adapter.name == "drive-test"
    assert adapter.kind == "google_drive"
    assert adapter.drive_root_folder_id == _ROOT_ID
    assert adapter.supports_writes is False

    # Exactly one files().get(...) call, with the right fields and the
    # ``supportsAllDrives`` flag (so shared drives work).
    get_calls = [c for c in files.calls if c[0] == "get"]
    assert len(get_calls) == 1
    assert get_calls[0][1]["fileId"] == _ROOT_ID
    assert get_calls[0][1]["fields"] == "id"
    assert get_calls[0][1]["supportsAllDrives"] is True


def test_construction_with_invalid_root_raises_drive_access_failed() -> None:
    files = FilesResource()
    files.error_queue["get"] = [_make_http_error(404)]

    with pytest.raises(DriveAccessFailed) as exc_info:
        _make_adapter(files, seed_root_get=False)

    err = exc_info.value
    assert err.folder_id == _ROOT_ID
    # The original 404 cause is preserved on the exception.
    assert err.cause is not None
    assert isinstance(err.cause, NonTransientError)
    assert err.cause.status == 404


def test_construction_requires_drive_service_or_credentials() -> None:
    with pytest.raises(ValueError):
        GoogleDriveSourceAdapter(
            name="drive-test",
            drive_root_folder_id=_ROOT_ID,
        )


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


def test_list_objects_emits_meta_for_two_files_in_root_folder() -> None:
    files = FilesResource(
        list_responses=[
            {
                "files": [
                    _file(id="file-a", name="a.jpg"),
                    _file(id="file-b", name="b.jpg", size="2048"),
                ],
            },
        ],
    )
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    assert len(metas) == 2
    assert all(isinstance(m, ObjectMeta) for m in metas)

    keys = sorted(m.key for m in metas)
    assert keys == ["file-a", "file-b"]

    # Lookup by key for stable assertions.
    by_key = {m.key: m for m in metas}

    a = by_key["file-a"]
    assert a.size_bytes == 1024
    assert a.last_modified == "2024-02-01T00:00:00Z"
    assert a.content_type == "image/jpeg"
    assert a.etag is None
    assert a.provider_hash is None  # md5Checksum is never used (req 2.4)
    assert a.user_metadata["drive_file_id"] == "file-a"
    assert a.user_metadata["drive_path"] == "a.jpg"
    assert a.user_metadata["createdTime"] == "2024-01-01T00:00:00Z"

    b = by_key["file-b"]
    assert b.size_bytes == 2048
    assert b.user_metadata["drive_path"] == "b.jpg"

    # The list call carried the right query and pageSize.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 1
    kwargs = list_calls[0][1]
    assert kwargs["q"] == f"'{_ROOT_ID}' in parents and trashed=false"
    assert kwargs["pageSize"] == 1000
    assert kwargs["supportsAllDrives"] is True
    assert kwargs["includeItemsFromAllDrives"] is True
    # nextPageToken / pageToken handling is exercised in a separate test;
    # the first call must not pass ``pageToken``.
    assert "pageToken" not in kwargs


def test_list_objects_recurses_into_subfolders_and_builds_drive_path() -> None:
    sub_id = "subfolder-id"
    files = FilesResource(
        list_responses_by_token={
            None: [
                # First call (root): one folder + one root-level file.
                {
                    "files": [
                        _file(id="root-file", name="top.jpg"),
                        _folder(id=sub_id, name="vacations"),
                    ],
                },
                # Second call (subfolder): two nested files.
                {
                    "files": [
                        _file(id="nested-1", name="img1.jpg"),
                        _file(id="nested-2", name="img2.jpg"),
                    ],
                },
            ],
        },
    )
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    by_key = {m.key: m for m in metas}
    # Folder is recursed into (not emitted as an Object); three files
    # surface in total.
    assert set(by_key) == {"root-file", "nested-1", "nested-2"}

    assert by_key["root-file"].user_metadata["drive_path"] == "top.jpg"
    assert by_key["nested-1"].user_metadata["drive_path"] == "vacations/img1.jpg"
    assert by_key["nested-2"].user_metadata["drive_path"] == "vacations/img2.jpg"

    # Two list calls: one against the root, one against the subfolder.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 2
    queries = [c[1]["q"] for c in list_calls]
    assert f"'{_ROOT_ID}' in parents and trashed=false" in queries
    assert f"'{sub_id}' in parents and trashed=false" in queries


def test_list_objects_paginates_via_next_page_token() -> None:
    files = FilesResource(
        list_responses_by_token={
            None: [
                {
                    "files": [_file(id="page1-file", name="p1.jpg")],
                    "nextPageToken": "TOKEN-2",
                },
            ],
            "TOKEN-2": [
                {
                    "files": [_file(id="page2-file", name="p2.jpg")],
                },
            ],
        },
    )
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    assert sorted(m.key for m in metas) == ["page1-file", "page2-file"]

    # Two list calls in order: first without pageToken, second with it.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 2
    assert "pageToken" not in list_calls[0][1]
    assert list_calls[1][1]["pageToken"] == "TOKEN-2"


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------


def test_read_bytes_streams_bytes_for_file_id() -> None:
    payload = b"\x89PNG\r\n\x1a\n" + b"x" * 4096
    files = FilesResource(media_responses={"file-a": payload})
    adapter = _make_adapter(files)

    streamed = b"".join(adapter.read_bytes("file-a"))

    assert streamed == payload
    # The adapter went through files().get_media(fileId=...).
    media_calls = [c for c in files.calls if c[0] == "get_media"]
    assert media_calls == [("get_media", {"fileId": "file-a"})]


# ---------------------------------------------------------------------------
# Read-only contract
# ---------------------------------------------------------------------------


def test_write_bytes_raises_read_only_source_error() -> None:
    files = FilesResource()
    adapter = _make_adapter(files)

    with pytest.raises(ReadOnlySourceError) as exc_info:
        adapter.write_bytes(
            "file-a",
            iter([b"data"]),
            size_bytes=4,
            content_type="image/jpeg",
            user_metadata={},
        )

    assert exc_info.value.adapter == "drive-test"
    assert exc_info.value.op == "write_bytes"


def test_set_tag_raises_read_only_source_error() -> None:
    files = FilesResource()
    adapter = _make_adapter(files)

    with pytest.raises(ReadOnlySourceError) as exc_info:
        adapter.set_tag("file-a", "mcps-quarantined-at", "2024-07-01T00:00:00Z")

    assert exc_info.value.adapter == "drive-test"
    assert exc_info.value.op == "set_tag"


def test_delete_raises_read_only_source_error() -> None:
    files = FilesResource()
    adapter = _make_adapter(files)

    with pytest.raises(ReadOnlySourceError) as exc_info:
        adapter.delete("file-a")

    assert exc_info.value.adapter == "drive-test"
    assert exc_info.value.op == "delete"


# ---------------------------------------------------------------------------
# Retry / error mapping
# ---------------------------------------------------------------------------


def test_http_error_503_is_transient_and_eventually_succeeds() -> None:
    files = FilesResource(
        list_responses=[
            {"files": [_file(id="file-a", name="a.jpg")]},
        ],
    )
    files.error_queue["list"] = [_make_http_error(503)]
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["file-a"]
    list_calls = [c for c in files.calls if c[0] == "list"]
    # One retry → two list invocations total.
    assert len(list_calls) == 2


def test_http_error_503_exhausts_after_max_retries() -> None:
    files = FilesResource()
    # Seed enough 503s to exhaust _FAST_RETRIES.max_retries=3 plus the
    # initial attempt.
    files.error_queue["list"] = [_make_http_error(503) for _ in range(10)]
    adapter = _make_adapter(files)

    with pytest.raises(RetriesExhausted):
        list(adapter.list_objects())


def test_http_error_403_is_non_transient_and_not_retried() -> None:
    files = FilesResource()
    files.error_queue["list"] = [_make_http_error(403)]
    adapter = _make_adapter(files)

    with pytest.raises(NonTransientError) as exc_info:
        list(adapter.list_objects())

    assert exc_info.value.status == 403
    list_calls = [c for c in files.calls if c[0] == "list"]
    # Exactly one attempt — no retry on non_transient.
    assert len(list_calls) == 1
