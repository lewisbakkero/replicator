"""Integration test: `GoogleDriveSourceAdapter` against a canned in-process fake.

Validates: Requirements 10.1, 10.2, 10.3, 10.8, 10.10.

This test stands the adapter up against a hand-rolled
``_FakeDriveService`` that replays the request-builder shape of the
real ``googleapiclient.discovery.build('drive', 'v3', ...)`` object:
``service.files().list(...).execute()``,
``service.files().get(...).execute()``, and
``service.files().get_media(fileId=...)``. Each call records its
kwargs into a per-fake ``calls`` log so the test can interrogate the
exact pagination / retry shape the adapter produced. The fake is
constructed in the same style as the in-process Drive fake used by
``tests/integration/test_full_run_dry.py`` and
``tests/integration/test_full_run_apply.py`` (``_FakeDriveService``
and ``_FilesResource``); we intentionally re-derive it here rather
than importing across test files so each integration test owns its
own fixture surface.

Coverage corresponds 1:1 to task 36's bullet points:

1. **Pagination** — seed three list pages tied together by
   ``nextPageToken`` and assert ``list_objects()`` walks every page,
   issuing one ``files().list`` call per page with the right token
   carried forward (req 10.1, 2.5).
2. **MimeType filtering** — seed the listing with files of mime
   ``image/jpeg``, ``video/mp4``, ``application/vnd.google-apps.document``
   (Google Doc, native), and ``application/octet-stream`` (other).
   The adapter itself only filters out folders; per req 10.2 / 10.3
   the Drive_Importer is the component that drops native docs and
   non-image/video files. The test therefore asserts the adapter
   emits *every* non-folder file with the original ``mimeType``
   preserved on ``ObjectMeta.content_type``, so the importer
   downstream can apply its allowlist (this is the contract the
   importer's unit tests rely on).
3. **Native-doc preservation** — explicit assertion that the Google
   Doc file is present in the adapter's listing (its ``content_type``
   begins with ``application/vnd.google-apps.``) so a downstream
   filter can recognise and skip it. The adapter itself never
   silently drops it (req 10.3 lives at the Drive_Importer layer).
4. **5xx-then-success retries** — the fake raises a real
   ``googleapiclient.errors.HttpError`` with status 503 on the first
   ``files().list`` call, then returns a canned success on the
   retry. Asserts the adapter's ``retry_transient`` decorator
   absorbs the transient error and the listing eventually succeeds
   (req 10.1, 2.6).
5. **Read-only contract** — calls to ``write_bytes``, ``set_tag``,
   and ``delete`` each raise :class:`mcps.errors.ReadOnlySourceError`
   (req 10.8) carrying the adapter's name and the offending op label.
6. **Inaccessible root folder → exit 75** — the fake's ``files().get``
   raises an ``HttpError`` 404 during construction. Asserts
   ``GoogleDriveSourceAdapter(...)`` raises
   :class:`mcps.errors.DriveAccessFailed` whose ``to_exit_code()``
   returns 75 (``ExitCode.DRIVE_ACCESS_FAILED``), which is the contract
   the CLI relies on to report a misconfigured Drive folder distinctly
   from any other startup failure (req 10.10).
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

import pytest

from mcps.config.model import RetriesConfig
from mcps.errors import (
    DriveAccessFailed,
    ExitCode,
    NonTransientError,
    ReadOnlySourceError,
)
from mcps.sources.base import ObjectMeta
from mcps.sources.drive import GoogleDriveSourceAdapter


# ---------------------------------------------------------------------------
# Fixed run-time values so assertions are stable
# ---------------------------------------------------------------------------

_ROOT_ID = "drive-root-folder-id-int"

# Tight retry config so the 5xx-then-success test finishes in a few
# milliseconds rather than the production-default several seconds.
# ``initial_backoff_ms`` is at the documented floor (100ms) and
# ``max_backoff_ms`` is at the floor (1000ms) so even worst-case
# scheduling stays well under a second per attempt.
_FAST_RETRIES = RetriesConfig(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=1000,
    request_timeout_ms=1000,
)


# ---------------------------------------------------------------------------
# HttpError construction helpers
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Stand-in for ``httplib2.Response`` carrying an HTTP status.

    ``googleapiclient.errors.HttpError`` reads ``resp.status`` and
    treats the response as a mapping for ``Content-Type`` lookups; the
    minimal shape below is sufficient for both code paths.
    """

    def __init__(self, status: int) -> None:
        self.status = status
        self.reason = "fake"

    def __getitem__(self, key: str) -> str:
        return ""


def _make_http_error(status: int) -> Exception:
    """Construct a real ``googleapiclient.errors.HttpError`` with ``status``.

    We use the genuine class (rather than a duck-typed stand-in) so the
    adapter's ``isinstance(exc, HttpError)`` check inside
    ``_map_drive_error`` matches and routes the error through the same
    transient/non-transient classifier the production code uses.
    """
    from googleapiclient.errors import (  # type: ignore[import-not-found]
        HttpError,
    )

    resp = _FakeResponse(status)
    return HttpError(resp, b'{"error":{"message":"boom"}}')


# ---------------------------------------------------------------------------
# In-process Drive fake
# ---------------------------------------------------------------------------


class _FileRequest:
    """One captured ``files().get(...)`` / ``files().list(...)`` call.

    The real request-builder pattern goes
    ``service.files().list(...).execute()``: ``list(...)`` returns a
    request object whose ``execute()`` invokes the canned response.
    The fake mirrors that shape so the adapter calls into it
    unchanged.
    """

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


class _FilesResource:
    """Stand-in for ``service.files()`` driving the adapter end-to-end.

    The fake supports two ways of seeding ``files().list`` responses:

    * ``list_responses_by_token``: a dict keyed by ``pageToken`` (or
      ``None`` for the first page) → queue of canned response dicts.
      This is the natural shape for the pagination test where the
      adapter walks ``page-1 → page-2 → page-3`` and the assertions
      target the per-token call shape.
    * ``list_responses``: a flat queue consumed in call order. This
      is convenient for tests that don't care about token routing
      (e.g. the 5xx-then-success retry test).

    ``error_queue`` lets a test seed a sequence of exceptions per op.
    Each call into the dispatcher pops the head of the matching queue
    and, if the popped value is an exception, raises it before any
    canned response is consulted. ``None`` entries are skipped, which
    makes "raise once, then succeed" trivial: ``[HttpError(503)]``
    raises on the first call and the second call falls through to
    the canned success.

    Per-call kwargs are recorded on ``self.calls`` as
    ``(op, kwargs)`` tuples so tests can assert on call order, count,
    and shape (``q``, ``pageToken``, ``pageSize`` etc.).
    """

    def __init__(
        self,
        *,
        list_responses: Optional[List[Dict[str, Any]]] = None,
        list_responses_by_token: Optional[
            Dict[Optional[str], List[Dict[str, Any]]]
        ] = None,
        get_responses: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self.list_responses: List[Dict[str, Any]] = list(list_responses or [])
        self.list_responses_by_token: Dict[
            Optional[str], List[Dict[str, Any]]
        ] = {
            k: list(v) for k, v in (list_responses_by_token or {}).items()
        }
        self.get_responses: Dict[str, Dict[str, Any]] = dict(get_responses or {})

        self.calls: List[tuple[str, Dict[str, Any]]] = []
        self.error_queue: Dict[str, List[Any]] = {}

    # ------------------------------------------------------------------
    # Real request-builder API surface used by the adapter
    # ------------------------------------------------------------------

    def list(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("list", kwargs, self)

    def get(self, **kwargs: Any) -> _FileRequest:
        return _FileRequest("get", kwargs, self)

    # ------------------------------------------------------------------
    # Internal dispatch
    # ------------------------------------------------------------------

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

    def _dispatch(self, op: str, kwargs: Dict[str, Any]) -> Any:
        # Record the call *before* the (possibly raising) error queue
        # is consulted so retry-shape assertions can count attempts
        # that were rejected before the canned response was returned.
        self.calls.append((op, kwargs))
        self._maybe_raise(op)

        if op == "get":
            file_id = kwargs.get("fileId")
            if file_id not in self.get_responses:
                raise KeyError(
                    f"no canned response for files().get(fileId={file_id!r})"
                )
            return self.get_responses[file_id]

        if op == "list":
            token = kwargs.get("pageToken")
            # Token-addressed queue takes priority; falls back to the
            # flat queue when the test seeded only one shape.
            if self.list_responses_by_token:
                pool = self.list_responses_by_token.get(token)
                if pool is None and token is None:
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


class _FakeDriveService:
    """Stand-in for the object returned by
    ``googleapiclient.discovery.build('drive', 'v3', ...)``.

    Every Drive operation goes through ``service.files()``; the fake
    delegates to a single ``_FilesResource`` so tests can both seed
    responses and inspect calls through the same handle.
    """

    def __init__(self, files: _FilesResource) -> None:
        self._files = files

    def files(self) -> _FilesResource:
        return self._files


# ---------------------------------------------------------------------------
# Drive `files` resource builders
# ---------------------------------------------------------------------------


def _file(
    *,
    id: str,
    name: str,
    mime_type: str = "image/jpeg",
    size: Optional[str] = "1024",
    created_time: str = "2024-01-01T00:00:00Z",
    modified_time: str = "2024-02-01T00:00:00Z",
    parent: str = _ROOT_ID,
) -> Dict[str, Any]:
    """Build a minimal ``files`` resource matching the adapter's projection.

    Native Google docs do not report a ``size`` field; pass
    ``size=None`` to mirror that wire shape. The adapter coerces
    missing-size to ``0`` (see ``GoogleDriveSourceAdapter._file_to_meta``)
    so downstream code can always call ``int`` on it.
    """
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


def _make_adapter(
    files: _FilesResource,
    *,
    root: str = _ROOT_ID,
    retries_config: Optional[RetriesConfig] = None,
    seed_root_get: bool = True,
) -> GoogleDriveSourceAdapter:
    """Build an adapter against a `_FilesResource`, seeding the access check.

    The adapter's constructor issues ``files().get(fileId=root,
    fields="id")``; we seed that response unless the caller wants to
    test the failure path explicitly via ``seed_root_get=False``.
    """
    if seed_root_get and root not in files.get_responses:
        files.get_responses[root] = {"id": root}
    return GoogleDriveSourceAdapter(
        name="drive-int",
        drive_root_folder_id=root,
        drive_service=_FakeDriveService(files),
        retries_config=retries_config or _FAST_RETRIES,
    )


# ---------------------------------------------------------------------------
# 1. Pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_objects_walks_every_page_via_next_page_token() -> None:
    """Three pages tied by ``nextPageToken`` produce every Object exactly once.

    The adapter must follow the continuation chain
    ``None → TOKEN-2 → TOKEN-3`` and stop once the final page reports
    no token (req 10.1, 2.5). We assert (a) every seeded file id
    surfaces in the iteration result, (b) exactly three list calls
    were issued, and (c) each call carried the expected ``pageToken``
    kwarg shape (absent on the first call, then ``TOKEN-2`` and
    ``TOKEN-3`` in that order).
    """
    files = _FilesResource(
        list_responses_by_token={
            None: [
                {
                    "files": [
                        _file(id="page1-a", name="a1.jpg"),
                        _file(id="page1-b", name="b1.jpg"),
                    ],
                    "nextPageToken": "TOKEN-2",
                },
            ],
            "TOKEN-2": [
                {
                    "files": [
                        _file(id="page2-a", name="a2.jpg"),
                    ],
                    "nextPageToken": "TOKEN-3",
                },
            ],
            "TOKEN-3": [
                {
                    "files": [
                        _file(id="page3-a", name="a3.jpg"),
                        _file(id="page3-b", name="b3.jpg"),
                    ],
                },
            ],
        },
    )
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    # (a) every seeded file is emitted exactly once.
    assert sorted(m.key for m in metas) == [
        "page1-a",
        "page1-b",
        "page2-a",
        "page3-a",
        "page3-b",
    ]

    # (b) exactly three list calls — one per page.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 3, (
        f"expected 3 paginated list calls, got {len(list_calls)}: "
        f"{list_calls!r}"
    )

    # (c) tokens advanced in order. The first call must NOT pass a
    # token (the adapter signals "new walk" by omitting it); the
    # subsequent calls must carry the tokens returned by the prior
    # response.
    assert "pageToken" not in list_calls[0][1]
    assert list_calls[1][1]["pageToken"] == "TOKEN-2"
    assert list_calls[2][1]["pageToken"] == "TOKEN-3"

    # Sanity: every list call carried the documented query and field
    # projection so the server-side response matches the adapter's
    # parser expectations.
    for op, kwargs in list_calls:
        assert kwargs["q"] == f"'{_ROOT_ID}' in parents and trashed=false"
        assert kwargs["pageSize"] == 1000
        assert kwargs["supportsAllDrives"] is True
        assert kwargs["includeItemsFromAllDrives"] is True


# ---------------------------------------------------------------------------
# 2. MimeType passthrough + 3. Native-doc preservation
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_objects_emits_every_non_folder_file_with_mime_type_preserved() -> None:
    """The adapter is a faithful enumerator: it does not filter by mimeType.

    Per the layered design, the *Drive_Importer* (req 10.2 / 10.3)
    applies the ``image/*`` / ``video/*`` allowlist and skips
    native Google Docs. The *adapter*'s job is to surface every
    non-folder file with its original ``mimeType`` preserved on
    ``ObjectMeta.content_type`` so the importer can apply that
    filter downstream. This test pins that contract: an
    ``image/jpeg``, a ``video/mp4``, a Google Doc
    (``application/vnd.google-apps.document``), and a binary
    blob (``application/octet-stream``) all surface in the listing,
    and the Doc's mimeType is preserved verbatim so the importer
    can recognise and skip it.

    Folders (``application/vnd.google-apps.folder``) are still
    consumed by the adapter itself: it recurses into them rather
    than emitting them as Objects (because Drive does not have
    Object semantics for folders). We seed an empty subfolder to
    exercise that branch and assert the folder id never surfaces
    in the iteration.
    """
    sub_id = "subfolder-empty"
    # Both the root listing and the empty-subfolder recursion call
    # ``files().list`` with no ``pageToken`` (each is a "first page"
    # of its own walk). We seed both responses under the ``None``
    # token bucket in call order: the dispatcher pops the root page
    # first, then the empty-subfolder page on the recursion leg.
    files = _FilesResource(
        list_responses_by_token={
            None: [
                # Root listing: four non-folder files (one image, one
                # video, one Google Doc, one binary blob) plus one
                # subfolder the adapter must recurse into rather than
                # emit as an Object.
                {
                    "files": [
                        _file(
                            id="img-jpeg",
                            name="photo.jpg",
                            mime_type="image/jpeg",
                        ),
                        _file(
                            id="vid-mp4",
                            name="clip.mp4",
                            mime_type="video/mp4",
                            size="2048",
                        ),
                        _file(
                            id="gdoc-native",
                            name="meeting-notes",
                            mime_type=(
                                "application/vnd.google-apps.document"
                            ),
                            size=None,  # native docs report no size
                        ),
                        _file(
                            id="bin-blob",
                            name="archive.bin",
                            mime_type="application/octet-stream",
                            size="512",
                        ),
                        _file(
                            id=sub_id,
                            name="empty-subfolder",
                            mime_type=(
                                "application/vnd.google-apps.folder"
                            ),
                            size=None,
                        ),
                    ],
                },
                # Empty-subfolder recursion: the adapter issues a
                # fresh list with no token; this page yields zero
                # files and zero ``nextPageToken``, terminating the
                # recursion.
                {"files": []},
            ],
        },
    )
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())
    by_key = {m.key: m for m in metas}

    # All four non-folder files surface; the folder does not.
    assert set(by_key) == {"img-jpeg", "vid-mp4", "gdoc-native", "bin-blob"}
    assert sub_id not in by_key

    # Native-doc preservation: the Google Doc's mimeType is preserved
    # verbatim on ``content_type`` so the Drive_Importer can apply
    # the req 10.3 native-doc skip downstream.
    gdoc = by_key["gdoc-native"]
    assert gdoc.content_type == "application/vnd.google-apps.document"
    # Native docs report no ``size`` field; the adapter coerces to 0
    # so downstream code can always call ``int`` on it.
    assert gdoc.size_bytes == 0

    # MimeType passthrough sanity for the rest of the listing.
    assert by_key["img-jpeg"].content_type == "image/jpeg"
    assert by_key["vid-mp4"].content_type == "video/mp4"
    assert by_key["bin-blob"].content_type == "application/octet-stream"

    # ObjectMeta shape sanity: keys are file ids, ``provider_hash`` is
    # always None (req 2.4 forbids using Drive's md5Checksum as
    # Content_Hash), and ``user_metadata`` carries the documented
    # Drive-specific fields.
    for m in metas:
        assert isinstance(m, ObjectMeta)
        assert m.provider_hash is None
        assert m.etag is None
        assert m.user_metadata["drive_file_id"] == m.key
        assert m.user_metadata["createdTime"] == "2024-01-01T00:00:00Z"

    # The adapter recursed into the empty subfolder: two list calls
    # surfaced (root + subfolder) even though the subfolder produced
    # no Objects.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 2, (
        "expected one list call against the root and one against the "
        f"empty subfolder; got {list_calls!r}"
    )
    queries = [c[1]["q"] for c in list_calls]
    assert f"'{_ROOT_ID}' in parents and trashed=false" in queries
    assert f"'{sub_id}' in parents and trashed=false" in queries


# ---------------------------------------------------------------------------
# 4. 5xx-then-success retries
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_objects_retries_through_503_then_succeeds() -> None:
    """A transient 503 on the first ``files().list`` is absorbed by retry.

    The adapter's ``_call`` wrapper maps ``HttpError(status=503)`` to
    ``TransientError`` and the ``retry_transient`` decorator absorbs
    it (req 2.6, 12.1). The second attempt returns the canned page
    and the listing surfaces the seeded file. We assert (a) the
    listing eventually succeeded, (b) exactly two list calls were
    recorded — the failed first attempt plus the successful retry —
    and (c) the surviving file's metadata is intact.
    """
    files = _FilesResource(
        list_responses=[
            {
                "files": [
                    _file(id="after-retry", name="recovered.jpg", size="4096"),
                ],
            },
        ],
    )
    files.error_queue["list"] = [_make_http_error(503)]
    adapter = _make_adapter(files)

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["after-retry"]
    assert metas[0].size_bytes == 4096
    assert metas[0].content_type == "image/jpeg"

    # The fake records the call before raising; one transient + one
    # success → two list calls in total.
    list_calls = [c for c in files.calls if c[0] == "list"]
    assert len(list_calls) == 2, (
        f"expected one transient + one successful list call; got "
        f"{len(list_calls)}: {list_calls!r}"
    )
    # Both calls were made against the same root parent with no
    # pageToken (the retry replays the original kwargs).
    for _op, kwargs in list_calls:
        assert kwargs["q"] == f"'{_ROOT_ID}' in parents and trashed=false"
        assert "pageToken" not in kwargs


# ---------------------------------------------------------------------------
# 5. Read-only contract
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_bytes_set_tag_and_delete_all_raise_read_only_source_error() -> None:
    """Each mutating method raises :class:`ReadOnlySourceError` (req 10.8).

    Drive is a Pull_Only_Source: the adapter must refuse every write-
    side method so the rest of the codebase (Replicator,
    Duplicate_Resolver) cannot accidentally tag or delete content
    in the operator's Drive folder. The exception carries the
    adapter's ``name`` and the offending op label so log records
    pinpoint which surface was invoked.
    """
    files = _FilesResource()
    adapter = _make_adapter(files)

    assert adapter.supports_writes is False

    with pytest.raises(ReadOnlySourceError) as wb_exc:
        adapter.write_bytes(
            "some-file-id",
            iter([b"data"]),
            size_bytes=4,
            content_type="image/jpeg",
            user_metadata={},
        )
    assert wb_exc.value.adapter == "drive-int"
    assert wb_exc.value.op == "write_bytes"

    with pytest.raises(ReadOnlySourceError) as st_exc:
        adapter.set_tag(
            "some-file-id",
            "mcps-quarantined-at",
            "2024-07-01T00:00:00Z",
        )
    assert st_exc.value.adapter == "drive-int"
    assert st_exc.value.op == "set_tag"

    with pytest.raises(ReadOnlySourceError) as del_exc:
        adapter.delete("some-file-id")
    assert del_exc.value.adapter == "drive-int"
    assert del_exc.value.op == "delete"


# ---------------------------------------------------------------------------
# 6. Inaccessible root folder → DriveAccessFailed → exit code 75
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_construction_with_inaccessible_root_raises_drive_access_failed_exit_75() -> None:
    """A 404 on the construction-time access check maps to exit 75.

    Per req 10.10 the adapter must fail fast at startup if the
    configured ``drive_root_folder_id`` is not accessible to the
    service account. The constructor folds *any* exception during the
    ``files().get(fileId=root, fields="id")`` probe — including a
    404 ``HttpError`` — into :class:`mcps.errors.DriveAccessFailed`,
    whose class-level ``exit_code`` is
    :attr:`ExitCode.DRIVE_ACCESS_FAILED` (= 75). The CLI's top-level
    ``except McpsError as exc: return int(exc.to_exit_code())``
    branch then surfaces that as the process exit code.
    """
    files = _FilesResource()
    files.error_queue["get"] = [_make_http_error(404)]

    with pytest.raises(DriveAccessFailed) as exc_info:
        _make_adapter(files, seed_root_get=False)

    err = exc_info.value
    assert err.folder_id == _ROOT_ID
    # The cause carries the mapped non-transient signal so log records
    # know it was a 404 rather than a transient-exhaustion.
    assert isinstance(err.cause, NonTransientError)
    assert err.cause.status == 404
    # The CLI maps DriveAccessFailed → exit code 75 via
    # ``McpsError.to_exit_code()``. This is the contract the
    # ``run_id``-keyed operator-facing exit code documentation in
    # design.md relies on.
    assert err.to_exit_code() == ExitCode.DRIVE_ACCESS_FAILED
    assert int(err.to_exit_code()) == 75
