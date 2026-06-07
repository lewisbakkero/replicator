"""`GoogleDriveSourceAdapter`: Google Drive read-only `SourceAdapter`.

This adapter is the production-side implementation of `SourceAdapter` for
Google Drive. Drive is a Pull_Only_Source: every mutating method on the
adapter raises :class:`mcps.errors.ReadOnlySourceError` so the rest of the
codebase (Replicator, Duplicate_Resolver) cannot accidentally write to the
operator's Drive folder (req 10.8). Tests substitute either an in-memory
fake (`mcps.sources.fake.FakeSourceAdapter`) or a hand-rolled
`FakeDriveService` injected through the ``drive_service=...`` constructor
seam (see ``tests/unit/test_drive_adapter_unit.py``).

Boundary of responsibility:

* **Listing / pagination** — handled here. ``list_objects`` calls
  ``files().list(...)`` with ``q="'<folder>' in parents and trashed=false"``,
  follows ``nextPageToken`` until exhaustion (req 10.1, 2.5), and recurses
  into subfolders so the caller sees a flat stream of every reachable
  Object under the configured root.
* **Streaming download** — handled here. ``read_bytes`` uses
  ``MediaIoBaseDownload`` against ``files().get_media(fileId=...)`` and
  yields the buffered bytes in 1 MiB chunks (req 2.4). Drive's downloader
  materialises one HTTP response at a time into a ``BytesIO``; this means
  peak resident memory for a single download is the file size. The
  Drive_Importer pipes one Object at a time, so peak resident memory at
  the run level stays bounded by the largest single Drive file.
* **Construction-time access check** — handled here. The constructor
  issues a ``files().get(fileId=root, fields="id")`` so a misconfigured
  Drive folder fails fast with :class:`mcps.errors.DriveAccessFailed`
  (exit code 75) before any listing work begins (req 10.10).
* **Retry / backoff** — every API call is wrapped with
  `mcps.retry.retry_transient`. ``_map_drive_error`` converts
  ``googleapiclient.errors.HttpError`` and ``socket``/``ssl`` timeouts
  into ``TransientError`` / ``NonTransientError`` at the call boundary;
  the decorator handles the rest (req 2.6).
* **Hashing** — no SHA-256 happens here. The listing path computes
  Content_Hash via `mcps.hashing.compute_content_hash`. Drive's
  ``md5Checksum`` is unreliable for Photos-style content and is therefore
  *never* used as Content_Hash; ``ObjectMeta.provider_hash`` is always
  ``None`` for Drive (req 2.4).

ObjectMeta key shape: this adapter emits ``key = file_id`` (the Drive file
id) so the Catalog's per-Object identifier is stable across name changes.
The relative path under the configured root is preserved in
``user_metadata["drive_path"]`` and the file's ``createdTime`` in
``user_metadata["createdTime"]`` so the Drive_Importer (task 25) can
build the destination key documented in req 10.6.

Validates: Requirements 2.4, 2.5, 2.6, 10.1, 10.8, 10.10.
"""

from __future__ import annotations

import io
import socket
import ssl
from typing import Any, Callable, Iterator, List, Mapping, Optional

from mcps.config.model import RetriesConfig
from mcps.errors import DriveAccessFailed, NonTransientError, ReadOnlySourceError
from mcps.hashing import CHUNK_SIZE
from mcps.retry import TransientError, classify_http, retry_transient
from mcps.sources.base import ObjectMeta, SourceAdapter


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

# Per req 10.1: "up to 5 retries per page request using exponential backoff
# between 1 and 30 seconds". The decorator's defaults match.
_DEFAULT_RETRIES_CONFIG = RetriesConfig(
    max_retries=5,
    initial_backoff_ms=1000,
    max_backoff_ms=30000,
    request_timeout_ms=30000,
)


# Mime type Drive uses for folders. We recurse into folders rather than
# emit them as Objects so the listing stream is a flat sequence of files.
_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

# Field projection applied to ``files().list`` and ``files().get``.
# Restricting fields keeps the response small and predictable; the
# Drive_Importer (task 25) reads ``createdTime`` from
# ``user_metadata["createdTime"]`` so we always request it here.
_FILE_FIELDS = (
    "id, name, size, mimeType, createdTime, modifiedTime, "
    "md5Checksum, parents"
)
_LIST_FIELDS = f"nextPageToken, files({_FILE_FIELDS})"


# ---------------------------------------------------------------------------
# Helpers: error mapping
# ---------------------------------------------------------------------------


def _map_drive_error(exc: BaseException) -> BaseException:
    """Map a ``googleapiclient`` / network error to TransientError /
    NonTransientError.

    Returns the *exception to raise*. The caller then ``raise`` it. We
    return rather than raise so the helper has no side effects on the
    traceback chain when called from inside an except block — the
    caller pairs ``raise mapped from exc`` with the original to
    preserve causation.

    Mapping rules (per design.md):

    * ``googleapiclient.errors.HttpError``: inspect ``exc.resp.status``.
      Transient codes (408/429/500/502/503/504) → ``TransientError``;
      non-transient codes (400/401/403/404) → ``NonTransientError``.
    * ``socket.timeout`` / ``ssl.SSLError`` / ``OSError`` (connection
      timeouts at the transport layer): ``TransientError`` with no
      status.

    Any other exception type is returned unchanged so the caller can
    re-raise it as-is. The retry decorator does not catch arbitrary
    exceptions, so unmapped errors propagate to the operator.
    """
    # Lazy import so test environments that lack google-api-python-client
    # can still import this module under heavy mocking.
    try:
        from googleapiclient.errors import (  # type: ignore[import-not-found]
            HttpError,
        )
    except ImportError:  # pragma: no cover - hard runtime dep
        HttpError = None  # type: ignore[assignment]

    if isinstance(exc, socket.timeout):
        return TransientError(
            status=None,
            retry_after_seconds=None,
            message=f"timeout: {type(exc).__name__}",
        )
    if isinstance(exc, ssl.SSLError):
        # SSLError covers handshake / read timeouts at the TLS layer.
        return TransientError(
            status=None,
            retry_after_seconds=None,
            message=f"ssl-error: {type(exc).__name__}",
        )

    if HttpError is not None and isinstance(exc, HttpError):
        resp = getattr(exc, "resp", None)
        status_raw = getattr(resp, "status", None)
        try:
            status = int(status_raw) if status_raw is not None else None
        except (TypeError, ValueError):
            status = None
        if status is None:
            return NonTransientError(status=0, body=str(exc))
        kind = classify_http(status, expect_404_as_absent=False)
        if kind == "transient":
            return TransientError(
                status=status,
                retry_after_seconds=None,
                message=str(exc),
            )
        # ok / non_transient / absent collapse to a hard failure here.
        return NonTransientError(status=status, body=str(exc))

    # Anything else: surface unchanged. The decorator does not catch
    # arbitrary exceptions, so this propagates as-is to the caller.
    return exc


# ---------------------------------------------------------------------------
# GoogleDriveSourceAdapter
# ---------------------------------------------------------------------------


class GoogleDriveSourceAdapter(SourceAdapter):
    """Read-only `SourceAdapter` backed by a Google Drive folder.

    Constructor arguments:

    * ``name``: logical Source name from the configuration
      (e.g. ``"drive-camera"``).
    * ``drive_root_folder_id``: id of the Drive folder under which the
      adapter recursively lists files. The constructor verifies the
      folder is accessible and raises :class:`DriveAccessFailed` if
      not (req 10.10).
    * ``drive_service``: explicit Drive service object (returned by
      ``googleapiclient.discovery.build('drive', 'v3', ...)``). Test
      seam: bypasses the default service construction so unit tests
      can inject a hand-rolled fake without touching the network.
    * ``retries_config``: retry parameters for the
      `mcps.retry.retry_transient` decorator. Defaults to a
      ``RetriesConfig`` of ``max_retries=5, initial_backoff_ms=1000,
      max_backoff_ms=30_000, request_timeout_ms=30_000`` — the values
      from req 10.1.
    * ``credentials``: optional ``google.auth`` credentials. If
      ``drive_service`` is ``None`` and ``credentials`` is provided,
      the constructor builds a service via
      ``googleapiclient.discovery.build('drive', 'v3',
      credentials=credentials, cache_discovery=False)``.

    If both ``drive_service`` and ``credentials`` are ``None`` the
    constructor raises ``ValueError``: credential resolution is the
    CLI's job (see ``mcps.credentials``); this adapter accepts the
    resolved service or credential.
    """

    kind = "google_drive"
    """Always ``"google_drive"``; participates as a Pull_Only_Source."""

    def __init__(
        self,
        *,
        name: str,
        drive_root_folder_id: str,
        drive_service: Optional[Any] = None,
        retries_config: Optional[RetriesConfig] = None,
        credentials: Optional[Any] = None,
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty str")
        if not isinstance(drive_root_folder_id, str) or not drive_root_folder_id:
            raise ValueError("drive_root_folder_id must be a non-empty str")

        self.name = name
        self.drive_root_folder_id = drive_root_folder_id
        self._retries_config = retries_config or _DEFAULT_RETRIES_CONFIG

        # Resolve the Drive service (test-seam → credentials → hard error).
        if drive_service is not None:
            self._service = drive_service
        elif credentials is not None:
            # Lazy import: keeps this module importable when
            # google-api-python-client is mocked out.
            from googleapiclient.discovery import (  # type: ignore[import-not-found]
                build,
            )

            self._service = build(
                "drive",
                "v3",
                credentials=credentials,
                cache_discovery=False,
            )
        else:
            raise ValueError(
                "GoogleDriveSourceAdapter requires either drive_service or "
                "credentials; credential resolution is the CLI's job"
            )

        # Build the retry decorator once; reuse for every wrapped call.
        self._retry = retry_transient(
            max_retries=self._retries_config.max_retries,
            initial_backoff_ms=self._retries_config.initial_backoff_ms,
            max_backoff_ms=self._retries_config.max_backoff_ms,
            request_timeout_ms=self._retries_config.request_timeout_ms,
        )

        # Construction-time access check. Any exception during this
        # probe — including transient HTTP errors that exhaust retries
        # — is folded into DriveAccessFailed so the operator sees a
        # single, well-typed failure mode at startup (req 10.10).
        try:
            self._call(
                "files.get(root)",
                lambda: self._service.files()
                .get(
                    fileId=drive_root_folder_id,
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute(),
            )
        except Exception as exc:  # noqa: BLE001 - intentional broad catch
            raise DriveAccessFailed(
                folder_id=drive_root_folder_id, cause=exc
            ) from exc

    # ------------------------------------------------------------------
    # Internal: error-translating call wrapper
    # ------------------------------------------------------------------

    def _call(self, op: str, fn: Callable[[], Any]) -> Any:
        """Run ``fn()`` under the retry decorator, mapping provider errors.

        ``op`` is a short label used in the operation-name slot of any
        eventual ``RetriesExhausted`` so logs identify which API call
        ran out of attempts. We build a thin wrapper whose
        ``__qualname__`` advertises ``op`` and decorate it on the spot;
        the decorator validation happens once at adapter construction
        time so this per-call cost is tiny.
        """

        def _wrapped() -> Any:
            try:
                return fn()
            except (TransientError, NonTransientError):
                # Already shaped — let the retry decorator handle.
                raise
            except Exception as exc:  # noqa: BLE001 - intentional broad catch
                mapped = _map_drive_error(exc)
                if mapped is exc:
                    # Unmapped: re-raise untouched. The decorator will
                    # not retry this; it propagates to the caller.
                    raise
                raise mapped from exc

        _wrapped.__qualname__ = f"GoogleDriveSourceAdapter.{op}"
        return self._retry(_wrapped)()

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def list_objects(self) -> Iterator[ObjectMeta]:
        """Recursively stream every reachable file under the configured root.

        Walks the Drive folder tree depth-first starting at
        ``self.drive_root_folder_id``. For each folder we paginate
        ``files().list(q="'<folder>' in parents and trashed=false", ...)``
        with ``pageSize=1000`` until ``nextPageToken`` is exhausted (req
        10.1, 2.5). Files are emitted as ``ObjectMeta`` immediately;
        subfolders are recursed into so the caller sees a flat stream
        of every reachable Object regardless of nesting depth.

        ObjectMeta shape:

        * ``key`` = the Drive file id. Using the file id (rather than a
          path) keeps the Catalog's per-Object identifier stable across
          rename / move operations on Drive.
        * ``size_bytes`` = ``int(file["size"])``. Drive returns sizes as
          decimal strings; we coerce to int. Native Google docs (which
          are skipped by the Drive_Importer's mimeType filter, req
          10.3) report no ``size`` field; we coerce that case to ``0``.
        * ``last_modified`` = ``file["modifiedTime"]`` (already
          ISO-8601 with trailing ``Z``).
        * ``content_type`` = ``file["mimeType"]``.
        * ``user_metadata`` = ``{"drive_file_id": file["id"],
          "drive_path": "<relative-path>", "createdTime":
          "<file.createdTime|empty>"}`` so the Drive_Importer (task 25)
          can build the destination key shape from req 10.6 without an
          extra round trip.
        * ``etag`` = ``None``.
        * ``provider_hash`` = ``None``. Drive's ``md5Checksum`` is
          unreliable for Photos-style content and is therefore never
          used as Content_Hash (req 2.4).
        """
        yield from self._list_folder(self.drive_root_folder_id, path_prefix="")

    def _list_folder(
        self,
        folder_id: str,
        *,
        path_prefix: str,
    ) -> Iterator[ObjectMeta]:
        """Yield every file under ``folder_id`` recursively.

        ``path_prefix`` is the relative path from the configured root to
        ``folder_id`` (no leading or trailing slash). The caller passes
        ``""`` for the configured root; this method appends each file's
        name to construct the value stored in
        ``user_metadata["drive_path"]``.
        """
        page_token: Optional[str] = None
        # Subfolders to recurse into after we have finished paging the
        # current folder. We collect them rather than recursing inline
        # so the generator's pagination state is not interleaved with
        # the recursion's pagination state.
        subfolders: List[tuple[str, str]] = []

        while True:
            kwargs: dict[str, Any] = {
                "q": f"'{folder_id}' in parents and trashed=false",
                "pageSize": 1000,
                "fields": _LIST_FIELDS,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if page_token is not None:
                kwargs["pageToken"] = page_token

            response = self._call(
                "files.list",
                lambda kwargs=kwargs: self._service.files()
                .list(**kwargs)
                .execute(),
            )

            for file in response.get("files", []) or []:
                mime_type = file.get("mimeType") or ""
                name = file.get("name") or ""
                child_path = f"{path_prefix}/{name}" if path_prefix else name

                if mime_type == _FOLDER_MIME_TYPE:
                    # Defer the recursion: collect the subfolder id
                    # plus the child path; we recurse after the
                    # outer pagination loop finishes.
                    subfolders.append((file["id"], child_path))
                    continue

                yield self._file_to_meta(file, drive_path=child_path)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        # Recurse into subfolders, depth-first.
        for child_id, child_path in subfolders:
            yield from self._list_folder(child_id, path_prefix=child_path)

    @staticmethod
    def _file_to_meta(file: Mapping[str, Any], *, drive_path: str) -> ObjectMeta:
        """Build an `ObjectMeta` from a Drive ``files`` resource.

        See `list_objects` for the field-by-field contract. Native
        Google docs (which the Drive_Importer would skip via req 10.3
        anyway) lack a ``size`` field; we coerce that to ``0`` rather
        than raise so a permissive list call still produces a usable
        ``ObjectMeta`` for downstream filtering.
        """
        size_raw = file.get("size")
        try:
            size = int(size_raw) if size_raw is not None else 0
        except (TypeError, ValueError):
            size = 0

        modified_time = file.get("modifiedTime") or ""
        mime_type = file.get("mimeType") or ""
        file_id = str(file.get("id") or "")
        created_time = file.get("createdTime") or ""

        user_metadata: dict[str, str] = {
            "drive_file_id": file_id,
            "drive_path": drive_path,
            "createdTime": created_time,
        }

        return ObjectMeta(
            key=file_id,
            size_bytes=size,
            last_modified=modified_time,
            content_type=mime_type or None,
            user_metadata=user_metadata,
            etag=None,
            provider_hash=None,
        )

    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Stream the file at ``key`` (a Drive file id) in 1 MiB chunks.

        Uses ``MediaIoBaseDownload(io.BytesIO(),
        files().get_media(fileId=key))`` (req 2.4). Drive's downloader
        materialises the response into a ``BytesIO`` one HTTP-response
        at a time; we then yield the buffered bytes back to the caller
        in ``CHUNK_SIZE``-sized slices so the streaming SHA-256 path
        sees the chunked shape it expects.

        Note: this materialises one full download in memory. For large
        files this is suboptimal compared to a true streaming
        download; ``MediaIoBaseDownload`` does not natively support
        streaming-out, so a future improvement could pipe the
        downloader's chunks to a temporary file and re-yield from
        there. The Drive_Importer pipes one Object at a time, so peak
        resident memory at the run level stays bounded by the largest
        single Drive file.
        """
        # Lazy import so the test environment can mock the module.
        from googleapiclient.http import (  # type: ignore[import-not-found]
            MediaIoBaseDownload,
        )

        buffer = io.BytesIO()

        def _download() -> bytes:
            request = self._service.files().get_media(fileId=key)
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _status, done = downloader.next_chunk()
            return buffer.getvalue()

        data = self._call("files.get_media", _download)

        for i in range(0, len(data), CHUNK_SIZE):
            yield data[i : i + CHUNK_SIZE]

    def get_metadata(self, key: str) -> ObjectMeta:
        """Return the ObjectMeta for the Drive file id ``key``.

        Issues ``files().get(fileId=key, fields=...)``. The returned
        ObjectMeta has the same shape as the values yielded by
        `list_objects` except that ``user_metadata["drive_path"]`` is
        the empty string — `get_metadata` does not know which folder
        the caller listed the file under. Callers that need the path
        should consult the listing-time value.
        """
        file = self._call(
            "files.get",
            lambda: self._service.files()
            .get(fileId=key, fields=_FILE_FIELDS, supportsAllDrives=True)
            .execute(),
        )
        return self._file_to_meta(file, drive_path="")

    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Always raises :class:`ReadOnlySourceError` (req 10.8)."""
        raise ReadOnlySourceError(adapter=self.name, op="write_bytes")

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Always raises :class:`ReadOnlySourceError` (req 10.8)."""
        raise ReadOnlySourceError(adapter=self.name, op="set_tag")

    def delete(self, key: str) -> None:
        """Always raises :class:`ReadOnlySourceError` (req 10.8)."""
        raise ReadOnlySourceError(adapter=self.name, op="delete")

    @property
    def supports_writes(self) -> bool:
        """Drive is read-only (req 10.8)."""
        return False


__all__ = [
    "GoogleDriveSourceAdapter",
]
