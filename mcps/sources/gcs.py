"""`GCSSourceAdapter`: Google Cloud Storage implementation of `SourceAdapter`.

This adapter is the production-side implementation of the abstract
`SourceAdapter` for GCS. It mirrors the structure of `S3SourceAdapter`
(`mcps.sources.s3`) so the codebase can route every GCS interaction
through a single seam without importing `google.cloud.storage` directly.
Tests substitute either an in-memory fake (`mcps.sources.fake.FakeSourceAdapter`)
or a hand-rolled `FakeGcsClient` injected through the ``gcs_client=...``
constructor seam.

Boundary of responsibility:

* **Pagination, streaming, metadata, tagging** — all handled here.
* **Retry / backoff** — every call is wrapped with
  `mcps.retry.retry_transient`. Adapter methods convert
  `google.api_core.exceptions.GoogleAPICallError` and timeout exceptions
  into `TransientError` / `NonTransientError` at the call boundary; the
  decorator handles the rest.
* **Hashing** — no SHA-256 happens here. The listing path computes
  Content_Hash via `mcps.hashing.compute_content_hash`. We expose the
  GCS-reported CRC32C (decoded from base64 by the SDK) via
  ``provider_hash`` for diagnostic / catalog purposes only — req 2.3
  forbids using the GCS MD5/CRC32C as Content_Hash identity.
* **mcps-* metadata vs tags** — GCS has no S3-style object tagging;
  every ``mcps-*`` key (including ``mcps-quarantined-at`` and
  ``mcps-tombstoned-at``) is stored in ``blob.metadata``. ``set_tag``
  reads the current metadata, patches the requested key, and writes it
  back via ``blob.patch()`` so existing entries are preserved.

Validates: Requirements 2.3, 2.5, 2.6, 5.7, 6.4, 6.5, 9.3, 9.5.
"""

from __future__ import annotations

import io
from typing import Any, Callable, Iterator, Mapping, Optional

from mcps.config.model import RetriesConfig
from mcps.errors import NonTransientError
from mcps.hashing import CHUNK_SIZE
from mcps.retry import TransientError, classify_http, retry_transient
from mcps.sources.base import ObjectMeta, SourceAdapter


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

_DEFAULT_RETRIES_CONFIG = RetriesConfig(
    max_retries=5,
    initial_backoff_ms=500,
    max_backoff_ms=30000,
    request_timeout_ms=30000,
)


# HTTP status codes the design classifies as transient (Requirement 12.1).
# Mirrors `mcps.retry.TRANSIENT_HTTP` but kept as a local constant so the
# error mapper can short-circuit without an extra import indirection.
_TRANSIENT_STATUSES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# Helpers: error mapping
# ---------------------------------------------------------------------------


def _map_gcs_error(exc: BaseException) -> BaseException:
    """Map a ``google.api_core`` / ``requests`` error to TransientError /
    NonTransientError.

    Returns the *exception to raise*. The caller then ``raise`` it. We
    return rather than raise so the helper has no side effects on the
    traceback chain when called from inside an except block — the
    caller pairs ``raise mapped from exc`` with the original to
    preserve causation.

    Mapping rules (per design.md):

    * ``GoogleAPICallError`` (and subclasses): inspect the ``.code``
      attribute (HTTP status). Transient codes (408/429/500/502/503/504)
      → ``TransientError``; non-transient codes (400/401/403/404) →
      ``NonTransientError``.
    * ``RetryError``: the underlying ``google.api_core`` retry logic
      gave up. Surface as ``TransientError`` so the outer retry
      decorator can decide whether to keep going.
    * ``requests.exceptions.Timeout``: connection-level timeout, no
      response. Surface as ``TransientError`` with no status.

    Any other exception type is returned unchanged so the caller can
    re-raise it as-is. The retry decorator does not catch arbitrary
    exceptions, so unmapped errors propagate to the operator.
    """
    # Lazy imports so test environments that lack google-cloud-storage
    # can still import this module under heavy mocking. The real
    # production paths always have these installed.
    try:
        from google.api_core.exceptions import (  # type: ignore[import-not-found]
            GoogleAPICallError,
            RetryError,
        )
    except ImportError:  # pragma: no cover - google-cloud-storage is a hard runtime dep
        GoogleAPICallError = None  # type: ignore[assignment]
        RetryError = None  # type: ignore[assignment]

    try:
        from requests.exceptions import Timeout as RequestsTimeout  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - requests ships with google-auth
        RequestsTimeout = None  # type: ignore[assignment]

    # Connection-level timeout (no HTTP response).
    if RequestsTimeout is not None and isinstance(exc, RequestsTimeout):
        return TransientError(
            status=None,
            retry_after_seconds=None,
            message=f"timeout: {type(exc).__name__}",
        )

    # google.api_core's own retry-exhaustion signal. Treat as transient
    # so the outer (mcps) retry decorator can decide.
    if RetryError is not None and isinstance(exc, RetryError):
        return TransientError(
            status=None,
            retry_after_seconds=None,
            message=f"retry-error: {exc!s}",
        )

    if GoogleAPICallError is not None and isinstance(exc, GoogleAPICallError):
        # ``code`` is the HTTP status for HTTP-derived subclasses; some
        # subclasses (e.g. transport-level errors) may report ``None``.
        status = getattr(exc, "code", None)
        if not isinstance(status, int):
            # No status — defensively treat as non-transient with the
            # original error body so the operator sees the cause.
            return NonTransientError(status=0, body=str(exc))

        if status in _TRANSIENT_STATUSES:
            return TransientError(
                status=int(status),
                retry_after_seconds=None,
                message=str(exc),
            )

        kind = classify_http(int(status), expect_404_as_absent=False)
        if kind == "transient":
            return TransientError(
                status=int(status),
                retry_after_seconds=None,
                message=str(exc),
            )
        # ok / non_transient / absent all collapse to a hard failure here.
        return NonTransientError(status=int(status), body=str(exc))

    # Anything else: surface unchanged. The decorator does not catch
    # arbitrary exceptions, so this propagates as-is to the caller.
    return exc


# ---------------------------------------------------------------------------
# Helper: stream-to-fileobj wrapper for upload_from_file
# ---------------------------------------------------------------------------


class _ChunkStream(io.RawIOBase):
    """Wrap an iterator of byte chunks as a file-like ``read(n)`` source.

    ``blob.upload_from_file`` reads from its source via ``read(n)``; for
    large payloads we want to avoid materialising the whole Object in
    memory just to upload it, so we expose a non-seekable file-like
    that pulls additional chunks from the iterator on demand.
    ``readable()`` is True; ``seek``/``tell`` are deliberately absent —
    the GCS resumable-upload path tolerates non-seekable sources by
    buffering each chunk internally.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        super().__init__()
        self._iter = iter(chunks)
        self._buffer = b""
        self._exhausted = False

    def readable(self) -> bool:  # noqa: D401 - RawIOBase contract
        return True

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        n = len(b)
        while len(self._buffer) < n and not self._exhausted:
            try:
                self._buffer += next(self._iter)
            except StopIteration:
                self._exhausted = True
                break
        m = min(n, len(self._buffer))
        b[:m] = self._buffer[:m]
        self._buffer = self._buffer[m:]
        return m


# ---------------------------------------------------------------------------
# GCSSourceAdapter
# ---------------------------------------------------------------------------


class GCSSourceAdapter(SourceAdapter):
    """`SourceAdapter` backed by a Google Cloud Storage bucket.

    Constructor arguments:

    * ``name``: logical Source name from the configuration
      (e.g. ``"gcs-archive"``).
    * ``bucket``: GCS bucket name.
    * ``prefix``: optional key prefix; ``list_objects`` paginates with
      ``prefix=prefix`` when supplied.
    * ``gcs_client``: explicit GCS ``Client`` object. Test seam:
      bypasses the default ``google.cloud.storage.Client()`` so unit
      tests can inject a hand-rolled fake without touching the
      network. The unit tests in
      ``tests/unit/test_gcs_adapter_unit.py`` use this seam.
    * ``retries_config``: retry parameters for the
      `mcps.retry.retry_transient` decorator. Defaults to a
      ``RetriesConfig`` of ``max_retries=5, initial_backoff_ms=500,
      max_backoff_ms=30_000, request_timeout_ms=30_000`` — the values
      from the design's RetriesConfig defaults.
    """

    kind = "gcs"

    def __init__(
        self,
        *,
        name: str,
        bucket: str,
        prefix: Optional[str] = None,
        gcs_client: Optional[Any] = None,
        retries_config: Optional[RetriesConfig] = None,
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty str")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("bucket must be a non-empty str")

        self.name = name
        self.bucket = bucket
        self.prefix = prefix
        self._retries_config = retries_config or _DEFAULT_RETRIES_CONFIG

        # Resolve the GCS client (test-seam → default Client()).
        if gcs_client is not None:
            self._client = gcs_client
        else:
            # Lazy import: keeps this module importable in environments
            # that mock-out google-cloud-storage at the test level.
            from google.cloud.storage import (  # type: ignore[import-not-found]
                Client,
            )

            self._client = Client()

        # Build the retry decorator once; reuse for every wrapped call.
        self._retry = retry_transient(
            max_retries=self._retries_config.max_retries,
            initial_backoff_ms=self._retries_config.initial_backoff_ms,
            max_backoff_ms=self._retries_config.max_backoff_ms,
            request_timeout_ms=self._retries_config.request_timeout_ms,
        )

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
                mapped = _map_gcs_error(exc)
                if mapped is exc:
                    # Unmapped: re-raise untouched. The decorator will
                    # not retry this; it propagates to the caller.
                    raise
                raise mapped from exc

        _wrapped.__qualname__ = f"GCSSourceAdapter.{op}"
        return self._retry(_wrapped)()

    # ------------------------------------------------------------------
    # Internal: blob -> ObjectMeta
    # ------------------------------------------------------------------

    @staticmethod
    def _format_updated(updated: Any) -> str:
        """Format a blob ``updated`` (``datetime`` or ``None``) as ISO-8601 UTC.

        Mirrors `S3SourceAdapter`'s ``LastModified`` formatting: emits
        ``YYYY-MM-DDTHH:MM:SS+00:00`` then rewrites the suffix to a
        trailing ``Z`` so the value matches design.md's
        ``ObjectRecord.last_modified`` shape. Returns the empty string
        when ``updated`` is missing.
        """
        if updated is None:
            return ""
        if hasattr(updated, "isoformat"):
            return updated.isoformat().replace("+00:00", "Z")
        return str(updated)

    @classmethod
    def _blob_to_meta(cls, blob: Any) -> ObjectMeta:
        """Build an `ObjectMeta` from a GCS ``Blob``.

        ``provider_hash`` is set to the blob's CRC32C (informational
        only — req 2.3 forbids using it as Content_Hash). ``etag`` is
        always ``None`` because GCS does not use S3-style ETags. The
        blob's ``metadata`` mapping (which carries every ``mcps-*``
        entry on this provider) is copied verbatim into
        ``user_metadata``.
        """
        size_raw = getattr(blob, "size", None)
        size = int(size_raw) if size_raw is not None else 0
        last_modified = cls._format_updated(getattr(blob, "updated", None))
        content_type = getattr(blob, "content_type", None)
        metadata_raw = getattr(blob, "metadata", None) or {}
        merged: dict[str, str] = {
            str(k): "" if v is None else str(v) for k, v in metadata_raw.items()
        }
        provider_hash_raw = getattr(blob, "crc32c", None)
        provider_hash = (
            None if provider_hash_raw in (None, "") else str(provider_hash_raw)
        )
        return ObjectMeta(
            key=str(blob.name),
            size_bytes=size,
            last_modified=last_modified,
            content_type=content_type,
            user_metadata=merged,
            etag=None,
            provider_hash=provider_hash,
        )

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def list_objects(self) -> Iterator[ObjectMeta]:
        """Stream ObjectMetas under ``self.prefix`` from GCS.

        Uses ``client.list_blobs(bucket, prefix=...)`` which returns an
        iterator that follows page tokens internally to exhaustion (req
        2.5). The list call itself is wrapped with retry; iteration
        over the resulting blob iterator is sequential and lazy. Each
        blob is converted to an `ObjectMeta` via ``_blob_to_meta``.

        ``provider_hash`` is set to the blob's CRC32C (informational
        only — req 2.3 forbids it being used as Content_Hash).
        ``etag`` is always ``None`` because GCS does not use S3-style
        ETags.
        """

        def _list() -> Any:
            if self.prefix:
                return self._client.list_blobs(self.bucket, prefix=self.prefix)
            return self._client.list_blobs(self.bucket)

        blob_iter = self._call("list_blobs", _list)
        for blob in blob_iter:
            yield self._blob_to_meta(blob)

    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Stream ``key`` from GCS in 1 MiB chunks via ``blob.open("rb")``.

        ``blob.open("rb")`` performs server-side CRC32C verification as
        the bytes flow through the SDK (req 2.3); a mismatch surfaces
        as a ``google.cloud.storage`` exception that the error mapper
        will classify before raising. Only the ``open`` call is
        retried; if the body stream raises mid-iteration the caller
        sees the underlying exception (a partial read cannot be safely
        re-attempted from where it stopped).
        """
        bucket = self._client.bucket(self.bucket)
        blob = bucket.blob(key)
        stream = self._call("blob_open", lambda: blob.open("rb"))
        try:
            while True:
                chunk = stream.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:  # noqa: BLE001 - best effort
                    pass

    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Write ``chunks`` to ``key`` via ``blob.upload_from_file``.

        We attach ``user_metadata`` (the ``mcps-source`` /
        ``mcps-content-sha256`` / ``mcps-replicated-at`` entries the
        Replicator adds at write time, req 6.4) to ``blob.metadata``
        *before* the upload so a single round-trip writes both the
        payload and the metadata. ``content_type`` is forwarded to
        ``upload_from_file`` so GCS records it on the new Object.

        Chunks are joined into a ``BytesIO`` so the SDK's
        ``upload_from_file`` (which expects a seekable file-like) is
        satisfied for both small and large payloads. For very large
        Objects this materialises the bytes in memory; the design's
        Replicator pipes one Object at a time so peak resident memory
        stays bounded by the largest single Object size.
        """
        # Keep the metadata mapping concrete and string-typed; GCS
        # rejects non-string values just like S3 does.
        metadata = {str(k): str(v) for k, v in dict(user_metadata).items()}

        bucket = self._client.bucket(self.bucket)
        blob = bucket.blob(key)
        # Setting blob.metadata before upload_from_file ensures the
        # metadata is applied to the freshly-created Object in the same
        # write — req 6.4 ("attach metadata entries at write time").
        blob.metadata = metadata

        body = b"".join(chunks)
        file_obj = io.BytesIO(body)

        if content_type is not None:
            self._call(
                "upload_from_file",
                lambda: blob.upload_from_file(
                    file_obj, content_type=content_type
                ),
            )
        else:
            self._call(
                "upload_from_file",
                lambda: blob.upload_from_file(file_obj),
            )

    def get_metadata(self, key: str) -> ObjectMeta:
        """Return the ObjectMeta for ``key`` via ``blob.reload()``.

        ``blob.reload()`` issues a single GET to the metadata endpoint
        and populates ``size`` / ``updated`` / ``content_type`` /
        ``metadata`` / ``crc32c`` on the local ``Blob`` instance. Used
        by the Replicator for post-write verification (req 6.5) and by
        the Duplicate_Resolver to inspect metadata before quarantine /
        delete decisions (req 5.10).
        """
        bucket = self._client.bucket(self.bucket)
        blob = bucket.blob(key)
        self._call("blob_reload", lambda: blob.reload())
        return self._blob_to_meta(blob)

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Attach the metadata entry ``(tag_key, tag_value)`` to ``key``.

        GCS has no S3-style object tagging, so the design stores
        quarantine / tombstone markers in ``blob.metadata`` instead.
        We ``reload()`` the blob first so the local view of metadata
        is up to date, patch the requested key into the dict, then
        ``patch()`` the blob to send the updated metadata back. Other
        existing entries are preserved unchanged.

        This is called for ``mcps-quarantined-at`` (req 5.7) and
        ``mcps-tombstoned-at`` (req 9.3).
        """
        bucket = self._client.bucket(self.bucket)
        blob = bucket.blob(key)
        self._call("blob_reload", lambda: blob.reload())
        existing = dict(blob.metadata or {})
        existing[tag_key] = tag_value
        blob.metadata = existing
        self._call("blob_patch", lambda: blob.patch())

    def delete(self, key: str) -> None:
        """Physically delete ``key`` from the bucket via ``blob.delete()``.

        Used for expired-quarantine deletes (req 5.9), hard tombstone
        propagation (req 9.5), and rollback after a post-write
        verification mismatch (req 6.5).
        """
        bucket = self._client.bucket(self.bucket)
        blob = bucket.blob(key)
        self._call("blob_delete", lambda: blob.delete())


__all__ = [
    "GCSSourceAdapter",
]
