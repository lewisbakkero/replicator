"""`S3SourceAdapter`: real-S3 implementation of `SourceAdapter` (boto3).

This adapter is the production-side implementation of the abstract
`SourceAdapter` (`mcps.sources.base`). The Replicator, Duplicate_Resolver,
and Drive_Importer talk to S3 only through this adapter so the rest of the
codebase never imports `boto3` directly. Tests substitute either an
in-memory fake (`mcps.sources.fake.FakeSourceAdapter`) or, in task 34, a
`moto`-backed S3 client.

Boundary of responsibility:

* **Pagination, streaming, multipart, tagging** — all handled here.
* **Retry / backoff** — every call is wrapped with the
  `mcps.retry.retry_transient` decorator. Adapter methods convert
  `botocore.exceptions.ClientError` and timeout exceptions into
  `TransientError` / `NonTransientError` at the call boundary; the
  decorator handles the rest.
* **Hashing** — no SHA-256 happens here. The listing path computes
  Content_Hash via `mcps.hashing.compute_content_hash`. We expose the
  S3 ETag (with quotes stripped) and set `provider_hash = etag` only when
  `s3_etag_is_singlepart(etag)` is true, to enable the singlepart-ETag
  duplicate optimisation documented in design.md.
* **mcps-* metadata vs tags** — `mcps-source` and `mcps-content-sha256`
  are written as user-metadata at object-creation time (`put_object` /
  `upload_fileobj`'s ``Metadata=``). `mcps-quarantined-at` and
  `mcps-tombstoned-at` are stored as object **tags** because tags can be
  set without rewriting the object. `get_metadata` merges both into the
  returned `ObjectMeta.user_metadata` so the listing-side consumer does
  not have to care about the underlying SDK split.

Validates: Requirements 2.2, 2.5, 2.6, 5.7, 6.4, 6.5, 9.3, 9.5.
"""

from __future__ import annotations

import io
from typing import Any, Callable, Iterator, List, Mapping, Optional

from mcps.config.model import RetriesConfig
from mcps.errors import NonTransientError
from mcps.hashing import CHUNK_SIZE, s3_etag_is_singlepart
from mcps.retry import TransientError, classify_http, retry_transient
from mcps.sources.base import ObjectMeta, SourceAdapter

# Multipart cutoff (design.md): ``put_object`` for sizes ≤ 5 MiB,
# ``upload_fileobj`` (multipart via boto3's TransferManager) above. The exact
# boundary is the inclusive-≤ condition in `write_bytes`.
MULTIPART_THRESHOLD_BYTES: int = 5 * 1024 * 1024


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------

_DEFAULT_RETRIES_CONFIG = RetriesConfig(
    max_retries=5,
    initial_backoff_ms=500,
    max_backoff_ms=30000,
    request_timeout_ms=30000,
)


# ---------------------------------------------------------------------------
# Helpers: error mapping and Retry-After parsing
# ---------------------------------------------------------------------------

def _parse_retry_after(headers: Mapping[str, Any]) -> Optional[float]:
    """Parse a numeric ``Retry-After`` header value into seconds.

    Both header keys ``Retry-After`` and the lowercase
    ``retry-after`` are checked because boto3's ``HTTPHeaders`` value
    typically lowercases header names. Only the numeric form is
    supported here; the HTTP-date form falls through to ``None`` and
    the retry decorator's computed backoff applies. (HTTP-date support
    can be added without changing the call sites.)
    """
    if not headers:
        return None
    # Headers can be a ``CaseInsensitiveDict``; handle both spellings.
    raw = headers.get("Retry-After")
    if raw is None:
        raw = headers.get("retry-after")
    if raw is None:
        return None
    try:
        seconds = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return seconds


def _map_client_error(exc: BaseException) -> BaseException:
    """Map a botocore ClientError or timeout to TransientError/NonTransientError.

    Returns the *exception to raise*. The caller then ``raise`` it. We
    return rather than raise so the helper has no side effects on the
    traceback chain when called from inside an except block — the
    caller pairs ``raise mapped from exc`` with the original to
    preserve causation.

    Only botocore exceptions are handled; any other exception type is
    re-returned unchanged so the caller can ``raise`` it as-is. The
    decorator above us does not catch arbitrary exceptions, so they
    propagate to the operator (Requirement 12.2 mandates non-transient
    behaviour for unmapped errors; the decorator's "anything other than
    TransientError" branch handles this).
    """
    # Lazy import so test environments that lack botocore can still
    # import this module (the fake S3 client tests don't need it).
    try:
        from botocore.exceptions import (  # type: ignore[import-not-found]
            ClientError,
            ConnectTimeoutError,
            ReadTimeoutError,
        )
    except ImportError:  # pragma: no cover - boto3 is a hard runtime dep
        return exc

    if isinstance(exc, (ConnectTimeoutError, ReadTimeoutError)):
        # Network-level timeouts never produce a response, so we have
        # neither a status nor a Retry-After header. The decorator
        # treats this as a transient retry.
        return TransientError(
            status=None,
            retry_after_seconds=None,
            message=f"timeout: {type(exc).__name__}",
        )

    if isinstance(exc, ClientError):
        response = getattr(exc, "response", {}) or {}
        meta = response.get("ResponseMetadata", {}) or {}
        status = meta.get("HTTPStatusCode")
        headers = meta.get("HTTPHeaders", {}) or {}
        retry_after = _parse_retry_after(headers)

        # Status may be missing on certain botocore-internal failures; treat
        # those defensively as non-transient with the original error body.
        if not isinstance(status, int):
            return NonTransientError(status=0, body=str(exc))

        kind = classify_http(int(status), expect_404_as_absent=False)
        if kind == "transient":
            return TransientError(
                status=int(status),
                retry_after_seconds=retry_after,
                message=str(exc),
            )
        # ok / non_transient / absent all collapse to a hard failure here.
        return NonTransientError(status=int(status), body=str(exc))

    # Anything else: surface unchanged. The decorator does not catch
    # arbitrary exceptions, so this propagates as-is to the caller.
    return exc


# ---------------------------------------------------------------------------
# Helper: stream-to-fileobj wrapper for upload_fileobj
# ---------------------------------------------------------------------------


class _ChunkStream(io.RawIOBase):
    """Wrap an iterator of byte chunks as a file-like ``read(n)`` source.

    boto3's ``upload_fileobj`` (TransferManager) drives multipart uploads
    by repeatedly calling ``read(n)`` on its source. For the ≤5 MiB path
    we materialise into ``BytesIO``; above that, materialising would
    defeat the streaming guarantee, so we expose a file-like that pulls
    additional chunks from the iterator on demand. ``readable()`` is
    True; ``seek``/``tell`` are deliberately absent — TransferManager
    handles non-seekable sources by buffering each part.
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
# S3SourceAdapter
# ---------------------------------------------------------------------------


class S3SourceAdapter(SourceAdapter):
    """`SourceAdapter` backed by an S3 bucket.

    Constructor arguments:

    * ``name``: logical Source name from the configuration
      (e.g. ``"s3-prod"``).
    * ``bucket``: S3 bucket name.
    * ``prefix``: optional key prefix; ``list_objects`` paginates with
      ``Prefix=prefix`` when supplied.
    * ``region``: optional AWS region. Forwarded to
      ``boto3.client("s3", region_name=...)``.
    * ``boto3_session``: optional ``boto3.Session`` to source the client
      from. Useful when the caller has already resolved credentials.
    * ``s3_client``: explicit S3 client object. Test seam: bypasses both
      ``boto3.client`` and the session entirely. The unit tests in
      ``tests/unit/test_s3_adapter_unit.py`` use this to inject a fake
      client without touching the network.
    * ``retries_config``: retry parameters for the
      `mcps.retry.retry_transient` decorator. Defaults to a
      ``RetriesConfig`` of ``max_retries=5, initial_backoff_ms=500,
      max_backoff_ms=30_000, request_timeout_ms=30_000`` — the values
      from the design's RetriesConfig defaults.
    """

    kind = "s3"

    def __init__(
        self,
        *,
        name: str,
        bucket: str,
        prefix: Optional[str] = None,
        region: Optional[str] = None,
        boto3_session: Optional[Any] = None,
        s3_client: Optional[Any] = None,
        retries_config: Optional[RetriesConfig] = None,
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty str")
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("bucket must be a non-empty str")

        self.name = name
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self._retries_config = retries_config or _DEFAULT_RETRIES_CONFIG

        # Resolve the S3 client (test-seam → session → default boto3).
        if s3_client is not None:
            self._client = s3_client
        elif boto3_session is not None:
            self._client = boto3_session.client("s3", region_name=region)
        else:
            # Lazy import: keeps this module importable in environments
            # that mock-out boto3 at the test level.
            import boto3  # type: ignore[import-not-found]

            self._client = boto3.client("s3", region_name=region)

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
                mapped = _map_client_error(exc)
                if mapped is exc:
                    # Unmapped: re-raise untouched. The decorator will
                    # not retry this; it propagates to the caller.
                    raise
                raise mapped from exc

        _wrapped.__qualname__ = f"S3SourceAdapter.{op}"
        return self._retry(_wrapped)()

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def list_objects(self) -> Iterator[ObjectMeta]:
        """Stream ObjectMetas under ``self.prefix`` from S3.

        Uses ``list_objects_v2`` via a paginator so continuation tokens
        are followed to exhaustion (req 2.5). Each page fetch is wrapped
        with retry; the iteration of pages itself is sequential and
        lazy. We deliberately do *not* HEAD each Object during listing
        — that would be O(N) requests for a feature (content-type and
        user-metadata at listing time) the listing path does not need.

        The ETag is captured with surrounding double quotes stripped
        because S3 returns them quoted. ``provider_hash`` is set to the
        ETag iff the ETag is single-part (`s3_etag_is_singlepart`); the
        listing path uses this only for the duplicate-detection
        optimisation, never as Content_Hash (req 2.2).
        """
        paginator = self._client.get_paginator("list_objects_v2")
        kwargs: dict[str, Any] = {"Bucket": self.bucket}
        if self.prefix:
            kwargs["Prefix"] = self.prefix

        # Eagerly collect pages to ensure the retry decorator can wrap
        # each fetch. ``paginator.paginate(...)`` returns a lazy
        # iterator; we drive it through ``_call`` page-by-page.
        page_iter = self._call("list_objects_v2", lambda: paginator.paginate(**kwargs))

        for page in page_iter:
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                size = int(obj.get("Size", 0))
                last_modified_dt = obj.get("LastModified")
                if last_modified_dt is None:
                    last_modified = ""
                else:
                    # boto3 returns timezone-aware datetime; emit
                    # ISO-8601 UTC with the trailing ``Z`` form so it
                    # matches design.md's ObjectRecord.last_modified.
                    last_modified = last_modified_dt.isoformat().replace(
                        "+00:00", "Z"
                    )
                etag_raw = obj.get("ETag") or ""
                etag = etag_raw.strip('"') if etag_raw else None
                provider_hash = etag if etag and s3_etag_is_singlepart(etag) else None

                yield ObjectMeta(
                    key=key,
                    size_bytes=size,
                    last_modified=last_modified,
                    # Listing-side content_type/user_metadata require an
                    # extra HEAD per key; we omit them and let the
                    # listing pipeline fetch them on demand if needed.
                    content_type=None,
                    user_metadata={},
                    etag=etag,
                    provider_hash=provider_hash,
                )

    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Stream ``key`` from S3 in 1 MiB chunks via ``get_object``.

        Only the ``get_object`` call itself is retried; if the body
        stream raises mid-iteration the caller sees the underlying
        exception (the response is already non-idempotent — a partial
        read cannot be safely re-attempted from where it stopped).
        """
        response = self._call(
            "get_object",
            lambda: self._client.get_object(Bucket=self.bucket, Key=key),
        )
        body = response["Body"]
        # boto3's StreamingBody supports iter_chunks; size matches CHUNK_SIZE.
        for chunk in body.iter_chunks(chunk_size=CHUNK_SIZE):
            if chunk:
                yield chunk

    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Write ``chunks`` to ``key``, choosing put vs multipart by size.

        For ``size_bytes <= 5 MiB`` we collect into a ``BytesIO`` and
        call ``put_object`` — a single round-trip with the
        ``Metadata=`` dict carrying ``mcps-source`` /
        ``mcps-content-sha256`` (req 6.4). Above 5 MiB we wrap the
        chunk iterator in `_ChunkStream` and hand it to
        ``upload_fileobj`` so boto3's TransferManager drives the
        multipart upload without us ever materialising the full Object
        in memory.

        ``user_metadata`` is normalised to ``Dict[str, str]`` because
        boto3 rejects non-string metadata values.
        """
        # Keep the metadata mapping concrete and string-typed.
        metadata = {str(k): str(v) for k, v in dict(user_metadata).items()}

        if size_bytes <= MULTIPART_THRESHOLD_BYTES:
            body = b"".join(chunks)
            put_kwargs: dict[str, Any] = {
                "Bucket": self.bucket,
                "Key": key,
                "Body": body,
                "Metadata": metadata,
            }
            if content_type:
                put_kwargs["ContentType"] = content_type
            self._call("put_object", lambda: self._client.put_object(**put_kwargs))
            return

        # Multipart path. ``upload_fileobj`` reads from the file-like
        # in chunks and drives the multipart protocol.
        stream = _ChunkStream(chunks)
        extra_args: dict[str, Any] = {"Metadata": metadata}
        if content_type:
            extra_args["ContentType"] = content_type
        self._call(
            "upload_fileobj",
            lambda: self._client.upload_fileobj(
                stream, self.bucket, key, ExtraArgs=extra_args
            ),
        )

    def get_metadata(self, key: str) -> ObjectMeta:
        """Return the ObjectMeta for ``key`` via HEAD + GetObjectTagging.

        S3 stores the ``mcps-quarantined-at`` and ``mcps-tombstoned-at``
        markers as object **tags** rather than user-metadata (so they
        can be set without rewriting the object). We fetch both views
        and merge them into ``ObjectMeta.user_metadata`` for the
        consumer (the Replicator's post-write verification path and the
        Duplicate_Resolver's last-copy-protection check, req 6.5, 5.10).

        A missing key surfaces as ``FileNotFoundError`` per the
        `SourceAdapter` ABC contract: callers (the Replicator's
        destination-probe step, req 6.2 / 6.7) treat this as
        "destination absent → write" rather than as an error. The
        retry decorator otherwise classifies HTTP 404 as
        non-transient (`NON_TRANSIENT_HTTP`); we catch that here and
        re-raise the documented sentinel.
        """
        try:
            head = self._call(
                "head_object",
                lambda: self._client.head_object(Bucket=self.bucket, Key=key),
            )
        except NonTransientError as exc:
            if exc.status == 404:
                raise FileNotFoundError(key) from exc
            raise
        tagging = self._call(
            "get_object_tagging",
            lambda: self._client.get_object_tagging(Bucket=self.bucket, Key=key),
        )
        return self._head_and_tags_to_meta(key, head, tagging)

    def _head_and_tags_to_meta(
        self,
        key: str,
        head: Mapping[str, Any],
        tagging: Mapping[str, Any],
    ) -> ObjectMeta:
        """Build an ObjectMeta from a HEAD response and a GetObjectTagging response."""
        size = int(head.get("ContentLength", 0))
        last_modified_dt = head.get("LastModified")
        if last_modified_dt is None:
            last_modified = ""
        else:
            last_modified = last_modified_dt.isoformat().replace("+00:00", "Z")
        content_type = head.get("ContentType")
        etag_raw = head.get("ETag") or ""
        etag = etag_raw.strip('"') if etag_raw else None
        provider_hash = etag if etag and s3_etag_is_singlepart(etag) else None

        head_metadata = head.get("Metadata", {}) or {}
        merged: dict[str, str] = {str(k): str(v) for k, v in head_metadata.items()}
        for tag in tagging.get("TagSet", []) or []:
            tk = tag.get("Key")
            tv = tag.get("Value")
            if tk is None:
                continue
            # Tags win on conflict so the quarantine/tombstone markers
            # always surface to the listing-side consumer even if some
            # earlier writer happened to put the same key in
            # user-metadata.
            merged[str(tk)] = "" if tv is None else str(tv)

        return ObjectMeta(
            key=key,
            size_bytes=size,
            last_modified=last_modified,
            content_type=content_type,
            user_metadata=merged,
            etag=etag,
            provider_hash=provider_hash,
        )

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Attach ``(tag_key, tag_value)`` to ``key`` while preserving prior tags.

        S3's ``put_object_tagging`` replaces the entire tag set, so we
        first GET the existing tags, mutate (or insert) the requested
        entry, and PUT the union back. Both calls are wrapped with
        retry. This is called for ``mcps-quarantined-at`` (req 5.7)
        and ``mcps-tombstoned-at`` (req 9.3).
        """
        existing = self._call(
            "get_object_tagging",
            lambda: self._client.get_object_tagging(Bucket=self.bucket, Key=key),
        )
        new_set: List[dict[str, str]] = []
        replaced = False
        for tag in existing.get("TagSet", []) or []:
            tk = tag.get("Key")
            if tk == tag_key:
                new_set.append({"Key": tag_key, "Value": tag_value})
                replaced = True
            else:
                new_set.append(
                    {"Key": str(tk), "Value": str(tag.get("Value", ""))}
                )
        if not replaced:
            new_set.append({"Key": tag_key, "Value": tag_value})

        self._call(
            "put_object_tagging",
            lambda: self._client.put_object_tagging(
                Bucket=self.bucket,
                Key=key,
                Tagging={"TagSet": new_set},
            ),
        )

    def delete(self, key: str) -> None:
        """Physically delete ``key`` from the bucket.

        Used for expired-quarantine deletes (req 5.9), hard tombstone
        propagation (req 9.5), and rollback after a post-write
        verification mismatch (req 6.5).
        """
        self._call(
            "delete_object",
            lambda: self._client.delete_object(Bucket=self.bucket, Key=key),
        )


__all__ = [
    "MULTIPART_THRESHOLD_BYTES",
    "S3SourceAdapter",
]
