"""In-memory `SourceAdapter` implementation for tests.

`FakeSourceAdapter` is the test double the property-based and unit tests
build on. It keeps every Object in a `dict[str, bytes]` and surfaces
provider-style metadata (size, content-type, last-modified, user-metadata,
tags) the same way the real S3 / GCS adapters will.

Two test affordances are baked in:

1. **Read-only mode**: pass ``supports_writes=False`` and every call to
   `write_bytes`, `set_tag`, or `delete` raises
   :class:`mcps.errors.ReadOnlySourceError` — matching the Drive adapter's
   contract (req 10.8).

2. **Call-site recording**: every public method call appends a
   ``(method_name, kwargs)`` tuple to ``self.call_log``. Tests assert on
   this log to confirm a write happened, didn't happen, or happened in a
   particular order.

Validates: Requirements 2.1, 10.8.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

from mcps.errors import ReadOnlySourceError
from mcps.hashing import CHUNK_SIZE
from mcps.sources.base import ObjectMeta, SourceAdapter


# Sentinel used by `get_metadata` / `read_bytes` when a key is missing. Mirrors
# the behaviour real adapters would give: a missing key on S3 is a 404 from
# `head_object` which surfaces to the caller as `FileNotFoundError` after
# error-mapping (the S3 adapter in task 17 does this conversion).
_MISSING = object()


# Minimal extension → MIME-type heuristic. The real adapters get content-type
# straight from the provider; this fake supplies a sensible default so
# `list_objects` and `get_metadata` produce stable values.
_CONTENT_TYPE_BY_EXT: Dict[str, str] = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".heic": "image/heic",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".m4v": "video/x-m4v",
    ".txt": "text/plain",
    ".bin": "application/octet-stream",
}

_DEFAULT_CONTENT_TYPE = "application/octet-stream"
_DEFAULT_LAST_MODIFIED = "2024-01-01T00:00:00Z"


def _guess_content_type(key: str) -> str:
    """Return a sensible default content-type for ``key`` based on its
    extension. The real S3 / GCS adapters get this straight from the
    provider; this fallback makes `FakeSourceAdapter` deterministic
    without forcing every test fixture to spell out a MIME type."""
    lower = key.lower()
    dot = lower.rfind(".")
    if dot < 0:
        return _DEFAULT_CONTENT_TYPE
    return _CONTENT_TYPE_BY_EXT.get(lower[dot:], _DEFAULT_CONTENT_TYPE)


class FakeSourceAdapter(SourceAdapter):
    """In-memory adapter backed by a ``dict[key, bytes]``.

    The fake is a drop-in for any of the real provider adapters. It is
    used both directly (unit tests) and as the building block for the
    integration-style fakes that wrap moto / in-process GCS / a canned
    Drive ``files().list`` response in later tasks.

    Construction is keyword-only so the call sites read clearly:

    .. code-block:: python

        adapter = FakeSourceAdapter(
            name="s3-prod",
            kind="s3",
            supports_writes=True,
            records={"photos/img.jpg": b"..."},
        )

    ``records``, ``metadata``, ``tags`` and ``last_modified`` are all
    optional; they default to empty dicts. Mutating the constructor
    arguments after construction does **not** affect the adapter — the
    fake takes a defensive copy of every input dict.
    """

    def __init__(
        self,
        *,
        name: str,
        kind: str,
        supports_writes: bool = True,
        records: Optional[Mapping[str, bytes]] = None,
        metadata: Optional[Mapping[str, Mapping[str, str]]] = None,
        tags: Optional[Mapping[str, Mapping[str, str]]] = None,
        last_modified: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.name = name
        self.kind = kind
        self._supports_writes = supports_writes

        # Defensive copies so tests that mutate their input dicts after
        # construction do not accidentally mutate the adapter's state.
        self.records: Dict[str, bytes] = dict(records or {})
        self.user_metadata: Dict[str, Dict[str, str]] = {
            k: dict(v) for k, v in (metadata or {}).items()
        }
        self.tags: Dict[str, Dict[str, str]] = {
            k: dict(v) for k, v in (tags or {}).items()
        }
        self.last_modified: Dict[str, str] = dict(last_modified or {})

        # ``call_log`` is the public observation surface. Each entry is
        # ``(method_name, kwargs_dict)``. Tests assert on this list to
        # confirm that, for example, a read-only adapter received zero
        # `write_bytes` calls during a Sync_Run.
        self.call_log: List[Tuple[str, Dict[str, Any]]] = []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_call(self, method: str, **kwargs: Any) -> None:
        """Append ``(method, kwargs)`` to ``self.call_log``.

        ``kwargs`` is captured by value so tests inspecting the log later
        see the arguments that were passed at the call site, not the
        in-memory state at assertion time.
        """
        self.call_log.append((method, dict(kwargs)))

    def _ensure_writable(self, op: str) -> None:
        """Raise :class:`ReadOnlySourceError` if writes are not supported.

        Centralises the read-only check used by `write_bytes`, `set_tag`,
        and `delete`. The error carries the adapter ``name`` and the
        operation ``op`` so the Manifest entry can identify which write
        was rejected (req 10.8).
        """
        if not self._supports_writes:
            raise ReadOnlySourceError(adapter=self.name, op=op)

    def _build_meta(self, key: str) -> ObjectMeta:
        """Construct an `ObjectMeta` for an existing ``key``.

        Tags and user-metadata are merged into a single mapping so
        downstream code can read both via ``ObjectMeta.user_metadata``
        without caring whether the value originally lived in a tag or
        in user-metadata. User-metadata wins on collision: that matches
        the real S3 adapter's listing-side behaviour (the listing path
        already has user-metadata in hand and only fetches tags
        secondarily).
        """
        if key not in self.records:
            raise FileNotFoundError(key)
        data = self.records[key]
        merged_metadata: Dict[str, str] = {}
        merged_metadata.update(self.tags.get(key, {}))
        merged_metadata.update(self.user_metadata.get(key, {}))
        return ObjectMeta(
            key=key,
            size_bytes=len(data),
            last_modified=self.last_modified.get(key, _DEFAULT_LAST_MODIFIED),
            content_type=_guess_content_type(key),
            user_metadata=dict(merged_metadata),
            etag=None,
            provider_hash=None,
        )

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def list_objects(self) -> Iterator[ObjectMeta]:
        """Yield one ObjectMeta per stored record.

        The iteration order follows ``sorted(self.records)`` so tests can
        rely on deterministic output regardless of dict-insertion order.
        """
        self._record_call("list_objects")
        for key in sorted(self.records):
            yield self._build_meta(key)

    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Yield ``key``'s bytes in `CHUNK_SIZE` chunks.

        Raises :class:`FileNotFoundError` if ``key`` is missing — this
        matches the post-error-mapping behaviour of the real adapters.
        """
        self._record_call("read_bytes", key=key)
        if key not in self.records:
            raise FileNotFoundError(key)
        data = self.records[key]
        # Empty inputs yield nothing; the empty-input SHA-256 is well
        # defined so callers never need a synthetic empty chunk.
        for i in range(0, len(data), CHUNK_SIZE):
            yield data[i:i + CHUNK_SIZE]

    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Materialise ``chunks`` into the in-memory store under ``key``.

        Stores the supplied ``user_metadata`` verbatim and updates
        ``last_modified`` to the run-time clock surrogate
        (``_DEFAULT_LAST_MODIFIED`` here — tests that care about
        timestamps inject them via the constructor's ``last_modified``
        argument). Raises :class:`ReadOnlySourceError` if writes are not
        supported on this adapter (req 10.8).
        """
        self._record_call(
            "write_bytes",
            key=key,
            size_bytes=size_bytes,
            content_type=content_type,
            user_metadata=dict(user_metadata),
        )
        self._ensure_writable("write_bytes")
        body = b"".join(chunks)
        self.records[key] = body
        self.user_metadata[key] = dict(user_metadata)
        # Preserve any pre-existing last_modified the test may have set;
        # otherwise stamp with the default sentinel so the resulting
        # ObjectMeta has a stable, comparable value.
        self.last_modified.setdefault(key, _DEFAULT_LAST_MODIFIED)

    def get_metadata(self, key: str) -> ObjectMeta:
        """Return the `ObjectMeta` for ``key``.

        Raises :class:`FileNotFoundError` if ``key`` is not present —
        the same error the real adapters raise after a 404 HEAD.
        """
        self._record_call("get_metadata", key=key)
        return self._build_meta(key)

    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Attach the tag ``(tag_key, tag_value)`` to ``key``.

        Tags are stored separately from user-metadata so callers can
        distinguish the two; ``list_objects`` / ``get_metadata`` merge
        them into ``ObjectMeta.user_metadata`` for the listing-side
        consumers. Raises :class:`ReadOnlySourceError` on read-only
        adapters (req 10.8) and :class:`FileNotFoundError` if the key
        is unknown — matching the real adapters' behaviour.
        """
        self._record_call(
            "set_tag", key=key, tag_key=tag_key, tag_value=tag_value
        )
        self._ensure_writable("set_tag")
        if key not in self.records:
            raise FileNotFoundError(key)
        self.tags.setdefault(key, {})[tag_key] = tag_value

    def delete(self, key: str) -> None:
        """Remove ``key`` from the in-memory store.

        Removes the bytes, the user-metadata, and any tags. Raises
        :class:`ReadOnlySourceError` on read-only adapters (req 10.8)
        and :class:`FileNotFoundError` if ``key`` is missing.
        """
        self._record_call("delete", key=key)
        self._ensure_writable("delete")
        if key not in self.records:
            raise FileNotFoundError(key)
        del self.records[key]
        self.user_metadata.pop(key, None)
        self.tags.pop(key, None)
        self.last_modified.pop(key, None)

    @property
    def supports_writes(self) -> bool:
        """Mirrors the constructor flag (req 10.8)."""
        return self._supports_writes


__all__ = ["FakeSourceAdapter"]
