"""`SourceAdapter` ABC and the `ObjectMeta` value type.

Every interaction with a real cloud provider — S3, GCS, Google Drive — flows
through the `SourceAdapter` interface defined here. The Replicator,
Duplicate_Resolver, and Drive_Importer talk only through this interface, so
unit tests can substitute the in-memory `FakeSourceAdapter` (see
`mcps.sources.fake`) without touching the network.

The interface is deliberately small:

* `list_objects()` is a streaming enumerator that follows pagination /
  continuation tokens to exhaustion (req 2.5).
* `read_bytes(key)` yields content chunks suitable for streaming SHA-256
  (req 2.2-2.4); the caller never has to materialise the full Object on
  local disk.
* `write_bytes(...)`, `set_tag(...)`, and `delete(...)` are the three
  mutating entry points. Read-only adapters (the Drive adapter, req 10.8)
  raise `ReadOnlySourceError` from each of them and report
  `supports_writes == False`.
* `get_metadata(key)` is the HEAD-equivalent the Replicator uses for
  post-write verification (req 6.5).

The ObjectMeta dataclass is the *provider-reported* view of an Object,
captured at listing time before any Content_Hash resolution. Content_Hash
itself is computed in `mcps.hashing.compute_content_hash` and lives in
`ObjectRecord` (catalog/model.py); ObjectMeta intentionally does not carry
a `content_hash` field so that the listing path is forced through the
hash-priority chain in design.md (mcps-content-sha256 metadata → Catalog
cache → streamed SHA-256).

Validates: Requirements 2.1, 10.8.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Mapping, Optional


@dataclass(frozen=True)
class ObjectMeta:
    """Provider-reported metadata about one Object, before Content_Hash.

    Fields mirror the design.md `ObjectMeta` definition:

    * ``key``: provider key, byte-for-byte (no normalisation).
    * ``size_bytes``: provider-reported size in bytes; non-negative.
    * ``last_modified``: ISO-8601 UTC, second precision, trailing ``Z``.
    * ``content_type``: provider-reported MIME type. May be ``None`` for
      providers that do not report one (e.g. some Drive native docs).
    * ``user_metadata``: custom metadata. The ``mcps-*`` keys (req 6.4,
      7.1) live here when the provider exposes them as user-metadata.
      For S3 this is the ``Metadata`` dict from the SDK; tags surface
      separately via the listing path (S3 stores ``mcps-quarantined-at``
      and ``mcps-tombstoned-at`` as tags rather than user-metadata).
      For uniformity across providers, the listing-side adapters merge
      tag values into this mapping under the same key names.
    * ``etag``: S3 ETag, with the surrounding quotes already stripped.
      ``None`` for non-S3 providers.
    * ``provider_hash``: provider-reported strong hash that *might* be
      used for the singlepart-ETag duplicate optimisation (S3) or as
      informational metadata (GCS CRC32C). Always ``None`` for Drive.
      This is **not** the canonical Content_Hash — req 2.2 forbids using
      the ETag as identity, and req 2.3 forbids using GCS MD5/CRC32C as
      identity. The canonical hash is computed in
      `mcps.hashing.compute_content_hash`.

    The class is frozen so instances are hashable and immutable; this
    matters for tests that build small ObjectMeta sets and assert
    equality after a round-trip through the adapter.
    """

    key: str
    size_bytes: int
    last_modified: str
    content_type: Optional[str]
    user_metadata: Mapping[str, str]
    etag: Optional[str]
    provider_hash: Optional[str]

    def __hash__(self) -> int:
        # `user_metadata` is typed as `Mapping[str, str]` and is most often a
        # plain `dict`, which is itself unhashable. We hash a stable, sorted
        # tuple of its items so two `ObjectMeta` values with equal mappings
        # (regardless of insertion order) hash to the same value, and so the
        # dataclass remains usable inside `set` / `dict` keys without forcing
        # callers to construct a frozen mapping at every call site.
        return hash(
            (
                self.key,
                self.size_bytes,
                self.last_modified,
                self.content_type,
                tuple(sorted(self.user_metadata.items()))
                if self.user_metadata
                else (),
                self.etag,
                self.provider_hash,
            )
        )


class SourceAdapter(ABC):
    """Abstract base class for one configured Source.

    Concrete subclasses are stateless with respect to the Catalog: they
    expose only the provider-side primitives the orchestration layer
    needs and never persist anything across runs themselves. The
    Catalog, Manifest, and run-level decisions all live above this
    interface.

    Subclasses MUST set the ``name`` and ``kind`` instance attributes
    before any of the abstract methods are called. ``kind`` is one of
    ``"s3"``, ``"gcs"``, or ``"google_drive"``; ``name`` is the logical
    Source name from the configuration (e.g. ``"s3-prod"``).
    """

    name: str
    """Logical Source name from the configuration (e.g. ``"s3-prod"``)."""

    kind: str
    """One of ``"s3"``, ``"gcs"``, or ``"google_drive"``."""

    @abstractmethod
    def list_objects(self) -> Iterator[ObjectMeta]:
        """Stream every Object under the configured prefix / folder.

        Implementations MUST follow continuation tokens / page tokens to
        exhaustion (req 2.5). Implementations SHOULD apply server-side
        filtering when cheap (e.g. the Drive adapter applies a server-side
        ``mimeType`` filter where supported).

        The returned iterator is consumed exactly once by the listing
        path. It is permitted to lazily fetch additional pages on each
        ``next()`` call; the abstract contract makes no statement about
        when the underlying provider call is issued.
        """

    @abstractmethod
    def read_bytes(self, key: str) -> Iterator[bytes]:
        """Yield content chunks for streaming SHA-256.

        The chunk boundaries are an implementation detail; callers MUST
        treat the concatenation of yielded bytes as the canonical content.
        The streaming contract guarantees that the full Object never has
        to be materialised on local disk (req 2.2-2.4, 7.2).
        """

    @abstractmethod
    def write_bytes(
        self,
        key: str,
        chunks: Iterator[bytes],
        size_bytes: int,
        content_type: Optional[str],
        user_metadata: Mapping[str, str],
    ) -> None:
        """Atomically write an Object to ``key``.

        ``user_metadata`` MUST include the ``mcps-*`` entries the
        Replicator attaches at write time (``mcps-source``,
        ``mcps-content-sha256``, ``mcps-replicated-at`` — req 6.4, 7.4).
        On read-only adapters this method raises
        :class:`mcps.errors.ReadOnlySourceError` (req 10.8).
        """

    @abstractmethod
    def get_metadata(self, key: str) -> ObjectMeta:
        """Return the current `ObjectMeta` for ``key`` (HEAD-equivalent).

        Used by the Replicator for post-write verification (req 6.5) and
        by the Duplicate_Resolver to inspect tags before quarantine /
        delete decisions. Raises :class:`FileNotFoundError` (or a
        provider-mapped equivalent) if no Object exists at ``key``.
        """

    @abstractmethod
    def set_tag(self, key: str, tag_key: str, tag_value: str) -> None:
        """Attach the tag ``(tag_key, tag_value)`` to ``key``.

        Used for ``mcps-quarantined-at`` (req 5.7) and
        ``mcps-tombstoned-at`` (req 9.3). On read-only adapters this
        method raises :class:`mcps.errors.ReadOnlySourceError` (req 10.8).
        S3 implementations use object tagging; GCS implementations patch
        ``blob.metadata`` since GCS lacks S3-style tags. Both must
        preserve any tags already present.
        """

    @abstractmethod
    def delete(self, key: str) -> None:
        """Physically delete the Object at ``key``.

        Used for expired-quarantine deletes (req 5.9) and hard tombstone
        propagation (req 9.5), and as a rollback step when post-write
        verification fails (req 6.5). On read-only adapters this method
        raises :class:`mcps.errors.ReadOnlySourceError` (req 10.8).
        """

    @property
    def supports_writes(self) -> bool:
        """``True`` if this adapter supports `write_bytes`/`set_tag`/`delete`.

        Defaults to ``True``; the Drive adapter overrides this to
        ``False`` (req 10.8). Callers can use this property to decide
        whether to attempt a destructive operation at all, rather than
        catching :class:`mcps.errors.ReadOnlySourceError` after the fact.
        """
        return True


__all__ = ["ObjectMeta", "SourceAdapter"]
