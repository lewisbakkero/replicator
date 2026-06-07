"""Data models for the on-disk Catalog: `ObjectRecord` and `Catalog`.

The Catalog is the persistent local index that maps Content_Hash to the set
of `ObjectRecord` instances sharing that hash. The on-disk format and the
round-trip parser/printer pair live in `mcps.catalog.parser` and
`mcps.catalog.printer` (task 4); this module owns only the in-memory shape.

Design references (`design.md`):

* `ObjectRecord` is a frozen, ordered dataclass so it is hashable and can be
  placed inside `frozenset` buckets.
* `Catalog.by_hash` is the canonical mapping `content_hash -> frozenset[ObjectRecord]`.
* `upsert` and `remove` return a *new* `Catalog` instance, leaving the prior
  one unmodified. This supports functional-style use in property tests
  (task 4 builds on this) without forcing every call site to construct
  intermediate copies by hand.

Validates: Requirements 3.2, 11.3, 11.5.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, Optional


@dataclass(frozen=True, order=True)
class ObjectRecord:
    """One Object in one Source, identified by its SHA-256 Content_Hash.

    Fields mirror the design.md `ObjectRecord` definition byte for byte:

    * ``source``: logical Source name (e.g. ``"s3-prod"``).
    * ``key``: provider key, byte-for-byte (no normalisation).
    * ``content_hash``: 64-char lowercase hex SHA-256.
    * ``size_bytes``: non-negative integer.
    * ``last_seen_at``: ISO-8601 UTC, second precision, trailing ``Z``.
    * ``last_modified``: provider-reported ISO-8601 UTC.
    * ``content_type``: provider-reported MIME type (may be ``None``).
    * ``quarantined_at``: set by the Duplicate_Resolver (req 5.7).
    * ``tombstoned_at``: set under ``delete_propagation=soft`` (req 9.3).
    * ``mcps_source_meta``: value of the ``mcps-source`` user-metadata on the
      live object, or ``None`` if absent (req 7.3, 7.4).

    The class is frozen + ordered so instances are hashable and can be
    deterministically sorted for the round-trip Catalog output (task 4).
    """

    source: str
    key: str
    content_hash: str
    size_bytes: int
    last_seen_at: str
    last_modified: str
    content_type: Optional[str]
    quarantined_at: Optional[str] = None
    tombstoned_at: Optional[str] = None
    mcps_source_meta: Optional[str] = None


@dataclass(eq=False)
class Catalog:
    """In-memory `content_hash -> frozenset[ObjectRecord]` index.

    The dataclass itself is mutable for ergonomics (callers may construct an
    empty Catalog and assign ``by_hash`` directly) but the mutating-looking
    operations ``upsert`` and ``remove`` return a *new* Catalog so that
    property tests can treat the Catalog as a pure value.

    Equality is defined as element-wise equality of ``by_hash``; two Catalogs
    constructed from the same set of `ObjectRecord` instances therefore
    compare equal regardless of insertion order (req 11.3).
    """

    by_hash: dict[str, frozenset[ObjectRecord]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Read-side helpers
    # ------------------------------------------------------------------

    def all_records(self) -> Iterator[ObjectRecord]:
        """Yield every `ObjectRecord` across every hash bucket.

        The iteration order is not guaranteed; callers that need a stable
        order should sort the result themselves.
        """
        for records in self.by_hash.values():
            yield from records

    def records_for_source(self, name: str) -> frozenset[ObjectRecord]:
        """Return the subset of records whose ``source == name``.

        Returns an empty frozenset (not ``None``) when no records match, so
        the caller can safely union / intersect the result.
        """
        return frozenset(r for r in self.all_records() if r.source == name)

    def cache_lookup(
        self,
        source: str,
        key: str,
        size: int,
        last_modified: str,
    ) -> Optional[str]:
        """Return the cached `content_hash` if and only if `(source, key)`
        matches a record AND its `size_bytes` and `last_modified` match the
        provided values byte-for-byte. Otherwise return ``None``.

        This is the primitive that lets the listing path skip re-hashing an
        Object whose provider-reported size and modification timestamp are
        unchanged since the last Sync_Run (req 3.7). When either field
        differs, the cached hash is treated as stale and ``None`` is returned
        so the caller re-hashes from current bytes (req 3.8).

        The lookup is O(n) over `all_records()`; n is the catalog size which
        is bounded by the number of Objects across all configured Sources,
        and each call is invoked at most once per listed Object, so the
        overall cost remains O(n^2) per Sync_Run in the worst case. A faster
        `(source, key) -> ObjectRecord` index can be introduced later if
        listing performance becomes a bottleneck; for now correctness wins.
        """
        for rec in self.all_records():
            if rec.source != source or rec.key != key:
                continue
            if rec.size_bytes == size and rec.last_modified == last_modified:
                return rec.content_hash
            # Found the (source, key) but size or mtime differs: stale.
            return None
        return None

    # ------------------------------------------------------------------
    # Functional-style mutators: each returns a fresh Catalog
    # ------------------------------------------------------------------

    def upsert(self, rec: ObjectRecord) -> "Catalog":
        """Return a new Catalog with ``rec`` inserted under its content_hash.

        Any prior record sharing ``(source, key)`` is removed first, even if
        it lives under a different ``content_hash`` bucket. This preserves
        the invariant that the Catalog never holds two records with the same
        ``(source, key)`` pair (req 11.5).
        """
        new_by_hash: dict[str, frozenset[ObjectRecord]] = {}
        for content_hash, records in self.by_hash.items():
            filtered = frozenset(
                r for r in records
                if not (r.source == rec.source and r.key == rec.key)
            )
            if filtered:
                new_by_hash[content_hash] = filtered

        bucket = new_by_hash.get(rec.content_hash, frozenset())
        new_by_hash[rec.content_hash] = bucket | {rec}
        return Catalog(by_hash=new_by_hash)

    def remove(self, source: str, key: str) -> "Catalog":
        """Return a new Catalog with any ``(source, key)`` record dropped.

        Empty hash buckets are pruned so equality with a freshly-constructed
        Catalog remains consistent.
        """
        new_by_hash: dict[str, frozenset[ObjectRecord]] = {}
        for content_hash, records in self.by_hash.items():
            filtered = frozenset(
                r for r in records
                if not (r.source == source and r.key == key)
            )
            if filtered:
                new_by_hash[content_hash] = filtered
        return Catalog(by_hash=new_by_hash)

    # ------------------------------------------------------------------
    # Equality / hashing
    # ------------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Catalog):
            return NotImplemented
        return self.by_hash == other.by_hash

    def __hash__(self) -> int:
        # ``frozenset(items())`` is hashable because each value is itself a
        # frozenset of frozen `ObjectRecord` instances.
        return hash(frozenset(self.by_hash.items()))


__all__ = ["ObjectRecord", "Catalog"]
