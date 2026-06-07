"""`Duplicate_Detector` — pure grouping of `ObjectRecord` by Content_Hash.

The detector walks every record in a `Catalog`, divides records into two
streams:

* **valid** records — those with a syntactically-acceptable `content_hash`
  (64-char lowercase hex SHA-256) and a non-negative `size_bytes`.
* **skipped** records — anything else (missing/empty/wrong-length/non-hex
  hash, or negative `size_bytes`). These are returned untouched in
  `DetectionResult.skipped_records` so the caller (e.g. the Manifest writer)
  can surface a per-record reason without re-walking the Catalog
  (req 4.6).

Valid records are then grouped by the composite key `(content_hash,
size_bytes)`. Requirement 4.2 says two records are duplicates only if both
their content hash *and* their size match. Two records sharing a hash but
disagreeing on size are therefore split into separate buckets and neither
becomes a duplicate group on its own; in practice such a collision would
indicate provider-side metadata corruption.

A bucket of size ≥ 2 becomes a `DuplicateGroup`. Members are sorted by
`(source, key)` so the on-disk Manifest is byte-deterministic. The group
list itself is sorted by `content_hash` ascending so two runs over the
same Catalog emit identical output regardless of dict iteration order
(req 4.4 — the order-independence property is exercised in task 21).

A group's label is `cross-source` when its members span ≥ 2 distinct
`Source` names, otherwise `same-source` (req 4.3).

Validates: Requirements 4.1, 4.2, 4.3, 4.5, 4.6.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mcps.catalog.model import Catalog, ObjectRecord


# A 64-character lowercase hex string, no embedded separators. We do not
# import a regex module here — len + character-class is faster and the
# detector is hot-loop code over potentially millions of records.
_HEX_DIGITS = frozenset("0123456789abcdef")
_CONTENT_HASH_LENGTH = 64

GroupLabel = Literal["cross-source", "same-source"]


@dataclass(frozen=True, order=True)
class DuplicateGroup:
    """One duplicate group: ≥ 2 `ObjectRecord` instances sharing
    `(content_hash, size_bytes)`.

    Fields:

    * ``content_hash`` — the shared 64-char lowercase hex SHA-256 of every
      member.
    * ``members`` — every member of the group as a sorted tuple. Ordering is
      `(source, key)` ascending, so the tuple is byte-deterministic. The
      tuple is *not* deduplicated by identity because the Catalog itself
      already enforces one record per `(source, key)` (req 11.5).
    * ``label`` — ``"cross-source"`` when ``{m.source for m in members}`` has
      cardinality ≥ 2, else ``"same-source"`` (req 4.3).
    * ``total_size_bytes`` — the sum of every member's `size_bytes`. By
      construction (group-by `(content_hash, size_bytes)`) this is exactly
      ``size_bytes * len(members)``; the field is precomputed so callers do
      not have to re-fold the tuple.

    The class is frozen + ordered so it composes cleanly inside frozenset
    fixtures used by Property 4 (task 21).
    """

    content_hash: str
    members: tuple[ObjectRecord, ...]
    label: GroupLabel
    total_size_bytes: int


@dataclass(frozen=True)
class DetectionResult:
    """The output of `detect_duplicates`.

    Two parallel streams:

    * ``groups`` — every duplicate group, sorted by ``content_hash``
      ascending (req 4.4 determinism).
    * ``skipped_records`` — every record diverted because its hash or size
      could not be evaluated (req 4.6). Sorted by ``(source, key)`` for
      stable Manifest output.

    When the Catalog is empty, *or* every Catalog bucket has fewer than two
    members with matching `(content_hash, size_bytes)`, ``groups`` is the
    empty tuple and the function returns without raising (req 4.5).
    """

    groups: tuple[DuplicateGroup, ...]
    skipped_records: tuple[ObjectRecord, ...]


# ---------------------------------------------------------------------------
# Internal predicates
# ---------------------------------------------------------------------------


def _is_valid_content_hash(value: object) -> bool:
    """Return True iff ``value`` is a 64-char lowercase hex string.

    The Catalog dataclass annotates `content_hash` as ``str`` but the field
    can still be the empty string for records ingested before a hash was
    computed. We accept exactly the SHA-256 hex shape so a bug in an
    upstream component cannot smuggle a non-hex hash into a duplicate
    group.
    """
    if not isinstance(value, str):
        return False
    if len(value) != _CONTENT_HASH_LENGTH:
        return False
    return all(c in _HEX_DIGITS for c in value)


def _is_valid_size(value: object) -> bool:
    """Return True iff ``value`` is a non-negative integer.

    Bool is rejected explicitly because ``bool`` is a subclass of ``int``
    in Python and ``True`` would otherwise pass the ``>= 0`` check.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    return value >= 0


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def detect_duplicates(catalog: Catalog) -> DetectionResult:
    """Return every duplicate group in ``catalog`` plus the skipped records.

    Pure function: the input Catalog is read but never mutated, and the
    result depends only on the set of `ObjectRecord` instances reachable
    via ``catalog.all_records()``. The function tolerates an empty Catalog
    by returning empty tuples (req 4.5).
    """
    skipped: list[ObjectRecord] = []
    # Bucket key: (content_hash, size_bytes). We deliberately key on size
    # too, so two records sharing a hash but disagreeing on size never end
    # up in the same group (req 4.2). The dict preserves insertion order
    # but we sort the final groups list for determinism.
    buckets: dict[tuple[str, int], list[ObjectRecord]] = {}

    for record in catalog.all_records():
        if not _is_valid_content_hash(record.content_hash):
            skipped.append(record)
            continue
        if not _is_valid_size(record.size_bytes):
            skipped.append(record)
            continue

        bucket_key = (record.content_hash, record.size_bytes)
        buckets.setdefault(bucket_key, []).append(record)

    groups: list[DuplicateGroup] = []
    for (content_hash, size_bytes), members in buckets.items():
        if len(members) < 2:
            # Singleton hashes are not duplicates (req 4.1: count >= 2).
            continue

        sorted_members = tuple(sorted(members, key=lambda r: (r.source, r.key)))
        distinct_sources = {m.source for m in sorted_members}
        label: GroupLabel = (
            "cross-source" if len(distinct_sources) >= 2 else "same-source"
        )
        total_size_bytes = size_bytes * len(sorted_members)

        groups.append(
            DuplicateGroup(
                content_hash=content_hash,
                members=sorted_members,
                label=label,
                total_size_bytes=total_size_bytes,
            )
        )

    # Final ordering of groups is by content_hash ascending so two runs over
    # the same Catalog emit identical group lists (req 4.4). When two groups
    # share a content_hash (impossible in practice because we keyed on
    # (hash, size)), the dataclass `order=True` falls through to `members`
    # which is itself a sorted tuple, keeping the comparison total.
    groups.sort(key=lambda g: g.content_hash)

    skipped_sorted = tuple(sorted(skipped, key=lambda r: (r.source, r.key)))
    return DetectionResult(
        groups=tuple(groups),
        skipped_records=skipped_sorted,
    )


__all__ = [
    "DuplicateGroup",
    "DetectionResult",
    "detect_duplicates",
]
