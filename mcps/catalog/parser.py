"""`Catalog_Parser` — streaming JSONL parser for the on-disk Catalog file.

The parser is the inverse of `mcps.catalog.printer.print_catalog`. It reads a
sequence of UTF-8 / LF-terminated JSONL lines (one `ObjectRecord` per line)
and rebuilds an in-memory `Catalog` via `Catalog.upsert`.

The first malformed line aborts the parse with `CatalogParseError(path, line)`
where ``line`` is 1-based. The parser never opens the file for writing nor
truncates it; on parse failure the on-disk file is left byte-for-byte
unchanged so that req 3.6 ("SHALL NOT modify, truncate, or overwrite the
existing Catalog file") is satisfied.

Two entry points are exposed:

* ``parse_catalog_file(path)`` — opens the file in read-only streaming mode,
  iterates its lines, and dispatches each to the same per-line parser used by
  ``parse_catalog``. Used by the CLI on Sync_Run startup.
* ``parse_catalog(text)`` — parses an in-memory string. Used by the
  round-trip property test (task 4 sub-task) so we never have to materialise
  a temporary file just to test the parser/printer pair.

Validates: Requirements 3.2, 3.4, 3.5, 3.6.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.errors import CatalogParseError


# ---------------------------------------------------------------------------
# Field schema
# ---------------------------------------------------------------------------

# The complete set of keys an `ObjectRecord` JSON object may contain.
_ALL_FIELDS: frozenset[str] = frozenset(
    {
        "source",
        "key",
        "content_hash",
        "size_bytes",
        "last_seen_at",
        "last_modified",
        "content_type",
        "quarantined_at",
        "tombstoned_at",
        "mcps_source_meta",
    }
)

# Required keys: the JSON object MUST contain these keys (their value may
# still be ``null`` where the dataclass field is ``Optional[str]`` — in
# practice that only applies to ``content_type``).
_REQUIRED_FIELDS: frozenset[str] = frozenset(
    {
        "source",
        "key",
        "content_hash",
        "size_bytes",
        "last_seen_at",
        "last_modified",
        "content_type",
    }
)

# Optional keys with a default value of ``None`` on the dataclass. Missing
# keys are treated as ``None``; explicit ``null`` values are equivalent.
_OPTIONAL_FIELDS: frozenset[str] = frozenset(
    {
        "quarantined_at",
        "tombstoned_at",
        "mcps_source_meta",
    }
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_catalog_file(path: str) -> Catalog:
    """Parse a Catalog from the file at ``path``.

    Opens the file in read-only mode (``"r"``) so the on-disk bytes are
    untouched. Lines are consumed one at a time so memory usage is O(1) in
    the file size beyond the resulting in-memory `Catalog`.

    Raises:
        CatalogParseError: on the first malformed line, with ``path`` and the
            1-based ``line`` number set.
    """
    catalog = Catalog()
    # ``open(..., encoding="utf-8")`` rejects UTF-8 with a BOM in the very
    # first line because the BOM is *not* valid JSON. That matches req 14.2 /
    # 15.2 (no BOM in our own output) and surfaces foreign BOMs as a parse
    # error on line 1.
    try:
        f = open(path, "r", encoding="utf-8", newline="")
    except OSError:
        # The CLI is responsible for translating "file missing" into an empty
        # Catalog (req 3.5). The parser only deals with files it can open.
        raise

    with f:
        for line_no, raw_line in enumerate(f, start=1):
            # ``raw_line`` keeps its trailing ``\n`` because we set
            # ``newline=""`` to disable universal newlines. The ``rstrip``
            # below strips a single trailing ``\n`` (and a stray ``\r`` in
            # case the file was authored on Windows); it does NOT strip
            # spaces or tabs, which would mask whitespace-only blank lines.
            line = raw_line
            if line.endswith("\n"):
                line = line[:-1]
            if line.endswith("\r"):
                line = line[:-1]
            if line == "":
                # Blank lines are not part of the format the printer emits;
                # treat them as a parse error so we never silently accept a
                # corrupted file (req 3.6).
                raise CatalogParseError(path=path, line=line_no)
            rec = _parse_line(line, path=path, line_no=line_no)
            catalog = catalog.upsert(rec)
    return catalog


def parse_catalog(text: str) -> Catalog:
    """Parse a Catalog from an in-memory string.

    Mirrors ``parse_catalog_file`` but takes a string rather than a path. The
    ``path`` field of any raised `CatalogParseError` is set to the synthetic
    sentinel ``"<memory>"`` because there is no real file to point at.
    """
    if text == "":
        return Catalog()

    catalog = Catalog()
    # ``str.split("\n")`` is preferred over ``splitlines()`` so we can detect
    # a missing terminating newline as a distinct case if needed; but for now
    # we accept both ``"a\nb"`` and ``"a\nb\n"`` and discard a single trailing
    # empty token produced by ``"a\nb\n".split("\n") == ["a", "b", ""]``.
    parts = text.split("\n")
    if parts and parts[-1] == "":
        parts = parts[:-1]

    for line_no, line in enumerate(parts, start=1):
        if line == "":
            raise CatalogParseError(path="<memory>", line=line_no)
        rec = _parse_line(line, path="<memory>", line_no=line_no)
        catalog = catalog.upsert(rec)
    return catalog


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_line(line: str, *, path: str, line_no: int) -> ObjectRecord:
    """Parse a single JSONL line into an `ObjectRecord`.

    Any failure — JSON syntax error, unknown key, missing required key, or
    type mismatch on a known key — is raised as a `CatalogParseError` whose
    ``path`` and ``line`` fields point at the offending line.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        raise CatalogParseError(path=path, line=line_no) from None

    if not isinstance(obj, dict):
        raise CatalogParseError(path=path, line=line_no)

    keys = obj.keys()

    # Reject unknown keys.
    unknown = [k for k in keys if k not in _ALL_FIELDS]
    if unknown:
        # Surface the first unknown key in the exception message so the
        # operator can grep for it, while keeping the structured fields
        # (path, line) stable for programmatic handling.
        err = CatalogParseError(path=path, line=line_no)
        err.args = (
            f"CatalogParseError(path={path!r}, line={line_no!r}) "
            f"unknown key: {unknown[0]!r}",
        )
        raise err

    # Reject missing required keys.
    missing = [k for k in _REQUIRED_FIELDS if k not in keys]
    if missing:
        err = CatalogParseError(path=path, line=line_no)
        err.args = (
            f"CatalogParseError(path={path!r}, line={line_no!r}) "
            f"missing required field: {missing[0]!r}",
        )
        raise err

    # Type-check every present field.
    if not _is_str(obj["source"]):
        raise _type_error(path, line_no, "source")
    if not _is_str(obj["key"]):
        raise _type_error(path, line_no, "key")
    if not _is_str(obj["content_hash"]):
        raise _type_error(path, line_no, "content_hash")
    if not _is_nonbool_int(obj["size_bytes"]):
        raise _type_error(path, line_no, "size_bytes")
    if not _is_str(obj["last_seen_at"]):
        raise _type_error(path, line_no, "last_seen_at")
    if not _is_str(obj["last_modified"]):
        raise _type_error(path, line_no, "last_modified")
    if not _is_optional_str(obj["content_type"]):
        raise _type_error(path, line_no, "content_type")

    # Optional-with-default fields: missing key is allowed (treated as None).
    quarantined_at = _opt_str_field(obj, "quarantined_at", path, line_no)
    tombstoned_at = _opt_str_field(obj, "tombstoned_at", path, line_no)
    mcps_source_meta = _opt_str_field(obj, "mcps_source_meta", path, line_no)

    return ObjectRecord(
        source=obj["source"],
        key=obj["key"],
        content_hash=obj["content_hash"],
        size_bytes=obj["size_bytes"],
        last_seen_at=obj["last_seen_at"],
        last_modified=obj["last_modified"],
        content_type=obj["content_type"],
        quarantined_at=quarantined_at,
        tombstoned_at=tombstoned_at,
        mcps_source_meta=mcps_source_meta,
    )


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_optional_str(v: Any) -> bool:
    return v is None or isinstance(v, str)


def _is_nonbool_int(v: Any) -> bool:
    # ``bool`` is a subclass of ``int`` in Python — exclude it explicitly so
    # ``"size_bytes": true`` is rejected as a type error.
    return isinstance(v, int) and not isinstance(v, bool)


def _opt_str_field(
    obj: dict[str, Any],
    name: str,
    path: str,
    line_no: int,
) -> Optional[str]:
    if name not in obj:
        return None
    v = obj[name]
    if not _is_optional_str(v):
        raise _type_error(path, line_no, name)
    return v


def _type_error(path: str, line_no: int, field: str) -> CatalogParseError:
    err = CatalogParseError(path=path, line=line_no)
    err.args = (
        f"CatalogParseError(path={path!r}, line={line_no!r}) "
        f"invalid type for field: {field!r}",
    )
    return err


__all__ = ["parse_catalog_file", "parse_catalog"]
