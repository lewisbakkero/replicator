"""`Manifest_Parser` — JSONL parser for the per-run Manifest file.

The parser is the inverse of `mcps.manifest.printer.print_manifest`. Per
req 15.4, a malformed line MUST NOT silently disappear: the parser
returns a structured `ParseError` with the 1-based line number and a
machine-readable category, and continues parsing the rest of the file so
operators can see every issue at once. The successfully-parsed records
and the per-line errors are returned as a `(records, errors)` tuple.

Per req 15.5, an I/O failure when opening the file is surfaced by
letting `OSError` propagate to the caller — we do not return a partial
sequence as a "successful" result. The CLI translates a top-level
`OSError` on the manifest path into `ManifestWriteError` /
`MANIFEST_UNAVAILABLE` exit code (67).

Two entry points are exposed:

* ``parse_manifest_file(path) -> (records, errors)`` — opens the file
  in read-only streaming mode and dispatches each line to the same
  per-line parser used by ``parse_manifest``.
* ``parse_manifest(text) -> (records, errors)`` — parses an in-memory
  string. Used by the round-trip property test so we never have to
  materialise a temporary file just to test the parser/printer pair.

Validates: Requirements 15.1, 15.3, 15.4, 15.5.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from mcps.manifest.model import Action, ManifestRecord, Result


# ---------------------------------------------------------------------------
# Field schema
# ---------------------------------------------------------------------------

# The complete set of keys a `ManifestRecord` JSON object may contain.
_ALL_FIELDS: frozenset[str] = frozenset(
    {
        "timestamp",
        "run_id",
        "action",
        "result",
        "source",
        "target",
        "key",
        "content_hash",
        "size_bytes",
        "error",
        "extra",
    }
)

# Required keys: the JSON object MUST contain these keys. The printer always
# emits every field (since `asdict` materialises all dataclass attributes,
# including those whose value is ``None``), so a well-formed Manifest line
# will always have all 11 keys; missing required keys signal a hand-edited
# or otherwise corrupted file.
_REQUIRED_FIELDS: frozenset[str] = frozenset(
    {
        "timestamp",
        "run_id",
        "action",
        "result",
        "source",
        "target",
        "key",
        "content_hash",
        "size_bytes",
        "error",
        "extra",
    }
)

# Categories used in `ParseError.category`. Tests reference these strings
# directly, so they form a stable surface and must not drift.
_CAT_JSON_SYNTAX = "json_syntax"
_CAT_MISSING_FIELD = "missing_field"
_CAT_UNKNOWN_KEY = "unknown_key"
_CAT_INVALID_VALUE = "invalid_value"
_CAT_UNKNOWN_ENUM = "unknown_enum"


# ---------------------------------------------------------------------------
# ParseError
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParseError:
    """One per-line parse failure recovered by the Manifest_Parser.

    Per req 15.4 the parser returns a structured error per failing line
    rather than aborting; this dataclass is that structured error.

    Fields:

    * ``line``: 1-based line number of the failing line.
    * ``category``: one of ``"json_syntax"``, ``"missing_field"``,
      ``"unknown_key"``, ``"invalid_value"``, ``"unknown_enum"``.
    * ``message``: human-readable description of the failure (operator
      diagnostic only — code should branch on ``category``).
    """

    line: int
    category: str
    message: str


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_manifest_file(path: str) -> tuple[list[ManifestRecord], list[ParseError]]:
    """Parse a Manifest from the file at ``path``.

    Opens the file in read-only mode (``"r"``) so the on-disk bytes are
    untouched. Lines are consumed one at a time so memory usage is O(1)
    in the file size beyond the resulting record list.

    Per req 15.5 an `OSError` on open propagates to the caller (e.g.
    ``FileNotFoundError`` for a non-existent path, ``PermissionError``
    for an unreadable file). Per req 15.4 a per-line parse failure
    appends a `ParseError` to the returned errors list and parsing
    continues.
    """
    records: list[ManifestRecord] = []
    errors: list[ParseError] = []

    # Let any OSError propagate per req 15.5. The CLI wraps this in
    # `ManifestWriteError` and exits with code 67.
    with open(path, "r", encoding="utf-8", newline="") as f:
        for line_no, raw_line in enumerate(f, start=1):
            line = _strip_terminator(raw_line)
            if line == "":
                # Blank lines are not part of the format the printer emits;
                # treat them as a json_syntax error so the issue is visible
                # without aborting the parse.
                errors.append(
                    ParseError(
                        line=line_no,
                        category=_CAT_JSON_SYNTAX,
                        message="blank line",
                    )
                )
                continue
            outcome = _parse_line(line, line_no)
            if isinstance(outcome, ParseError):
                errors.append(outcome)
            else:
                records.append(outcome)

    return records, errors


def parse_manifest(text: str) -> tuple[list[ManifestRecord], list[ParseError]]:
    """Parse a Manifest from an in-memory string.

    Mirrors ``parse_manifest_file`` but takes a string. The empty string
    parses to ``([], [])`` rather than raising; an empty Manifest is a
    valid Manifest containing zero records.
    """
    if text == "":
        return [], []

    records: list[ManifestRecord] = []
    errors: list[ParseError] = []

    # ``str.split("\n")`` is preferred over ``splitlines()`` so we can
    # detect a missing terminating newline as a distinct case if needed.
    # We accept both ``"a\nb"`` and ``"a\nb\n"`` and discard a single
    # trailing empty token produced by ``"a\nb\n".split("\n") == ["a",
    # "b", ""]``.
    parts = text.split("\n")
    if parts and parts[-1] == "":
        parts = parts[:-1]

    for line_no, raw_line in enumerate(parts, start=1):
        # ``raw_line`` here has no trailing ``\n`` (split removed it). A
        # line from a CRLF-terminated source still has a trailing ``\r``
        # though; strip that too so manifests authored on Windows can be
        # parsed without complaint, even though we never emit CRLF.
        line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
        if line == "":
            errors.append(
                ParseError(
                    line=line_no,
                    category=_CAT_JSON_SYNTAX,
                    message="blank line",
                )
            )
            continue
        outcome = _parse_line(line, line_no)
        if isinstance(outcome, ParseError):
            errors.append(outcome)
        else:
            records.append(outcome)

    return records, errors


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _strip_terminator(raw_line: str) -> str:
    """Strip a single trailing ``\\n`` and (if present) ``\\r``.

    ``open(..., newline="")`` keeps the line terminator intact so the
    parser sees the exact bytes on disk; we strip exactly the JSONL
    terminator and nothing else.
    """
    line = raw_line
    if line.endswith("\n"):
        line = line[:-1]
    if line.endswith("\r"):
        line = line[:-1]
    return line


def _parse_line(line: str, line_no: int) -> ManifestRecord | ParseError:
    """Parse a single JSONL line into a `ManifestRecord` or a `ParseError`.

    All failure modes return a `ParseError` rather than raising; the
    caller appends it to the errors list and continues.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        return ParseError(
            line=line_no,
            category=_CAT_JSON_SYNTAX,
            message=f"invalid JSON: {e.msg}",
        )

    if not isinstance(obj, dict):
        return ParseError(
            line=line_no,
            category=_CAT_JSON_SYNTAX,
            message=f"expected JSON object, got {type(obj).__name__}",
        )

    keys = obj.keys()

    # Reject unknown keys so a typo isn't silently dropped.
    unknown = sorted(k for k in keys if k not in _ALL_FIELDS)
    if unknown:
        return ParseError(
            line=line_no,
            category=_CAT_UNKNOWN_KEY,
            message=f"unknown key: {unknown[0]!r}",
        )

    # Reject missing required keys.
    missing = sorted(k for k in _REQUIRED_FIELDS if k not in keys)
    if missing:
        return ParseError(
            line=line_no,
            category=_CAT_MISSING_FIELD,
            message=f"missing required field: {missing[0]!r}",
        )

    # Type-check every present field. We accept ``None`` for the optional
    # fields (``source``, ``target``, ``key``, ``content_hash``,
    # ``size_bytes``, ``error``); the printer always emits these as JSON
    # ``null`` when unset.
    if not _is_str(obj["timestamp"]):
        return _invalid_value(line_no, "timestamp", obj["timestamp"], "string")
    if not _is_str(obj["run_id"]):
        return _invalid_value(line_no, "run_id", obj["run_id"], "string")

    if not _is_str(obj["action"]):
        return _invalid_value(line_no, "action", obj["action"], "string")
    try:
        action = Action(obj["action"])
    except ValueError:
        return ParseError(
            line=line_no,
            category=_CAT_UNKNOWN_ENUM,
            message=f"unknown action: {obj['action']!r}",
        )

    if not _is_str(obj["result"]):
        return _invalid_value(line_no, "result", obj["result"], "string")
    try:
        result = Result(obj["result"])
    except ValueError:
        return ParseError(
            line=line_no,
            category=_CAT_UNKNOWN_ENUM,
            message=f"unknown result: {obj['result']!r}",
        )

    if not _is_optional_str(obj["source"]):
        return _invalid_value(line_no, "source", obj["source"], "string-or-null")
    if not _is_optional_str(obj["target"]):
        return _invalid_value(line_no, "target", obj["target"], "string-or-null")
    if not _is_optional_str(obj["key"]):
        return _invalid_value(line_no, "key", obj["key"], "string-or-null")
    if not _is_optional_str(obj["content_hash"]):
        return _invalid_value(line_no, "content_hash", obj["content_hash"], "string-or-null")
    if not _is_optional_nonbool_int(obj["size_bytes"]):
        return _invalid_value(line_no, "size_bytes", obj["size_bytes"], "integer-or-null")
    if not _is_optional_str(obj["error"]):
        return _invalid_value(line_no, "error", obj["error"], "string-or-null")
    if not _is_str_str_mapping(obj["extra"]):
        return _invalid_value(
            line_no, "extra", obj["extra"], "object with string values"
        )

    return ManifestRecord(
        timestamp=obj["timestamp"],
        run_id=obj["run_id"],
        action=action,
        result=result,
        source=obj["source"],
        target=obj["target"],
        key=obj["key"],
        content_hash=obj["content_hash"],
        size_bytes=obj["size_bytes"],
        error=obj["error"],
        extra=dict(obj["extra"]),
    )


def _invalid_value(
    line_no: int, field: str, value: Any, expected: str
) -> ParseError:
    return ParseError(
        line=line_no,
        category=_CAT_INVALID_VALUE,
        message=f"invalid value for {field!r}: expected {expected}, got {type(value).__name__}",
    )


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_optional_str(v: Any) -> bool:
    return v is None or isinstance(v, str)


def _is_optional_nonbool_int(v: Any) -> bool:
    if v is None:
        return True
    # ``bool`` is a subclass of ``int`` in Python — exclude it explicitly so
    # ``"size_bytes": true`` is rejected as a type error.
    return isinstance(v, int) and not isinstance(v, bool)


def _is_str_str_mapping(v: Any) -> bool:
    """Return True iff ``v`` is a dict whose keys and values are all strings."""
    if not isinstance(v, dict):
        return False
    for k, val in v.items():
        if not isinstance(k, str) or not isinstance(val, str):
            return False
    return True


__all__ = [
    "ParseError",
    "parse_manifest",
    "parse_manifest_file",
]
