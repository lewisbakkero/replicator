"""`Manifest_Printer` — JSONL serialisation for the per-run Manifest.

The printer is the inverse of `mcps.manifest.parser.parse_manifest`. It
walks a sequence of `ManifestRecord` values in input order and emits one
JSON object per line. Output is UTF-8 with no BOM and uses LF (``\\n``)
line terminators (req 14.2, 15.2).

Three entry points are exposed:

* ``print_manifest_record(record) -> str`` — single-line JSON, no trailing
  newline. Used by `Manifest_Writer` to serialise one record at a time as
  it streams to disk.
* ``print_manifest(records) -> str`` — joined LF-terminated lines for the
  whole sequence. Used by the round-trip property test and any in-memory
  consumer that wants the bytes without going through a file.
* ``write_manifest_file(records, path) -> None`` — non-atomic streaming
  write helper used by tests; production code uses `Manifest_Writer` for
  append-only line-atomic writes.

JSON encoding details (per design.md and the Catalog_Printer precedent):

* ``json.dumps(asdict(rec), sort_keys=True, separators=(",",":"),
  ensure_ascii=False)``.
* ``Action`` and ``Result`` are ``(str, Enum)`` subclasses, so
  ``json.dumps`` renders their values as bare strings (``"replicate"``,
  ``"success"``) without needing a custom encoder.
* The optional ``extra`` mapping is preserved as a normal JSON object;
  ``asdict`` carries through plain ``dict`` instances unchanged.

Validates: Requirements 14.1, 14.2, 15.1, 15.2.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Iterable

from mcps.manifest.model import ManifestRecord


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def print_manifest_record(record: ManifestRecord) -> str:
    """Serialise one `ManifestRecord` to a single-line JSON string.

    The returned string has no trailing newline; callers that want JSONL
    must append ``"\\n"`` themselves. ``Manifest_Writer.append`` does this
    on every call.
    """
    return _encode(record)


def print_manifest(records: Iterable[ManifestRecord]) -> str:
    """Return the JSONL serialisation of ``records``.

    An empty iterable serialises to the empty string. Each record is
    rendered on its own line terminated with ``"\\n"``; the result
    therefore always ends with ``"\\n"`` when at least one record is
    present (req 15.2).
    """
    parts: list[str] = []
    for rec in records:
        parts.append(_encode(rec))
        parts.append("\n")
    return "".join(parts)


def write_manifest_file(records: Iterable[ManifestRecord], path: str) -> None:
    """Streaming non-atomic write of ``records`` to ``path`` in JSONL.

    Production code uses `Manifest_Writer` (which adds line-atomic
    append-mode semantics and raises `ManifestWriteError` on I/O
    failure). This helper exists for the round-trip property test so we
    can render a Manifest to disk without instantiating the writer's
    threading machinery.

    The file is opened in text mode with explicit UTF-8 encoding and
    ``newline=""`` so we control the terminator: every line ends with a
    single ``"\\n"`` (LF), never ``"\\r\\n"`` (req 15.2).
    """
    with open(path, "w", encoding="utf-8", newline="") as f:
        for rec in records:
            f.write(_encode(rec))
            f.write("\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _encode(rec: ManifestRecord) -> str:
    """Render one `ManifestRecord` as a single-line JSON string.

    ``sort_keys=True`` puts JSON keys in alphabetical order so two equal
    records produce byte-identical output. ``separators=(",",":")`` strips
    whitespace; ``ensure_ascii=False`` preserves non-ASCII characters as
    UTF-8 (the file encoding used by both ``print_manifest`` consumers
    and `Manifest_Writer`).
    """
    return json.dumps(
        asdict(rec),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


__all__ = ["print_manifest_record", "print_manifest", "write_manifest_file"]
