"""`Catalog_Printer` — deterministic JSONL serialisation for the Catalog.

The printer is the inverse of `mcps.catalog.parser.parse_catalog`. It walks
every `ObjectRecord` in the Catalog, sorts them by ``(content_hash, source,
key)`` ascending, and emits one JSON object per line using a fixed
serialisation that is byte-identical for two invocations on equal Catalogs.

Two entry points are exposed:

* ``print_catalog(c) -> str`` — returns the deterministic JSONL string. Used
  by the round-trip property test and any caller that wants to inspect the
  serialisation in memory.
* ``write_catalog(c, path) -> None`` — atomic file write via
  ``tempfile.NamedTemporaryFile`` in the same parent directory plus
  ``os.replace``. The temp file lives on the same filesystem as the target
  so ``os.replace`` is atomic (req 3.1).

JSON encoding details (per design.md):

* ``json.dumps(asdict(rec), sort_keys=True, separators=(",",":"),
  ensure_ascii=False)``.
* Each line terminated with a single ``\\n`` (LF). UTF-8 with no BOM.
* The output of two calls on equal Catalogs is byte-identical (req 3.3).

Validates: Requirements 3.1, 3.3.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from typing import Iterable, Iterator

from mcps.catalog.model import Catalog, ObjectRecord


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def print_catalog(catalog: Catalog) -> str:
    """Return the deterministic JSONL serialisation of ``catalog``.

    An empty Catalog serialises to the empty string. Each record is rendered
    as a single line terminated with ``\\n``; the result therefore always
    ends with ``\\n`` when at least one record is present.
    """
    return "".join(_emit_lines(catalog))


def write_catalog(catalog: Catalog, path: str) -> None:
    """Atomically write the JSONL serialisation of ``catalog`` to ``path``.

    The write is performed by creating a `tempfile.NamedTemporaryFile` in
    the same parent directory as ``path`` (so ``os.replace`` is atomic on
    the same filesystem), writing every line, ``fsync``'ing the file, and
    swapping it into place with ``os.replace``. On success the on-disk file
    transitions from the prior contents to the new contents in a single
    atomic operation; on any error the temp file is removed and the prior
    on-disk file is left untouched (req 3.1).
    """
    parent = os.path.dirname(os.path.abspath(path)) or "."
    # ``delete=False`` so we control teardown explicitly: we need the file to
    # outlive the ``with`` block so we can ``os.replace`` it into place.
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",          # we emit LF terminators ourselves
        dir=parent,
        prefix=".mcps-catalog-",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = tmp.name
    try:
        for line in _emit_lines(catalog):
            tmp.write(line)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp_path, path)
    except BaseException:
        # Either the write raised, or os.replace failed. Clean up the temp
        # file so we never leave ``.mcps-catalog-*.tmp`` files lying around;
        # the existing on-disk catalog (if any) is untouched.
        try:
            tmp.close()
        except Exception:
            pass
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            # ``os.replace`` succeeded but a later step (none currently)
            # failed; nothing to clean up.
            pass
        raise


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _emit_lines(catalog: Catalog) -> Iterator[str]:
    """Yield ``"<json>\\n"`` lines in deterministic order."""
    for rec in _sorted_records(catalog):
        yield _encode(rec) + "\n"


def _sorted_records(catalog: Catalog) -> list[ObjectRecord]:
    """Return every record in the Catalog sorted by (content_hash, source, key).

    Two equal Catalogs produce the same sorted list because:

    * The set of records is identical (Catalog equality is element-wise).
    * The sort key (content_hash, source, key) is unique within a valid
      Catalog (req 11.5: at most one record per (source, key); two records
      sharing all three fields would be the *same* record).
    """
    return sorted(
        catalog.all_records(),
        key=lambda r: (r.content_hash, r.source, r.key),
    )


def _encode(rec: ObjectRecord) -> str:
    """Serialise one `ObjectRecord` to a single-line JSON string.

    ``sort_keys=True`` sorts JSON keys alphabetically so the output is
    independent of the dataclass field order. ``separators=(",",":")``
    removes whitespace; ``ensure_ascii=False`` preserves non-ASCII
    characters as UTF-8 (which is the default file encoding).
    """
    return json.dumps(
        asdict(rec),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


__all__ = ["print_catalog", "write_catalog"]
