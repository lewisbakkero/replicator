"""`Manifest_Writer` — streaming append-mode writer for the Manifest file.

The writer is the production sink for `ManifestRecord` values produced
by every component (Replicator, Drive_Importer, Duplicate_Resolver,
Inconsistency_Detector). It opens the configured Manifest file in
append mode (``"a"``) so concurrent runs never truncate each other's
output, serialises one record at a time via
`mcps.manifest.printer.print_manifest_record`, writes the line plus a
single ``"\\n"`` terminator, and flushes after every record so a crash
mid-run leaves a forensically useful Manifest on disk (req 14.6).

Threading model:

* The Replicator and Drive_Importer submit per-object operations to a
  bounded `ThreadPoolExecutor`. Multiple worker threads can therefore
  call `Manifest_Writer.append` concurrently.
* A single `threading.Lock` (the "single-thread Lock for line atomicity"
  the design and task brief call out) serialises the
  ``serialise → write → flush`` critical section so two concurrent
  appends never interleave their bytes mid-line.

Error handling:

* Any `OSError` raised by `open`, `write`, `flush`, or `close` is
  wrapped in `ManifestWriteError(path, cause)` per req 14.6 / 14.7.
  Once construction succeeds the file handle is held open until
  `close()` (or `__exit__`) so per-record open/close overhead is paid
  once per run.

Validates: Requirements 14.1, 14.2, 14.6, 14.7.
"""

from __future__ import annotations

import threading
from types import TracebackType
from typing import Iterable, Optional, Type

from mcps.errors import ManifestWriteError
from mcps.manifest.model import ManifestRecord
from mcps.manifest.printer import print_manifest_record


class ManifestWriter:
    """Append-only line-atomic writer for the Manifest JSONL file.

    Construction opens the file in append mode and acquires an internal
    `threading.Lock`. Subsequent `append` calls serialise their record,
    write it under the lock, and flush. Use as a context manager when
    the lifetime is scoped to a single Sync_Run::

        with ManifestWriter(manifest_path) as mw:
            mw.append(record)

    or call `close()` explicitly.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        # The Lock guards the write-flush critical section. We initialise
        # it before opening the file so a failed open leaves the writer
        # in a coherent (closed) state.
        self._lock = threading.Lock()
        self._closed = False
        try:
            # ``newline=""`` so we control the terminator: every record is
            # followed by exactly one ``"\n"`` we write ourselves, never
            # ``"\r\n"`` (req 15.2). UTF-8 is the only encoding the format
            # supports (req 15.2 — no BOM is implied because Python's UTF-8
            # codec does not write a BOM unless ``utf-8-sig`` is requested).
            self._file = open(path, "a", encoding="utf-8", newline="")
        except OSError as e:
            raise ManifestWriteError(path=path, cause=e) from e

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "ManifestWriter":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        # Always close, even on exception, so the file handle is never
        # leaked. ``close`` itself wraps `OSError` in
        # `ManifestWriteError`; we let that propagate so an I/O failure
        # at exit is visible to the operator.
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, record: ManifestRecord) -> None:
        """Append one `ManifestRecord` to the Manifest as a single JSONL line.

        The serialise → write → flush sequence is performed under the
        instance lock so concurrent calls from worker threads never
        interleave their bytes. ``flush`` is called on every record so
        the on-disk file is always at least one whole-line behind the
        in-memory state — never partway through a line.

        Raises:
            ManifestWriteError: on I/O failure or if `close()` was
                already called.
        """
        # Serialise outside the lock — the printer is pure and re-entrant,
        # and keeping the lock window tight reduces contention between
        # worker threads. This is safe because `ManifestRecord` is a
        # frozen dataclass, so the value cannot change between
        # serialisation and write.
        line = print_manifest_record(record)

        with self._lock:
            if self._closed:
                raise ManifestWriteError(
                    path=self._path,
                    cause=ValueError("writer is closed"),
                )
            try:
                self._file.write(line)
                self._file.write("\n")
                self._file.flush()
            except OSError as e:
                raise ManifestWriteError(path=self._path, cause=e) from e

    def append_many(self, records: Iterable[ManifestRecord]) -> None:
        """Convenience wrapper that calls `append` once per record.

        This is not atomic across the whole iterable — each record is
        written under its own lock acquisition. Callers that need the
        whole batch to land or none to land should use
        `Catalog_Printer.write_catalog` semantics on a separate file.
        """
        for record in records:
            self.append(record)

    def close(self) -> None:
        """Flush and close the underlying file handle.

        Idempotent: a second call after the first is a no-op. Any
        `OSError` from the final flush or close is wrapped in
        `ManifestWriteError`.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                # Flush before close so the OS-level buffers are flushed
                # under our control; close() will also flush, but raising
                # the more specific error here is friendlier.
                self._file.flush()
            except OSError as e:
                # Still attempt to close the handle so we don't leak.
                try:
                    self._file.close()
                except OSError:
                    pass
                raise ManifestWriteError(path=self._path, cause=e) from e
            try:
                self._file.close()
            except OSError as e:
                raise ManifestWriteError(path=self._path, cause=e) from e

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def path(self) -> str:
        return self._path

    @property
    def closed(self) -> bool:
        return self._closed


__all__ = ["ManifestWriter"]
