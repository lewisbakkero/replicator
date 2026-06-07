"""`Drive_Importer` — pull-only flow from a Pull_Only_Source into the
configured `drive_destination` Replicated_Source.

This module owns the Drive → Replicated_Source pipeline. The Replicator
(`mcps.replication`) handles bidirectional S3 ↔ GCS replication; the
`Drive_Importer` is the one-way path that pulls files out of a Drive
folder and lands them in `drive_destination`. Drive itself is read-only
(req 10.8): the `GoogleDriveSourceAdapter` raises `ReadOnlySourceError`
on any mutating call, and this module is careful never to issue one.

Pipeline contract (req 10.1-10.10, design.md "Drive_Importer"):

1. **List + filter**. Walk `drive_adapter.list_objects()` (already
   paginated to exhaustion by the adapter, req 2.5 / 10.1). Apply the
   client-side mimeType allowlist:

   * `mimeType` starts with ``application/vnd.google-apps.`` →
     ``DRIVE_SKIP_NDOC`` Manifest entry, ``skip_native_doc`` += 1,
     continue (req 10.3).
   * `mimeType` does not start with ``image/`` or ``video/`` →
     ``DRIVE_SKIP_UNSUP`` Manifest entry, ``skip_unsupported`` += 1,
     continue (req 10.2).

2. **Stream-hash** the bytes via
   ``stream_sha256(drive_adapter.read_bytes(file_id))`` (req 10.4).

3. **Existence check** against any Replicated_Source. If the hash
   appears in a record whose ``source`` is a Replicated_Source name,
   emit ``DRIVE_SKIP_EXIST`` and continue (req 10.5).

4. **Build destination key** of the shape
   ``google-drive/<YYYY|unknown-year>/<MM|unknown-month>/<file-id>__<sanitised-name>``
   (req 10.6). The year/month components come from
   ``meta.user_metadata["createdTime"]``; on parse failure the
   importer emits ``DRIVE_WARN_TIME`` and substitutes
   ``unknown-year`` / ``unknown-month`` (req 10.7). The name is
   pulled from ``meta.user_metadata.get("drive_path")`` (the relative
   path the adapter recorded at listing time) and sanitised by
   replacing every byte outside ``[A-Za-z0-9._-]`` with ``_``. The
   file-id is the value of ``meta.user_metadata["drive_file_id"]``,
   falling back to ``meta.key`` (which the Drive adapter sets to the
   file id anyway).

5. **Write** the bytes through ``destination_adapter.write_bytes``,
   with ``mcps_metadata = {"mcps-source": <drive_source_name>,
   "mcps-content-sha256": <hash>, "mcps-replicated-at": <ISO-8601
   UTC seconds>}``. Setting ``mcps-source`` to the Drive Source's
   name means subsequent runs treat the destination copy as
   canonical and never try to "replicate it back" to Drive (which
   would fail anyway because Drive is read-only).

6. **Errors**. Any download or write exception is caught at the
   per-file level, recorded as a ``DRIVE_DOWNLOAD_E`` Manifest entry
   with the offending file id and the exception's repr, and the run
   continues with the next file (req 10.9).

`Drive_Importer.plan(...)` is the read-only counterpart used by the
Cold_Start `Reconciliation_Reporter` (task 27): it returns the count
of Drive files that would be imported under `--apply` (passing the
mimeType filter and whose hash is absent from every Replicated_Source).
The plan path also walks ``read_bytes`` to compute hashes; this matches
the Cold_Start cost the Reconciliation_Report estimates (req 18.5),
where every Drive file contributes its full byte size to the
``estimated_bytes_to_hash`` figure.

Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7,
10.8, 10.9, 10.10.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from mcps.catalog.model import Catalog
from mcps.hashing import stream_sha256
from mcps.manifest.model import Action, ManifestRecord, Result
from mcps.manifest.writer import ManifestWriter
from mcps.sources.base import ObjectMeta, SourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


MCPS_SOURCE_KEY = "mcps-source"
MCPS_CONTENT_SHA256_KEY = "mcps-content-sha256"
MCPS_REPLICATED_AT_KEY = "mcps-replicated-at"
"""User-metadata keys attached to the destination at import time.

Mirrors `mcps.replication`'s constants by name and by value so the
listing path on subsequent runs reads them through the same
priority chain (req 7.1)."""


_NATIVE_DOC_PREFIX = "application/vnd.google-apps."
"""Server-side mimeType prefix Drive uses for Docs/Sheets/Slides etc.
Anything matching this is skipped with ``DRIVE_SKIP_NDOC`` (req 10.3)."""


_SANITISE_RE = re.compile(r"[^A-Za-z0-9._-]")
"""Any byte outside ``[A-Za-z0-9._-]`` in the destination name is replaced
with ``_`` per req 10.6. The character class intentionally allows ``.``
``_`` and ``-`` so common image/video file names round-trip without
losing their visible structure (e.g. ``IMG_1234.JPG`` stays
``IMG_1234.JPG``)."""


# ---------------------------------------------------------------------------
# DriveImportStats
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DriveImportStats:
    """Counter struct returned by `DriveImporter.import_files`.

    Fields:

    * ``discovered``: total Drive files yielded by ``list_objects``
      this run, before any filtering.
    * ``imported``: count of files successfully written to
      ``drive_destination`` (one ``DRIVE_IMPORT_OK`` Manifest entry
      each).
    * ``skip_existing``: count of files whose hash was already present
      in some Replicated_Source (one ``DRIVE_SKIP_EXIST`` entry each,
      req 10.5).
    * ``skip_unsupported``: count of files filtered out for an
      unsupported mimeType (one ``DRIVE_SKIP_UNSUP`` entry each,
      req 10.2).
    * ``skip_native_doc``: count of files filtered out for a Google
      native-doc mimeType (one ``DRIVE_SKIP_NDOC`` entry each, req
      10.3).
    * ``download_error``: count of files that errored during
      hashing or writing (one ``DRIVE_DOWNLOAD_E`` entry each, req
      10.9).
    * ``warning_missing_created_time``: count of files for which
      ``createdTime`` could not be parsed (one ``DRIVE_WARN_TIME``
      entry each, req 10.7). Independent of ``imported`` — a file may
      both warn and import.
    """

    discovered: int = 0
    imported: int = 0
    skip_existing: int = 0
    skip_unsupported: int = 0
    skip_native_doc: int = 0
    download_error: int = 0
    warning_missing_created_time: int = 0


@dataclass
class _MutableStats:
    """Internal mutable counterpart of `DriveImportStats`.

    Kept private; ``DriveImporter.import_files`` returns a frozen
    `DriveImportStats` to its caller.
    """

    discovered: int = 0
    imported: int = 0
    skip_existing: int = 0
    skip_unsupported: int = 0
    skip_native_doc: int = 0
    download_error: int = 0
    warning_missing_created_time: int = 0

    def freeze(self) -> DriveImportStats:
        return DriveImportStats(
            discovered=self.discovered,
            imported=self.imported,
            skip_existing=self.skip_existing,
            skip_unsupported=self.skip_unsupported,
            skip_native_doc=self.skip_native_doc,
            download_error=self.download_error,
            warning_missing_created_time=self.warning_missing_created_time,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso_seconds(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ``YYYY-MM-DDTHH:MM:SSZ`` (UTC, second precision)."""
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_iso_ms(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ISO-8601 millisecond UTC with trailing Z."""
    dt = now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _parse_year_month(created_time: str) -> Optional[tuple[str, str]]:
    """Parse ``createdTime`` and return ``(YYYY, MM)`` or ``None`` on failure.

    Accepts the ISO-8601 forms Drive returns (``YYYY-MM-DDTHH:MM:SS.sssZ``
    most commonly) plus the ``+00:00``-style suffix Python's
    ``datetime.fromisoformat`` understands. ``None`` triggers the
    ``DRIVE_WARN_TIME`` Manifest entry and the ``unknown-year`` /
    ``unknown-month`` substitution (req 10.7).
    """
    if not created_time:
        return None
    text = created_time.strip()
    if not text:
        return None
    # ``fromisoformat`` doesn't natively handle a trailing ``Z`` until 3.11
    # in some shapes; normalise it here so the parser is consistent across
    # Python versions.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}", f"{dt.month:02d}"


def _sanitise_name(name: str) -> str:
    """Replace every byte outside ``[A-Za-z0-9._-]`` in ``name`` with ``_``.

    Returns ``""`` when ``name`` is empty or ``None``. The empty-string
    case is permitted by the destination-key regex (``[A-Za-z0-9._-]*``)
    even though most Drive files have a populated name; we tolerate it
    so a name comprised entirely of forbidden characters doesn't become
    a hard failure for the import.
    """
    if not name:
        return ""
    return _SANITISE_RE.sub("_", name)


def _name_from_path(drive_path: str) -> str:
    """Return the basename of ``drive_path``.

    The Drive adapter records ``user_metadata["drive_path"]`` as the
    relative path under the configured root (e.g.
    ``"vacations/IMG_1234.JPG"``). We slice off everything before the
    last ``/`` so the destination name is just the file name. If
    ``drive_path`` is empty or has no separator, the whole value is
    returned (which is itself the file name in that case).
    """
    if not drive_path:
        return ""
    return drive_path.rsplit("/", 1)[-1]


def _content_type_or_default(meta: ObjectMeta) -> Optional[str]:
    """Pass through `meta.content_type` unchanged.

    This helper exists so future changes (e.g. mapping the Drive
    mimeType to a different output content type) have one obvious
    place to live.
    """
    return meta.content_type


# ---------------------------------------------------------------------------
# DriveImporter
# ---------------------------------------------------------------------------


class DriveImporter:
    """Per-Sync_Run Drive → ``drive_destination`` importer.

    Constructor parameters (all keyword-only):

    * ``drive_adapter`` — read-only `SourceAdapter` for the Drive
      Source. Must surface ``mcps-source = drive_adapter.name`` only
      via the ``ObjectMeta`` it returns; the importer does not write
      to it.
    * ``destination_adapter`` — the writable `SourceAdapter` for
      ``drive_destination`` (kind ``s3`` or ``gcs``). The importer
      calls ``write_bytes`` on this adapter once per imported file.
    * ``drive_source_name`` — the configuration name of the Drive
      Source. Used as the value of the ``mcps-source`` user-metadata
      attached to every imported destination Object (req 10.6).
    * ``run_id`` — UUIDv4 hex shared by every Manifest entry the
      importer emits this run.
    * ``now`` — wall-clock callable returning a UTC ``datetime``. The
      importer stamps Manifest timestamps and the ``mcps-replicated-at``
      user-metadata via this clock so property tests remain
      deterministic.
    """

    def __init__(
        self,
        *,
        drive_adapter: SourceAdapter,
        destination_adapter: SourceAdapter,
        drive_source_name: str,
        run_id: str = "00000000",
        now: Callable[[], datetime] = _default_now,
    ) -> None:
        if not isinstance(drive_source_name, str) or not drive_source_name:
            raise ValueError("drive_source_name must be a non-empty str")
        self._drive_adapter = drive_adapter
        self._destination_adapter = destination_adapter
        self._drive_source_name = drive_source_name
        self._run_id = run_id
        self._now = now

    # ------------------------------------------------------------------
    # plan()
    # ------------------------------------------------------------------

    def plan(
        self,
        catalog: Catalog,
        replicated_source_names: tuple[str, ...],
    ) -> int:
        """Return the would-import count for the Reconciliation_Reporter.

        Walks ``drive_adapter.list_objects()`` and, for each file that
        passes the mimeType filter (req 10.2 / 10.3), computes its
        Content_Hash via ``stream_sha256(read_bytes(...))``. A file
        contributes to the count iff its hash is **not** present in
        any record whose ``source`` is in
        ``replicated_source_names``.

        ``plan(...)`` does not consult ``mcps-content-sha256`` user
        metadata for a shortcut: Drive does not round-trip mcps-*
        keys (the Drive adapter uses ``user_metadata`` only as a
        carrier for ``drive_file_id`` / ``drive_path`` /
        ``createdTime``), so the streaming hash is the only path
        that gives a correct answer for Cold_Start budgeting (req
        18.5).

        Errors during hashing are conservatively counted as
        *not-imported* — i.e. the file does not contribute to the
        plan total. The Reconciliation_Report's
        ``estimated_bytes_to_hash`` figure already accounts for the
        bytes spent attempting the hash; pretending a failed hash
        would have been imported would over-state the count.
        """
        existing_hashes = self._existing_replicated_hashes(
            catalog, replicated_source_names
        )

        would_import = 0
        for meta in self._drive_adapter.list_objects():
            mime = (meta.content_type or "").lower()
            if mime.startswith(_NATIVE_DOC_PREFIX):
                continue
            if not (mime.startswith("image/") or mime.startswith("video/")):
                continue
            try:
                content_hash = stream_sha256(
                    self._drive_adapter.read_bytes(meta.key)
                )
            except Exception:  # noqa: BLE001 — hashing is best-effort
                continue
            if content_hash in existing_hashes:
                continue
            would_import += 1

        return would_import

    # ------------------------------------------------------------------
    # import_files()
    # ------------------------------------------------------------------

    def import_files(
        self,
        catalog: Catalog,
        replicated_source_names: tuple[str, ...],
        *,
        manifest_writer: ManifestWriter,
    ) -> DriveImportStats:
        """Run the full import pipeline and return per-action counts.

        See module docstring for the full contract. This method does
        not raise: per-file failures are caught and recorded as
        ``DRIVE_DOWNLOAD_E`` Manifest entries (req 10.9), and the
        run continues with the next file. The only exceptions that
        escape are programming errors (e.g. a malformed
        ``ManifestWriter`` raising ``ManifestWriteError``); those
        propagate to the caller intentionally.
        """
        stats = _MutableStats()
        existing_hashes = self._existing_replicated_hashes(
            catalog, replicated_source_names
        )

        for meta in self._drive_adapter.list_objects():
            stats.discovered += 1
            mime = (meta.content_type or "").lower()

            # --- Filter step (req 10.2 / 10.3) ---------------------------
            if mime.startswith(_NATIVE_DOC_PREFIX):
                self._emit_skip_native_doc(manifest_writer, meta=meta)
                stats.skip_native_doc += 1
                continue
            if not (mime.startswith("image/") or mime.startswith("video/")):
                self._emit_skip_unsupported(manifest_writer, meta=meta)
                stats.skip_unsupported += 1
                continue

            # --- Hashing + existence check + write -----------------------
            self._import_one(
                meta=meta,
                existing_hashes=existing_hashes,
                manifest_writer=manifest_writer,
                stats=stats,
            )

        return stats.freeze()

    # ------------------------------------------------------------------
    # Per-file pipeline
    # ------------------------------------------------------------------

    def _import_one(
        self,
        *,
        meta: ObjectMeta,
        existing_hashes: frozenset[str],
        manifest_writer: ManifestWriter,
        stats: _MutableStats,
    ) -> None:
        """Hash, dedupe, and write a single Drive file.

        Wraps the entire body in a try/except so any provider-side
        error (transport failure, retries-exhausted, write
        failure) becomes a ``DRIVE_DOWNLOAD_E`` Manifest entry and
        the import loop continues (req 10.9).
        """
        # 1. Stream-hash the bytes (req 10.4).
        try:
            content_hash = stream_sha256(self._drive_adapter.read_bytes(meta.key))
        except Exception as exc:  # noqa: BLE001 — provider-mapped
            self._emit_download_error(
                manifest_writer,
                meta=meta,
                error=repr(exc),
            )
            stats.download_error += 1
            return

        # 2. Existence check (req 10.5).
        if content_hash in existing_hashes:
            self._emit_skip_existing(
                manifest_writer, meta=meta, content_hash=content_hash
            )
            stats.skip_existing += 1
            return

        # 3. Build destination key (req 10.6, 10.7).
        dst_key, time_warning = self._build_destination_key(meta=meta)
        if time_warning:
            self._emit_warning_missing_created_time(
                manifest_writer, meta=meta
            )
            stats.warning_missing_created_time += 1

        # 4. Stream-write to ``drive_destination`` (req 10.6).
        try:
            chunks = self._drive_adapter.read_bytes(meta.key)
            self._destination_adapter.write_bytes(
                dst_key,
                chunks,
                meta.size_bytes,
                _content_type_or_default(meta),
                {
                    MCPS_SOURCE_KEY: self._drive_source_name,
                    MCPS_CONTENT_SHA256_KEY: content_hash,
                    MCPS_REPLICATED_AT_KEY: _now_iso_seconds(self._now),
                },
            )
        except Exception as exc:  # noqa: BLE001 — provider-mapped
            self._emit_download_error(
                manifest_writer,
                meta=meta,
                error=repr(exc),
            )
            stats.download_error += 1
            return

        # 5. Success Manifest entry.
        self._emit_import_ok(
            manifest_writer,
            meta=meta,
            dst_key=dst_key,
            content_hash=content_hash,
        )
        stats.imported += 1

    # ------------------------------------------------------------------
    # Destination key construction
    # ------------------------------------------------------------------

    def _build_destination_key(
        self, *, meta: ObjectMeta
    ) -> tuple[str, bool]:
        """Return ``(dst_key, time_warning)`` for ``meta`` per req 10.6 / 10.7.

        ``time_warning`` is True when ``createdTime`` could not be
        parsed and the substitution ``unknown-year``/``unknown-month``
        was used. The caller emits ``DRIVE_WARN_TIME`` once per
        warning (req 10.7).

        The destination key components:

        * year / month from a successfully-parsed ``createdTime`` or
          the literal ``unknown-year`` / ``unknown-month`` strings.
        * file id from ``user_metadata["drive_file_id"]`` falling back
          to ``meta.key`` (which the Drive adapter sets to the file
          id by default).
        * sanitised name from
          ``basename(user_metadata.get("drive_path", "") or
          meta.key)`` after the ``[^A-Za-z0-9._-] -> _``
          substitution.
        """
        created_time = meta.user_metadata.get("createdTime", "")
        ym = _parse_year_month(created_time)
        if ym is None:
            year, month = "unknown-year", "unknown-month"
            time_warning = True
        else:
            year, month = ym
            time_warning = False

        file_id = meta.user_metadata.get("drive_file_id") or meta.key
        drive_path = meta.user_metadata.get("drive_path") or meta.key
        sanitised = _sanitise_name(_name_from_path(drive_path))

        dst_key = f"google-drive/{year}/{month}/{file_id}__{sanitised}"
        return dst_key, time_warning

    # ------------------------------------------------------------------
    # Catalog helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _existing_replicated_hashes(
        catalog: Catalog,
        replicated_source_names: tuple[str, ...],
    ) -> frozenset[str]:
        """Return every Content_Hash present in any Replicated_Source.

        A record contributes its ``content_hash`` iff its ``source``
        name appears in ``replicated_source_names`` and the record is
        not tombstoned/quarantined. Tombstoned and quarantined
        records are excluded so a Drive file matching a
        soft-deleted hash is allowed to import again — matching the
        intent of req 10.5 (the hash is "absent from every
        configured Replicated_Source" once the only live copy is
        gone).
        """
        replicated = set(replicated_source_names)
        hashes: set[str] = set()
        for rec in catalog.all_records():
            if rec.source not in replicated:
                continue
            if rec.tombstoned_at is not None or rec.quarantined_at is not None:
                continue
            hashes.add(rec.content_hash)
        return frozenset(hashes)

    # ------------------------------------------------------------------
    # Manifest emission helpers
    # ------------------------------------------------------------------

    def _emit_skip_native_doc(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_SKIP_NDOC,
                result=Result.SKIPPED,
                source=self._drive_source_name,
                key=meta.key,
                size_bytes=meta.size_bytes,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "mime_type": meta.content_type or "",
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )

    def _emit_skip_unsupported(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_SKIP_UNSUP,
                result=Result.SKIPPED,
                source=self._drive_source_name,
                key=meta.key,
                size_bytes=meta.size_bytes,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "mime_type": meta.content_type or "",
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )

    def _emit_skip_existing(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
        content_hash: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_SKIP_EXIST,
                result=Result.SKIPPED,
                source=self._drive_source_name,
                key=meta.key,
                content_hash=content_hash,
                size_bytes=meta.size_bytes,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )

    def _emit_warning_missing_created_time(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_WARN_TIME,
                result=Result.SKIPPED,
                source=self._drive_source_name,
                key=meta.key,
                size_bytes=meta.size_bytes,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "createdTime": meta.user_metadata.get("createdTime", ""),
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )

    def _emit_import_ok(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
        dst_key: str,
        content_hash: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_IMPORT_OK,
                result=Result.SUCCESS,
                source=self._drive_source_name,
                target=self._destination_adapter.name,
                key=dst_key,
                content_hash=content_hash,
                size_bytes=meta.size_bytes,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )

    def _emit_download_error(
        self,
        manifest_writer: ManifestWriter,
        *,
        meta: ObjectMeta,
        error: str,
    ) -> None:
        manifest_writer.append(
            ManifestRecord(
                timestamp=_now_iso_ms(self._now),
                run_id=self._run_id,
                action=Action.DRIVE_DOWNLOAD_E,
                result=Result.ERROR,
                source=self._drive_source_name,
                key=meta.key,
                size_bytes=meta.size_bytes,
                error=error,
                extra={
                    "drive_file_id": meta.user_metadata.get(
                        "drive_file_id", meta.key
                    ),
                    "drive_path": meta.user_metadata.get("drive_path", ""),
                },
            )
        )


__all__ = [
    "MCPS_SOURCE_KEY",
    "MCPS_CONTENT_SHA256_KEY",
    "MCPS_REPLICATED_AT_KEY",
    "DriveImportStats",
    "DriveImporter",
]
