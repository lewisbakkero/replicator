"""mcps command-line interface.

This module is the entry point referenced by `pyproject.toml`'s console
script (``mcps = mcps.cli:main``). It wires together every component
built in tasks 2-29 into a single Sync_Run pipeline:

    detect_legacy_config -> Config_Parser -> Credential_Manager
    -> writer_lock -> Catalog_Parser -> Cold_Start decision
    -> list every Source -> (Reconciliation_Reporter on Cold_Start)
    -> Duplicate_Resolver -> Replicator -> Drive_Importer
    -> Inconsistency_Detector -> Catalog_Printer -> SUMMARY

The CLI is a single-verb argparse surface (``mcps [...flags...]``) per
design.md. The module exposes three public callables:

* ``parse_args(argv) -> argparse.Namespace`` — argparse construction +
  parse.
* ``run(args, ...) -> int`` — pipeline orchestration. Pure of process
  effects beyond logging and FS; tests inject ``adapter_factory`` /
  ``credential_manager`` / ``now`` to bypass real provider SDKs.
* ``main(argv=None) -> int`` — top-level entry point used by the
  console script. Translates :class:`mcps.errors.McpsError` subclasses
  into the exit codes documented in design.md ("CLI Surface > Exit
  codes").

Validates: Requirements 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 18.3,
18.4, 18.6, 18.7, 19.1, 19.2, 19.3, plus the McpsError -> exit-code
table.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, TextIO

from .catalog.model import Catalog, ObjectRecord
from .catalog.parser import parse_catalog_file
from .catalog.printer import write_catalog
from .concurrency import writer_lock
from .config.model import Config, SourceConfig
from .config.parser import default_config_path, parse_config_file
from .credentials import Credential_Manager, ResolvedCredentials
from .drive_import import DriveImporter
from .duplicates.detector import detect_duplicates
from .duplicates.resolver import (
    DuplicateResolver,
    pick_canonical,
)
from .errors import (
    ColdStartListingFailed,
    ExitCode,
    LegacyConfigDetected,
    McpsError,
)
from .hashing import compute_content_hash
from .logging_setup import bind_run_id, setup_logging
from .manifest.model import Action, ManifestRecord, Result
from .manifest.writer import ManifestWriter
from .reconciliation import (
    Inconsistency_Detector,
    Reconciliation_Reporter,
)
from .replication import Replicator
from .sources.base import SourceAdapter

__all__ = [
    "detect_legacy_config",
    "parse_args",
    "run",
    "main",
]


# ===========================================================================
# Legacy config detection (preserved from earlier task 12 implementation)
# ===========================================================================

_LEGACY_CONFIG_FILENAME = "config.ini"
_LEGACY_AWS_SECTION = "aws_credentials"
_LEGACY_PLAINTEXT_KEYS = frozenset({"aws_access_key_id", "aws_secret_access_key"})


def _strip_inline_comment(line: str) -> str:
    """Drop an inline ``;`` or ``#`` comment from an INI line.

    `configparser` only treats `;` and `#` as comments at the start of
    a line, but real-world `config.ini` files often have inline
    comments after key names. We strip them defensively so a key like
    ``aws_access_key_id ; old`` is still recognised as
    ``aws_access_key_id``.
    """
    for sep in (";", "#"):
        idx = line.find(sep)
        if idx >= 0:
            line = line[:idx]
    return line


def _extract_section_keys(path: str) -> dict[str, set[str]]:
    """Return a mapping of `section_name -> set of option names` for ``path``.

    Only section headers (``[name]``) and option-name positions are
    read; option *values* are never inspected, never stored, and never
    returned, so this helper cannot leak credential material.
    """
    sections: dict[str, set[str]] = {}
    current: Optional[str] = None

    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\r\n")
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith(";") or stripped.startswith("#"):
                continue
            if stripped.startswith("[") and stripped.endswith("]"):
                section_name = stripped[1:-1].strip()
                if section_name:
                    current = section_name
                    sections.setdefault(current, set())
                continue
            # Continuation line for a multi-line value: leading whitespace.
            if line and line[0] in (" ", "\t"):
                continue
            if current is None:
                continue
            without_inline_comment = _strip_inline_comment(stripped)
            sep_positions = [
                pos
                for pos in (
                    without_inline_comment.find("="),
                    without_inline_comment.find(":"),
                )
                if pos >= 0
            ]
            if not sep_positions:
                continue
            key = without_inline_comment[: min(sep_positions)].strip()
            if key:
                sections[current].add(key)

    return sections


def detect_legacy_config(cwd: str) -> None:
    """Detect a legacy plaintext-credential ``config.ini`` and refuse to start.

    Validates: Requirement 1.5.
    """
    path = os.path.join(cwd, _LEGACY_CONFIG_FILENAME)
    if not os.path.isfile(path):
        return None

    sections = _extract_section_keys(path)
    aws_keys = sections.get(_LEGACY_AWS_SECTION)
    if aws_keys is None:
        return None

    if aws_keys & _LEGACY_PLAINTEXT_KEYS:
        raise LegacyConfigDetected(path)

    return None


# ===========================================================================
# Argparse
# ===========================================================================


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Build the argparse surface and parse ``argv``.

    Mode flags ``--dry-run`` and ``--apply`` are mutually exclusive
    (req 13.1). When neither is supplied the resolver defaults to
    ``--dry-run`` and emits a stderr warning (handled in ``run``, not
    here, so the warning appears on the same stream as the rest of
    the run's logs).
    """
    parser = argparse.ArgumentParser(
        prog="mcps",
        description=(
            "MultiCloud Photo Sync — deduplicated bidirectional "
            "replication between AWS S3 and Google Cloud Storage, "
            "plus pull-only Google Drive import."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to the configuration file (default: ./mcps.config.yaml).",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Plan only — never modify, tag, or delete any Object. "
            "Mutually exclusive with --apply."
        ),
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Execute planned actions against the configured Sources. "
            "Mutually exclusive with --dry-run."
        ),
    )

    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help=(
            "Bypass the interactive quarantine confirmation. Required "
            "for non-interactive (cron / systemd) Apply runs."
        ),
    )
    parser.add_argument(
        "--first-pass-confirmed",
        action="store_true",
        help=(
            "Authorise destructive actions on a Cold_Start --apply "
            "run after reviewing the Reconciliation_Report. No-op on "
            "non-Cold_Start runs."
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARN", "WARNING", "ERROR"],
        default="INFO",
        help="Logger level (default: INFO).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Override the run id (default: a fresh UUID4 hex).",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Override runtime.catalog_path from the configuration.",
    )
    parser.add_argument(
        "--manifest-dir",
        default=None,
        help="Override runtime.manifest_dir from the configuration.",
    )
    parser.add_argument(
        "--lock-path",
        default=None,
        help=(
            "Override runtime.lock_path. Defaults to "
            "<catalog_path>.lock when neither configuration nor flag "
            "supplies one."
        ),
    )
    return parser.parse_args(argv)


# ===========================================================================
# Run
# ===========================================================================


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso_seconds(now: Callable[[], datetime]) -> str:
    """Render ``now()`` as ``YYYY-MM-DDTHH:MM:SSZ`` (UTC)."""
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


def _compact_timestamp(iso_seconds: str) -> str:
    """``YYYY-MM-DDTHH:MM:SSZ`` -> ``YYYYMMDDTHHMMSSZ``."""
    return iso_seconds.replace("-", "").replace(":", "")


def _manifest_filename(started_at_iso: str, run_id: str) -> str:
    """Build the per-run Manifest filename per design.md req 14.1."""
    return f"manifest-{_compact_timestamp(started_at_iso)}-{run_id}.jsonl"


def _build_default_adapter(
    src: SourceConfig,
    *,
    aws_credentials: Optional[ResolvedCredentials],
    gcp_credentials: Optional[ResolvedCredentials],
    drive_credentials: Optional[ResolvedCredentials],
) -> SourceAdapter:
    """Construct a real provider adapter for ``src``.

    Imports are lazy so the property tests (which never reach this
    function because they pass an ``adapter_factory``) do not pull in
    the heavy SDKs just to import :mod:`mcps.cli`.
    """
    if src.kind == "s3":
        from .sources.s3 import S3SourceAdapter  # lazy import

        kwargs: dict[str, Any] = {
            "name": src.name,
            "bucket": src.bucket,
            "prefix": src.prefix,
            "region": src.region,
        }
        if aws_credentials is not None and aws_credentials.boto3_session is not None:
            kwargs["boto3_session"] = aws_credentials.boto3_session
        return S3SourceAdapter(**kwargs)

    if src.kind == "gcs":
        from .sources.gcs import GCSSourceAdapter  # lazy import

        return GCSSourceAdapter(
            name=src.name,
            bucket=src.bucket,
            prefix=src.prefix,
        )

    if src.kind == "google_drive":
        from .sources.drive import GoogleDriveSourceAdapter  # lazy import

        creds = drive_credentials.google_credentials if drive_credentials else None
        return GoogleDriveSourceAdapter(
            name=src.name,
            drive_root_folder_id=src.drive_root_folder_id,
            credentials=creds,
        )

    raise ValueError(f"unknown source kind: {src.kind!r}")


def _resolve_credentials(
    config: Config, credential_manager: Credential_Manager
) -> tuple[
    Optional[ResolvedCredentials],
    Optional[ResolvedCredentials],
    Optional[ResolvedCredentials],
]:
    """Resolve credentials for every kind referenced by ``config.sources``."""
    needed = {s.kind for s in config.sources}
    aws_creds: Optional[ResolvedCredentials] = None
    gcp_creds: Optional[ResolvedCredentials] = None
    drive_creds: Optional[ResolvedCredentials] = None
    if "s3" in needed:
        aws_creds = credential_manager.resolve_aws()
    if "gcs" in needed:
        gcp_creds = credential_manager.resolve_gcp()
    if "google_drive" in needed:
        drive_creds = credential_manager.resolve_drive()
    return aws_creds, gcp_creds, drive_creds


def _build_object_records(
    config: Config,
    adapters: Mapping[str, SourceAdapter],
    catalog_at_start: Catalog,
    *,
    now: Callable[[], datetime],
    cold_start: bool,
    manifest_writer: ManifestWriter,
    run_id: str,
    bytes_streamed_for: set[tuple[str, str]],
) -> list[ObjectRecord]:
    """List every Source and resolve each Object's Content_Hash.

    On listing failure we either skip-and-continue (non-Cold_Start,
    req 2.7) by appending a LIST_ERROR Manifest entry, or abort the
    whole run with `ColdStartListingFailed` (req 18.6) on Cold_Start.

    ``bytes_streamed_for`` is mutated as a side effect: every
    ``(source_name, key)`` for which `compute_content_hash` had to
    stream the bytes (no `mcps-content-sha256` shortcut and no Catalog
    cache hit) is added so the Reconciliation_Reporter's
    ``estimated_bytes_to_hash`` figure accurately reflects the
    Cold_Start cost (req 18.5).
    """
    last_seen = _now_iso_seconds(now)
    records: list[ObjectRecord] = []

    for src in config.sources:
        adapter = adapters[src.name]
        try:
            metas = list(adapter.list_objects())
        except Exception as exc:  # noqa: BLE001 - provider-mapped
            if cold_start:
                # req 18.6: abort the Cold_Start without producing the
                # Reconciliation_Report. The CLI's outer error handler
                # maps this to exit code 77.
                raise ColdStartListingFailed(
                    source_name=src.name,
                    source_kind=src.kind,
                    cause=exc,
                ) from exc
            # Non-Cold_Start: emit a LIST_ERROR Manifest entry and
            # skip the rest of this Source (req 2.7).
            manifest_writer.append(
                ManifestRecord(
                    timestamp=_now_iso_ms(now),
                    run_id=run_id,
                    action=Action.LIST_ERROR,
                    result=Result.ERROR,
                    source=src.name,
                    error=repr(exc),
                )
            )
            continue

        for meta in metas:
            try:
                # The hash priority chain has three steps; we record
                # whether step 3 (streaming) was used by checking the
                # value's source. For the ``estimated_bytes_to_hash``
                # estimator we only need the per-record decision, so
                # we re-derive it here (cheap: dict lookup + length /
                # charset check) rather than threading a second
                # return value through ``compute_content_hash``.
                from_metadata = meta.user_metadata.get("mcps-content-sha256", "")
                from_metadata_valid = (
                    isinstance(from_metadata, str)
                    and len(from_metadata) == 64
                    and all(c in "0123456789abcdef" for c in from_metadata)
                )
                cache_hit = (
                    None
                    if from_metadata_valid
                    else catalog_at_start.cache_lookup(
                        src.name,
                        meta.key,
                        meta.size_bytes,
                        meta.last_modified,
                    )
                )
                content_hash = compute_content_hash(
                    adapter, meta, catalog_at_start
                )
                streamed = not from_metadata_valid and cache_hit is None
            except Exception as exc:  # noqa: BLE001 - provider-mapped
                # Per-Object hash failure: emit HASH_ERROR and skip
                # the record (req 2.8).
                manifest_writer.append(
                    ManifestRecord(
                        timestamp=_now_iso_ms(now),
                        run_id=run_id,
                        action=Action.HASH_ERROR,
                        result=Result.ERROR,
                        source=src.name,
                        key=meta.key,
                        size_bytes=meta.size_bytes,
                        error=repr(exc),
                    )
                )
                continue

            if streamed:
                bytes_streamed_for.add((src.name, meta.key))

            records.append(
                ObjectRecord(
                    source=src.name,
                    key=meta.key,
                    content_hash=content_hash,
                    size_bytes=meta.size_bytes,
                    last_seen_at=last_seen,
                    last_modified=meta.last_modified,
                    content_type=meta.content_type,
                    quarantined_at=meta.user_metadata.get(
                        "mcps-quarantined-at"
                    ),
                    tombstoned_at=meta.user_metadata.get(
                        "mcps-tombstoned-at"
                    ),
                    mcps_source_meta=meta.user_metadata.get(
                        "mcps-source"
                    ),
                )
            )

    return records


def _replication_error_hashes(manifest_path: str) -> frozenset[str]:
    """Read the just-written Manifest and return REPLICATION_ERROR hashes."""
    from .manifest.parser import parse_manifest_file  # lazy

    if not os.path.isfile(manifest_path):
        return frozenset()
    records, _errors = parse_manifest_file(manifest_path)
    return frozenset(
        r.content_hash
        for r in records
        if r.action == Action.REPLICATION_ERROR and r.content_hash
    )


def _build_canonical_for(
    records: list[ObjectRecord],
    canonical_source_priority: tuple[str, ...],
    replicated_source_names: frozenset[str],
) -> Callable[[ObjectRecord], str]:
    """Return a ``record -> canonical_key`` callable applying req 5.1.

    For each Content_Hash present in the observed records, we precompute
    the canonical pick using the same tie-break used by the
    Duplicate_Resolver. The returned closure looks up the canonical
    record for ``record.content_hash`` and returns its key.
    """
    by_hash: dict[str, list[ObjectRecord]] = {}
    for r in records:
        if r.source not in replicated_source_names:
            continue
        by_hash.setdefault(r.content_hash, []).append(r)

    canonical_keys: dict[str, str] = {}
    for content_hash, group in by_hash.items():
        # Synthesise a single-hash group for `pick_canonical`. The
        # detector's `DuplicateGroup` requires a label/total_size; we
        # only need the canonical-pick output, so build the bare
        # minimum.
        from .duplicates.detector import DuplicateGroup  # lazy

        members = tuple(sorted(group, key=lambda r: (r.source, r.key)))
        synthetic = DuplicateGroup(
            content_hash=content_hash,
            members=members,
            label=("cross-source" if len({m.source for m in members}) >= 2 else "same-source"),
            total_size_bytes=sum(m.size_bytes for m in members if m.size_bytes >= 0),
        )
        choice = pick_canonical(
            synthetic,
            canonical_source_priority=canonical_source_priority,
        )
        canonical_keys[content_hash] = choice.canonical.key

    def _canonical_for(record: ObjectRecord) -> str:
        return canonical_keys.get(record.content_hash, record.key)

    return _canonical_for


# ---------------------------------------------------------------------------
# run() — the orchestration entry point
# ---------------------------------------------------------------------------


def run(
    args: argparse.Namespace,
    *,
    env: Optional[Mapping[str, str]] = None,
    stderr: Optional[TextIO] = None,
    stdout: Optional[TextIO] = None,
    cwd: Optional[str] = None,
    adapter_factory: Optional[Callable[[SourceConfig], SourceAdapter]] = None,
    credential_manager: Optional[Credential_Manager] = None,
    now: Optional[Callable[[], datetime]] = None,
) -> int:
    """Run one Sync_Run end-to-end and return the process exit code.

    Test seams (all keyword-only):

    * ``adapter_factory(src)`` — invoked once per `SourceConfig` to
      build the adapter for that Source. Defaults to a closure over
      `_build_default_adapter` that uses the resolved AWS / GCP /
      Drive credentials. Property tests pass a closure that returns
      `FakeSourceAdapter` instances so the run never touches the
      network.
    * ``credential_manager`` — defaults to a fresh
      :class:`Credential_Manager`. Tests can pass a stub that
      bypasses credential resolution entirely.
    * ``now`` — wall-clock callable returning a UTC ``datetime``.
      Defaults to :func:`datetime.now(timezone.utc)`.

    Returns the process exit code:

    * 0 (`OK`) — successful run, no errors.
    * 76 (`FIRST_PASS_REVIEW_REQUIRED`) — Cold_Start ``--apply``
      without ``--first-pass-confirmed`` (req 18.3).
    * 78 (`INCONSISTENCY_DETECTED`) — divergent hashes observed and
      ``replication.fail_on_inconsistency=true`` (req 19.3).
    * Any other `McpsError`'s ``exit_code`` when the run aborts before
      completion. The translation is performed by :func:`main`; ``run``
      itself raises `McpsError` for the caller to handle.
    """
    if env is None:
        env = os.environ
    if stderr is None:
        stderr = sys.stderr
    if stdout is None:
        stdout = sys.stdout
    if cwd is None:
        cwd = os.getcwd()
    if now is None:
        now = _default_now

    # --- Step 0: resolve dry-run/apply mode (req 13.1) -----------------
    dry_run: bool = bool(args.dry_run)
    apply_mode: bool = bool(args.apply)
    if not dry_run and not apply_mode:
        print(
            "warning: neither --dry-run nor --apply supplied; "
            "defaulting to --dry-run",
            file=stderr,
        )
        dry_run = True

    # --- Step 1: legacy config.ini detection (req 1.5) ------------------
    detect_legacy_config(cwd)

    # --- Step 2: parse the configuration -------------------------------
    config_path = args.config or default_config_path(cwd)
    config, _config_format = parse_config_file(config_path)

    # CLI overrides for runtime paths (req 13.1 flags --catalog, etc.).
    catalog_path: str = args.catalog or config.runtime.catalog_path
    manifest_dir: str = args.manifest_dir or config.runtime.manifest_dir
    lock_path: str = (
        args.lock_path
        or config.runtime.lock_path
        or f"{catalog_path}.lock"
    )

    # --- Step 3: credentials -------------------------------------------
    cm = credential_manager if credential_manager is not None else Credential_Manager()
    aws_creds, gcp_creds, drive_creds = _resolve_credentials(config, cm)

    # --- Step 4: run id + logging --------------------------------------
    run_id = args.run_id or uuid.uuid4().hex
    started_at_iso = _now_iso_seconds(now)
    logger = setup_logging(level=args.log_level)

    # Default adapter factory closes over the resolved creds; tests
    # override it with a closure returning FakeSourceAdapter.
    if adapter_factory is None:

        def _factory(src: SourceConfig) -> SourceAdapter:
            return _build_default_adapter(
                src,
                aws_credentials=aws_creds,
                gcp_credentials=gcp_creds,
                drive_credentials=drive_creds,
            )

        adapter_factory = _factory

    with bind_run_id(run_id):
        return _run_locked(
            args=args,
            config=config,
            run_id=run_id,
            started_at_iso=started_at_iso,
            catalog_path=catalog_path,
            manifest_dir=manifest_dir,
            lock_path=lock_path,
            dry_run=dry_run,
            apply_mode=apply_mode,
            adapter_factory=adapter_factory,
            stdout=stdout,
            stderr=stderr,
            now=now,
            logger=logger,
        )


def _run_locked(
    *,
    args: argparse.Namespace,
    config: Config,
    run_id: str,
    started_at_iso: str,
    catalog_path: str,
    manifest_dir: str,
    lock_path: str,
    dry_run: bool,
    apply_mode: bool,
    adapter_factory: Callable[[SourceConfig], SourceAdapter],
    stdout: TextIO,
    stderr: TextIO,
    now: Callable[[], datetime],
    logger: logging.Logger,
) -> int:
    """The post-lock body of :func:`run`. Split out so ``run`` stays small."""
    # --- Step 5: acquire the writer lock (req 16.5) --------------------
    with writer_lock(lock_path, run_id):
        # --- Step 6: load Catalog (req 3.5, 3.6) -----------------------
        try:
            catalog_at_start = parse_catalog_file(catalog_path)
        except FileNotFoundError:
            # Missing file → empty Catalog (req 3.5).
            catalog_at_start = Catalog()

        # --- Step 7: Cold_Start determination ------------------------
        cold_start = not any(catalog_at_start.all_records())

        # Non-Cold_Start with --first-pass-confirmed: WARN + treat as
        # no-op (req 18.7). The flag is still parsed but ignored.
        if not cold_start and args.first_pass_confirmed:
            logger.warning(
                "first-pass-confirmed ignored on non-cold-start run",
                extra={"event": "mcps.cli.first_pass_ignored"},
            )

        # --- Step 8: open the Manifest --------------------------------
        os.makedirs(manifest_dir, exist_ok=True)
        manifest_path = os.path.join(
            manifest_dir, _manifest_filename(started_at_iso, run_id)
        )

        # Build adapters once.
        adapters: dict[str, SourceAdapter] = {}
        for src in config.sources:
            adapters[src.name] = adapter_factory(src)

        replicated_names = tuple(
            s.name for s in config.replicated_sources()
        )

        with ManifestWriter(manifest_path) as manifest_writer:
            # --- Step 9: list every Source --------------------------
            bytes_streamed_for: set[tuple[str, str]] = set()
            object_records = _build_object_records(
                config,
                adapters,
                catalog_at_start,
                now=now,
                cold_start=cold_start,
                manifest_writer=manifest_writer,
                run_id=run_id,
                bytes_streamed_for=bytes_streamed_for,
            )

            # Build the working Catalog for downstream consumers.
            working_catalog = Catalog()
            for r in object_records:
                working_catalog = working_catalog.upsert(r)

            # --- Step 10: Cold_Start Reconciliation_Reporter -------
            # On Cold_Start we also need the would-import count for
            # the Reconciliation_Report. We compute it lazily.
            if cold_start:
                _emit_cold_start_report(
                    config=config,
                    adapters=adapters,
                    object_records=object_records,
                    working_catalog=working_catalog,
                    bytes_streamed_for=bytes_streamed_for,
                    run_id=run_id,
                    started_at_iso=started_at_iso,
                    stdout=stdout,
                    manifest_dir=manifest_dir,
                )

            # --- Step 11: Cold_Start two-step Apply gate -----------
            # Cold_Start --apply WITHOUT --first-pass-confirmed:
            # destructive_writes_allowed=False; suppress quarantine /
            # physical-delete entirely; then exit 76 (req 18.3).
            destructive_allowed = True
            suppress_quarantine = False
            cold_start_unconfirmed_apply = False
            if cold_start and apply_mode and not args.first_pass_confirmed:
                destructive_allowed = False
                suppress_quarantine = True
                cold_start_unconfirmed_apply = True

            # --- Step 12: Duplicate_Resolver ---------------------
            if not dry_run and not suppress_quarantine:
                resolver = DuplicateResolver(
                    adapters=adapters,
                    canonical_source_priority=config.duplicates.canonical_source_priority,
                    quarantine_retention_days=config.duplicates.quarantine_retention_days,
                    run_id=run_id,
                    now=now,
                    confirm=lambda count, total_bytes: True
                    if args.auto_approve
                    else _interactive_confirm(count, total_bytes, stderr),
                )
                detection = detect_duplicates(working_catalog)
                removals = resolver.plan_removals(detection)
                resolver.quarantine(
                    removals,
                    catalog=working_catalog,
                    manifest_writer=manifest_writer,
                    dry_run=False,
                    auto_approve=args.auto_approve,
                    isatty=sys.stdin.isatty(),
                )
                resolver.physically_delete_expired(
                    working_catalog,
                    manifest_writer=manifest_writer,
                )
            elif dry_run:
                resolver = DuplicateResolver(
                    adapters=adapters,
                    canonical_source_priority=config.duplicates.canonical_source_priority,
                    quarantine_retention_days=config.duplicates.quarantine_retention_days,
                    run_id=run_id,
                    now=now,
                )
                detection = detect_duplicates(working_catalog)
                removals = resolver.plan_removals(detection)
                resolver.quarantine(
                    removals,
                    catalog=working_catalog,
                    manifest_writer=manifest_writer,
                    dry_run=True,
                    auto_approve=True,
                    isatty=False,
                )

            # --- Step 13: Replicator ----------------------------
            if apply_mode:
                replicator = Replicator(
                    adapters=adapters,
                    canonical_source_priority=config.duplicates.canonical_source_priority,
                    on_key_conflict=config.replication.on_key_conflict,
                    fail_on_conflict=config.replication.fail_on_conflict,
                    destructive_writes_allowed=destructive_allowed,
                    delete_propagation=config.replication.delete_propagation,
                    tombstone_retention_days=config.replication.tombstone_retention_days,
                    run_id=run_id,
                    now=now,
                )
                plan = replicator.plan(
                    working_catalog,
                    replicated_source_names=replicated_names,
                )
                replicator.replicate(plan, manifest_writer=manifest_writer)

            # --- Step 14: Drive_Importer ------------------------
            if apply_mode and config.photos.drive_source and config.photos.drive_destination:
                drive_adapter = adapters.get(config.photos.drive_source)
                dst_adapter = adapters.get(config.photos.drive_destination)
                if drive_adapter is not None and dst_adapter is not None:
                    importer = DriveImporter(
                        drive_adapter=drive_adapter,
                        destination_adapter=dst_adapter,
                        drive_source_name=config.photos.drive_source,
                        run_id=run_id,
                        now=now,
                    )
                    importer.import_files(
                        working_catalog,
                        replicated_names,
                        manifest_writer=manifest_writer,
                    )

            # Replication may have just rewritten the working catalog
            # state on disk on each adapter; re-list the sources so
            # the Inconsistency_Detector sees the post-replication
            # state. We tolerate listing failures here as a
            # post-replication best-effort (a failure here does not
            # invalidate the run's writes).
            post_replication_records = _list_post_replication(
                config, adapters
            )

            # --- Step 15: Inconsistency_Detector ----------------
            replication_errors = _replication_error_hashes(manifest_path)
            replicated_set = frozenset(replicated_names)
            canonical_for = _build_canonical_for(
                post_replication_records,
                config.duplicates.canonical_source_priority,
                replicated_set,
            )
            detector = Inconsistency_Detector()
            inconsistency_report = detector.analyse(
                catalog_at_start=catalog_at_start,
                object_records_observed=post_replication_records,
                replicated_source_names=replicated_set,
                replication_error_hashes=replication_errors,
                canonical_for=canonical_for,
            )
            inconsistency_exit_addend = detector.emit(
                inconsistency_report,
                manifest_writer=manifest_writer,
                logger=logger,
                fail_on_inconsistency=config.replication.fail_on_inconsistency,
            )

            # --- Step 16: SUMMARY Manifest entry ---------------
            manifest_writer.append(
                ManifestRecord(
                    timestamp=_now_iso_ms(now),
                    run_id=run_id,
                    action=Action.SUMMARY,
                    result=Result.SUCCESS,
                    extra={
                        "discovered": str(len(object_records)),
                        "cold_start": "true" if cold_start else "false",
                        "dry_run": "true" if dry_run else "false",
                        "apply": "true" if apply_mode else "false",
                        "first_pass_confirmed": "true"
                        if args.first_pass_confirmed
                        else "false",
                        "divergent_hashes_count": str(
                            len(inconsistency_report.divergent_hashes)
                        ),
                    },
                )
            )

        # --- Step 17: persist Catalog ---------------------------
        # The on-disk Catalog reflects the working Catalog (which the
        # Replicator + Duplicate_Resolver may have logically updated).
        # In a richer implementation we would propagate
        # quarantined_at / tombstoned_at into the records; for now we
        # write the working catalog as-listed plus any explicit
        # markers we already captured at listing time.
        write_catalog(working_catalog, catalog_path)

    # --- Step 18: exit-code computation -----------------------
    if cold_start_unconfirmed_apply:
        return int(ExitCode.FIRST_PASS_REVIEW_REQUIRED)
    if inconsistency_exit_addend:
        return int(ExitCode.INCONSISTENCY_DETECTED)
    return int(ExitCode.OK)


def _emit_cold_start_report(
    *,
    config: Config,
    adapters: Mapping[str, SourceAdapter],
    object_records: list[ObjectRecord],
    working_catalog: Catalog,
    bytes_streamed_for: set[tuple[str, str]],
    run_id: str,
    started_at_iso: str,
    stdout: TextIO,
    manifest_dir: str,
) -> None:
    """Build the Reconciliation_Report and emit it to stdout + disk."""
    # Compute duplicate groups over the freshly-observed Catalog so
    # the Reporter can count same-source / cross-source duplicates.
    detection = detect_duplicates(working_catalog)

    # Drive_Importer would-import count (req 18.1 (e)).
    drive_would_import = 0
    if config.photos.drive_source and config.photos.drive_destination:
        drive_adapter = adapters.get(config.photos.drive_source)
        dst_adapter = adapters.get(config.photos.drive_destination)
        if drive_adapter is not None and dst_adapter is not None:
            try:
                importer = DriveImporter(
                    drive_adapter=drive_adapter,
                    destination_adapter=dst_adapter,
                    drive_source_name=config.photos.drive_source,
                )
                replicated_names = tuple(
                    s.name for s in config.replicated_sources()
                )
                drive_would_import = importer.plan(
                    working_catalog, replicated_names
                )
            except Exception:  # noqa: BLE001 — best effort
                drive_would_import = 0

    source_kinds = {s.name: s.kind for s in config.sources}

    def _streamed(rec: ObjectRecord) -> bool:
        return (rec.source, rec.key) in bytes_streamed_for

    reporter = Reconciliation_Reporter()
    report = reporter.build(
        catalog_at_start=Catalog(),  # Cold_Start: empty by definition
        object_records=object_records,
        duplicate_groups=detection.groups,
        drive_would_import_count=drive_would_import,
        bytes_to_hash_estimator=_streamed,
        run_id=run_id,
        started_at=started_at_iso,
        source_kinds=source_kinds,
    )
    reporter.emit(report, stdout=stdout, manifest_dir=manifest_dir)


def _list_post_replication(
    config: Config,
    adapters: Mapping[str, SourceAdapter],
) -> list[ObjectRecord]:
    """Re-list every Source after replication for the Inconsistency_Detector.

    Failures are tolerated: a Source that fails to list at this
    second pass simply contributes no records, which is the same as
    "we could not observe drift for this Source on this run". The
    primary listing already produced the LIST_ERROR entry that
    accounts for the failure.
    """
    records: list[ObjectRecord] = []
    last_seen = _now_iso_seconds(_default_now)
    for src in config.sources:
        adapter = adapters.get(src.name)
        if adapter is None:
            continue
        try:
            metas = list(adapter.list_objects())
        except Exception:  # noqa: BLE001
            continue
        for meta in metas:
            content_hash = meta.user_metadata.get("mcps-content-sha256")
            if not (
                isinstance(content_hash, str)
                and len(content_hash) == 64
                and all(c in "0123456789abcdef" for c in content_hash)
            ):
                # No valid metadata-stored hash; skip — the
                # Inconsistency_Detector compares hash sets and can
                # only do so with a known hash.
                continue
            records.append(
                ObjectRecord(
                    source=src.name,
                    key=meta.key,
                    content_hash=content_hash,
                    size_bytes=meta.size_bytes,
                    last_seen_at=last_seen,
                    last_modified=meta.last_modified,
                    content_type=meta.content_type,
                    quarantined_at=meta.user_metadata.get(
                        "mcps-quarantined-at"
                    ),
                    tombstoned_at=meta.user_metadata.get(
                        "mcps-tombstoned-at"
                    ),
                    mcps_source_meta=meta.user_metadata.get(
                        "mcps-source"
                    ),
                )
            )
    return records


def _interactive_confirm(
    count: int, total_bytes: int, stderr: TextIO
) -> bool:
    """Prompt the operator for quarantine confirmation.

    Returns True iff the operator types ``y`` or ``yes`` (case
    insensitive). Used by the Duplicate_Resolver when ``--apply`` is
    selected without ``--auto-approve`` and stdin is a terminal
    (req 5.5).
    """
    print(
        f"About to quarantine {count} object(s) totalling {total_bytes} bytes.",
        file=stderr,
    )
    print("Continue? [y/N]: ", end="", file=stderr)
    try:
        response = input().strip().lower()
    except EOFError:
        return False
    return response in ("y", "yes")


# ===========================================================================
# main()
# ===========================================================================


def main(argv: Optional[list[str]] = None) -> int:
    """Top-level CLI entry point.

    Translates :class:`McpsError` subclasses into exit codes per the
    table in design.md ("CLI Surface > Exit codes"). Argparse-level
    failures (``--help``, mutually-exclusive group violation) reach
    this function as ``SystemExit``; we let them propagate so the
    smoke test can assert on the SystemExit code.

    The ``doctor`` subcommand (``mcps doctor --check-iam``) is
    dispatched to :func:`mcps.doctor.doctor_main` before the main
    Sync_Run argparse runs. It supports the migration plan's
    "rotate the leaked AWS credentials" step (design.md "Migration
    Plan", step 1) and is therefore reachable independently of any
    Sync_Run configuration.
    """
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    if raw_argv and raw_argv[0] == "doctor":
        from .doctor import doctor_main  # lazy import: doctor is rare

        return doctor_main(raw_argv[1:])

    args = parse_args(argv)
    try:
        return run(args)
    except McpsError as exc:
        # Best-effort stderr message. Production runs also have the
        # JSON logger active, but the logger may have been torn down
        # before we reached this branch (e.g. the error fired during
        # config parsing, before logging was configured).
        print(
            f"mcps: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return int(exc.to_exit_code())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
