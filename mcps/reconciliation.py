"""`Reconciliation_Reporter` and `Inconsistency_Detector` data models.

This module hosts two related but independent components:

* `Reconciliation_Reporter` produces a Cold_Start summary the operator
  reviews before authorising the destructive arm of an Apply run
  (req 18). It is a **pure function** over the empty Catalog snapshot,
  the streamed `ObjectRecord` set produced by listing every configured
  Source, the duplicate groups computed by `Duplicate_Detector`
  (task 20), and the would-import count produced by
  `DriveImporter.plan(...)` (task 25).

* `Inconsistency_Detector` runs at the end of every Sync_Run,
  immediately after the Replicator completes, and surfaces any drift
  between Replicated_Sources that no `replication-error` Manifest entry
  already explains (req 19). Its `analyse(...)` method is pure; its
  `emit(...)` method writes one structured INFO summary record plus
  one WARN log record per divergent Content_Hash to the supplied
  logger and returns the run's exit-code contribution (0 or 1).

Both components are kept in the same module because they share the
same on-disk inputs (the run-start Catalog snapshot and the observed
`ObjectRecord` stream) and the CLI threads them through the same
plumbing.

Validates: Requirements 18.1, 18.2, 18.5, 19.1, 19.2, 19.3.

Properties exercised by tests in this module:

* Property 15 — Reconciliation_Report completeness and determinism.
* Property 17 — Inconsistency_Detector soundness.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Optional, Sequence, TextIO

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.duplicates.detector import DuplicateGroup
from mcps.manifest.writer import ManifestWriter


# ---------------------------------------------------------------------------
# Source-kind constants
# ---------------------------------------------------------------------------


_KIND_S3 = "s3"
_KIND_GCS = "gcs"
_KIND_DRIVE = "google_drive"

_VALID_KINDS = frozenset({_KIND_S3, _KIND_GCS, _KIND_DRIVE})


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PerSourceCounts:
    """Per-Source aggregate counters for one Source.

    Fields mirror design.md's `PerSourceCounts`:

    * ``object_count`` — number of `ObjectRecord` instances whose
      ``source`` matches the dict key in
      `ReconciliationReport.per_source`.
    * ``total_bytes`` — sum of ``size_bytes`` across those records.
      Records with negative ``size_bytes`` are excluded from the sum
      because they are also excluded from the duplicate detector
      (req 4.6) and counting them here would over-state the figure
      the operator uses to decide whether to authorise destructive
      actions.
    * ``distinct_content_hashes`` — cardinality of the set of
      ``content_hash`` values seen for that Source. Records whose
      hash is missing/empty are still counted (the detector skips
      them but the per-Source totals must still surface them so an
      operator can see how many records arrived without a hash).
    """

    object_count: int
    total_bytes: int
    distinct_content_hashes: int


@dataclass(frozen=True)
class CrossSourceDiff:
    """Cross-Source-kind partitioning of the global Content_Hash set.

    The `Reconciliation_Reporter` collapses every configured Source to
    its kind (``"s3"`` / ``"gcs"`` / ``"google_drive"``) and partitions
    the union of all observed Content_Hashes into the five disjoint
    buckets below (req 18.1 (b)). Every field is the cardinality of one
    bucket; the buckets are mutually exclusive and exhaustive.

    * ``s3_only`` — Content_Hashes present in some ``s3`` Source and
      absent from every ``gcs`` and ``google_drive`` Source.
    * ``gcs_only`` — analogous for ``gcs``.
    * ``drive_only`` — analogous for ``google_drive``.
    * ``exactly_two`` — Content_Hashes present in exactly two of the
      three kinds.
    * ``all_three`` — Content_Hashes present in all three kinds.
    """

    s3_only: int
    gcs_only: int
    drive_only: int
    exactly_two: int
    all_three: int


@dataclass(frozen=True, eq=True)
class ReconciliationReport:
    """Structured Cold_Start reconciliation summary.

    Field semantics (design.md "Reconciliation_Reporter" + Property 15
    closed-form):

    * ``run_id``: UUID-like run identifier; the same value used for the
      Manifest filename (req 14.1) so the on-disk
      ``reconciliation-<UTC-timestamp>-<run-id>.txt`` lines up with the
      Manifest.
    * ``started_at``: ISO-8601 UTC second precision (``YYYY-MM-DDTHH:MM:SSZ``).
      Used unchanged in the human-readable summary and reformatted to
      the compact ``YYYYMMDDTHHMMSSZ`` shape for the on-disk filename
      (req 18.2).
    * ``per_source``: sparse map ``source_name -> PerSourceCounts``. A
      Source name appears iff at least one of its records is in the
      input ``object_records`` iterable.
    * ``cross_source_diff``: see `CrossSourceDiff`.
    * ``same_source_dup_groups``: sparse map ``source_name -> count``.
      Each duplicate group with ``label == "same-source"`` contributes
      one to the count keyed by every member's shared source. Sources
      with zero same-source groups are omitted.
    * ``cross_source_dup_groups``: total number of duplicate groups
      whose label is ``"cross-source"`` (members span ≥ 2 distinct
      Sources).
    * ``drive_would_import``: passed-in count from
      `DriveImporter.plan(...)`; the Reporter does not recompute it.
    * ``estimated_bytes_to_hash``: sum of ``size_bytes`` across every
      record for which ``bytes_to_hash_estimator(record)`` returned
      ``True`` (req 18.5). Negative ``size_bytes`` are excluded for the
      same reason as `PerSourceCounts.total_bytes`.

    The dataclass is frozen for value-semantics. ``per_source`` and
    ``same_source_dup_groups`` are stored as ``dict`` instances rather
    than `MappingProxyType` because the typing surface in design.md
    declares plain ``dict[str, ...]``; equality between two reports
    therefore reduces to dict equality, which is order-independent and
    matches the property the test suite asserts.
    """

    run_id: str
    started_at: str
    per_source: dict[str, PerSourceCounts] = field(default_factory=dict)
    cross_source_diff: CrossSourceDiff = field(
        default_factory=lambda: CrossSourceDiff(0, 0, 0, 0, 0)
    )
    same_source_dup_groups: dict[str, int] = field(default_factory=dict)
    cross_source_dup_groups: int = 0
    drive_would_import: int = 0
    estimated_bytes_to_hash: int = 0


# ---------------------------------------------------------------------------
# Reconciliation_Reporter
# ---------------------------------------------------------------------------


class Reconciliation_Reporter:
    """Pure builder + emitter for `ReconciliationReport`.

    The class name uses an underscore (`Reconciliation_Reporter`) to
    match design.md verbatim — every cross-reference in the design and
    in `tasks.md` reads ``Reconciliation_Reporter``, so the symbol
    should land in code with that exact spelling. The same convention
    applies to `Inconsistency_Detector` (task 28).

    The methods are intentionally instance methods on a class with no
    state, rather than free functions, so that:

    1. Future additions (a redaction filter, a localised number
       formatter) have a natural home, and
    2. Tests and the CLI can mock the type when they need to inject a
       different implementation.
    """

    # ------------------------------------------------------------------
    # build()
    # ------------------------------------------------------------------

    def build(
        self,
        *,
        catalog_at_start: Catalog,
        object_records: Iterable[ObjectRecord],
        duplicate_groups: Sequence[DuplicateGroup],
        drive_would_import_count: int,
        bytes_to_hash_estimator: Callable[[ObjectRecord], bool],
        run_id: str,
        started_at: str,
        source_kinds: Mapping[str, str],
    ) -> ReconciliationReport:
        """Compute the `ReconciliationReport` from listed records and groups.

        Pure: depends only on the values of its arguments and not on the
        order in which ``object_records`` is yielded. Permutation-
        invariance is the headline property of test
        ``test_reconciliation_report_permutation_invariant`` (Property 15).

        ``catalog_at_start`` is accepted to mirror the Inconsistency_
        Detector's signature (task 28) and to make the Cold_Start
        precondition explicit at the call site; the Reporter itself does
        not read from it. design.md is clear that on Cold_Start the
        Catalog snapshot is empty (req 18.1).

        ``source_kinds`` is the mapping ``source_name -> kind``
        (``"s3"``, ``"gcs"``, ``"google_drive"``). The Reporter uses it
        to bucket each Source's Content_Hash set into one of the three
        kinds for the cross-source diff. A record whose ``source`` is
        not present in ``source_kinds`` is still counted in
        ``per_source`` but contributes to no kind in the cross-source
        diff — the absence is treated as a configuration warning the
        caller is responsible for surfacing elsewhere (the Reporter is
        a pure function and refuses to log).
        """
        # We materialise the iterable once so we can walk it twice (per-
        # source counts AND streaming-hash byte total). Materialising into
        # a list also makes it impossible for an iterator to be exhausted
        # by the first walk and silently produce zero results in the
        # second.
        records: list[ObjectRecord] = list(object_records)

        per_source = self._compute_per_source(records)
        cross_source_diff = self._compute_cross_source_diff(records, source_kinds)
        same_source_dup_groups = self._compute_same_source_dup_groups(
            duplicate_groups
        )
        cross_source_dup_groups = sum(
            1 for g in duplicate_groups if g.label == "cross-source"
        )
        estimated_bytes_to_hash = self._compute_estimated_bytes(
            records, bytes_to_hash_estimator
        )

        return ReconciliationReport(
            run_id=run_id,
            started_at=started_at,
            per_source=per_source,
            cross_source_diff=cross_source_diff,
            same_source_dup_groups=same_source_dup_groups,
            cross_source_dup_groups=cross_source_dup_groups,
            drive_would_import=int(drive_would_import_count),
            estimated_bytes_to_hash=estimated_bytes_to_hash,
        )

    # ------------------------------------------------------------------
    # emit()
    # ------------------------------------------------------------------

    def emit(
        self,
        report: ReconciliationReport,
        *,
        stdout: TextIO,
        manifest_dir: str,
    ) -> None:
        """Write ``report`` to ``stdout`` AND to ``<manifest_dir>/...txt``.

        File path: ``<manifest_dir>/reconciliation-<YYYYMMDDTHHMMSSZ>-<run-id>.txt``
        where ``<YYYYMMDDTHHMMSSZ>`` is ``report.started_at`` reformatted
        from ``YYYY-MM-DDTHH:MM:SSZ`` to the compact form documented for
        the Manifest filename in req 14.1 / 18.2.

        The on-disk write is atomic: a tempfile in ``manifest_dir`` is
        written, fsynced, and ``os.replace``-d into place, so an
        interrupted run never leaves a half-written reconciliation
        report on disk. The ``manifest_dir`` directory must already
        exist (the CLI's startup checks ensure this — req 14.7).
        """
        text = self._render(report)

        # Write to stdout first. `stdout.write` is synchronous and any
        # exception surfaces before the on-disk write so the caller's
        # error mapping treats both paths uniformly.
        stdout.write(text)
        try:
            stdout.flush()
        except (AttributeError, ValueError):
            # Some test stdouts (StringIO) don't need flush; ignore the
            # narrow set of "doesn't apply / already closed" exceptions
            # that flush() can raise on those substitutes.
            pass

        compact_ts = self._compact_timestamp(report.started_at)
        filename = f"reconciliation-{compact_ts}-{report.run_id}.txt"
        target_path = os.path.join(manifest_dir, filename)

        # Atomic write: tempfile in the same directory + os.replace.
        os.makedirs(manifest_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=manifest_dir,
            prefix=".reconciliation-",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp.write(text)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = tmp.name
        os.replace(tmp_path, target_path)

    # ------------------------------------------------------------------
    # Internal: per-source counts
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_per_source(
        records: Sequence[ObjectRecord],
    ) -> dict[str, PerSourceCounts]:
        """Return ``source_name -> PerSourceCounts`` over ``records``.

        Only sources that appear in ``records`` are present in the
        result. ``total_bytes`` excludes records with negative
        ``size_bytes`` (defensive — the catalog model permits any int,
        but a negative value is non-physical and the duplicate detector
        already excludes such records, req 4.6).
        """
        # Use a single pass with three accumulators per source. We
        # materialise distinct hashes through a per-source set and
        # convert to its cardinality at the end so the function is O(n)
        # in the number of records.
        counts: dict[str, int] = {}
        bytes_sum: dict[str, int] = {}
        hashes: dict[str, set[str]] = {}

        for r in records:
            counts[r.source] = counts.get(r.source, 0) + 1
            if isinstance(r.size_bytes, int) and r.size_bytes >= 0:
                bytes_sum[r.source] = bytes_sum.get(r.source, 0) + r.size_bytes
            else:
                # Initialise the entry so the source still gets a 0 entry
                # rather than missing. Subsequent positive-size records
                # for the same source still accumulate normally.
                bytes_sum.setdefault(r.source, 0)
            hashes.setdefault(r.source, set()).add(r.content_hash)

        return {
            name: PerSourceCounts(
                object_count=counts[name],
                total_bytes=bytes_sum.get(name, 0),
                distinct_content_hashes=len(hashes.get(name, set())),
            )
            for name in counts
        }

    # ------------------------------------------------------------------
    # Internal: cross-source diff
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_cross_source_diff(
        records: Sequence[ObjectRecord],
        source_kinds: Mapping[str, str],
    ) -> CrossSourceDiff:
        """Partition the union Content_Hash set across the three kinds.

        Records whose ``source`` is not in ``source_kinds`` or whose
        ``source_kinds`` entry is an unknown value are excluded from
        every kind set; they have no place in the cross-source diff.

        The partitioning is exhaustive over the union set: every hash
        falls into exactly one of {s3_only, gcs_only, drive_only,
        exactly_two, all_three}. A hash that is in zero kinds (because
        its source's kind is missing/invalid) is excluded from every
        bucket.
        """
        s3_hashes: set[str] = set()
        gcs_hashes: set[str] = set()
        drive_hashes: set[str] = set()

        for r in records:
            kind = source_kinds.get(r.source)
            if kind == _KIND_S3:
                s3_hashes.add(r.content_hash)
            elif kind == _KIND_GCS:
                gcs_hashes.add(r.content_hash)
            elif kind == _KIND_DRIVE:
                drive_hashes.add(r.content_hash)
            # Unknown kinds are silently dropped — see method docstring.

        s3_only = 0
        gcs_only = 0
        drive_only = 0
        exactly_two = 0
        all_three = 0

        # Walk the union once. ``in`` on a set is O(1) so the overall
        # cost is O(|union|).
        union = s3_hashes | gcs_hashes | drive_hashes
        for h in union:
            in_s3 = h in s3_hashes
            in_gcs = h in gcs_hashes
            in_drive = h in drive_hashes
            present = (in_s3, in_gcs, in_drive)
            count = sum(present)
            if count == 1:
                if in_s3:
                    s3_only += 1
                elif in_gcs:
                    gcs_only += 1
                else:
                    drive_only += 1
            elif count == 2:
                exactly_two += 1
            elif count == 3:
                all_three += 1
            # count == 0 is impossible because h came from the union.

        return CrossSourceDiff(
            s3_only=s3_only,
            gcs_only=gcs_only,
            drive_only=drive_only,
            exactly_two=exactly_two,
            all_three=all_three,
        )

    # ------------------------------------------------------------------
    # Internal: same-source duplicate groups
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_same_source_dup_groups(
        duplicate_groups: Sequence[DuplicateGroup],
    ) -> dict[str, int]:
        """Return ``source_name -> count`` of same-source duplicate groups.

        A same-source group has all its members in one Source by
        construction (see `mcps.duplicates.detector`), so the source
        name is read off ``members[0].source``. Sources with zero
        same-source groups are omitted from the result (sparse), which
        matches the convention used for ``per_source``.
        """
        counts: dict[str, int] = {}
        for g in duplicate_groups:
            if g.label != "same-source":
                continue
            if not g.members:
                # A same-source group with no members would itself be a
                # detector bug — defend against it by skipping rather
                # than indexing into an empty tuple.
                continue
            source_name = g.members[0].source
            counts[source_name] = counts.get(source_name, 0) + 1
        return counts

    # ------------------------------------------------------------------
    # Internal: estimated bytes to hash
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_estimated_bytes(
        records: Sequence[ObjectRecord],
        bytes_to_hash_estimator: Callable[[ObjectRecord], bool],
    ) -> int:
        """Return the sum of ``size_bytes`` across records flagged True.

        Negative or non-int sizes are skipped — see the docstring of
        `_compute_per_source` for the rationale.
        """
        total = 0
        for r in records:
            if not bytes_to_hash_estimator(r):
                continue
            if isinstance(r.size_bytes, int) and r.size_bytes >= 0:
                total += r.size_bytes
        return total

    # ------------------------------------------------------------------
    # Internal: rendering
    # ------------------------------------------------------------------

    @staticmethod
    def _compact_timestamp(started_at: str) -> str:
        """Convert ``YYYY-MM-DDTHH:MM:SSZ`` to ``YYYYMMDDTHHMMSSZ``.

        Falls back to a sanitised form (drop ``-``, ``:``, the
        trailing fragment if any) when the input does not match the
        canonical shape so the file is still createable on every
        POSIX filesystem the tool targets. The CLI's startup contract
        normalises ``started_at`` before passing it in (req 18.2), so
        the fall-back path is defensive only.
        """
        compact = (
            started_at.replace("-", "")
            .replace(":", "")
            .replace(" ", "")
        )
        return compact

    def _render(self, report: ReconciliationReport) -> str:
        """Return the human-readable text shown to the operator.

        The format is intentionally plain text (no JSON, no markdown)
        because the file is meant to be read by humans during the
        Cold_Start review (req 18). Sections are separated by blank
        lines and labelled headers; per-Source counts are rendered in
        a fixed-width table-like layout.

        The text is byte-deterministic for a given ``report`` so two
        emit() calls on equal reports produce byte-identical files;
        this makes the on-disk file friendly to ``diff`` if the
        operator wants to compare two reconciliation runs.
        """
        lines: list[str] = []
        lines.append("MultiCloud_Photo_Sync — Cold_Start Reconciliation Report")
        lines.append("=" * 60)
        lines.append(f"run_id     : {report.run_id}")
        lines.append(f"started_at : {report.started_at}")
        lines.append("")

        # --- per-source ----------------------------------------------
        lines.append("Per-Source counts")
        lines.append("-" * 60)
        if not report.per_source:
            lines.append("  (no Sources had records)")
        else:
            # Sort source names so output is byte-deterministic.
            for name in sorted(report.per_source):
                counts = report.per_source[name]
                lines.append(
                    f"  {name}: "
                    f"objects={counts.object_count} "
                    f"bytes={counts.total_bytes} "
                    f"distinct_hashes={counts.distinct_content_hashes}"
                )
        lines.append("")

        # --- cross-source diff ---------------------------------------
        diff = report.cross_source_diff
        lines.append("Cross-Source diff (by Source kind)")
        lines.append("-" * 60)
        lines.append(f"  s3_only      : {diff.s3_only}")
        lines.append(f"  gcs_only     : {diff.gcs_only}")
        lines.append(f"  drive_only   : {diff.drive_only}")
        lines.append(f"  exactly_two  : {diff.exactly_two}")
        lines.append(f"  all_three    : {diff.all_three}")
        lines.append("")

        # --- duplicate groups ----------------------------------------
        lines.append("Duplicate groups")
        lines.append("-" * 60)
        if report.same_source_dup_groups:
            for name in sorted(report.same_source_dup_groups):
                lines.append(
                    f"  same-source ({name}): "
                    f"{report.same_source_dup_groups[name]}"
                )
        else:
            lines.append("  same-source: 0")
        lines.append(f"  cross-source: {report.cross_source_dup_groups}")
        lines.append("")

        # --- drive plan + cost estimate ------------------------------
        lines.append("Drive_Importer plan")
        lines.append("-" * 60)
        lines.append(f"  would_import: {report.drive_would_import}")
        lines.append("")

        lines.append("Estimated cost")
        lines.append("-" * 60)
        lines.append(
            f"  estimated_bytes_to_hash: {report.estimated_bytes_to_hash}"
        )
        lines.append("")

        return "\n".join(lines) + "\n"


__all__ = [
    "PerSourceCounts",
    "CrossSourceDiff",
    "ReconciliationReport",
    "Reconciliation_Reporter",
    "DivergentHash",
    "InconsistencyReport",
    "Inconsistency_Detector",
]


# ---------------------------------------------------------------------------
# Inconsistency_Detector — data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DivergentHash:
    """One Content_Hash that diverges between Replicated_Sources.

    Mirrors the design.md ``DivergentHash`` schema verbatim. Fields:

    * ``content_hash`` — the 64-char lowercase hex SHA-256 that is
      present in some Replicated_Source but absent from at least one
      other Replicated_Source after replication completes (req 19.2).
    * ``present_in`` — the tuple of Replicated_Source names that
      observed at least one record carrying ``content_hash`` in the
      current Sync_Run, sorted lexicographically so the output is
      byte-deterministic.
    * ``absent_from`` — the tuple of Replicated_Source names that did
      *not* observe a record with ``content_hash`` in the current
      Sync_Run, also sorted lexicographically.
    * ``canonical_key`` — the ``key`` of the canonical
      `ObjectRecord` for this Content_Hash, picked via the same
      tie-break used by the Duplicate_Resolver (req 5.1). The
      canonical key surfaces in the WARN log so an operator has one
      stable identifier per divergent Content_Hash.
    """

    content_hash: str
    present_in: tuple[str, ...]
    absent_from: tuple[str, ...]
    canonical_key: str


@dataclass(frozen=True)
class InconsistencyReport:
    """Output of `Inconsistency_Detector.analyse(...)`.

    Fields (req 19.1, design.md "Inconsistency_Detector"):

    * ``per_source_new`` — sparse map ``source_name -> count`` where
      the count is the number of ``(source, key)`` pairs present in
      ``object_records_observed`` for that source but absent from the
      same source's set of pairs in ``catalog_at_start``. A source
      with zero new pairs is omitted from the dict.
    * ``per_source_removed`` — sparse map ``source_name -> count``
      where the count is the number of ``(source, key)`` pairs
      present in ``catalog_at_start`` for that source but absent from
      the observed set for that source. Sources with zero removed
      pairs are omitted.
    * ``divergent_hashes`` — tuple of `DivergentHash` records, sorted
      by ``content_hash`` ascending so the on-disk WARN entries are
      byte-deterministic across runs over equal inputs.

    The dataclass is frozen so consumers may treat the report as a
    pure value (it composes inside larger reports the CLI builds for
    its end-of-run SUMMARY record).
    """

    per_source_new: dict[str, int] = field(default_factory=dict)
    per_source_removed: dict[str, int] = field(default_factory=dict)
    divergent_hashes: tuple[DivergentHash, ...] = ()


# ---------------------------------------------------------------------------
# Inconsistency_Detector — implementation
# ---------------------------------------------------------------------------


class Inconsistency_Detector:
    """Detect drift between Replicated_Sources after replication.

    Class name uses an underscore (``Inconsistency_Detector``) to match
    design.md and ``tasks.md`` verbatim — every cross-reference reads
    ``Inconsistency_Detector``, so the symbol lands in code with that
    spelling.

    The detector has two methods:

    * ``analyse(...)`` is a **pure function** that takes the run-start
      Catalog snapshot, the observed `ObjectRecord` stream, the set of
      Replicated_Source names, the set of Content_Hashes that recorded
      a ``replication-error`` entry in the Manifest, and a
      ``canonical_for`` hash-to-record callable. It returns an
      `InconsistencyReport` with the per-Source new / removed counts
      (req 19.1) and the divergent-hashes tuple (req 19.2 minus
      replication errors per req 19.3).
    * ``emit(...)`` extends the SUMMARY log record with the per-Source
      counts and divergence count, appends one WARN log record per
      divergent Content_Hash, and returns 1 when
      ``fail_on_inconsistency=True`` and at least one divergence was
      observed (req 19.2 / 19.3).

    The class holds no state across calls; instance methods are used
    so the CLI and tests can mock the type when they need to inject
    a different implementation.
    """

    # ------------------------------------------------------------------
    # analyse()
    # ------------------------------------------------------------------

    def analyse(
        self,
        *,
        catalog_at_start: Catalog,
        object_records_observed: Iterable[ObjectRecord],
        replicated_source_names: frozenset[str],
        replication_error_hashes: frozenset[str],
        canonical_for: Callable[[ObjectRecord], str],
    ) -> InconsistencyReport:
        """Compute per-Source diff counts and the divergent-hash set.

        Pure: depends only on the values of its arguments and not on
        the order in which ``object_records_observed`` is yielded. This
        is the headline property exercised by Property 17.

        Symmetric difference for per-Source counts:

        * ``per_source_new[name] = |observed_pairs(name) \\
          start_pairs(name)|`` where each ``pairs`` set is built over
          ``(source, key)`` tuples (req 19.1 (a)).
        * ``per_source_removed[name] = |start_pairs(name) \\
          observed_pairs(name)|`` (req 19.1 (b)).

        Both counts are computed for every source name that appears in
        either set (the Catalog snapshot or the observed stream). Sources
        with a zero count are omitted from the result so the dict stays
        sparse.

        Divergent hashes (req 19.2):

        * Build per-Replicated_Source observed Content_Hash sets.
          Pull_Only_Sources are excluded — their ``source`` names are
          not in ``replicated_source_names``.
        * A Content_Hash is divergent iff it is present in at least
          one Replicated_Source's hash set AND absent from at least
          one other Replicated_Source's hash set.
        * Divergent Content_Hashes that recorded a ``replication-error``
          entry in the Manifest are excluded (req 19.3) — those are
          already accounted for in the run's error counter.
        * For each remaining divergent hash, ``canonical_for(record)``
          (where ``record`` is one of the observed records carrying
          that hash) yields the canonical key. The detector picks the
          observed record sorted by ``(source, key)`` so the same
          canonical key is produced regardless of input ordering; the
          caller's ``canonical_for`` callable is then expected to be a
          deterministic function of that record.
        """
        # Materialise the observed iterable once so we can walk it
        # multiple times without exhausting an iterator.
        observed = list(object_records_observed)

        # --- per-Source new / removed -------------------------------
        observed_pairs_per_source: dict[str, set[tuple[str, str]]] = {}
        for r in observed:
            observed_pairs_per_source.setdefault(r.source, set()).add(
                (r.source, r.key)
            )

        start_pairs_per_source: dict[str, set[tuple[str, str]]] = {}
        for r in catalog_at_start.all_records():
            start_pairs_per_source.setdefault(r.source, set()).add(
                (r.source, r.key)
            )

        per_source_new: dict[str, int] = {}
        per_source_removed: dict[str, int] = {}
        all_source_names = set(observed_pairs_per_source) | set(
            start_pairs_per_source
        )
        for name in all_source_names:
            observed_set = observed_pairs_per_source.get(name, set())
            start_set = start_pairs_per_source.get(name, set())
            new_count = len(observed_set - start_set)
            removed_count = len(start_set - observed_set)
            if new_count:
                per_source_new[name] = new_count
            if removed_count:
                per_source_removed[name] = removed_count

        # --- divergent hashes (Replicated_Sources only) ------------
        # Restrict to records whose source is a Replicated_Source —
        # Pull_Only_Sources (Drive) are excluded from divergence
        # analysis per the task brief and design.md.
        records_per_replicated: dict[str, list[ObjectRecord]] = {}
        hashes_per_replicated: dict[str, set[str]] = {}
        for r in observed:
            if r.source not in replicated_source_names:
                continue
            records_per_replicated.setdefault(r.source, []).append(r)
            hashes_per_replicated.setdefault(r.source, set()).add(
                r.content_hash
            )

        # Build the union Content_Hash set across Replicated_Sources.
        # We include every Replicated_Source name in the iteration even
        # when it produced no observed records, so the "absent_from"
        # tuple correctly lists empty Replicated_Sources for any hash
        # observed elsewhere.
        replicated_names_sorted = tuple(sorted(replicated_source_names))
        all_hashes: set[str] = set()
        for name in replicated_names_sorted:
            all_hashes.update(hashes_per_replicated.get(name, set()))

        # For canonical_for(record) lookup we need one observed record
        # per content_hash — the canonical pick. The detector sorts
        # observed records carrying a hash by (source, key) and picks
        # the smallest, then defers to the caller's canonical_for to
        # surface the canonical key.
        records_per_hash: dict[str, list[ObjectRecord]] = {}
        for r in observed:
            if r.source not in replicated_source_names:
                continue
            records_per_hash.setdefault(r.content_hash, []).append(r)

        divergent: list[DivergentHash] = []
        for content_hash in sorted(all_hashes):
            if content_hash in replication_error_hashes:
                # req 19.3: any hash with a replication-error is
                # excluded from divergence analysis because the run's
                # error counter already surfaces it.
                continue

            present_in: list[str] = []
            absent_from: list[str] = []
            for name in replicated_names_sorted:
                if content_hash in hashes_per_replicated.get(name, set()):
                    present_in.append(name)
                else:
                    absent_from.append(name)

            # A divergent hash is present in some Replicated_Source AND
            # absent from at least one other (req 19.2). Skipping when
            # either list is empty filters out hashes that are uniformly
            # present (or uniformly absent — which would mean we never
            # added them to the union set in the first place).
            if not present_in or not absent_from:
                continue

            # Pick the canonical observed record for this hash. We sort
            # by (source, key) so the choice is deterministic given the
            # record set; the caller's ``canonical_for`` callable then
            # decides what key to surface (typically the same record's
            # key, but the indirection lets the CLI use the proper
            # Requirement 5.1 tie-break that depends on
            # ``canonical_source_priority`` etc).
            candidates = sorted(
                records_per_hash.get(content_hash, []),
                key=lambda rec: (rec.source, rec.key),
            )
            if not candidates:
                # All records for this hash were filtered out (e.g.
                # Pull_Only_Sources only) — the union build above
                # already restricted to Replicated_Sources, so this
                # branch is defensive and skipped.
                continue
            canonical_key = canonical_for(candidates[0])

            divergent.append(
                DivergentHash(
                    content_hash=content_hash,
                    present_in=tuple(present_in),
                    absent_from=tuple(absent_from),
                    canonical_key=canonical_key,
                )
            )

        return InconsistencyReport(
            per_source_new=per_source_new,
            per_source_removed=per_source_removed,
            divergent_hashes=tuple(divergent),
        )

    # ------------------------------------------------------------------
    # emit()
    # ------------------------------------------------------------------

    def emit(
        self,
        report: InconsistencyReport,
        *,
        manifest_writer: Optional[ManifestWriter],
        logger: logging.Logger,
        fail_on_inconsistency: bool,
    ) -> int:
        """Emit the SUMMARY extension and one WARN per divergent hash.

        Behaviour:

        * One INFO log record at event ``mcps.reconciliation.summary``
          carries the per-Source counts and the divergence count
          (req 19.1, extending the Sync_Run SUMMARY record from
          req 14.5). The structured payload travels through the
          ``extra=`` channel of the stdlib logger so the
          `JsonFormatter` renders it inside the JSON line.
        * One WARN log record per `DivergentHash` is appended to
          ``logger`` at event
          ``mcps.reconciliation.inconsistency``, carrying
          ``content_hash``, ``present_in``, ``absent_from``, and
          ``canonical_key`` (req 19.2).
        * Returns 0 when no divergent hashes were observed; returns 1
          when divergent hashes were observed AND
          ``fail_on_inconsistency`` is True (req 19.2 / 19.3). The
          CLI folds this into its exit-code computation.

        ``manifest_writer`` is accepted for parity with the design's
        signature but is not used by the default emission path: the
        SUMMARY and WARN records live on the structured log channel,
        not the per-record JSONL Manifest. The parameter is left in
        the API so a future wiring (e.g. mirroring divergences into
        the Manifest as ``REPLICATION_ERROR`` entries) can be added
        without breaking callers. Passing ``None`` is supported and
        is the convention used by the property test.
        """
        # Suppress unused warning while preserving the API; see the
        # docstring above for why the parameter is retained.
        _ = manifest_writer

        # SUMMARY extension (req 19.1). The payload is attached via
        # ``extra=`` so the JsonFormatter renders the per-Source counts
        # alongside the documented top-level fields. Sorting the dict
        # outputs makes the structured log record byte-deterministic
        # for golden-file regression tests.
        summary_payload = {
            "per_source_new": dict(sorted(report.per_source_new.items())),
            "per_source_removed": dict(
                sorted(report.per_source_removed.items())
            ),
            "divergent_hashes_count": len(report.divergent_hashes),
        }
        logger.info(
            "inconsistency-summary",
            extra={
                "event": "mcps.reconciliation.summary",
                **summary_payload,
            },
        )

        # WARN per divergent hash (req 19.2). The detector already
        # sorted ``divergent_hashes`` by content_hash ascending so the
        # WARN order is deterministic across runs over equal inputs.
        for div in report.divergent_hashes:
            logger.warning(
                "inconsistency-divergent-hash",
                extra={
                    "event": "mcps.reconciliation.inconsistency",
                    "content_hash": div.content_hash,
                    "present_in": list(div.present_in),
                    "absent_from": list(div.absent_from),
                    "canonical_key": div.canonical_key,
                },
            )

        if fail_on_inconsistency and report.divergent_hashes:
            return 1
        return 0
