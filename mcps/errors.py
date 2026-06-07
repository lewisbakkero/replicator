"""Exception hierarchy and exit codes for MultiCloud_Photo_Sync.

This module is the single source of truth for the `McpsError` hierarchy and
the `ExitCode` IntEnum. The CLI uses `to_exit_code()` to translate any
`McpsError` raised at the top level into a process exit code.

The exit-code values follow the BSD `sysexits.h` convention (64 = usage/config
family, 71-75 = service-related) so that systemd / cron post-processing can
distinguish operational errors from data errors.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Optional


class ExitCode(IntEnum):
    """Process exit codes emitted by the `mcps` CLI.

    Values match the table in design.md ("CLI Surface > Exit codes"). The
    integer values are part of the operator-facing contract (cron / systemd
    post-processing) and MUST NOT be reordered.
    """

    OK = 0
    RUN_HAD_ERRORS = 2
    CONFIG_INVALID = 64
    CATALOG_INVALID = 65
    LEGACY_CONFIG = 66
    MANIFEST_UNAVAILABLE = 67
    CREDENTIAL_FAILED = 71
    CONFLICT_FAILURE = 72
    LOCK_CONFLICT = 73
    INTERACTIVE_REQUIRED = 74
    DRIVE_ACCESS_FAILED = 75
    FIRST_PASS_REVIEW_REQUIRED = 76
    COLD_START_LISTING_FAILED = 77
    INCONSISTENCY_DETECTED = 78


class McpsError(Exception):
    """Base class for every domain-level error raised by mcps.

    Subclasses set the ``exit_code`` class attribute to the appropriate
    `ExitCode` value. The default of `RUN_HAD_ERRORS` matches the design's
    rule that "anything inside the per-object loop ... continues; the run
    exits non-zero at the end if any such record was emitted (exit code 2,
    RUN_HAD_ERRORS)".
    """

    exit_code: ExitCode = ExitCode.RUN_HAD_ERRORS

    def to_exit_code(self) -> ExitCode:
        """Return the `ExitCode` the CLI should map this error to."""
        return self.exit_code


class ConfigError(McpsError):
    """Configuration file failed schema validation.

    Validates: Requirements 17.7, 17.8, 17.9, 8.7.
    """

    exit_code = ExitCode.CONFIG_INVALID

    def __init__(
        self,
        path: str,
        line: Optional[int] = None,
        field: Optional[str] = None,
    ) -> None:
        self.path = path
        self.line = line
        self.field = field
        super().__init__(
            f"ConfigError(path={path!r}, line={line!r}, field={field!r})"
        )


class LegacyConfigDetected(McpsError):
    """A plaintext-credential `config.ini` was detected at startup.

    Validates: Requirement 1.5.
    """

    exit_code = ExitCode.LEGACY_CONFIG

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"LegacyConfigDetected(path={path!r})")


class CredentialError(McpsError):
    """Provider credentials could not be resolved or were rejected.

    Validates: Requirements 1.3, 1.4.
    """

    exit_code = ExitCode.CREDENTIAL_FAILED

    def __init__(self, provider: str, sources_tried: "tuple[str, ...] | list[str]") -> None:
        self.provider = provider
        # Store as a tuple so the field is immutable and hashable for tests.
        self.sources_tried = tuple(sources_tried)
        super().__init__(
            f"CredentialError(provider={provider!r}, "
            f"sources_tried={list(self.sources_tried)!r})"
        )


class CatalogParseError(McpsError):
    """The on-disk Catalog file is present but unparseable.

    Validates: Requirement 3.6.
    """

    exit_code = ExitCode.CATALOG_INVALID

    def __init__(self, path: str, line: Optional[int] = None) -> None:
        self.path = path
        self.line = line
        super().__init__(f"CatalogParseError(path={path!r}, line={line!r})")


class LockConflict(McpsError):
    """Another live process holds the writer lock.

    Validates: Requirement 16.5.
    """

    exit_code = ExitCode.LOCK_CONFLICT

    def __init__(self, holder_pid: int) -> None:
        self.holder_pid = holder_pid
        super().__init__(f"LockConflict(holder_pid={holder_pid!r})")


class RetriesExhausted(McpsError):
    """The retry decorator exceeded `max_retries` for an operation.

    Validates: Requirements 2.6, 12.5.

    The design lists this exception in two slightly different forms:
    `RetriesExhausted(operation, last)` in the hierarchy diagram and
    `RetriesExhausted(last=e, attempts=attempt)` in the retry decorator. We
    expose all three of (operation, last, attempts) as fields so the CLI and
    tests can rely on whichever the caller supplied.
    """

    # Per-object retries are recorded as Manifest entries; the run still
    # exits with RUN_HAD_ERRORS at the top level.
    exit_code = ExitCode.RUN_HAD_ERRORS

    def __init__(
        self,
        operation: Optional[str] = None,
        last: Optional[BaseException] = None,
        attempts: Optional[int] = None,
    ) -> None:
        self.operation = operation
        self.last = last
        self.attempts = attempts
        super().__init__(
            f"RetriesExhausted(operation={operation!r}, "
            f"last={last!r}, attempts={attempts!r})"
        )


class NonTransientError(McpsError):
    """An HTTP response classified as non-transient (e.g. 400/401/403/404).

    Validates: Requirement 12.2.
    """

    exit_code = ExitCode.RUN_HAD_ERRORS

    def __init__(self, status: int, body: Optional[str] = None) -> None:
        self.status = status
        self.body = body
        super().__init__(f"NonTransientError(status={status!r}, body={body!r})")


class ReplicationVerifyMismatch(McpsError):
    """Post-write verification disagreed with the source Content_Hash or size.

    Validates: Requirement 6.5.
    """

    exit_code = ExitCode.RUN_HAD_ERRORS

    def __init__(
        self,
        src: str,
        dst: str,
        key: str,
        expected: str,
        observed: str,
    ) -> None:
        self.src = src
        self.dst = dst
        self.key = key
        self.expected = expected
        self.observed = observed
        super().__init__(
            f"ReplicationVerifyMismatch(src={src!r}, dst={dst!r}, key={key!r}, "
            f"expected={expected!r}, observed={observed!r})"
        )


class ReadOnlySourceError(McpsError):
    """A write-side method was invoked on a read-only adapter (Drive).

    Validates: Requirement 10.8.

    No specific exit code: this surfaces as a per-record error inside the
    per-object loop, so the run exits with RUN_HAD_ERRORS at the top level.
    """

    exit_code = ExitCode.RUN_HAD_ERRORS

    def __init__(self, adapter: str, op: str) -> None:
        self.adapter = adapter
        self.op = op
        super().__init__(f"ReadOnlySourceError(adapter={adapter!r}, op={op!r})")


class ManifestWriteError(McpsError):
    """Writing to the Manifest failed (directory missing or I/O error).

    Validates: Requirements 14.6, 14.7.
    """

    exit_code = ExitCode.MANIFEST_UNAVAILABLE

    def __init__(self, path: str, cause: Optional[BaseException] = None) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"ManifestWriteError(path={path!r}, cause={cause!r})")


class LastCopyProtectionViolation(McpsError):
    """A delete/quarantine would have removed the last live copy of a hash.

    Validates: Requirements 5.10, 5.11, 9.6, 9.7.
    """

    exit_code = ExitCode.RUN_HAD_ERRORS

    def __init__(self, content_hash: str, source: str) -> None:
        self.content_hash = content_hash
        self.source = source
        super().__init__(
            f"LastCopyProtectionViolation(content_hash={content_hash!r}, "
            f"source={source!r})"
        )


class ColdStartListingFailed(McpsError):
    """A Cold_Start Sync_Run aborted because a Source listing failed.

    Validates: Requirement 18.6.
    """

    exit_code = ExitCode.COLD_START_LISTING_FAILED

    def __init__(
        self,
        source_name: str,
        source_kind: str,
        cause: Optional[BaseException] = None,
    ) -> None:
        self.source_name = source_name
        self.source_kind = source_kind
        self.cause = cause
        super().__init__(
            f"ColdStartListingFailed(source_name={source_name!r}, "
            f"source_kind={source_kind!r}, cause={cause!r})"
        )


class InteractiveConfirmationRequired(McpsError):
    """Apply mode requires interactive confirmation but stdin is not a terminal.

    Raised by the `Duplicate_Resolver` when ``--apply`` is selected without
    ``--auto-approve`` and standard input is not a TTY (req 5.6). The CLI
    maps this exit code to ``INTERACTIVE_REQUIRED`` (74) so operators know
    they must supply ``--auto-approve`` (or run from a terminal) to proceed.

    Validates: Requirement 5.6.
    """

    exit_code = ExitCode.INTERACTIVE_REQUIRED

    def __init__(self, message: str = "interactive confirmation is required") -> None:
        self.message = message
        super().__init__(f"InteractiveConfirmationRequired({message!r})")


class DriveAccessFailed(McpsError):
    """Construction-time access check failed for a Google Drive root folder.

    Raised when the `GoogleDriveSourceAdapter` cannot resolve the configured
    ``drive_root_folder_id`` against the Drive service. The CLI maps this
    exit code to ``DRIVE_ACCESS_FAILED`` (75) so operators can distinguish
    a misconfigured Drive folder from any other start-up failure.

    Validates: Requirement 10.10.
    """

    exit_code = ExitCode.DRIVE_ACCESS_FAILED

    def __init__(
        self,
        folder_id: str,
        cause: Optional[BaseException] = None,
    ) -> None:
        self.folder_id = folder_id
        self.cause = cause
        super().__init__(
            f"DriveAccessFailed(folder_id={folder_id!r}, cause={cause!r})"
        )


__all__ = [
    "ExitCode",
    "McpsError",
    "ConfigError",
    "LegacyConfigDetected",
    "CredentialError",
    "CatalogParseError",
    "LockConflict",
    "RetriesExhausted",
    "NonTransientError",
    "ReplicationVerifyMismatch",
    "ReadOnlySourceError",
    "ManifestWriteError",
    "LastCopyProtectionViolation",
    "ColdStartListingFailed",
    "DriveAccessFailed",
    "InteractiveConfirmationRequired",
]
