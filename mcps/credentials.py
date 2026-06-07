"""Credential_Manager — resolves AWS / GCP / Drive credentials.

This module implements the chain documented in design.md ("Credential
resolution chain"):

- AWS: env vars (``AWS_ACCESS_KEY_ID`` + ``AWS_SECRET_ACCESS_KEY`` + optional
  ``AWS_SESSION_TOKEN``) → named profile (``AWS_PROFILE``) → instance/container
  role via ``boto3.Session().get_credentials()``.
- GCP: ``GOOGLE_APPLICATION_CREDENTIALS`` service-account file → Application
  Default Credentials via ``google.auth.default()``.
- Drive: piggybacks on the GCP chain with the ``drive.readonly`` scope only.

Each ``resolve_*`` method runs the chain inside a ``ThreadPoolExecutor`` with a
10-second wall-clock guard (req 1.3). When no source in a chain produces a
complete credential set the resolver raises :class:`CredentialError` with the
provider name and the sources that were tried.

The Credential_Manager itself does NOT validate credentials against an actual
provider API call — that is the source adapter's job. A 401 / invalid-creds
error observed during a Sync_Run should be classified by the adapter using
:func:`classify_credential_error` and re-raised as :class:`CredentialError`
per Requirement 1.4.

Validates: Requirements 1.1, 1.2, 1.3, 1.4, 10.10.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from .errors import CredentialError


# Default 10-second wall-clock guard for any single chain (req 1.3).
DEFAULT_RESOLVE_TIMEOUT_SECONDS: float = 10.0

# Drive scope: read-only is the only scope the SourceAdapter ever needs
# because Drive is a Pull_Only_Source (req 10.8).
DRIVE_READONLY_SCOPE: str = "https://www.googleapis.com/auth/drive.readonly"


@dataclass(frozen=True)
class ResolvedCredentials:
    """Result of a successful credential resolution.

    The provider-specific payload is opaque to the rest of the system: AWS
    callers receive a ``boto3.Session`` ready to construct a typed client,
    while GCP / Drive callers receive a ``google.auth.credentials.Credentials``
    instance. Both fields are :data:`None` for the non-applicable provider.
    """

    provider: str  # one of "aws" | "gcp" | "drive"
    source: str    # which slot in the chain produced the credential
    boto3_session: Optional[Any] = None
    google_credentials: Optional[Any] = None


# ---- Default factories (lazy imports so unit tests do not require the SDKs) -


def _default_aws_session_factory(**kwargs: Any) -> Any:
    import boto3  # imported lazily so tests can substitute a fake factory

    return boto3.Session(**kwargs)


def _default_google_auth_default(scopes: Optional[Sequence[str]] = None) -> Any:
    import google.auth  # lazy import

    return google.auth.default(scopes=scopes)


def _default_google_service_account_loader(
    path: str, scopes: Optional[Sequence[str]] = None
) -> Any:
    from google.oauth2 import service_account  # lazy import

    return service_account.Credentials.from_service_account_file(path, scopes=scopes)


class Credential_Manager:
    """Resolves AWS, GCP, and Drive credentials with a 10-second guard.

    Parameters
    ----------
    aws_session_factory:
        Callable invoked as ``factory()`` for the env / instance-role steps and
        ``factory(profile_name=...)`` for the named-profile step. Defaults to
        ``boto3.Session``.
    google_auth_default:
        Callable invoked as ``factory(scopes=...)`` returning ``(creds,
        project_id)`` per ``google.auth.default``. Defaults to
        ``google.auth.default``.
    google_service_account_loader:
        Callable invoked as ``factory(path, scopes=...)`` returning a
        ``google.auth.credentials.Credentials`` instance. Defaults to
        ``google.oauth2.service_account.Credentials.from_service_account_file``.
    timeout_seconds:
        Wall-clock guard for the entire chain (default 10s, req 1.3).
        Tests may pass a smaller value.
    """

    def __init__(
        self,
        *,
        aws_session_factory: Optional[Callable[..., Any]] = None,
        google_auth_default: Optional[Callable[..., Any]] = None,
        google_service_account_loader: Optional[Callable[..., Any]] = None,
        timeout_seconds: float = DEFAULT_RESOLVE_TIMEOUT_SECONDS,
    ) -> None:
        self._aws_session_factory = aws_session_factory or _default_aws_session_factory
        self._google_auth_default = google_auth_default or _default_google_auth_default
        self._google_service_account_loader = (
            google_service_account_loader or _default_google_service_account_loader
        )
        self._timeout_seconds = float(timeout_seconds)

    # ---- AWS chain ------------------------------------------------------

    def resolve_aws(self) -> ResolvedCredentials:
        """Run the AWS credential chain (env → profile → instance-role).

        Returns the first complete credential set as ``ResolvedCredentials``
        with ``provider="aws"``. Raises :class:`CredentialError` if no source
        yields a complete set within the configured timeout.
        """
        sources_tried: list[str] = []

        def _inner() -> ResolvedCredentials:
            # Step 1 — environment variables (req 1.1.a).
            sources_tried.append("env")
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            if access_key and secret_key:
                try:
                    session = self._aws_session_factory()
                except Exception:
                    session = None
                if session is not None and _aws_session_has_credentials(session):
                    return ResolvedCredentials(
                        provider="aws", source="env", boto3_session=session
                    )

            # Step 2 — named AWS_PROFILE (req 1.1.b).
            sources_tried.append("profile")
            profile = os.environ.get("AWS_PROFILE")
            if profile:
                try:
                    session = self._aws_session_factory(profile_name=profile)
                except Exception:
                    session = None
                if session is not None and _aws_session_has_credentials(session):
                    return ResolvedCredentials(
                        provider="aws", source="profile", boto3_session=session
                    )

            # Step 3 — instance / container role via the default chain
            # (req 1.1.c).
            sources_tried.append("instance-role")
            try:
                session = self._aws_session_factory()
            except Exception:
                session = None
            if session is not None and _aws_session_has_credentials(session):
                return ResolvedCredentials(
                    provider="aws", source="instance-role", boto3_session=session
                )

            raise CredentialError(
                provider="aws", sources_tried=tuple(sources_tried)
            )

        return self._run_with_timeout("aws", _inner, sources_tried)

    # ---- GCP chain ------------------------------------------------------

    def resolve_gcp(
        self, scopes: Optional[Sequence[str]] = None
    ) -> ResolvedCredentials:
        """Run the GCP credential chain (service-account file → ADC).

        ``scopes`` is forwarded both to the service-account loader and to
        ``google.auth.default``. Pass ``None`` to use the SDK default scopes.
        """
        sources_tried: list[str] = []
        scopes_list: Optional[list[str]] = list(scopes) if scopes else None

        def _inner() -> ResolvedCredentials:
            # Step 1 — GOOGLE_APPLICATION_CREDENTIALS service-account file
            # (req 1.2.a).
            sources_tried.append("service_account_file")
            sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if sa_path and os.path.isfile(sa_path):
                try:
                    creds = self._google_service_account_loader(
                        sa_path, scopes=scopes_list
                    )
                except Exception:
                    creds = None
                if creds is not None:
                    return ResolvedCredentials(
                        provider="gcp",
                        source="service_account_file",
                        google_credentials=creds,
                    )

            # Step 2 — Application Default Credentials (req 1.2.b).
            sources_tried.append("application_default")
            try:
                result = self._google_auth_default(scopes=scopes_list)
            except Exception:
                result = None
            if result is not None:
                # google.auth.default returns (credentials, project_id); we
                # only care about credentials here.
                creds = result[0] if isinstance(result, tuple) else result
                if creds is not None:
                    return ResolvedCredentials(
                        provider="gcp",
                        source="application_default",
                        google_credentials=creds,
                    )

            raise CredentialError(
                provider="gcp", sources_tried=tuple(sources_tried)
            )

        return self._run_with_timeout("gcp", _inner, sources_tried)

    # ---- Drive (drive.readonly scope) ----------------------------------

    def resolve_drive(self) -> ResolvedCredentials:
        """Resolve credentials for the read-only Drive adapter.

        Same SA file as ``resolve_gcp`` but pinned to ``drive.readonly``
        scope only (req 10.10). The returned ``ResolvedCredentials`` has
        ``provider="drive"`` so callers can distinguish Drive errors from
        generic GCP errors.
        """
        try:
            gcp = self.resolve_gcp(scopes=[DRIVE_READONLY_SCOPE])
        except CredentialError as exc:
            # Re-raise so the provider attribute names the failing chain.
            raise CredentialError(
                provider="drive", sources_tried=exc.sources_tried
            ) from exc
        return ResolvedCredentials(
            provider="drive",
            source=gcp.source,
            google_credentials=gcp.google_credentials,
        )

    # ---- 10-second wall-clock guard ------------------------------------

    def _run_with_timeout(
        self,
        provider: str,
        fn: Callable[[], ResolvedCredentials],
        sources_tried: list[str],
    ) -> ResolvedCredentials:
        """Execute ``fn`` in a worker thread and enforce ``timeout_seconds``.

        ``sources_tried`` is shared with ``fn`` (closure) so that on timeout
        we can report which sources had been attempted before the deadline.
        """
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mcps-cred")
        try:
            future = executor.submit(fn)
            try:
                return future.result(timeout=self._timeout_seconds)
            except FuturesTimeoutError:
                # Append a sentinel naming the timeout so callers (and the
                # CLI's error renderer) can distinguish a chain that ran to
                # completion without finding credentials from a chain that
                # hit the wall-clock deadline.
                timeout_marker = (
                    f"timeout-after-{self._timeout_seconds:g}s"
                )
                raise CredentialError(
                    provider=provider,
                    sources_tried=tuple(sources_tried) + (timeout_marker,),
                ) from None
        finally:
            # Do not block on stuck threads; the SDK call inside fn is
            # uninterruptible from Python, but we must not leak the timeout
            # to the caller's wall clock.
            executor.shutdown(wait=False)


def _aws_session_has_credentials(session: Any) -> bool:
    """Return True iff ``session.get_credentials()`` yields a usable credential.

    A "usable" credential here means ``get_frozen_credentials()`` returns a
    structure with a non-empty ``access_key`` and ``secret_key``. A botocore
    ``Credentials`` whose chain found nothing returns ``None`` from
    ``get_credentials()``; we treat that as "this step did not produce a
    complete set" and fall through to the next chain step.
    """
    try:
        creds = session.get_credentials()
    except Exception:
        return False
    if creds is None:
        return False
    try:
        frozen = creds.get_frozen_credentials()
    except Exception:
        return False
    if frozen is None:
        return False
    if not getattr(frozen, "access_key", None):
        return False
    if not getattr(frozen, "secret_key", None):
        return False
    return True


# ---- 401 / invalid-credentials classifier ---------------------------------


def classify_credential_error(exc: BaseException) -> bool:
    """Return True if ``exc`` looks like an invalid-credentials error.

    Best-effort classifier consulted by source adapters when an SDK call
    fails inside a Sync_Run. The adapter wraps the original exception in a
    :class:`CredentialError` per Requirement 1.4 only when this returns
    True; other errors flow through the normal retry / per-record-error
    paths.

    Recognised patterns:

    - ``botocore.exceptions.NoCredentialsError`` /
      ``PartialCredentialsError`` (the AWS SDK could not assemble a complete
      set at call time).
    - ``botocore.exceptions.ClientError`` whose response carries HTTP 401 or
      403, or whose error code is one of the well-known auth failures
      (``InvalidAccessKeyId``, ``SignatureDoesNotMatch``, ``ExpiredToken``,
      ``InvalidToken``, ``AuthFailure``).
    - ``google.auth.exceptions.RefreshError`` (the GCP SDK could not
      refresh an access token from the configured credentials).
    - ``googleapiclient.errors.HttpError`` with HTTP 401 or 403.

    Imports are guarded so that callers without the relevant SDK installed
    still get a useful answer (``False``).
    """
    # boto3 / botocore -------------------------------------------------------
    try:
        from botocore.exceptions import (  # type: ignore[import-not-found]
            ClientError,
            NoCredentialsError,
            PartialCredentialsError,
        )

        if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
            return True
        if isinstance(exc, ClientError):
            response = getattr(exc, "response", {}) or {}
            err = response.get("Error", {}) or {}
            code = err.get("Code", "")
            status = (
                response.get("ResponseMetadata", {}) or {}
            ).get("HTTPStatusCode")
            if status in (401, 403):
                return True
            if code in {
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "ExpiredToken",
                "InvalidToken",
                "AuthFailure",
                "AccessDenied",
            }:
                return True
    except ImportError:
        pass

    # google-auth ------------------------------------------------------------
    try:
        from google.auth.exceptions import RefreshError  # type: ignore[import-not-found]

        if isinstance(exc, RefreshError):
            return True
    except ImportError:
        pass

    # googleapiclient --------------------------------------------------------
    try:
        from googleapiclient.errors import HttpError  # type: ignore[import-not-found]

        if isinstance(exc, HttpError):
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status in (401, 403):
                return True
    except ImportError:
        pass

    return False


__all__ = [
    "Credential_Manager",
    "ResolvedCredentials",
    "DRIVE_READONLY_SCOPE",
    "DEFAULT_RESOLVE_TIMEOUT_SECONDS",
    "classify_credential_error",
]
