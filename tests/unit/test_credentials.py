"""Example-based unit tests for :mod:`mcps.credentials`.

These tests cover every failure mode named in Requirements 1.1-1.4 and 10.10:

- AWS env / profile / instance-role chain success cases.
- AWS chain exhausted → ``CredentialError(provider="aws", sources_tried=...)``.
- AWS chain wall-clock timeout (10s guard) → ``CredentialError`` naming the
  timeout in ``sources_tried``.
- GCP service-account-file success.
- GCP application-default success when no SA file is set.
- GCP chain exhausted → ``CredentialError(provider="gcp", sources_tried=...)``.
- ``resolve_drive`` defaults to ``drive.readonly`` scope.

The tests do not exercise real boto3 / google.auth code paths; instead they
rely on the constructor-injected factories (``aws_session_factory``,
``google_auth_default``, ``google_service_account_loader``) per the design's
test-seam guidance.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import pytest

from mcps.credentials import (
    DRIVE_READONLY_SCOPE,
    Credential_Manager,
    ResolvedCredentials,
    classify_credential_error,
)
from mcps.errors import CredentialError, ExitCode


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeFrozenCredentials:
    def __init__(self, access_key: str, secret_key: str) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.token: Optional[str] = None


class _FakeBotoCredentials:
    """Mimics the bits of ``botocore.credentials.Credentials`` we touch."""

    def __init__(self, access_key: str, secret_key: str) -> None:
        self._frozen = _FakeFrozenCredentials(access_key, secret_key)

    def get_frozen_credentials(self) -> _FakeFrozenCredentials:
        return self._frozen


class _FakeBotoSession:
    """Mimics the bits of ``boto3.Session`` we touch.

    ``has_creds=False`` simulates ``Session.get_credentials() is None``,
    which is what botocore returns when its own internal chain (env, file,
    metadata service) found nothing.
    """

    def __init__(
        self,
        *,
        has_creds: bool = True,
        profile_name: Optional[str] = None,
        access_key: str = "AKIAEXAMPLE",
        secret_key: str = "secret",
    ) -> None:
        self.profile_name = profile_name
        self._has_creds = has_creds
        self._creds = (
            _FakeBotoCredentials(access_key, secret_key) if has_creds else None
        )

    def get_credentials(self) -> Optional[_FakeBotoCredentials]:
        return self._creds


def _make_aws_factory(
    *,
    default_has_creds: bool = True,
    profile_has_creds: bool = True,
    record: Optional[list[dict]] = None,
):
    """Build an injectable ``aws_session_factory`` for tests."""

    def factory(**kwargs):
        if record is not None:
            record.append(dict(kwargs))
        if "profile_name" in kwargs:
            return _FakeBotoSession(
                has_creds=profile_has_creds,
                profile_name=kwargs["profile_name"],
            )
        return _FakeBotoSession(has_creds=default_has_creds)

    return factory


# ---------------------------------------------------------------------------
# AWS chain
# ---------------------------------------------------------------------------


def test_aws_env_vars_resolve_to_env_source(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "shh")
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    calls: list[dict] = []
    cm = Credential_Manager(
        aws_session_factory=_make_aws_factory(record=calls),
    )

    result = cm.resolve_aws()

    assert isinstance(result, ResolvedCredentials)
    assert result.provider == "aws"
    assert result.source == "env"
    assert result.boto3_session is not None
    # First call should be the env-step factory call (no profile_name).
    assert calls[0] == {}


def test_aws_profile_only_resolves_to_profile_source(monkeypatch) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("AWS_PROFILE", "mcps-prod")

    calls: list[dict] = []
    cm = Credential_Manager(
        aws_session_factory=_make_aws_factory(record=calls),
    )

    result = cm.resolve_aws()

    assert result.provider == "aws"
    assert result.source == "profile"
    # The factory call carrying profile_name must be exactly the configured
    # AWS_PROFILE value (req 1.1.b).
    assert any(c.get("profile_name") == "mcps-prod" for c in calls)


def test_aws_instance_role_resolves_when_env_and_profile_absent(monkeypatch) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    cm = Credential_Manager(aws_session_factory=_make_aws_factory())

    result = cm.resolve_aws()

    assert result.provider == "aws"
    assert result.source == "instance-role"
    assert result.boto3_session is not None


def test_aws_chain_exhausted_raises_credential_error(monkeypatch) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    cm = Credential_Manager(
        aws_session_factory=_make_aws_factory(default_has_creds=False),
    )

    with pytest.raises(CredentialError) as excinfo:
        cm.resolve_aws()

    err = excinfo.value
    assert err.provider == "aws"
    # All three slots in the chain must be reported when nothing succeeds
    # (req 1.3 — name the failing provider and identify which sources were
    # attempted).
    assert err.sources_tried == ("env", "profile", "instance-role")
    assert err.exit_code == ExitCode.CREDENTIAL_FAILED


def test_aws_partial_env_falls_through_to_instance_role(monkeypatch) -> None:
    """Only AWS_ACCESS_KEY_ID set → env step is incomplete → fall through."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    cm = Credential_Manager(aws_session_factory=_make_aws_factory())

    result = cm.resolve_aws()

    # env was incomplete (missing secret); profile was absent; the
    # instance-role default chain should be the resolved source.
    assert result.source == "instance-role"


def test_aws_timeout_raises_credential_error_naming_timeout(monkeypatch) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    def slow_factory(**kwargs):
        # Simulate a hung metadata-service call that exceeds the wall-clock
        # guard. The 10s default guard would make the test slow, so we
        # override timeout_seconds to a small value below.
        time.sleep(1.0)
        return _FakeBotoSession(has_creds=True)

    cm = Credential_Manager(
        aws_session_factory=slow_factory,
        timeout_seconds=0.05,
    )

    with pytest.raises(CredentialError) as excinfo:
        cm.resolve_aws()

    err = excinfo.value
    assert err.provider == "aws"
    # A timeout marker is appended so the operator-facing error names the
    # 10s-guard breach distinctly from a clean exhaustion.
    assert any("timeout" in s for s in err.sources_tried)


# ---------------------------------------------------------------------------
# GCP chain
# ---------------------------------------------------------------------------


class _FakeGoogleCreds:
    def __init__(self, label: str, scopes: Optional[list[str]] = None) -> None:
        self.label = label
        self.scopes = list(scopes) if scopes else None


def test_gcp_service_account_file_resolves_to_sa_source(monkeypatch, tmp_path) -> None:
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_path))

    sa_calls: list[tuple[str, Optional[list[str]]]] = []
    adc_calls: list[Any] = []

    def sa_loader(path: str, scopes=None):
        sa_calls.append((path, list(scopes) if scopes else None))
        return _FakeGoogleCreds("sa", scopes=list(scopes) if scopes else None)

    def adc(scopes=None):
        adc_calls.append(scopes)
        return (_FakeGoogleCreds("adc"), "fake-project")

    cm = Credential_Manager(
        google_auth_default=adc,
        google_service_account_loader=sa_loader,
    )

    result = cm.resolve_gcp()

    assert result.provider == "gcp"
    assert result.source == "service_account_file"
    assert isinstance(result.google_credentials, _FakeGoogleCreds)
    # ADC must NOT be consulted when the SA file already produced a creds
    # object (req 1.2 — the chain stops at the first complete set).
    assert sa_calls == [(str(sa_path), None)]
    assert adc_calls == []


def test_gcp_falls_back_to_adc_when_sa_env_unset(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    sa_called = False

    def sa_loader(path: str, scopes=None):
        nonlocal sa_called
        sa_called = True
        raise AssertionError("SA loader should not be called")

    def adc(scopes=None):
        return (_FakeGoogleCreds("adc", scopes=scopes), "fake-project")

    cm = Credential_Manager(
        google_auth_default=adc,
        google_service_account_loader=sa_loader,
    )

    result = cm.resolve_gcp()

    assert result.source == "application_default"
    assert sa_called is False


def test_gcp_falls_back_to_adc_when_sa_file_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv(
        "GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "does-not-exist.json")
    )

    def sa_loader(path: str, scopes=None):
        raise AssertionError("SA loader should not be called when file is missing")

    def adc(scopes=None):
        return (_FakeGoogleCreds("adc"), "fake-project")

    cm = Credential_Manager(
        google_auth_default=adc,
        google_service_account_loader=sa_loader,
    )

    result = cm.resolve_gcp()

    assert result.source == "application_default"


def test_gcp_chain_exhausted_raises_credential_error(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    def adc(scopes=None):
        # Simulate ADC returning no credentials.
        return (None, None)

    cm = Credential_Manager(google_auth_default=adc)

    with pytest.raises(CredentialError) as excinfo:
        cm.resolve_gcp()

    err = excinfo.value
    assert err.provider == "gcp"
    assert err.sources_tried == ("service_account_file", "application_default")
    assert err.exit_code == ExitCode.CREDENTIAL_FAILED


def test_gcp_adc_raising_is_treated_as_no_creds(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    def adc(scopes=None):
        raise RuntimeError("no metadata service")

    cm = Credential_Manager(google_auth_default=adc)

    with pytest.raises(CredentialError) as excinfo:
        cm.resolve_gcp()

    assert excinfo.value.provider == "gcp"
    assert "application_default" in excinfo.value.sources_tried


# ---------------------------------------------------------------------------
# Drive — drive.readonly scope only
# ---------------------------------------------------------------------------


def test_resolve_drive_pins_drive_readonly_scope(monkeypatch, tmp_path) -> None:
    sa_path = tmp_path / "sa.json"
    sa_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_path))

    captured: list[Optional[list[str]]] = []

    def sa_loader(path: str, scopes=None):
        captured.append(list(scopes) if scopes else None)
        return _FakeGoogleCreds("sa", scopes=list(scopes) if scopes else None)

    cm = Credential_Manager(google_service_account_loader=sa_loader)

    result = cm.resolve_drive()

    assert result.provider == "drive"
    assert result.source == "service_account_file"
    assert captured == [[DRIVE_READONLY_SCOPE]]
    assert result.google_credentials.scopes == [DRIVE_READONLY_SCOPE]


def test_resolve_drive_failure_reports_provider_drive(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    def adc(scopes=None):
        return (None, None)

    cm = Credential_Manager(google_auth_default=adc)

    with pytest.raises(CredentialError) as excinfo:
        cm.resolve_drive()

    err = excinfo.value
    # Drive is a Pull_Only_Source; a failed Drive resolution should surface
    # under provider="drive" (so the CLI can map it to DRIVE_ACCESS_FAILED if
    # appropriate, though the mapping itself lives in the CLI).
    assert err.provider == "drive"
    assert "service_account_file" in err.sources_tried
    assert "application_default" in err.sources_tried


# ---------------------------------------------------------------------------
# 401 / invalid-credentials classifier
# ---------------------------------------------------------------------------


def test_classify_credential_error_recognises_botocore_no_credentials() -> None:
    pytest.importorskip("botocore")
    from botocore.exceptions import NoCredentialsError

    assert classify_credential_error(NoCredentialsError()) is True


def test_classify_credential_error_recognises_403_client_error() -> None:
    pytest.importorskip("botocore")
    from botocore.exceptions import ClientError

    err = ClientError(
        error_response={
            "Error": {"Code": "AccessDenied", "Message": "no"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        operation_name="ListObjectsV2",
    )
    assert classify_credential_error(err) is True


def test_classify_credential_error_returns_false_for_unrelated_exception() -> None:
    assert classify_credential_error(ValueError("unrelated")) is False
