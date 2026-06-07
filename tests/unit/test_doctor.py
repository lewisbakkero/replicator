"""Unit tests for :mod:`mcps.doctor`.

These tests cover the IAM rotation self-check that supports the
migration plan's step 1 (rotate the leaked AWS credentials, design.md
"Migration Plan"). They use a stub IAM client (no boto3) so the test
suite stays hermetic and never touches AWS.

Validates: design migration plan step 1; supports Requirement 1.5.
"""

from __future__ import annotations

import io
from typing import Any, Optional

import pytest

from mcps.credentials import Credential_Manager, ResolvedCredentials
from mcps.doctor import (
    LEAKED_AWS_ACCESS_KEY_ID,
    check_iam,
    doctor_main,
)
from mcps.errors import CredentialError, ExitCode


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _StubIamClient:
    """Stub that mimics the slice of ``boto3.client('iam')`` we touch.

    Provides ``get_user`` (returns ``{"User": {"UserName": ...}}``) and
    ``list_access_keys`` (returns ``{"AccessKeyMetadata": [...]}``).
    """

    def __init__(
        self,
        *,
        user_name: str = "mcps-test-user",
        access_keys: Optional[list[dict[str, str]]] = None,
        get_user_exc: Optional[BaseException] = None,
        list_keys_exc: Optional[BaseException] = None,
    ) -> None:
        self.user_name = user_name
        self.access_keys = list(access_keys or [])
        self.get_user_exc = get_user_exc
        self.list_keys_exc = list_keys_exc
        self.list_access_keys_calls: list[dict[str, Any]] = []

    def get_user(self) -> dict:
        if self.get_user_exc is not None:
            raise self.get_user_exc
        return {"User": {"UserName": self.user_name}}

    def list_access_keys(self, *, UserName: str) -> dict:  # noqa: N803
        self.list_access_keys_calls.append({"UserName": UserName})
        if self.list_keys_exc is not None:
            raise self.list_keys_exc
        return {"AccessKeyMetadata": list(self.access_keys)}


class _StubCredentialManager:
    """Stub Credential_Manager that returns a marker session.

    The real :class:`Credential_Manager.resolve_aws` returns a
    ``ResolvedCredentials`` carrying a ``boto3.Session``; we substitute
    a sentinel object since the doctor only forwards it to the
    ``iam_client_factory`` injection point.
    """

    def __init__(
        self,
        *,
        session: Any = object(),
        raise_error: Optional[BaseException] = None,
    ) -> None:
        self._session = session
        self._raise_error = raise_error

    def resolve_aws(self) -> ResolvedCredentials:
        if self._raise_error is not None:
            raise self._raise_error
        return ResolvedCredentials(
            provider="aws",
            source="env",
            boto3_session=self._session,
        )


def _meta(key_id: str, status: str) -> dict[str, str]:
    return {"AccessKeyId": key_id, "Status": status}


# ---------------------------------------------------------------------------
# check_iam — core assertion behaviour
# ---------------------------------------------------------------------------


def test_check_iam_passes_when_leaked_key_absent() -> None:
    """A user with no record of the leaked key id passes (key was deleted)."""
    iam = _StubIamClient(
        user_name="rotated-user",
        access_keys=[_meta("AKIANEWKEYABCDEFGHIJ", "Active")],
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc == 0
    output = stderr.getvalue()
    assert "PASS" in output
    assert LEAKED_AWS_ACCESS_KEY_ID in output
    assert "absent" in output
    assert "rotated-user" in output
    # Sanity: the doctor really did call list_access_keys for the
    # bound user.
    assert iam.list_access_keys_calls == [{"UserName": "rotated-user"}]


def test_check_iam_passes_when_leaked_key_inactive() -> None:
    """An ``Inactive`` leaked key passes (deactivated, awaiting deletion)."""
    iam = _StubIamClient(
        user_name="rotated-user",
        access_keys=[
            _meta("AKIANEWKEYABCDEFGHIJ", "Active"),
            _meta(LEAKED_AWS_ACCESS_KEY_ID, "Inactive"),
        ],
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc == 0
    output = stderr.getvalue()
    assert "PASS" in output
    assert "Inactive" in output
    assert LEAKED_AWS_ACCESS_KEY_ID in output


def test_check_iam_fails_when_leaked_key_active() -> None:
    """An ``Active`` leaked key fails — the rotation has not happened."""
    iam = _StubIamClient(
        user_name="leaked-user",
        access_keys=[_meta(LEAKED_AWS_ACCESS_KEY_ID, "Active")],
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc != 0
    output = stderr.getvalue()
    assert "FAIL" in output
    assert "ACTIVE" in output
    assert LEAKED_AWS_ACCESS_KEY_ID in output
    # The FAIL message must point the operator at the migration doc.
    assert "MIGRATION.md" in output


def test_check_iam_fails_when_leaked_key_has_unknown_status() -> None:
    """A status value the doctor does not recognise is treated as FAIL.

    AWS today returns only ``Active`` / ``Inactive``, but a future enum
    value must not be silently treated as a pass.
    """
    iam = _StubIamClient(
        access_keys=[_meta(LEAKED_AWS_ACCESS_KEY_ID, "Quarantined")],
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc != 0
    assert "unrecognised status" in stderr.getvalue()


# ---------------------------------------------------------------------------
# Override the leaked key id (test-only seam)
# ---------------------------------------------------------------------------


def test_check_iam_honors_overridden_leaked_key_id() -> None:
    """The ``leaked_key_id`` parameter steers the assertion target.

    Used so the unit tests can exercise the absent / inactive / active
    branches against synthetic key ids without ever embedding the
    leaked secret elsewhere.
    """
    custom = "AKIATESTONLY00000000"
    iam = _StubIamClient(
        access_keys=[_meta(custom, "Inactive")],
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        leaked_key_id=custom,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc == 0
    assert custom in stderr.getvalue()


# ---------------------------------------------------------------------------
# Failure modes that surface as McpsError exit codes
# ---------------------------------------------------------------------------


def test_check_iam_returns_credential_exit_code_when_resolve_aws_fails() -> None:
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(
            raise_error=CredentialError(
                provider="aws",
                sources_tried=("env", "profile", "instance-role"),
            ),
        ),
        iam_client_factory=lambda _session: pytest.fail(
            "iam_client_factory should not run when credentials fail"
        ),
    )

    assert rc == int(ExitCode.CREDENTIAL_FAILED)
    assert "FAIL" in stderr.getvalue()


def test_check_iam_returns_credential_exit_code_when_iam_call_raises() -> None:
    """An SDK error during ``GetUser`` / ``ListAccessKeys`` is mapped."""
    iam = _StubIamClient(
        get_user_exc=RuntimeError("AccessDenied: iam:GetUser"),
    )
    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc == int(ExitCode.CREDENTIAL_FAILED)
    output = stderr.getvalue()
    assert "FAIL" in output
    assert "iam:GetUser" in output or "GetUser" in output


def test_check_iam_returns_credential_exit_code_when_no_session() -> None:
    """A credential manager that returns a session-less payload fails."""
    class _NoSessionCM:
        def resolve_aws(self) -> ResolvedCredentials:
            return ResolvedCredentials(
                provider="aws", source="env", boto3_session=None
            )

    stderr = io.StringIO()

    rc = check_iam(
        stderr=stderr,
        credential_manager=_NoSessionCM(),  # type: ignore[arg-type]
    )

    assert rc == int(ExitCode.CREDENTIAL_FAILED)
    assert "no boto3 session" in stderr.getvalue()


# ---------------------------------------------------------------------------
# doctor_main / argparse surface
# ---------------------------------------------------------------------------


def test_doctor_main_dispatches_to_check_iam() -> None:
    iam = _StubIamClient(
        access_keys=[_meta(LEAKED_AWS_ACCESS_KEY_ID, "Inactive")],
    )
    stderr = io.StringIO()

    rc = doctor_main(
        ["--check-iam"],
        stderr=stderr,
        credential_manager=_StubCredentialManager(),
        iam_client_factory=lambda _session: iam,
    )

    assert rc == 0
    assert "PASS" in stderr.getvalue()


def test_doctor_main_without_check_flag_returns_non_zero_and_prints_help() -> None:
    stderr = io.StringIO()

    rc = doctor_main([], stderr=stderr)

    assert rc != 0
    output = stderr.getvalue()
    assert "--check-iam" in output


def test_doctor_main_help_exits_zero() -> None:
    """``mcps doctor --help`` follows argparse convention (SystemExit(0))."""
    with pytest.raises(SystemExit) as exc_info:
        doctor_main(["--help"])
    assert exc_info.value.code == 0
