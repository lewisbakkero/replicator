"""``mcps doctor`` — operational self-checks.

The doctor surface is a small set of diagnostic subcommands that an
operator can run independently of a Sync_Run to verify the deployment
is healthy. It is intentionally separate from ``mcps.cli.run`` because:

- It is destructive-free by design (read-only IAM / config queries).
- It must work even when a full Sync_Run cannot start — for example
  before the legacy ``config.ini`` has been removed.
- It is intended to be wired into operator playbooks and migration
  procedures (see ``MIGRATION.md``).

The first check, ``--check-iam``, supports the migration plan's
"rotate the leaked AWS credentials" step (design.md "Migration Plan",
step 1). It calls ``iam:GetUser`` + ``iam:ListAccessKeys`` and asserts
that the leaked key id (``AKIAYQ4K35M7H3INY75N``) is either absent
from the bound IAM user's access-key list (deleted) or present with
``Status == "Inactive"`` (deactivated).

Validates: design migration plan step 1; supports Requirement 1.5.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Callable, Optional, TextIO

from .credentials import Credential_Manager
from .errors import CredentialError, McpsError


# The leaked AWS access key id from the legacy ``config.ini``. Hard-coded
# so the doctor check can be run as a one-shot from an operator's
# terminal without any configuration. The corresponding secret access
# key is NOT stored anywhere in this repository — see ``MIGRATION.md``.
LEAKED_AWS_ACCESS_KEY_ID = "AKIAYQ4K35M7H3INY75N"


__all__ = [
    "LEAKED_AWS_ACCESS_KEY_ID",
    "build_doctor_parser",
    "check_iam",
    "doctor_main",
]


# ---------------------------------------------------------------------------
# IAM check
# ---------------------------------------------------------------------------


def _iam_status_for_key(
    iam_client: Any,
    *,
    leaked_key_id: str,
) -> tuple[str, Optional[str]]:
    """Return ``(status, user_name)`` for ``leaked_key_id`` on the bound user.

    ``status`` is one of:

    - ``"absent"`` — the leaked key id is not in the user's access-key
      list (it has been deleted).
    - ``"inactive"`` — the leaked key id is present with
      ``Status == "Inactive"``.
    - ``"active"`` — the leaked key id is present with
      ``Status == "Active"``. This is the FAIL case.
    - ``"unknown"`` — the leaked key id is present with a status we do
      not recognise (e.g. a future AWS-side enum value). Treated as a
      FAIL for safety.

    ``user_name`` is the IAM user name returned by ``GetUser`` so the
    operator can see which identity the check ran against. May be
    ``None`` if ``GetUser`` returned an unexpected shape (defensive
    guard; AWS in practice always returns a User payload).
    """
    user_response = iam_client.get_user()
    user = user_response.get("User", {}) if isinstance(user_response, dict) else {}
    user_name = user.get("UserName") if isinstance(user, dict) else None

    if not user_name:
        # Without a user name we cannot list access keys for "the bound
        # user". We surface this as a check failure rather than crash.
        return ("unknown", None)

    list_response = iam_client.list_access_keys(UserName=user_name)
    metadata_list = (
        list_response.get("AccessKeyMetadata", [])
        if isinstance(list_response, dict)
        else []
    )

    for meta in metadata_list:
        if not isinstance(meta, dict):
            continue
        if meta.get("AccessKeyId") != leaked_key_id:
            continue
        status = meta.get("Status")
        if status == "Inactive":
            return ("inactive", user_name)
        if status == "Active":
            return ("active", user_name)
        return ("unknown", user_name)

    return ("absent", user_name)


def check_iam(
    *,
    stderr: TextIO,
    leaked_key_id: str = LEAKED_AWS_ACCESS_KEY_ID,
    credential_manager: Optional[Credential_Manager] = None,
    iam_client_factory: Optional[Callable[[Any], Any]] = None,
) -> int:
    """Run the IAM self-check and return a process exit code.

    Resolves AWS credentials through the same chain ``mcps`` uses, calls
    ``iam:GetUser`` + ``iam:ListAccessKeys`` on the bound IAM user, and
    asserts that ``leaked_key_id`` is either absent or has
    ``Status == "Inactive"``. Prints a single-line PASS / FAIL summary
    to ``stderr``.

    Test seams (all keyword-only):

    - ``leaked_key_id`` overrides the default ``AKIAYQ4K35M7H3INY75N``
      so unit tests can exercise the assertion logic against synthesised
      key ids.
    - ``credential_manager`` overrides the default
      :class:`Credential_Manager` so tests can bypass real AWS chain
      resolution.
    - ``iam_client_factory(boto3_session) -> iam_client`` overrides
      the real ``boto3.Session.client('iam')`` constructor; tests pass
      a closure returning a stub IAM client with ``get_user`` and
      ``list_access_keys`` methods.

    Returns the process exit code:

    - ``0`` — leaked key is absent or inactive (PASS).
    - ``1`` — leaked key is active or its status is not recognised
      (FAIL).
    - The exit code of the wrapped :class:`McpsError` (typically
      ``71`` :class:`CredentialError`) when credential resolution
      fails or AWS returns an error before the assertion can run.
    """
    cm = credential_manager if credential_manager is not None else Credential_Manager()

    try:
        aws_creds = cm.resolve_aws()
    except McpsError as exc:
        print(
            f"FAIL: could not resolve AWS credentials: "
            f"{type(exc).__name__}: {exc}",
            file=stderr,
        )
        return int(exc.to_exit_code())

    session = aws_creds.boto3_session
    if session is None:
        # Defensive: resolve_aws is documented to return a session for
        # provider="aws"; surface a CredentialError exit code if we
        # ever hit this path so the operator sees a typed failure.
        print(
            "FAIL: AWS credential chain returned no boto3 session",
            file=stderr,
        )
        return int(CredentialError(
            provider="aws", sources_tried=("doctor-no-session",)
        ).to_exit_code())

    if iam_client_factory is None:
        def iam_client_factory(s: Any) -> Any:
            return s.client("iam")

    try:
        iam_client = iam_client_factory(session)
    except Exception as exc:  # noqa: BLE001 - SDK-mapped
        print(
            f"FAIL: could not construct IAM client: "
            f"{type(exc).__name__}: {exc}",
            file=stderr,
        )
        return int(CredentialError(
            provider="aws", sources_tried=("doctor-iam-client",)
        ).to_exit_code())

    try:
        status, user_name = _iam_status_for_key(
            iam_client, leaked_key_id=leaked_key_id
        )
    except Exception as exc:  # noqa: BLE001 - SDK-mapped
        print(
            f"FAIL: iam:GetUser / iam:ListAccessKeys call failed: "
            f"{type(exc).__name__}: {exc}",
            file=stderr,
        )
        return int(CredentialError(
            provider="aws", sources_tried=("doctor-iam-call",)
        ).to_exit_code())

    user_label = user_name or "<unknown>"

    if status == "absent":
        print(
            f"PASS: leaked AWS access key {leaked_key_id} is absent "
            f"from IAM user {user_label} (deleted).",
            file=stderr,
        )
        return 0

    if status == "inactive":
        print(
            f"PASS: leaked AWS access key {leaked_key_id} is present "
            f"on IAM user {user_label} but Status=Inactive.",
            file=stderr,
        )
        return 0

    if status == "active":
        print(
            f"FAIL: leaked AWS access key {leaked_key_id} is still "
            f"ACTIVE on IAM user {user_label}. Rotate it now — see "
            f"MIGRATION.md step 1.",
            file=stderr,
        )
        return 1

    # status == "unknown": be loud about it; treat as FAIL.
    print(
        f"FAIL: leaked AWS access key {leaked_key_id} is present on "
        f"IAM user {user_label} with an unrecognised status. Treating "
        f"as not-rotated. See MIGRATION.md step 1.",
        file=stderr,
    )
    return 1


# ---------------------------------------------------------------------------
# Argparse for `mcps doctor`
# ---------------------------------------------------------------------------


def build_doctor_parser() -> argparse.ArgumentParser:
    """Build the ``mcps doctor`` subcommand parser.

    Kept separate from :func:`mcps.cli.parse_args` so doctor lives in
    its own module and the main CLI can dispatch by detecting
    ``argv[0] == "doctor"``.
    """
    parser = argparse.ArgumentParser(
        prog="mcps doctor",
        description=(
            "Operational self-checks for the mcps deployment. Run "
            "with --check-iam to verify the leaked AWS access key has "
            "been rotated."
        ),
    )
    parser.add_argument(
        "--check-iam",
        action="store_true",
        help=(
            "Verify that the leaked AWS access key "
            f"{LEAKED_AWS_ACCESS_KEY_ID} is no longer Active on the "
            "bound IAM user. Calls iam:GetUser and "
            "iam:ListAccessKeys; exits 0 if the key is absent or "
            "Inactive, non-zero otherwise."
        ),
    )
    return parser


def doctor_main(
    argv: Optional[list[str]] = None,
    *,
    stderr: Optional[TextIO] = None,
    credential_manager: Optional[Credential_Manager] = None,
    iam_client_factory: Optional[Callable[[Any], Any]] = None,
) -> int:
    """Top-level entry point for ``mcps doctor``.

    Dispatches the requested check (currently only ``--check-iam``)
    and translates failures into exit codes. Argparse-level failures
    propagate as :class:`SystemExit`.
    """
    if stderr is None:
        stderr = sys.stderr

    parser = build_doctor_parser()
    args = parser.parse_args(argv)

    if not args.check_iam:
        parser.print_help(stderr)
        print(
            "\nerror: mcps doctor requires a check flag "
            "(e.g. --check-iam).",
            file=stderr,
        )
        return 2

    return check_iam(
        stderr=stderr,
        credential_manager=credential_manager,
        iam_client_factory=iam_client_factory,
    )
