"""Smoke test for the legacy `config.ini` detector.

Validates: Requirement 1.5.

This test exercises the operator scenario described in Requirement 1.5: the
operator runs `mcps` from a directory that still contains the historical
`config.ini` with plaintext AWS keys, and the system refuses to start with
exit code 66 (`LEGACY_CONFIG`).

The module covers two layers:

* In-process tests that call :func:`mcps.cli.detect_legacy_config` directly
  and assert on the raised :class:`LegacyConfigDetected` exception. These
  run in milliseconds and validate the helper's contract.
* A true subprocess smoke test that invokes ``python -m mcps --dry-run``
  with a planted ``config.ini`` in the working directory. This is the
  operator-facing surface — exit code 66, the absolute file path on stderr,
  and (critically) no credential VALUE leaked. The last assertion confirms
  the :class:`mcps.redaction.Redactor` is wired through the startup error
  path so a misconfigured system cannot accidentally publish secrets.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from mcps.cli import detect_legacy_config
from mcps.errors import ExitCode, LegacyConfigDetected


# ---------------------------------------------------------------------------
# Fixtures: realistic legacy config bodies
# ---------------------------------------------------------------------------

# Realistic-looking legacy file matching the shape of `config.ini` checked
# into older versions of this repository (uploader.py / delete.py era).
_REALISTIC_LEGACY_INI = """
[aws_credentials]
aws_access_key_id = AKIAEXAMPLEKEYID1234
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
region = us-west-2

[google_drive]
folder_id = 1A2B3C4D5E6F7G8H9I0J
service_account_file = /etc/mcps/service-account.json

[s3]
bucket = my-photo-backup-bucket
"""

_LEGACY_FILE_NO_PLAINTEXT_KEYS = """
[aws_credentials]
region = us-west-2

[s3]
bucket = my-photo-backup-bucket
"""


# Credential VALUES that must never appear in stderr. Kept as a module-level
# tuple so the subprocess test below can iterate it without re-parsing the
# planted file.
_PLAINTEXT_AKIA_VALUE = "AKIAEXAMPLEKEYID1234"
_PLAINTEXT_SECRET_VALUE = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
_PLAINTEXT_DRIVE_FOLDER = "1A2B3C4D5E6F7G8H9I0J"


# ---------------------------------------------------------------------------
# In-process tests (cheap, fast)
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_smoke_realistic_legacy_config_is_rejected_with_exit_code_66(
    tmp_path: Path,
) -> None:
    """Operator scenario: planted `config.ini` with plaintext AWS keys.

    The detector must refuse to proceed and surface the documented exit
    code so cron / systemd post-processing can distinguish this failure
    from other startup failures.
    """

    legacy_path = tmp_path / "config.ini"
    legacy_path.write_text(_REALISTIC_LEGACY_INI, encoding="utf-8")

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(legacy_path)
    assert exc_info.value.to_exit_code() == ExitCode.LEGACY_CONFIG
    assert int(exc_info.value.to_exit_code()) == 66


@pytest.mark.smoke
def test_smoke_legacy_config_without_plaintext_keys_is_accepted(
    tmp_path: Path,
) -> None:
    """Planted `config.ini` without `aws_access_key_id` / `aws_secret_access_key`
    must not trigger the legacy guard. The migrated config form keeps
    `[aws_credentials]` for non-secret options like `region`, and that is
    fine — secrets must come from env vars / profiles / instance roles.
    """

    legacy_path = tmp_path / "config.ini"
    legacy_path.write_text(_LEGACY_FILE_NO_PLAINTEXT_KEYS, encoding="utf-8")

    # Returns None and does not raise.
    assert detect_legacy_config(str(tmp_path)) is None


@pytest.mark.smoke
def test_smoke_no_config_ini_present_is_accepted(tmp_path: Path) -> None:
    """No legacy file present → helper is a no-op."""

    assert detect_legacy_config(str(tmp_path)) is None


# ---------------------------------------------------------------------------
# True subprocess smoke test (operator surface)
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_smoke_subprocess_legacy_config_exits_66_and_does_not_leak_values(
    tmp_path: Path,
) -> None:
    """End-to-end: ``python -m mcps --dry-run`` rejects a planted `config.ini`.

    Asserts the three operator-facing guarantees of Requirement 1.5:

    1. Exit code is 66 (`ExitCode.LEGACY_CONFIG`) — distinguishable from
       every other startup failure (config-invalid 64, catalog-invalid 65,
       credential 71, ...).
    2. Stderr names the absolute path of the offending file so operators
       know which file to migrate.
    3. No credential VALUE planted in the file appears anywhere in stderr.
       This is the secret-handling guarantee: the detector must not log
       any value it read from the file. The planted body contains a real
       AKIA-shaped key and a 40-char secret — neither must be observable.
    """

    legacy_path = tmp_path / "config.ini"
    legacy_path.write_text(_REALISTIC_LEGACY_INI, encoding="utf-8")

    # Realpath because macOS resolves /var → /private/var (and friends),
    # so the path observed by ``os.getcwd()`` inside the subprocess can
    # differ from the lexical ``tmp_path``. Compare both forms below.
    expected_path_lexical = str(legacy_path)
    expected_path_resolved = str(Path(legacy_path).resolve())

    completed = subprocess.run(
        [sys.executable, "-m", "mcps", "--dry-run"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        # Strip variables that could change the run's behaviour (e.g.
        # AWS_PROFILE pointing at a real profile). The legacy guard runs
        # before any credential resolution, but a clean env makes the
        # test reproducible across developer machines.
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
            "HOME": os.environ.get("HOME", ""),
            # Keep PYTHONIOENCODING explicit so stderr is decodable on
            # CI runners that default to ASCII.
            "PYTHONIOENCODING": "utf-8",
        },
    )

    # 1. Exit code is exactly 66.
    assert completed.returncode == int(ExitCode.LEGACY_CONFIG), (
        f"expected exit code {int(ExitCode.LEGACY_CONFIG)} "
        f"(LEGACY_CONFIG); got {completed.returncode}.\n"
        f"stderr={completed.stderr!r}\nstdout={completed.stdout!r}"
    )
    assert completed.returncode == 66

    # 2. Stderr names the absolute path of the planted file. We accept
    #    either the lexical form (``tmp_path/config.ini``) or the
    #    realpath form, since macOS's /var → /private/var symlink makes
    #    both representations correct.
    stderr = completed.stderr
    assert (
        expected_path_lexical in stderr or expected_path_resolved in stderr
    ), (
        f"expected stderr to name {expected_path_lexical!r} "
        f"(or its realpath {expected_path_resolved!r}); "
        f"got stderr={stderr!r}"
    )

    # 3. No credential VALUE planted in the file appears in stderr. This
    #    is the secret-handling guarantee: even though the detector reads
    #    the file's structure (section names + option names), it must not
    #    surface any *value*. The planted body contains a real AKIA-shaped
    #    key, a 40-char secret, and a Drive folder id — none are allowed
    #    to leak.
    for forbidden_value in (
        _PLAINTEXT_AKIA_VALUE,
        _PLAINTEXT_SECRET_VALUE,
        _PLAINTEXT_DRIVE_FOLDER,
    ):
        assert forbidden_value not in stderr, (
            f"credential value {forbidden_value!r} leaked into stderr: "
            f"{stderr!r}"
        )
        # Defence in depth: also check stdout. The CLI writes everything
        # to stderr, but a regression that flips a stream would otherwise
        # silently smuggle the secret into stdout.
        assert forbidden_value not in completed.stdout, (
            f"credential value {forbidden_value!r} leaked into stdout: "
            f"{completed.stdout!r}"
        )
