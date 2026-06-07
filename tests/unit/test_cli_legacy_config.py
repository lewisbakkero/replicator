"""Unit tests for `mcps.cli.detect_legacy_config`.

Validates: Requirement 1.5 ("If a configuration file containing the keys
`aws_access_key_id` or `aws_secret_access_key` in plaintext is present at the
legacy path `config.ini` ... refuse to start, exit with a non-zero exit code,
and emit an error record directing the operator to migrate ..., without
reading any value from that file.").
"""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from mcps.cli import detect_legacy_config
from mcps.errors import ExitCode, LegacyConfigDetected


def _write_ini(cwd: Path, body: str) -> Path:
    """Plant a `config.ini` containing `body`, dedented so triple-quoted
    Python strings can be indented for readability without breaking the INI
    grammar (real INI files have no leading whitespace on section / key
    lines).
    """

    path = cwd / "config.ini"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Negative cases (no exception, returns None).
# ---------------------------------------------------------------------------


def test_returns_none_when_no_config_ini_present(tmp_path: Path) -> None:
    # No file at all → helper is a no-op.
    assert detect_legacy_config(str(tmp_path)) is None


def test_returns_none_when_config_ini_has_no_aws_credentials_section(
    tmp_path: Path,
) -> None:
    _write_ini(
        tmp_path,
        """
        [other_section]
        unrelated_key = some-value
        """,
    )
    assert detect_legacy_config(str(tmp_path)) is None


def test_returns_none_when_aws_credentials_has_only_unrelated_keys(
    tmp_path: Path,
) -> None:
    _write_ini(
        tmp_path,
        """
        [aws_credentials]
        region = us-west-2
        endpoint_url = https://example.com
        """,
    )
    assert detect_legacy_config(str(tmp_path)) is None


def test_returns_none_when_relevant_key_lives_in_other_section(
    tmp_path: Path,
) -> None:
    # The plaintext-key check is scoped to the [aws_credentials] section: an
    # `aws_access_key_id` option in some unrelated section is not the legacy
    # file shape Requirement 1.5 is targeting.
    _write_ini(
        tmp_path,
        """
        [other_section]
        aws_access_key_id = AKIAEXAMPLE
        aws_secret_access_key = secret
        [aws_credentials]
        region = us-west-2
        """,
    )
    assert detect_legacy_config(str(tmp_path)) is None


# ---------------------------------------------------------------------------
# Positive cases (raises LegacyConfigDetected).
# ---------------------------------------------------------------------------


def test_raises_when_aws_credentials_contains_access_key_id(
    tmp_path: Path,
) -> None:
    path = _write_ini(
        tmp_path,
        """
        [aws_credentials]
        aws_access_key_id = AKIAEXAMPLE
        region = us-west-2
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(path)


def test_raises_when_aws_credentials_contains_secret_access_key(
    tmp_path: Path,
) -> None:
    path = _write_ini(
        tmp_path,
        """
        [aws_credentials]
        aws_secret_access_key = supersecretvalue
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(path)


def test_raises_when_aws_credentials_contains_both_keys(tmp_path: Path) -> None:
    path = _write_ini(
        tmp_path,
        """
        [aws_credentials]
        aws_access_key_id = AKIAEXAMPLE
        aws_secret_access_key = supersecretvalue
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(path)


def test_raises_when_relevant_key_is_in_aws_credentials_alongside_other_section(
    tmp_path: Path,
) -> None:
    path = _write_ini(
        tmp_path,
        """
        [other_section]
        unrelated_key = unrelated-value
        [aws_credentials]
        aws_access_key_id = AKIAEXAMPLE
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(path)


# ---------------------------------------------------------------------------
# Exit-code contract.
# ---------------------------------------------------------------------------


def test_legacy_config_detected_maps_to_exit_code_66(tmp_path: Path) -> None:
    _write_ini(
        tmp_path,
        """
        [aws_credentials]
        aws_access_key_id = AKIAEXAMPLE
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    # Both the dynamic to_exit_code() and the static class attribute must
    # equal LEGACY_CONFIG (66).
    assert exc_info.value.to_exit_code() == ExitCode.LEGACY_CONFIG
    assert int(exc_info.value.exit_code) == 66


# ---------------------------------------------------------------------------
# "Never reads values" guard: a syntactically broken value (e.g. with stray
# `=` signs that some parsers would choke on) must not influence detection.
# ---------------------------------------------------------------------------


def test_detection_ignores_value_content(tmp_path: Path) -> None:
    # The value of `aws_access_key_id` here would be hostile to a strict
    # parser, but the helper only inspects the key name and the section
    # header, so detection should still fire.
    path = _write_ini(
        tmp_path,
        """
        [aws_credentials]
        aws_access_key_id = AKIA = something = with = many = equals
        """,
    )

    with pytest.raises(LegacyConfigDetected) as exc_info:
        detect_legacy_config(str(tmp_path))

    assert exc_info.value.path == str(path)


def test_path_argument_is_not_modified_during_detection(
    tmp_path: Path,
) -> None:
    # Sanity check: detect_legacy_config never writes to the file. We capture
    # mtime + content before and after invocation.
    body = (
        "[aws_credentials]\n"
        "region = us-west-2\n"
    )
    path = _write_ini(tmp_path, body)
    before_mtime = os.path.getmtime(path)
    before_content = path.read_text(encoding="utf-8")

    detect_legacy_config(str(tmp_path))

    assert path.read_text(encoding="utf-8") == before_content
    assert os.path.getmtime(path) == before_mtime
