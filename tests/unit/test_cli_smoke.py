"""Smoke tests for the ``mcps`` CLI surface.

Validates: design.md "Testing Strategy > Smoke tests" — ``mcps --help``
exits 0.

These tests stay minimal so they run in milliseconds and can be invoked
on every push without pulling in the heavy provider SDKs. The end-to-end
``python -m mcps`` subprocess form is exercised in the integration tier
(task 34) and the dedicated smoke tier (task 40).
"""

from __future__ import annotations

import pytest

from mcps.cli import main


def test_mcps_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    """``mcps --help`` exits with code 0 and writes the usage banner to stdout.

    argparse raises ``SystemExit(0)`` from ``--help``; we let that
    propagate from :func:`main` and assert on the captured exit code.
    """
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    # ``SystemExit.code`` may be ``None``, ``0``, or a string; argparse's
    # ``--help`` always emits ``SystemExit(0)``.
    assert exc_info.value.code == 0

    captured = capsys.readouterr()
    assert "mcps" in captured.out
    assert "--config" in captured.out
    assert "--dry-run" in captured.out
    assert "--apply" in captured.out


def test_mcps_dry_run_apply_are_mutually_exclusive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Supplying both ``--dry-run`` and ``--apply`` exits non-zero.

    argparse's mutually-exclusive group emits a ``SystemExit(2)`` for
    this kind of usage error, distinct from ``SystemExit(0)`` for
    ``--help``.
    """
    with pytest.raises(SystemExit) as exc_info:
        main(["--dry-run", "--apply"])

    assert exc_info.value.code != 0
    captured = capsys.readouterr()
    assert "not allowed with argument" in captured.err or "mutually exclusive" in captured.err
