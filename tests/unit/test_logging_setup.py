"""Unit tests for ``mcps.logging_setup``.

Covers:

- :class:`JsonFormatter` JSON shape (5 documented fields, single line of
  JSON per record) — req 14.3.
- Level mapping: stdlib ``WARNING`` is rendered as ``"WARN"``; the other
  four levels pass through unchanged — req 14.3.
- ``run_id`` propagation via :func:`bind_run_id`, including the default
  ``"unset"`` sentinel before any binding is active and the per-record
  ``extra={"run_id": ...}`` override path — req 14.5.
- :class:`Redactor` integration: regex-detected secrets in the message
  body and allowlisted-field values passed via ``extra=`` are both
  redacted — req 14.4.
- :func:`setup_logging` installs a single handler writing to stderr at
  the requested level, accepts ``"WARN"`` alongside the stdlib spellings,
  and is idempotent across repeated calls — req 14.3.

Validates: Requirements 14.3, 14.4, 14.5.
"""

from __future__ import annotations

import io
import json
import logging
import sys

import pytest

from mcps.logging_setup import (
    JsonFormatter,
    bind_run_id,
    setup_logging,
)
from mcps.redaction import REDACTED


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    *,
    level: int = logging.INFO,
    name: str = "mcps.test",
    msg: str = "hello",
    args=None,
    extra: dict | None = None,
) -> logging.LogRecord:
    """Construct a ``LogRecord`` directly (no logger plumbing required)."""
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=args,
        exc_info=None,
    )
    if extra:
        for key, value in extra.items():
            setattr(record, key, value)
    return record


def _format_one(record: logging.LogRecord) -> dict:
    """Format a record through ``JsonFormatter`` and decode the JSON line."""
    out = JsonFormatter().format(record)
    # Single-line invariant: no embedded newlines in the formatter output.
    assert "\n" not in out
    return json.loads(out)


@pytest.fixture(autouse=True)
def _reset_mcps_logger():
    """Snapshot and restore the ``mcps`` logger between tests so handlers
    installed by :func:`setup_logging` in one test do not leak into the
    next."""
    logger = logging.getLogger("mcps")
    saved_handlers = list(logger.handlers)
    saved_level = logger.level
    saved_propagate = logger.propagate
    try:
        yield
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
        for handler in saved_handlers:
            logger.addHandler(handler)
        logger.setLevel(saved_level)
        logger.propagate = saved_propagate


# ---------------------------------------------------------------------------
# JSON shape
# ---------------------------------------------------------------------------


def test_format_emits_documented_fields() -> None:
    """The 5 documented fields are present and have the right shape."""
    record = _make_record(msg="a message")

    decoded = _format_one(record)

    assert set(decoded.keys()) >= {
        "timestamp",
        "level",
        "run_id",
        "event",
        "message",
    }
    assert decoded["message"] == "a message"
    assert decoded["level"] == "INFO"
    assert decoded["event"] == "mcps.test"
    assert decoded["run_id"] == "unset"
    # Timestamp shape: ISO-8601 UTC with millisecond precision and Z.
    ts = decoded["timestamp"]
    assert ts.endswith("Z")
    # ``YYYY-MM-DDTHH:MM:SS.mmmZ`` is exactly 24 characters.
    assert len(ts) == 24
    assert ts[10] == "T"
    assert ts[19] == "."


def test_format_uses_record_name_as_default_event() -> None:
    record = _make_record(name="mcps.replication", msg="x")
    assert _format_one(record)["event"] == "mcps.replication"


def test_format_uses_explicit_event_extra_when_provided() -> None:
    record = _make_record(
        name="mcps.replication",
        msg="x",
        extra={"event": "replicate-success"},
    )
    assert _format_one(record)["event"] == "replicate-success"


def test_format_outputs_single_line_json() -> None:
    """No embedded newlines, parseable as JSON, terminator-free."""
    out = JsonFormatter().format(_make_record(msg="hello"))
    assert "\n" not in out
    assert "\r" not in out
    # ``json.loads`` succeeding confirms it is a complete JSON object.
    json.loads(out)


def test_format_renders_args_via_get_message() -> None:
    """``record.getMessage()`` runs printf-style formatting on the args."""
    record = _make_record(msg="value=%s", args=("42",))
    assert _format_one(record)["message"] == "value=42"


# ---------------------------------------------------------------------------
# Level mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "level_int,expected",
    [
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARN"),
        (logging.ERROR, "ERROR"),
        (logging.CRITICAL, "ERROR"),
    ],
)
def test_level_mapping(level_int: int, expected: str) -> None:
    """Stdlib ``WARNING`` is collapsed to ``WARN``; ``CRITICAL`` to ``ERROR``."""
    record = _make_record(level=level_int)
    assert _format_one(record)["level"] == expected


# ---------------------------------------------------------------------------
# run_id propagation
# ---------------------------------------------------------------------------


def test_run_id_default_is_unset_outside_bind() -> None:
    record = _make_record()
    assert _format_one(record)["run_id"] == "unset"


def test_bind_run_id_sets_run_id_field() -> None:
    with bind_run_id("abc12345"):
        record = _make_record()
        decoded = _format_one(record)
    assert decoded["run_id"] == "abc12345"


def test_bind_run_id_resets_on_exit() -> None:
    with bind_run_id("abc12345"):
        pass
    record = _make_record()
    assert _format_one(record)["run_id"] == "unset"


def test_bind_run_id_nested_restores_prior_value() -> None:
    with bind_run_id("outer"):
        with bind_run_id("inner"):
            assert _format_one(_make_record())["run_id"] == "inner"
        assert _format_one(_make_record())["run_id"] == "outer"
    assert _format_one(_make_record())["run_id"] == "unset"


def test_per_record_run_id_overrides_contextvar() -> None:
    """``extra={"run_id": ...}`` wins over the contextvar binding."""
    with bind_run_id("from-ctx"):
        record = _make_record(extra={"run_id": "from-extra"})
        assert _format_one(record)["run_id"] == "from-extra"


# ---------------------------------------------------------------------------
# Redaction integration
# ---------------------------------------------------------------------------


def test_redactor_scrubs_aws_access_key_id_in_message() -> None:
    record = _make_record(msg="connecting with key AKIAABCDEFGHIJKLMNOP now")
    decoded = _format_one(record)
    assert "AKIAABCDEFGHIJKLMNOP" not in decoded["message"]
    assert REDACTED in decoded["message"]


def test_redactor_scrubs_allowlisted_field_in_extras() -> None:
    record = _make_record(
        msg="auth attempt",
        extra={"client_secret": "shhh"},
    )
    decoded = _format_one(record)
    assert decoded["client_secret"] == REDACTED


def test_redactor_scrubs_bearer_token_in_message() -> None:
    record = _make_record(msg="header: Authorization: Bearer abc.DEF-123")
    decoded = _format_one(record)
    assert "abc.DEF-123" not in decoded["message"]
    assert REDACTED in decoded["message"]


def test_redactor_does_not_alter_innocent_message() -> None:
    plain = "no secrets here, all good"
    record = _make_record(msg=plain)
    assert _format_one(record)["message"] == plain


# ---------------------------------------------------------------------------
# setup_logging behaviour
# ---------------------------------------------------------------------------


def test_setup_logging_returns_mcps_logger() -> None:
    logger = setup_logging("INFO")
    assert logger.name == "mcps"


def test_setup_logging_installs_single_stderr_handler() -> None:
    logger = setup_logging("INFO")
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr
    assert isinstance(handler.formatter, JsonFormatter)


def test_setup_logging_is_idempotent() -> None:
    """Repeated calls converge to a single handler."""
    setup_logging("INFO")
    setup_logging("DEBUG")
    setup_logging("WARN")
    logger = logging.getLogger("mcps")
    assert len(logger.handlers) == 1


def test_setup_logging_accepts_warn_alias() -> None:
    logger = setup_logging("WARN")
    assert logger.level == logging.WARNING


def test_setup_logging_rejects_unknown_level() -> None:
    with pytest.raises(ValueError):
        setup_logging("VERBOSE")


def test_setup_logging_debug_emits_debug_logs(capsys) -> None:
    """At level=DEBUG, DEBUG records reach the handler."""
    setup_logging("DEBUG")
    logger = logging.getLogger("mcps.test")

    logger.debug("debug-msg")
    logger.info("info-msg")

    captured = capsys.readouterr()
    # Each record is its own line on stderr, with LF only.
    lines = [ln for ln in captured.err.split("\n") if ln]
    assert len(lines) == 2
    decoded = [json.loads(ln) for ln in lines]
    levels = [r["level"] for r in decoded]
    messages = [r["message"] for r in decoded]
    assert levels == ["DEBUG", "INFO"]
    assert messages == ["debug-msg", "info-msg"]


def test_setup_logging_info_filters_debug(capsys) -> None:
    setup_logging("INFO")
    logger = logging.getLogger("mcps.test")

    logger.debug("debug-msg")
    logger.info("info-msg")

    captured = capsys.readouterr()
    lines = [ln for ln in captured.err.split("\n") if ln]
    assert len(lines) == 1
    decoded = json.loads(lines[0])
    assert decoded["level"] == "INFO"
    assert decoded["message"] == "info-msg"


def test_setup_logging_writes_to_stderr_not_stdout(capsys) -> None:
    setup_logging("INFO")
    logging.getLogger("mcps.test").info("hi")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "hi" in captured.err


def test_setup_logging_emits_lf_terminated_lines(capsys) -> None:
    """JSONL convention: one record per line, LF only (no CRLF)."""
    setup_logging("INFO")
    logger = logging.getLogger("mcps.test")

    logger.info("first")
    logger.info("second")

    captured = capsys.readouterr()
    assert "\r" not in captured.err
    # Three pieces split by \n: two records and a trailing empty string
    # (because each record ends with the terminator).
    parts = captured.err.split("\n")
    assert parts[-1] == ""
    assert len(parts) == 3
    json.loads(parts[0])
    json.loads(parts[1])


def test_setup_logging_propagates_run_id_through_logger(capsys) -> None:
    """End-to-end: ``bind_run_id`` + a real logger call produces the
    expected JSON line on stderr."""
    setup_logging("INFO")
    logger = logging.getLogger("mcps.replication")

    with bind_run_id("run-9876"):
        logger.info("replicating")

    captured = capsys.readouterr()
    line = captured.err.strip()
    decoded = json.loads(line)
    assert decoded["run_id"] == "run-9876"
    assert decoded["event"] == "mcps.replication"
    assert decoded["message"] == "replicating"
    assert decoded["level"] == "INFO"


def test_handler_format_writes_json_line_to_its_stream() -> None:
    """Direct verification that the configured handler writes a single
    JSON-encoded line per record to its stream."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(JsonFormatter())
    handler.handle(_make_record(msg="payload"))

    out = stream.getvalue()
    assert out.endswith("\n")
    decoded = json.loads(out.rstrip("\n"))
    assert decoded["message"] == "payload"
    assert decoded["level"] == "INFO"
