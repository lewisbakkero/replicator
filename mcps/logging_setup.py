"""Structured JSON logging for the ``mcps`` package.

This module configures Python's stdlib ``logging`` to emit one JSON object
per record on stderr. Every record is run through ``Redactor.scrub_record``
before serialisation so credential-shaped substrings and allowlisted-field
values can never leak through the log channel.

Public surface:

- :class:`JsonFormatter` — a ``logging.Formatter`` that emits one line of
  JSON per record with the fields documented in design.md req 14.3:
  ``timestamp`` (ISO-8601 UTC, millisecond precision), ``level`` (one of
  ``DEBUG`` / ``INFO`` / ``WARN`` / ``ERROR``), ``run_id``, ``event``, and
  ``message``.
- :func:`setup_logging` — configures the ``mcps`` logger hierarchy:
  removes prior handlers, attaches a single ``StreamHandler(stream=stderr)``
  with the :class:`JsonFormatter`, and sets the level. Returns the root
  ``mcps`` logger.
- :func:`bind_run_id` — context manager that binds a ``run_id`` to the
  current execution context via a :class:`contextvars.ContextVar`. Inside
  the ``with`` block every emitted record automatically carries the bound
  ``run_id`` field; outside the block it falls back to ``"unset"``.

Validates: Requirements 14.3, 14.4, 14.5.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Iterator, Optional

from mcps.redaction import Redactor


# ---------------------------------------------------------------------------
# run_id context variable
# ---------------------------------------------------------------------------

#: Holds the active ``run_id`` for the current execution context. Defaulting
#: to ``"unset"`` means a record emitted before :func:`bind_run_id` is
#: entered (e.g. during early CLI startup) still carries the documented
#: ``run_id`` field with a sentinel value rather than being missing entirely.
_RUN_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "mcps_run_id", default="unset"
)


# ---------------------------------------------------------------------------
# Level mapping (stdlib level int -> design.md level enum string)
# ---------------------------------------------------------------------------

#: design.md req 14.3 enumerates DEBUG/INFO/WARN/ERROR. Python's stdlib
#: spells WARNING in full and adds a separate CRITICAL level; we collapse
#: those onto the documented enum so log consumers see exactly the four
#: values they expect.
_LEVEL_MAP: dict[int, str] = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "ERROR",
}


def _format_timestamp(record_created: float) -> str:
    """Return ``record.created`` (a Unix timestamp) as ISO-8601 UTC at
    millisecond precision with a trailing ``Z`` suffix.

    Example: ``"2024-04-01T12:34:56.789Z"``.
    """
    dt = datetime.fromtimestamp(record_created, tz=timezone.utc)
    # ``isoformat`` would emit ``+00:00`` and microsecond precision; we
    # build the string explicitly so the design.md format is exact.
    millis = int(dt.microsecond / 1000)
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{millis:03d}Z"


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """Format each ``LogRecord`` as one line of JSON.

    Output fields (per design.md req 14.3):

    - ``timestamp``: ISO-8601 UTC, millisecond precision, trailing ``Z``.
    - ``level``: one of ``DEBUG`` / ``INFO`` / ``WARN`` / ``ERROR``.
    - ``run_id``: the value bound by :func:`bind_run_id`, or the literal
      ``"unset"`` if no binding is active. A ``run_id`` carried on the
      ``LogRecord`` itself (e.g. via ``extra={"run_id": ...}``) takes
      precedence over the contextvar so callers can override on a
      per-record basis.
    - ``event``: ``record.event`` if the caller passed one via ``extra``;
      otherwise the logger name (``record.name``).
    - ``message``: the formatted log message via ``record.getMessage()``.

    Every record is passed through :class:`mcps.redaction.Redactor` before
    serialisation; any extra-field credential payloads or message-text
    secrets are replaced with the literal ``[REDACTED]`` (req 14.4).

    The constructor takes no positional arguments. A custom ``Redactor``
    instance can be injected in tests to verify the integration without
    re-instantiating one per record.
    """

    def __init__(self, redactor: Optional[Redactor] = None) -> None:
        super().__init__()
        self._redactor = redactor if redactor is not None else Redactor()

    # ------------------------------------------------------------------
    # Required override
    # ------------------------------------------------------------------

    def format(self, record: logging.LogRecord) -> str:
        # Build the documented field set first, then hand it to the
        # Redactor. ``getMessage()`` runs ``record.msg % record.args`` so
        # callers can use printf-style formatting; if args is empty
        # ``msg`` is returned unchanged.
        message = record.getMessage()

        # ``event`` falls back to the logger name when the caller did not
        # pass an explicit event marker. Logger names are conventionally
        # dotted module paths (e.g. ``mcps.replication``) which double as
        # a coarse event taxonomy.
        event = getattr(record, "event", None) or record.name

        # Per-record override wins over the contextvar binding.
        run_id = getattr(record, "run_id", None)
        if not run_id:
            run_id = _RUN_ID.get()

        payload: dict = {
            "timestamp": _format_timestamp(record.created),
            "level": _LEVEL_MAP.get(record.levelno, record.levelname),
            "run_id": run_id,
            "event": event,
            "message": message,
        }

        # Carry any caller-supplied ``extra=`` fields through to the log
        # output, but never let them shadow the documented top-level
        # fields. Stdlib stuffs ``extra`` keys directly onto the record
        # alongside its own attributes, so we inspect ``record.__dict__``
        # and skip the well-known stdlib names.
        for key, value in record.__dict__.items():
            if key in _STDLIB_RECORD_ATTRS:
                continue
            if key in payload:
                # Documented top-level fields are immutable.
                continue
            payload[key] = value

        # Redact AFTER the payload is fully assembled. The Redactor walks
        # the dict, applies the field-name allowlist (req 14.4), and runs
        # the regex chain over every string value.
        scrubbed = self._redactor.scrub_record(payload)

        # ``sort_keys=True`` and the compact separator pair make the
        # output deterministic for golden-file tests.
        return json.dumps(
            scrubbed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )


#: Attribute names on ``LogRecord`` that originate from the stdlib itself
#: rather than user-supplied ``extra=`` data. The set is exact; if a future
#: stdlib release adds a new attribute, callers who use that name in
#: ``extra=`` would otherwise see it silently dropped from the formatted
#: output. The list mirrors CPython's ``logging.LogRecord`` source.
_STDLIB_RECORD_ATTRS: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        # Documented payload keys — already covered above but listed
        # here defensively in case a caller passes them via ``extra=``.
        "message",
        "asctime",
        "taskName",
    }
)


# ---------------------------------------------------------------------------
# Logger configuration
# ---------------------------------------------------------------------------


_MCPS_LOGGER_NAME = "mcps"


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure the ``mcps`` logger and return it.

    - Removes any existing handlers from the ``mcps`` logger so repeated
      calls (e.g. across test cases) do not stack handlers and double-log.
    - Attaches a single ``StreamHandler`` writing to ``sys.stderr`` with
      the :class:`JsonFormatter`.
    - Sets the logger level to ``level``. ``"WARN"`` is accepted as an
      alias for ``logging.WARNING`` to match the design.md enum spelling.
    - Sets ``propagate=False`` so the structured JSON line is not also
      replayed through the root logger's default text handler if a host
      application has one configured.

    The returned logger is the package root; child loggers obtained via
    ``logging.getLogger("mcps.replication")``, etc., inherit the handler
    automatically.
    """
    logger = logging.getLogger(_MCPS_LOGGER_NAME)

    # Drop any previously installed handlers so repeated setup_logging
    # calls converge to the documented single-handler state.
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    logger.setLevel(_normalise_level(level))
    logger.propagate = False

    return logger


def _normalise_level(level: str) -> int:
    """Convert a design.md level-enum string to a stdlib level int.

    ``"WARN"`` -> ``logging.WARNING``; the other four (``DEBUG``,
    ``INFO``, ``ERROR``, plus the unmapped ``CRITICAL``) pass through to
    ``logging.getLevelName`` unchanged.
    """
    upper = level.upper()
    if upper == "WARN":
        return logging.WARNING
    # ``getLevelName`` is bidirectional in stdlib: given a string it
    # returns the corresponding int (or the string back if unknown).
    resolved = logging.getLevelName(upper)
    if isinstance(resolved, int):
        return resolved
    raise ValueError(f"Unknown log level: {level!r}")


# ---------------------------------------------------------------------------
# run_id binding
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def bind_run_id(run_id: str) -> Iterator[None]:
    """Bind ``run_id`` to the current execution context.

    Inside the ``with`` block, every record emitted through any logger in
    the ``mcps`` hierarchy carries ``"run_id": run_id`` in its JSON
    output. On exit the contextvar is restored to its prior value, so
    nested or sequential bindings behave intuitively.

    Implemented with :class:`contextvars.ContextVar` so the binding
    propagates correctly across ``await`` boundaries and explicit
    ``concurrent.futures`` ``Context.run`` calls. Plain
    ``ThreadPoolExecutor.submit`` does *not* propagate contextvars by
    default; callers wanting per-thread inheritance can copy the current
    context with :func:`contextvars.copy_context`.
    """
    token = _RUN_ID.set(run_id)
    try:
        yield
    finally:
        _RUN_ID.reset(token)


__all__ = [
    "JsonFormatter",
    "bind_run_id",
    "setup_logging",
]
