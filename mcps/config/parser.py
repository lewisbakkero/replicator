"""`Config_Parser` — TOML / YAML loader for the on-disk configuration file.

The parser is the inverse of `mcps.config.printer.print_config`. It loads a
TOML or YAML file (extensions ``.toml``, ``.yaml``, ``.yml``; max 1 MiB),
enforces the six top-level sections defined in design.md and req 17.4,
rejects unknown keys with a descriptive error and a best-effort line
number, validates source kinds (``s3`` / ``gcs`` / ``google_drive``), and
raises `ConfigError` with a dotted-path ``field`` on missing or
out-of-range values.

Three entry points are exposed:

* ``parse_config_file(path) -> (Config, format)`` — opens the file in
  read-only mode, validates extension and size, dispatches to the
  in-memory parser, and returns the parsed Config plus the detected
  format ("toml" or "yaml"). Used by the CLI on Sync_Run startup.
* ``parse_config(text, *, format) -> Config`` — in-memory variant used
  by the round-trip property test so we never have to materialise a
  temporary file just to test the parser/printer pair.
* ``default_config_path(cwd) -> str`` — returns ``<cwd>/mcps.config.yaml``
  per req 17.2.

Line-number tracking is best-effort. tomllib / PyYAML do not expose
per-key line numbers, so on errors we scan the original text for the
offending key's first occurrence at the start of a line. If the key
isn't found (e.g. the field is missing entirely, or the parse is
in-memory and no text is retained) ``ConfigError.line`` is left as
``None``.

Validates: Requirements 17.1, 17.2, 17.3, 17.4, 17.5, 17.7, 17.8, 17.9.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

try:  # pragma: no cover - Python 3.11+
    import tomllib  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

import yaml

from mcps.config.model import (
    SOURCE_KINDS,
    Config,
    DuplicatesConfig,
    PhotosConfig,
    ReplicationConfig,
    RetriesConfig,
    RuntimeConfig,
    SourceConfig,
)
from mcps.errors import ConfigError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Maximum on-disk size of a configuration file (req 17.1, 17.3).
MAX_CONFIG_SIZE_BYTES: int = 1024 * 1024  # 1 MiB

#: File extensions accepted by ``parse_config_file`` (req 17.1).
_TOML_EXTS: frozenset[str] = frozenset({".toml"})
_YAML_EXTS: frozenset[str] = frozenset({".yaml", ".yml"})

#: The six top-level sections required by req 17.4.
_TOP_LEVEL_SECTIONS: tuple[str, ...] = (
    "sources",
    "replication",
    "duplicates",
    "photos",
    "retries",
    "runtime",
)

#: Allowed keys per section. The parser rejects any key not in these sets
#: (req 17.7) so a typo never silently picks up a default value.
_SOURCE_KEYS: frozenset[str] = frozenset(
    {"name", "kind", "bucket", "prefix", "region", "drive_root_folder_id"}
)
_REPLICATION_KEYS: frozenset[str] = frozenset(
    {
        "pairs",
        "on_key_conflict",
        "fail_on_conflict",
        "delete_propagation",
        "tombstone_retention_days",
        "fail_on_inconsistency",
    }
)
_DUPLICATES_KEYS: frozenset[str] = frozenset(
    {"canonical_source_priority", "quarantine_retention_days"}
)
_PHOTOS_KEYS: frozenset[str] = frozenset({"drive_source", "drive_destination"})
_RETRIES_KEYS: frozenset[str] = frozenset(
    {"max_retries", "initial_backoff_ms", "max_backoff_ms", "request_timeout_ms"}
)
_RUNTIME_KEYS: frozenset[str] = frozenset(
    {"catalog_path", "manifest_dir", "max_concurrent_transfers", "lock_path"}
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def default_config_path(cwd: str) -> str:
    """Return the default configuration path ``<cwd>/mcps.config.yaml`` (req 17.2)."""
    return os.path.join(cwd, "mcps.config.yaml")


def parse_config_file(path: str) -> tuple[Config, str]:
    """Load a Config from the file at ``path``.

    Returns ``(config, format)`` where ``format`` is ``"toml"`` or ``"yaml"``.

    Raises:
        OSError: if the file is missing or unreadable. The CLI translates
            this into a descriptive error (req 17.3).
        ConfigError: if the extension is unsupported, the file exceeds 1
            MiB, or the contents fail validation (req 17.1, 17.3, 17.7,
            17.8, 17.9).
    """
    # Validate extension up front (req 17.1).
    ext = os.path.splitext(path)[1].lower()
    if ext in _TOML_EXTS:
        format_name = "toml"
    elif ext in _YAML_EXTS:
        format_name = "yaml"
    else:
        raise ConfigError(
            path=path,
            field=None,
            line=None,
        )

    # Validate size BEFORE reading so a multi-GiB file never reaches the
    # parser. ``os.path.getsize`` raises ``FileNotFoundError`` (an OSError
    # subclass) for missing files; we let that propagate so the CLI can
    # emit a "file not found" error rather than a generic ConfigError.
    size = os.path.getsize(path)
    if size > MAX_CONFIG_SIZE_BYTES:
        err = ConfigError(path=path, field=None, line=None)
        err.args = (
            f"ConfigError(path={path!r}) file too large: "
            f"{size} bytes > {MAX_CONFIG_SIZE_BYTES} bytes (1 MiB)",
        )
        raise err

    # Read the file as UTF-8 text. We retain the text so we can scan it
    # for line numbers when raising ConfigError.
    with open(path, "r", encoding="utf-8", newline="") as f:
        text = f.read()

    config = _parse_text(text, format_name=format_name, path=path)
    return config, format_name


def parse_config(text: str, *, format: str) -> Config:
    """Parse a Config from an in-memory string.

    ``format`` MUST be ``"toml"`` or ``"yaml"``. Mirrors
    ``parse_config_file`` but takes a string rather than a path. The
    ``path`` field of any raised `ConfigError` is set to the synthetic
    sentinel ``"<memory>"``.
    """
    if format not in ("toml", "yaml"):
        raise ConfigError(path="<memory>", field=None, line=None)
    return _parse_text(text, format_name=format, path="<memory>")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_text(text: str, *, format_name: str, path: str) -> Config:
    """Parse ``text`` as ``format_name`` and validate the resulting tree."""
    if format_name == "toml":
        try:
            data = tomllib.loads(text)
        except tomllib.TOMLDecodeError as e:
            err = ConfigError(path=path, field=None, line=None)
            err.args = (f"ConfigError(path={path!r}) invalid TOML: {e}",)
            raise err from None
    else:  # yaml
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            err = ConfigError(path=path, field=None, line=None)
            err.args = (f"ConfigError(path={path!r}) invalid YAML: {e}",)
            raise err from None
        # ``yaml.safe_load("")`` returns ``None``; treat as an empty dict so
        # the missing-section error fires below for every required section.
        if data is None:
            data = {}

    if not isinstance(data, dict):
        err = ConfigError(path=path, field=None, line=None)
        err.args = (
            f"ConfigError(path={path!r}) top-level structure must be a mapping",
        )
        raise err

    return _validate_top_level(data, text=text, path=path)


def _validate_top_level(data: dict[str, Any], *, text: str, path: str) -> Config:
    """Validate the top-level dict and build a `Config`.

    Rejects unknown top-level keys (req 17.7) and missing top-level
    sections (req 17.4 / 17.8). Each section is then validated by its
    own helper which builds the corresponding dataclass and rewraps any
    `ConfigError` raised by the model with the dotted-path field name.
    """
    # Reject unknown top-level keys.
    unknown = sorted(k for k in data.keys() if k not in _TOP_LEVEL_SECTIONS)
    if unknown:
        bad = unknown[0]
        raise _config_error(
            path=path,
            text=text,
            field=bad,
            key_for_line=bad,
            message=f"unknown top-level key: {bad!r}",
        )

    # Reject missing required top-level sections (req 17.4).
    missing = [s for s in _TOP_LEVEL_SECTIONS if s not in data]
    if missing:
        bad = missing[0]
        raise _config_error(
            path=path,
            text=text,
            field=bad,
            key_for_line=None,
            message=f"missing required top-level section: {bad!r}",
        )

    sources = _build_sources(data["sources"], text=text, path=path)
    replication = _build_replication(data["replication"], text=text, path=path)
    duplicates = _build_duplicates(data["duplicates"], text=text, path=path)
    photos = _build_photos(data["photos"], text=text, path=path)
    retries = _build_retries(data["retries"], text=text, path=path)
    runtime = _build_runtime(data["runtime"], text=text, path=path)

    return Config(
        sources=sources,
        replication=replication,
        duplicates=duplicates,
        photos=photos,
        retries=retries,
        runtime=runtime,
    )


# --- Section builders ------------------------------------------------------


def _build_sources(
    raw: Any, *, text: str, path: str
) -> tuple[SourceConfig, ...]:
    """Validate and construct the ``sources`` list."""
    if not isinstance(raw, list):
        raise _config_error(
            path=path,
            text=text,
            field="sources",
            key_for_line="sources",
            message=f"'sources' must be a list, got {type(raw).__name__}",
        )

    out: list[SourceConfig] = []
    for i, item in enumerate(raw):
        prefix = f"sources[{i}]"
        if not isinstance(item, dict):
            raise _config_error(
                path=path,
                text=text,
                field=prefix,
                key_for_line=None,
                message=f"{prefix} must be a mapping, got {type(item).__name__}",
            )

        # Reject unknown per-source keys (req 17.7).
        unknown = sorted(k for k in item.keys() if k not in _SOURCE_KEYS)
        if unknown:
            bad = unknown[0]
            raise _config_error(
                path=path,
                text=text,
                field=f"{prefix}.{bad}",
                key_for_line=bad,
                message=f"unknown key: {prefix}.{bad}",
            )

        # Both `name` and `kind` are required; SourceConfig has no defaults
        # for them. Surface a missing-required-field error (req 17.8) before
        # calling the constructor so the message is precise.
        if "name" not in item:
            raise _config_error(
                path=path,
                text=text,
                field=f"{prefix}.name",
                key_for_line=None,
                message=f"missing required field: {prefix}.name",
            )
        if "kind" not in item:
            raise _config_error(
                path=path,
                text=text,
                field=f"{prefix}.kind",
                key_for_line=None,
                message=f"missing required field: {prefix}.kind",
            )

        # Reject `kind` values that aren't one of the documented literals
        # (req 17.7) before SourceConfig.__post_init__ raises a generic
        # error, so the message names "kind" rather than the model's path="" form.
        kind = item["kind"]
        if kind not in SOURCE_KINDS:
            raise _config_error(
                path=path,
                text=text,
                field=f"{prefix}.kind",
                key_for_line="kind",
                message=(
                    f"{prefix}.kind={kind!r} is not one of "
                    f"{list(SOURCE_KINDS)!r}"
                ),
            )

        try:
            src = SourceConfig(**item)
        except ConfigError as e:
            # Rewrap the model-layer error with the dotted path.
            field = f"{prefix}.{e.field}" if e.field else prefix
            raise _config_error(
                path=path,
                text=text,
                field=field,
                key_for_line=e.field,
                message=f"invalid value for {field}",
            ) from None
        out.append(src)
    return tuple(out)


def _build_replication(
    raw: Any, *, text: str, path: str
) -> ReplicationConfig:
    if not isinstance(raw, dict):
        raise _config_error(
            path=path,
            text=text,
            field="replication",
            key_for_line="replication",
            message=f"'replication' must be a mapping, got {type(raw).__name__}",
        )

    _reject_unknown(
        raw, allowed=_REPLICATION_KEYS, parent="replication", text=text, path=path
    )

    # `pairs` arrives as a list of two-element lists from TOML/YAML; convert
    # each pair to a tuple of strings so the model's frozen dataclass holds
    # an immutable, hashable value.
    kwargs: dict[str, Any] = dict(raw)
    if "pairs" in kwargs:
        kwargs["pairs"] = _coerce_pairs(
            kwargs["pairs"], text=text, path=path, parent="replication.pairs"
        )

    try:
        return ReplicationConfig(**kwargs)
    except ConfigError as e:
        field = f"replication.{e.field}" if e.field else "replication"
        raise _config_error(
            path=path,
            text=text,
            field=field,
            key_for_line=e.field,
            message=f"invalid value for {field}",
        ) from None


def _build_duplicates(
    raw: Any, *, text: str, path: str
) -> DuplicatesConfig:
    if not isinstance(raw, dict):
        raise _config_error(
            path=path,
            text=text,
            field="duplicates",
            key_for_line="duplicates",
            message=f"'duplicates' must be a mapping, got {type(raw).__name__}",
        )
    _reject_unknown(
        raw, allowed=_DUPLICATES_KEYS, parent="duplicates", text=text, path=path
    )

    kwargs: dict[str, Any] = dict(raw)
    if "canonical_source_priority" in kwargs:
        prio = kwargs["canonical_source_priority"]
        if not isinstance(prio, list) or not all(isinstance(s, str) for s in prio):
            raise _config_error(
                path=path,
                text=text,
                field="duplicates.canonical_source_priority",
                key_for_line="canonical_source_priority",
                message=(
                    "duplicates.canonical_source_priority must be a list of strings"
                ),
            )
        kwargs["canonical_source_priority"] = tuple(prio)

    try:
        return DuplicatesConfig(**kwargs)
    except ConfigError as e:
        field = f"duplicates.{e.field}" if e.field else "duplicates"
        raise _config_error(
            path=path,
            text=text,
            field=field,
            key_for_line=e.field,
            message=f"invalid value for {field}",
        ) from None


def _build_photos(raw: Any, *, text: str, path: str) -> PhotosConfig:
    if not isinstance(raw, dict):
        raise _config_error(
            path=path,
            text=text,
            field="photos",
            key_for_line="photos",
            message=f"'photos' must be a mapping, got {type(raw).__name__}",
        )
    _reject_unknown(
        raw, allowed=_PHOTOS_KEYS, parent="photos", text=text, path=path
    )

    # PhotosConfig fields are Optional[str]; type-check explicitly.
    for name in ("drive_source", "drive_destination"):
        if name in raw and raw[name] is not None and not isinstance(raw[name], str):
            raise _config_error(
                path=path,
                text=text,
                field=f"photos.{name}",
                key_for_line=name,
                message=f"photos.{name} must be a string or null",
            )

    try:
        return PhotosConfig(**raw)
    except ConfigError as e:  # pragma: no cover - PhotosConfig has no validation
        field = f"photos.{e.field}" if e.field else "photos"
        raise _config_error(
            path=path,
            text=text,
            field=field,
            key_for_line=e.field,
            message=f"invalid value for {field}",
        ) from None


def _build_retries(raw: Any, *, text: str, path: str) -> RetriesConfig:
    if not isinstance(raw, dict):
        raise _config_error(
            path=path,
            text=text,
            field="retries",
            key_for_line="retries",
            message=f"'retries' must be a mapping, got {type(raw).__name__}",
        )
    _reject_unknown(
        raw, allowed=_RETRIES_KEYS, parent="retries", text=text, path=path
    )

    try:
        return RetriesConfig(**raw)
    except ConfigError as e:
        field = f"retries.{e.field}" if e.field else "retries"
        raise _config_error(
            path=path,
            text=text,
            field=field,
            key_for_line=e.field,
            message=f"invalid value for {field}",
        ) from None


def _build_runtime(raw: Any, *, text: str, path: str) -> RuntimeConfig:
    if not isinstance(raw, dict):
        raise _config_error(
            path=path,
            text=text,
            field="runtime",
            key_for_line="runtime",
            message=f"'runtime' must be a mapping, got {type(raw).__name__}",
        )
    _reject_unknown(
        raw, allowed=_RUNTIME_KEYS, parent="runtime", text=text, path=path
    )

    # Type-check the string-valued fields up front so the model-layer error
    # message stays precise when only the integer field is wrong.
    for name in ("catalog_path", "manifest_dir"):
        if name in raw and not isinstance(raw[name], str):
            raise _config_error(
                path=path,
                text=text,
                field=f"runtime.{name}",
                key_for_line=name,
                message=f"runtime.{name} must be a string",
            )
    if (
        "lock_path" in raw
        and raw["lock_path"] is not None
        and not isinstance(raw["lock_path"], str)
    ):
        raise _config_error(
            path=path,
            text=text,
            field="runtime.lock_path",
            key_for_line="lock_path",
            message="runtime.lock_path must be a string or null",
        )

    try:
        return RuntimeConfig(**raw)
    except ConfigError as e:
        field = f"runtime.{e.field}" if e.field else "runtime"
        raise _config_error(
            path=path,
            text=text,
            field=field,
            key_for_line=e.field,
            message=f"invalid value for {field}",
        ) from None


# --- Shared utilities ------------------------------------------------------


def _reject_unknown(
    raw: dict[str, Any],
    *,
    allowed: frozenset[str],
    parent: str,
    text: str,
    path: str,
) -> None:
    """Raise `ConfigError` for the first unknown key in ``raw``."""
    unknown = sorted(k for k in raw.keys() if k not in allowed)
    if unknown:
        bad = unknown[0]
        raise _config_error(
            path=path,
            text=text,
            field=f"{parent}.{bad}",
            key_for_line=bad,
            message=f"unknown key: {parent}.{bad}",
        )


def _coerce_pairs(
    raw: Any, *, text: str, path: str, parent: str
) -> tuple[tuple[str, str], ...]:
    """Coerce the YAML/TOML representation of ``replication.pairs`` into tuples.

    Both YAML and TOML serialise tuple-of-tuples as list-of-lists. The model
    expects ``tuple[tuple[str, str], ...]`` so we coerce the outer and inner
    sequences here. Any structural mismatch is surfaced as a `ConfigError`
    rather than letting an opaque ``TypeError`` escape from the dataclass.
    """
    if not isinstance(raw, list):
        raise _config_error(
            path=path,
            text=text,
            field=parent,
            key_for_line="pairs",
            message=f"{parent} must be a list of two-element lists",
        )
    out: list[tuple[str, str]] = []
    for i, pair in enumerate(raw):
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) != 2
            or not all(isinstance(s, str) for s in pair)
        ):
            raise _config_error(
                path=path,
                text=text,
                field=f"{parent}[{i}]",
                key_for_line="pairs",
                message=f"{parent}[{i}] must be a two-element list of strings",
            )
        out.append((pair[0], pair[1]))
    return tuple(out)


def _config_error(
    *,
    path: str,
    text: Optional[str],
    field: Optional[str],
    key_for_line: Optional[str],
    message: str,
) -> ConfigError:
    """Build a `ConfigError` with a best-effort line number.

    ``key_for_line`` is the local key name (without dotted prefix) to scan
    for in ``text``; if it can't be located the line stays ``None``. The
    message overrides the default ``ConfigError`` repr so operators see a
    descriptive diagnostic.
    """
    line = _find_line_for_key(text, key_for_line)
    err = ConfigError(path=path, line=line, field=field)
    err.args = (
        f"ConfigError(path={path!r}, line={line!r}, field={field!r}) {message}",
    )
    return err


_KEY_LINE_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _find_line_for_key(text: Optional[str], key: Optional[str]) -> Optional[int]:
    """Best-effort: return the 1-based line number where ``key`` first appears.

    Matches lines whose first non-whitespace token is ``key:`` (YAML),
    ``key =`` (TOML), or ``[key]`` (TOML section header). Returns ``None``
    if ``text`` is empty, ``key`` is empty, or no such line is found.
    """
    if not text or not key:
        return None
    pattern = _KEY_LINE_RE_CACHE.get(key)
    if pattern is None:
        # The key may contain regex metacharacters in pathological cases;
        # escape it before splicing into the alternation.
        escaped = re.escape(key)
        pattern = re.compile(
            rf"^\s*(?:{escaped}\s*[:=]|\[{escaped}\])"
        )
        _KEY_LINE_RE_CACHE[key] = pattern
    for i, line in enumerate(text.split("\n"), start=1):
        if pattern.match(line):
            return i
    return None


__all__ = [
    "MAX_CONFIG_SIZE_BYTES",
    "default_config_path",
    "parse_config_file",
    "parse_config",
]
