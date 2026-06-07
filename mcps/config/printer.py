"""`Config_Printer` — TOML / YAML serialisation for the in-memory Config.

The printer is the inverse of `mcps.config.parser.parse_config`. It walks
every section of the in-memory `Config`, converts the nested dataclass tree
into a plain-Python ``dict`` whose values are TOML/YAML primitives, and emits
either TOML (via ``tomli_w.dumps``) or YAML (via
``yaml.safe_dump(sort_keys=False, default_flow_style=False)``).

Two entry points are exposed:

* ``print_config(config, format="yaml") -> str`` — return the deterministic
  serialisation. Used by the round-trip property test.
* ``write_config_file(config, path, format=None) -> None`` — atomic write
  via ``tempfile.NamedTemporaryFile`` plus ``os.replace``. ``format`` is
  inferred from the extension when omitted.

Format-specific details:

* **YAML.** ``sort_keys=False`` so section ordering follows the printer
  (sources first, runtime last). ``default_flow_style=False`` forces block
  layout for readability. ``allow_unicode=True`` preserves non-ASCII
  characters in source names and Drive folder ids.
* **TOML.** ``tomli_w.dumps`` does not accept ``None`` values (TOML has no
  null), so ``Optional[...]`` fields whose value is ``None`` are omitted
  from the dict before encoding. The parser treats those keys as missing
  and falls back to the dataclass default, which round-trips to the same
  ``None``.

Tuple-of-tuples for ``replication.pairs`` and tuple-of-strings for
``duplicates.canonical_source_priority`` serialise as lists; the parser
coerces them back to tuples on the way in.

Validates: Requirements 17.5, 17.6.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import tomli_w
import yaml

from mcps.config.model import Config, SourceConfig
from mcps.errors import ConfigError


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def print_config(config: Config, format: str = "yaml") -> str:
    """Return the serialised string for ``config`` in the requested format.

    ``format`` MUST be ``"toml"`` or ``"yaml"``. Two invocations on equal
    Configs are byte-identical for both formats.
    """
    if format not in ("toml", "yaml"):
        raise ConfigError(path="<memory>", field=None, line=None)

    payload = _config_to_dict(config, drop_none=(format == "toml"))

    if format == "toml":
        return tomli_w.dumps(payload)

    # YAML: explicit `default_flow_style=False` forces block style; keep
    # section order by disabling key sorting.
    return yaml.safe_dump(
        payload,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )


def write_config_file(
    config: Config, path: str, format: str | None = None
) -> None:
    """Atomically write ``config`` to ``path``.

    ``format`` may be ``"toml"`` or ``"yaml"``; if ``None`` the format is
    inferred from the file extension (``.toml`` → toml, ``.yaml`` / ``.yml``
    → yaml). The write is performed by creating a `tempfile.NamedTemporaryFile`
    in the same parent directory as ``path`` (so ``os.replace`` is atomic on
    the same filesystem), writing the serialised text, ``fsync``'ing, and
    swapping it into place; on any error the temp file is removed and the
    prior on-disk file is left untouched.
    """
    if format is None:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".toml":
            format = "toml"
        elif ext in (".yaml", ".yml"):
            format = "yaml"
        else:
            raise ConfigError(path=path, field=None, line=None)

    text = print_config(config, format=format)

    parent = os.path.dirname(os.path.abspath(path)) or "."
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=parent,
        prefix=".mcps-config-",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = tmp.name
    try:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp.close()
        except Exception:
            pass
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _config_to_dict(config: Config, *, drop_none: bool) -> dict[str, Any]:
    """Convert a `Config` into a plain-Python tree suitable for TOML/YAML.

    ``drop_none`` is True for TOML serialisation (TOML has no null) and
    False for YAML. Either way, ``tuple`` instances are converted to
    ``list``; dataclasses are walked field-by-field rather than via
    ``asdict`` so we have control over ordering and None handling.
    """
    return {
        "sources": [_source_to_dict(s, drop_none=drop_none) for s in config.sources],
        "replication": _drop_none_if(
            {
                "pairs": [list(p) for p in config.replication.pairs],
                "on_key_conflict": config.replication.on_key_conflict,
                "fail_on_conflict": config.replication.fail_on_conflict,
                "delete_propagation": config.replication.delete_propagation,
                "tombstone_retention_days": config.replication.tombstone_retention_days,
                "fail_on_inconsistency": config.replication.fail_on_inconsistency,
            },
            drop_none=drop_none,
        ),
        "duplicates": _drop_none_if(
            {
                "canonical_source_priority": list(
                    config.duplicates.canonical_source_priority
                ),
                "quarantine_retention_days": config.duplicates.quarantine_retention_days,
            },
            drop_none=drop_none,
        ),
        "photos": _drop_none_if(
            {
                "drive_source": config.photos.drive_source,
                "drive_destination": config.photos.drive_destination,
            },
            drop_none=drop_none,
        ),
        "retries": _drop_none_if(
            {
                "max_retries": config.retries.max_retries,
                "initial_backoff_ms": config.retries.initial_backoff_ms,
                "max_backoff_ms": config.retries.max_backoff_ms,
                "request_timeout_ms": config.retries.request_timeout_ms,
            },
            drop_none=drop_none,
        ),
        "runtime": _drop_none_if(
            {
                "catalog_path": config.runtime.catalog_path,
                "manifest_dir": config.runtime.manifest_dir,
                "max_concurrent_transfers": config.runtime.max_concurrent_transfers,
                "lock_path": config.runtime.lock_path,
            },
            drop_none=drop_none,
        ),
    }


def _source_to_dict(src: SourceConfig, *, drop_none: bool) -> dict[str, Any]:
    """Render a SourceConfig as an ordered dict.

    Only the fields relevant to the source's ``kind`` are emitted in YAML
    (the irrelevant Optional fields stay as ``None`` in YAML, which the
    parser accepts). For TOML the ``drop_none`` path strips them out
    entirely since TOML cannot represent ``None``.
    """
    return _drop_none_if(
        {
            "name": src.name,
            "kind": src.kind,
            "bucket": src.bucket,
            "prefix": src.prefix,
            "region": src.region,
            "drive_root_folder_id": src.drive_root_folder_id,
        },
        drop_none=drop_none,
    )


def _drop_none_if(d: dict[str, Any], *, drop_none: bool) -> dict[str, Any]:
    """Return ``d`` unchanged when ``drop_none`` is False; else strip None values."""
    if not drop_none:
        return d
    return {k: v for k, v in d.items() if v is not None}


__all__ = ["print_config", "write_config_file"]
