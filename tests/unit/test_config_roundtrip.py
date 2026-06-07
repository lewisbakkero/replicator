# Feature: multicloud-photo-sync, Property 3: Config round-trip
"""Round-trip tests for `Config_Parser` and `Config_Printer`.

Hypothesis property + example-based tests for the on-disk configuration
file format (TOML and YAML).

The property under test (design.md, "Correctness Properties — Property 3:
Config round-trip") is:

    For every valid in-memory Config ``c`` and every supported format ``f``,
        parse_config(print_config(c, format=f), format=f) == c
    field-by-field across all six top-level sections including
    ``replication.fail_on_inconsistency``.

Validates: Requirements 17.1, 17.2, 17.3, 17.4, 17.5, 17.6, 17.7, 17.8, 17.9.
"""

from __future__ import annotations

import os
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.config.model import (
    Config,
    DuplicatesConfig,
    PhotosConfig,
    ReplicationConfig,
    RetriesConfig,
    RuntimeConfig,
    SourceConfig,
)
from mcps.config.parser import (
    MAX_CONFIG_SIZE_BYTES,
    default_config_path,
    parse_config,
    parse_config_file,
)
from mcps.config.printer import print_config, write_config_file
from mcps.errors import ConfigError, ExitCode


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# A small pool of source names so the cross-section references (replication
# pairs, canonical priority, photos drive_source / drive_destination) are
# realistic — each refers to a name that actually exists in the generated
# Config.
_SOURCE_NAMES: tuple[str, ...] = (
    "s3-prod",
    "s3-archive",
    "gcs-primary",
    "gcs-cold",
    "drive-camera",
    "drive-takeout",
)

_S3_BUCKETS: tuple[str, ...] = ("prod-bucket", "archive-bucket", "cold-bucket")
_GCS_BUCKETS: tuple[str, ...] = ("primary-bucket", "cold-store")
_DRIVE_FOLDERS: tuple[str, ...] = ("0BABCDEF", "0BXYZ123", "1A2B3C4D")
_S3_REGIONS: tuple[Optional[str], ...] = (None, "us-east-1", "eu-west-1")
_PREFIXES: tuple[Optional[str], ...] = (None, "photos/", "media/2024/")


@st.composite
def _source_kind(draw) -> str:
    return draw(st.sampled_from(("s3", "gcs", "google_drive")))


@st.composite
def _source_configs(draw, name: str) -> SourceConfig:
    kind = draw(_source_kind())
    if kind == "s3":
        return SourceConfig(
            name=name,
            kind="s3",
            bucket=draw(st.sampled_from(_S3_BUCKETS)),
            prefix=draw(st.sampled_from(_PREFIXES)),
            region=draw(st.sampled_from(_S3_REGIONS)),
        )
    if kind == "gcs":
        return SourceConfig(
            name=name,
            kind="gcs",
            bucket=draw(st.sampled_from(_GCS_BUCKETS)),
            prefix=draw(st.sampled_from(_PREFIXES)),
        )
    return SourceConfig(
        name=name,
        kind="google_drive",
        drive_root_folder_id=draw(st.sampled_from(_DRIVE_FOLDERS)),
    )


@st.composite
def configs(draw) -> Config:
    """Generate a structurally valid `Config` with internally-consistent references.

    * 1..4 SourceConfigs of mixed kinds, drawn from a pool of names so each
      generated Config has unique source names.
    * Replication pairs only reference names of replicated (s3 / gcs)
      sources present in the Config.
    * canonical_source_priority only includes names that are present.
    * photos.drive_source and drive_destination only refer to names that
      exist (or None).
    """
    n = draw(st.integers(min_value=1, max_value=4))
    chosen_names = draw(
        st.lists(
            st.sampled_from(_SOURCE_NAMES),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    sources = tuple(draw(_source_configs(name=n_)) for n_ in chosen_names)

    replicated_names = [s.name for s in sources if s.kind in ("s3", "gcs")]
    drive_names = [s.name for s in sources if s.kind == "google_drive"]

    # Replication pairs: optional, only between distinct replicated sources.
    if len(replicated_names) >= 2:
        pairs_count = draw(st.integers(min_value=0, max_value=2))
        pairs_list: list[tuple[str, str]] = []
        for _ in range(pairs_count):
            a = draw(st.sampled_from(replicated_names))
            b_choices = [n for n in replicated_names if n != a]
            b = draw(st.sampled_from(b_choices))
            pairs_list.append((a, b))
        pairs = tuple(pairs_list)
    else:
        pairs = ()

    replication = ReplicationConfig(
        pairs=pairs,
        on_key_conflict=draw(st.sampled_from(("skip", "rename", "overwrite"))),
        fail_on_conflict=draw(st.booleans()),
        delete_propagation=draw(st.sampled_from(("none", "soft", "hard"))),
        tombstone_retention_days=draw(st.integers(min_value=1, max_value=3650)),
        fail_on_inconsistency=draw(st.booleans()),
    )

    if replicated_names:
        prio_size = draw(st.integers(min_value=0, max_value=len(replicated_names)))
        prio = tuple(
            draw(
                st.lists(
                    st.sampled_from(replicated_names),
                    min_size=prio_size,
                    max_size=prio_size,
                    unique=True,
                )
            )
        )
    else:
        prio = ()
    duplicates = DuplicatesConfig(
        canonical_source_priority=prio,
        quarantine_retention_days=draw(st.integers(min_value=1, max_value=3650)),
    )

    drive_source = draw(
        st.one_of(st.none(), st.sampled_from(drive_names) if drive_names else st.none())
    )
    drive_destination = draw(
        st.one_of(
            st.none(),
            st.sampled_from(replicated_names) if replicated_names else st.none(),
        )
    )
    photos = PhotosConfig(
        drive_source=drive_source,
        drive_destination=drive_destination,
    )

    retries = RetriesConfig(
        max_retries=draw(st.integers(min_value=1, max_value=10)),
        initial_backoff_ms=draw(st.integers(min_value=100, max_value=10_000)),
        max_backoff_ms=draw(st.integers(min_value=1_000, max_value=300_000)),
        request_timeout_ms=draw(st.integers(min_value=1_000, max_value=120_000)),
    )

    runtime = RuntimeConfig(
        catalog_path=draw(
            st.sampled_from(
                (
                    "./mcps.catalog.jsonl",
                    "/var/lib/mcps/catalog.jsonl",
                    "./alt.catalog.jsonl",
                )
            )
        ),
        manifest_dir=draw(
            st.sampled_from(("./manifests", "/var/log/mcps", "./out"))
        ),
        max_concurrent_transfers=draw(st.integers(min_value=1, max_value=64)),
        lock_path=draw(
            st.one_of(
                st.none(),
                st.sampled_from(("/tmp/mcps.lock", "/var/run/mcps.lock")),
            )
        ),
    )

    return Config(
        sources=sources,
        replication=replication,
        duplicates=duplicates,
        photos=photos,
        retries=retries,
        runtime=runtime,
    )


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(c=configs(), format=st.sampled_from(("toml", "yaml")))
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_config_roundtrip(c: Config, format: str) -> None:
    """``parse_config(print_config(c, format), format=format) == c``.

    Validates: Requirement 17.6.
    """
    rendered = print_config(c, format=format)
    parsed = parse_config(rendered, format=format)
    assert parsed == c


@pytest.mark.property
@given(c=configs(), format=st.sampled_from(("toml", "yaml")))
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_config_printer_is_byte_deterministic(c: Config, format: str) -> None:
    """Two invocations of ``print_config`` on equal inputs are byte-identical."""
    out1 = print_config(c, format=format)
    out2 = print_config(c, format=format)
    assert out1 == out2


# ---------------------------------------------------------------------------
# Example-based tests — defaults, file I/O, validation
# ---------------------------------------------------------------------------


def _make_minimal_config() -> Config:
    """Smallest valid Config: one S3 source, all section defaults elsewhere."""
    return Config(
        sources=(SourceConfig(name="s3-prod", kind="s3", bucket="prod-bucket"),),
        replication=ReplicationConfig(),
        duplicates=DuplicatesConfig(),
        photos=PhotosConfig(),
        retries=RetriesConfig(),
        runtime=RuntimeConfig(),
    )


# --- default_config_path ----------------------------------------------------


def test_default_config_path_returns_yaml_under_cwd():
    assert default_config_path("/home/user") == "/home/user/mcps.config.yaml"
    assert default_config_path(".") == "./mcps.config.yaml"


# --- parse_config_file: extension and size validation ---------------------


def test_parse_config_file_missing_path_raises_oserror(tmp_path):
    target = tmp_path / "nope.yaml"
    with pytest.raises(OSError):
        parse_config_file(str(target))


def test_parse_config_file_unsupported_extension_raises_config_error(tmp_path):
    target = tmp_path / "config.txt"
    target.write_text("sources = []\n", encoding="utf-8")
    with pytest.raises(ConfigError) as info:
        parse_config_file(str(target))
    assert info.value.path == str(target)
    assert info.value.to_exit_code() == ExitCode.CONFIG_INVALID


def test_parse_config_file_too_large_raises_config_error(tmp_path):
    target = tmp_path / "huge.yaml"
    # Write a YAML file that is syntactically valid but exceeds 1 MiB. The
    # bulk is a single comment line padded with spaces so we don't have to
    # generate a structurally enormous payload.
    padding = " " * (MAX_CONFIG_SIZE_BYTES + 1024)
    target.write_text(f"# {padding}\nsources: []\n", encoding="utf-8")
    assert os.path.getsize(target) > MAX_CONFIG_SIZE_BYTES
    with pytest.raises(ConfigError) as info:
        parse_config_file(str(target))
    assert "too large" in str(info.value).lower() or "1 mib" in str(info.value).lower()


def test_parse_config_file_at_size_boundary_succeeds(tmp_path):
    """A file at exactly 1 MiB is accepted (max is inclusive)."""
    target = tmp_path / "ok.yaml"
    config = _make_minimal_config()
    text = print_config(config, format="yaml")
    # Pad with whitespace so the file size is exactly 1 MiB.
    pad_count = MAX_CONFIG_SIZE_BYTES - len(text.encode("utf-8"))
    if pad_count > 0:
        text = text + ("\n# " + "x" * (pad_count - 4)) + "\n"
    target.write_text(text, encoding="utf-8")
    # Truncate to exact size if rounding pushed us over.
    if os.path.getsize(target) > MAX_CONFIG_SIZE_BYTES:
        target.write_bytes(target.read_bytes()[:MAX_CONFIG_SIZE_BYTES])
    parsed, fmt = parse_config_file(str(target))
    assert fmt == "yaml"
    assert parsed == config


# --- parse_config_file: write/read round-trip on disk ---------------------


@pytest.mark.parametrize("format,suffix", [("yaml", ".yaml"), ("toml", ".toml")])
def test_write_then_parse_config_file_roundtrip(tmp_path, format, suffix):
    config = _make_minimal_config()
    target = tmp_path / f"mcps.config{suffix}"
    write_config_file(config, str(target), format=format)
    parsed, parsed_format = parse_config_file(str(target))
    assert parsed_format == format
    assert parsed == config


def test_write_config_file_infers_format_from_extension(tmp_path):
    config = _make_minimal_config()
    yaml_target = tmp_path / "mcps.config.yaml"
    toml_target = tmp_path / "mcps.config.toml"
    write_config_file(config, str(yaml_target))
    write_config_file(config, str(toml_target))
    yaml_text = yaml_target.read_text(encoding="utf-8")
    toml_text = toml_target.read_text(encoding="utf-8")
    # YAML uses block-style sections.
    assert "sources:" in yaml_text
    # TOML emits ``sources`` as either ``[[sources]]`` array-of-tables or
    # the inline ``sources = [...]`` form depending on tomli_w's heuristic;
    # what matters is that one of the two appears AND the file parses back.
    assert "[replication]" in toml_text
    assert ("[[sources]]" in toml_text) or ("sources = [" in toml_text)


def test_write_config_file_unsupported_extension_raises(tmp_path):
    config = _make_minimal_config()
    target = tmp_path / "mcps.config.json"
    with pytest.raises(ConfigError):
        write_config_file(config, str(target))


# --- parse_config: unknown keys and missing fields ------------------------


def test_unknown_top_level_key_raises_with_field_name():
    text = """
sources: []
replication: {}
duplicates: {}
photos: {}
retries: {}
runtime: {}
unknown_field: 42
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "unknown_field"
    # Best-effort line number: the key appears on a real line in the source.
    assert info.value.line is not None
    assert info.value.line >= 1


def test_unknown_section_key_includes_dotted_path():
    text = """
sources: []
replication:
  unknown_field: 1
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "replication.unknown_field"
    assert info.value.line is not None


def test_unknown_source_key_includes_indexed_dotted_path():
    text = """
sources:
  - name: s3-prod
    kind: s3
    bucket: prod-bucket
    unknown_field: oops
replication: {}
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "sources[0].unknown_field"


def test_missing_required_top_level_section():
    text = """
sources: []
replication: {}
duplicates: {}
photos: {}
retries: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "runtime"


def test_missing_required_source_kind_uses_dotted_path():
    text = """
sources:
  - name: s3-prod
    bucket: prod-bucket
replication: {}
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "sources[0].kind"


def test_invalid_source_kind_uses_dotted_path():
    text = """
sources:
  - name: weird
    kind: azure
replication: {}
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "sources[0].kind"


def test_out_of_range_max_retries_uses_dotted_path():
    text = """
sources:
  - name: s3-prod
    kind: s3
    bucket: prod-bucket
replication: {}
duplicates: {}
photos: {}
retries:
  max_retries: 11
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "retries.max_retries"
    assert info.value.line is not None


def test_out_of_range_tombstone_retention_days():
    text = """
sources:
  - name: s3-prod
    kind: s3
    bucket: prod-bucket
replication:
  tombstone_retention_days: 99999
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "replication.tombstone_retention_days"


def test_invalid_on_key_conflict_value():
    text = """
sources:
  - name: s3-prod
    kind: s3
    bucket: prod-bucket
replication:
  on_key_conflict: merge
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "replication.on_key_conflict"


def test_fail_on_inconsistency_must_be_bool():
    text = """
sources:
  - name: s3-prod
    kind: s3
    bucket: prod-bucket
replication:
  fail_on_inconsistency: yes-please
duplicates: {}
photos: {}
retries: {}
runtime: {}
"""
    with pytest.raises(ConfigError) as info:
        parse_config(text, format="yaml")
    assert info.value.field == "replication.fail_on_inconsistency"


# --- TOML inputs -----------------------------------------------------------


def test_parse_minimal_toml_config():
    text = """
[[sources]]
name = "s3-prod"
kind = "s3"
bucket = "prod-bucket"

[replication]

[duplicates]

[photos]

[retries]

[runtime]
"""
    parsed = parse_config(text, format="toml")
    assert len(parsed.sources) == 1
    assert parsed.sources[0].name == "s3-prod"
    assert parsed.replication == ReplicationConfig()
    assert parsed.duplicates == DuplicatesConfig()
    assert parsed.photos == PhotosConfig()
    assert parsed.retries == RetriesConfig()
    assert parsed.runtime == RuntimeConfig()


def test_invalid_format_argument_rejected():
    with pytest.raises(ConfigError):
        parse_config("anything", format="json")  # type: ignore[arg-type]


# --- TOML cannot represent None — round-trip via field omission -----------


def test_toml_roundtrip_with_none_optional_fields_via_omission():
    """An S3 source with prefix=None / region=None round-trips through TOML.

    TOML has no ``null`` representation; the printer omits ``None`` fields
    and the parser treats missing keys as the dataclass default (None). The
    round-trip therefore preserves the original Config exactly.
    """
    config = Config(
        sources=(SourceConfig(name="s3-prod", kind="s3", bucket="prod-bucket"),),
        replication=ReplicationConfig(),
        duplicates=DuplicatesConfig(),
        photos=PhotosConfig(drive_source=None, drive_destination=None),
        retries=RetriesConfig(),
        runtime=RuntimeConfig(lock_path=None),
    )
    text = print_config(config, format="toml")
    parsed = parse_config(text, format="toml")
    assert parsed == config


# --- Printer determinism ---------------------------------------------------


def test_yaml_output_keeps_section_ordering():
    """``sort_keys=False`` so sections appear in printer-defined order."""
    config = _make_minimal_config()
    text = print_config(config, format="yaml")
    # Each section header appears exactly in this order.
    sections = ["sources:", "replication:", "duplicates:", "photos:", "retries:", "runtime:"]
    positions = [text.index(s) for s in sections]
    assert positions == sorted(positions)


def test_toml_output_uses_table_layout_for_sections():
    """TOML output uses ``[section]`` headers for the six fixed sections."""
    config = _make_minimal_config()
    text = print_config(config, format="toml")
    # ``sources`` may render as either ``[[sources]]`` (array of tables) or
    # the inline ``sources = [...]`` form; both are valid TOML and round-trip
    # identically, so accept either.
    assert ("[[sources]]" in text) or ("sources = [" in text)
    assert "[replication]" in text
    assert "[runtime]" in text
