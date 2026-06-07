# Feature: multicloud-photo-sync, Property 1: Catalog round-trip
"""Round-trip tests for `Catalog_Parser` and `Catalog_Printer`.

Hypothesis property + example-based tests for the on-disk Catalog format.

The property under test (design.md, "Correctness Properties — Property 1:
Catalog round-trip") is:

  For every valid in-memory Catalog ``c``,
      parse_catalog(print_catalog(c)) == c
  and ``print_catalog`` is byte-deterministic on equal inputs.

Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog, ObjectRecord
from mcps.catalog.parser import parse_catalog, parse_catalog_file
from mcps.catalog.printer import print_catalog, write_catalog
from mcps.errors import CatalogParseError, ExitCode


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# A small pool of source names so collisions on (source, content_hash) and
# (source, key) are realistic in generated catalogs.
_SOURCE_POOL: tuple[str, ...] = (
    "s3-prod",
    "s3-archive",
    "gcs-primary",
    "gcs-cold",
    "drive-camera",
)

# A small pool of 64-char lowercase hex hashes. Collisions across different
# (source, key) pairs are intentional so generated catalogs exercise
# multi-member buckets in `Catalog.by_hash`.
_HASH_POOL: tuple[str, ...] = tuple(
    (chr(ord("a") + i) * 64) for i in range(10)
) + (
    "0" * 64,
    "f" * 64,
)

# A small pool of MIME-like content types (plus None).
_CONTENT_TYPE_POOL: tuple[Optional[str], ...] = (
    None,
    "image/jpeg",
    "image/png",
    "image/heic",
    "video/mp4",
    "video/quicktime",
    "application/octet-stream",
)

# Bounded epoch range covering 2000-01-01 .. 2099-12-31 so the resulting
# ISO-8601 strings always have a fixed-width 20-char layout.
_EPOCH_MIN = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
_EPOCH_MAX = int(datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())


@st.composite
def iso_timestamps(draw) -> str:
    """ISO-8601 UTC timestamp string with second precision, ``YYYY-MM-DDTHH:MM:SSZ``."""
    epoch = draw(st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX))
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@st.composite
def maybe_iso_timestamps(draw) -> Optional[str]:
    """Either ``None`` or a generated ISO-8601 timestamp."""
    if draw(st.booleans()):
        return None
    return draw(iso_timestamps())


# Keys may contain any printable Unicode except NUL. Use a printable filter so
# generated keys survive round-trips through ``open(..., encoding="utf-8")``
# and are easy to read in failing examples.
_KEY_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),  # surrogates
        blacklist_characters=("\x00", "\n", "\r"),
    ),
    min_size=1,
    max_size=80,
)


@st.composite
def object_records(draw) -> ObjectRecord:
    return ObjectRecord(
        source=draw(st.sampled_from(_SOURCE_POOL)),
        key=draw(_KEY_TEXT),
        content_hash=draw(st.sampled_from(_HASH_POOL)),
        size_bytes=draw(st.integers(min_value=0, max_value=10**9)),
        last_seen_at=draw(iso_timestamps()),
        last_modified=draw(iso_timestamps()),
        content_type=draw(st.sampled_from(_CONTENT_TYPE_POOL)),
        quarantined_at=draw(maybe_iso_timestamps()),
        tombstoned_at=draw(maybe_iso_timestamps()),
        mcps_source_meta=draw(
            st.one_of(st.none(), st.sampled_from(_SOURCE_POOL)),
        ),
    )


@st.composite
def catalogs(draw) -> Catalog:
    """Strategy producing valid Catalogs of 0..200 records.

    The Catalog invariant (req 11.5: at most one record per (source, key)) is
    preserved by feeding records through `Catalog.upsert`, which de-duplicates
    by `(source, key)` automatically.
    """
    n = draw(st.integers(min_value=0, max_value=200))
    records = draw(st.lists(object_records(), min_size=n, max_size=n))
    cat = Catalog()
    for rec in records:
        cat = cat.upsert(rec)
    return cat


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(c=catalogs())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_catalog_roundtrip_in_memory(c: Catalog) -> None:
    """``parse_catalog(print_catalog(c)) == c`` for every valid Catalog ``c``.

    Validates: Requirement 3.4.
    """
    rendered = print_catalog(c)
    parsed = parse_catalog(rendered)
    assert parsed == c


@pytest.mark.property
@given(c=catalogs())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_catalog_printer_is_byte_deterministic(c: Catalog) -> None:
    """Two invocations of ``print_catalog`` on equal inputs are byte-identical.

    Validates: Requirement 3.3.
    """
    out1 = print_catalog(c)
    out2 = print_catalog(c)
    assert out1 == out2
    # And after a parse-then-print cycle, the bytes are still identical.
    out3 = print_catalog(parse_catalog(out1))
    assert out1 == out3


# ---------------------------------------------------------------------------
# Example-based tests
# ---------------------------------------------------------------------------


def _make_record(
    *,
    source: str = "s3-prod",
    key: str = "photos/IMG_0001.jpg",
    content_hash: str = "a" * 64,
    size_bytes: int = 1024,
    last_seen_at: str = "2024-01-01T00:00:00Z",
    last_modified: str = "2023-12-31T23:59:59Z",
    content_type: Optional[str] = "image/jpeg",
    quarantined_at: Optional[str] = None,
    tombstoned_at: Optional[str] = None,
    mcps_source_meta: Optional[str] = None,
) -> ObjectRecord:
    return ObjectRecord(
        source=source,
        key=key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        last_seen_at=last_seen_at,
        last_modified=last_modified,
        content_type=content_type,
        quarantined_at=quarantined_at,
        tombstoned_at=tombstoned_at,
        mcps_source_meta=mcps_source_meta,
    )


def test_print_catalog_empty_catalog_returns_empty_string() -> None:
    assert print_catalog(Catalog()) == ""


def test_parse_catalog_empty_string_returns_empty_catalog() -> None:
    assert parse_catalog("") == Catalog()


def test_parse_catalog_file_empty_file_returns_empty_catalog(tmp_path) -> None:
    """A zero-byte file is a valid Catalog with zero records (req 3.5 doesn't apply
    to existing-but-empty files; req 3.2 does — the format simply contains no rows).
    """
    p = tmp_path / "empty.jsonl"
    p.write_bytes(b"")
    assert parse_catalog_file(str(p)) == Catalog()


def test_roundtrip_single_record_in_memory() -> None:
    rec = _make_record()
    cat = Catalog().upsert(rec)
    assert parse_catalog(print_catalog(cat)) == cat


def test_print_catalog_emits_one_line_per_record_with_lf_terminator() -> None:
    rec = _make_record()
    cat = Catalog().upsert(rec)
    rendered = print_catalog(cat)
    assert rendered.endswith("\n")
    assert rendered.count("\n") == 1


def test_print_catalog_uses_compact_json_with_sorted_keys() -> None:
    """Format details required by design.md.

    * No whitespace between separators.
    * Keys appear in alphabetical order via ``sort_keys=True``.
    """
    rec = _make_record()
    line = print_catalog(Catalog().upsert(rec)).rstrip("\n")
    # No whitespace between key/value or between fields.
    assert ", " not in line
    assert ": " not in line
    # ``sort_keys=True`` puts the alphabetically-first key (``content_hash``)
    # before all others.
    decoded = json.loads(line)
    assert list(decoded.keys()) == sorted(decoded.keys())


def test_print_catalog_sorts_records_by_hash_source_key() -> None:
    a = _make_record(content_hash="0" * 64, source="s3-prod", key="a")
    b = _make_record(content_hash="0" * 64, source="s3-prod", key="b")
    c = _make_record(content_hash="0" * 64, source="t3-prod", key="a")
    d = _make_record(content_hash="1" * 64, source="s3-prod", key="a")
    cat = Catalog().upsert(d).upsert(c).upsert(b).upsert(a)

    rendered = print_catalog(cat)
    lines = rendered.rstrip("\n").split("\n")
    decoded = [json.loads(line) for line in lines]
    keys_in_order = [(d["content_hash"], d["source"], d["key"]) for d in decoded]
    assert keys_in_order == sorted(keys_in_order)


def test_parse_catalog_malformed_line_raises_with_correct_line_number() -> None:
    rec = _make_record()
    good = print_catalog(Catalog().upsert(rec)).rstrip("\n")
    text = good + "\n" + "{not valid json" + "\n"
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 2
    assert excinfo.value.exit_code == ExitCode.CATALOG_INVALID


def test_parse_catalog_missing_required_field_raises() -> None:
    rec = _make_record()
    obj = json.loads(print_catalog(Catalog().upsert(rec)).rstrip("\n"))
    obj.pop("size_bytes")
    text = json.dumps(obj) + "\n"
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 1
    assert "size_bytes" in str(excinfo.value)


def test_parse_catalog_unknown_key_raises() -> None:
    rec = _make_record()
    obj = json.loads(print_catalog(Catalog().upsert(rec)).rstrip("\n"))
    obj["unexpected_field"] = "oops"
    text = json.dumps(obj) + "\n"
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 1
    assert "unexpected_field" in str(excinfo.value)


def test_parse_catalog_wrong_type_raises() -> None:
    """``size_bytes: "1024"`` (string instead of int) must be rejected."""
    rec = _make_record()
    obj = json.loads(print_catalog(Catalog().upsert(rec)).rstrip("\n"))
    obj["size_bytes"] = "1024"
    text = json.dumps(obj) + "\n"
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 1
    assert "size_bytes" in str(excinfo.value)


def test_parse_catalog_size_bytes_bool_is_rejected() -> None:
    """``True`` is an ``int`` subclass in Python; must still be rejected."""
    rec = _make_record()
    obj = json.loads(print_catalog(Catalog().upsert(rec)).rstrip("\n"))
    obj["size_bytes"] = True
    text = json.dumps(obj) + "\n"
    with pytest.raises(CatalogParseError):
        parse_catalog(text)


def test_parse_catalog_blank_line_is_rejected() -> None:
    rec = _make_record()
    good = print_catalog(Catalog().upsert(rec)).rstrip("\n")
    text = good + "\n\n"  # extra blank line in the middle/end
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 2


def test_parse_catalog_top_level_array_is_rejected() -> None:
    """Each line must be a JSON object, not an array."""
    text = json.dumps([1, 2, 3]) + "\n"
    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog(text)
    assert excinfo.value.line == 1


def test_parse_catalog_optional_fields_default_to_none_when_missing() -> None:
    """``quarantined_at`` / ``tombstoned_at`` / ``mcps_source_meta`` are
    Optional[str] with a default of ``None``: missing keys parse to ``None``.
    """
    rec = _make_record()
    obj = json.loads(print_catalog(Catalog().upsert(rec)).rstrip("\n"))
    obj.pop("quarantined_at")
    obj.pop("tombstoned_at")
    obj.pop("mcps_source_meta")
    text = json.dumps(obj) + "\n"
    parsed = parse_catalog(text)
    only = list(parsed.all_records())[0]
    assert only.quarantined_at is None
    assert only.tombstoned_at is None
    assert only.mcps_source_meta is None


# ---------------------------------------------------------------------------
# write_catalog / parse_catalog_file
# ---------------------------------------------------------------------------


def test_write_catalog_then_parse_catalog_file_roundtrip(tmp_path) -> None:
    rec1 = _make_record(source="s3-prod", key="photos/A.jpg", content_hash="a" * 64)
    rec2 = _make_record(source="gcs-cold", key="photos/B.jpg", content_hash="b" * 64)
    cat = Catalog().upsert(rec1).upsert(rec2)
    target = tmp_path / "catalog.jsonl"
    write_catalog(cat, str(target))
    assert parse_catalog_file(str(target)) == cat


def test_write_catalog_produces_byte_for_byte_match_with_print_catalog(tmp_path) -> None:
    rec = _make_record()
    cat = Catalog().upsert(rec)
    target = tmp_path / "catalog.jsonl"
    write_catalog(cat, str(target))
    on_disk = target.read_text(encoding="utf-8")
    assert on_disk == print_catalog(cat)


def test_write_catalog_does_not_leave_partial_file_on_success(tmp_path) -> None:
    """After a successful write, only the target file exists in the directory.

    Specifically, no ``.mcps-catalog-*.tmp`` temp file remains.
    """
    rec = _make_record()
    cat = Catalog().upsert(rec)
    target = tmp_path / "catalog.jsonl"
    write_catalog(cat, str(target))
    siblings = sorted(p.name for p in tmp_path.iterdir())
    assert siblings == ["catalog.jsonl"]


def test_write_catalog_replaces_existing_file_atomically(tmp_path) -> None:
    """A second write overwrites the first; the prior file is replaced atomically."""
    target = tmp_path / "catalog.jsonl"

    rec1 = _make_record(content_hash="a" * 64)
    cat1 = Catalog().upsert(rec1)
    write_catalog(cat1, str(target))
    assert parse_catalog_file(str(target)) == cat1

    rec2 = _make_record(content_hash="b" * 64, key="other.jpg")
    cat2 = Catalog().upsert(rec2)
    write_catalog(cat2, str(target))
    assert parse_catalog_file(str(target)) == cat2

    siblings = sorted(p.name for p in tmp_path.iterdir())
    assert siblings == ["catalog.jsonl"]


def test_parse_catalog_file_parse_failure_leaves_file_unchanged(tmp_path) -> None:
    """A failed parse must NOT mutate the on-disk file (req 3.6)."""
    target = tmp_path / "catalog.jsonl"
    bad_content = b"{not valid json\n"
    target.write_bytes(bad_content)

    with pytest.raises(CatalogParseError) as excinfo:
        parse_catalog_file(str(target))
    assert excinfo.value.path == str(target)
    assert excinfo.value.line == 1

    # Bytes are exactly what was written before the failed parse.
    assert target.read_bytes() == bad_content


def test_parse_catalog_file_with_unicode_key_roundtrips(tmp_path) -> None:
    """``ensure_ascii=False`` means non-ASCII keys appear verbatim in UTF-8."""
    rec = _make_record(key="фото/закат.jpg", content_type="image/jpeg")
    cat = Catalog().upsert(rec)
    target = tmp_path / "catalog.jsonl"
    write_catalog(cat, str(target))
    # File is valid UTF-8 and contains the original bytes of the key.
    raw = target.read_bytes()
    assert "фото/закат.jpg".encode("utf-8") in raw
    assert parse_catalog_file(str(target)) == cat
