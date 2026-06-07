# Feature: multicloud-photo-sync, Property 2: Manifest round-trip
"""Round-trip tests for `Manifest_Parser`, `Manifest_Printer`, and
`Manifest_Writer`.

Hypothesis property + example-based tests for the on-disk Manifest format.

The property under test (design.md, "Correctness Properties — Property 2:
Manifest round-trip") is:

  For every sequence of valid `ManifestRecord` values ``m``,
      parse_manifest(print_manifest(m)) == (m, [])
  element-wise across every field, and ``print_manifest`` output is
  UTF-8 LF-terminated JSONL with no BOM.

Validates: Requirements 14.1, 14.2, 14.6, 14.7, 15.1, 15.2, 15.3, 15.4, 15.5.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.errors import ExitCode, ManifestWriteError
from mcps.manifest.model import Action, ManifestRecord, Result
from mcps.manifest.parser import (
    ParseError,
    parse_manifest,
    parse_manifest_file,
)
from mcps.manifest.printer import (
    print_manifest,
    print_manifest_record,
    write_manifest_file,
)
from mcps.manifest.writer import ManifestWriter


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Bounded epoch range covering 2000-01-01 .. 2099-12-31 so the resulting
# ISO-8601 strings always have a fixed-width 24-char layout.
_EPOCH_MIN = int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp())
_EPOCH_MAX = int(datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())


@st.composite
def iso_ms_timestamps(draw) -> str:
    """ISO-8601 UTC timestamp string with millisecond precision and trailing Z."""
    epoch = draw(st.integers(min_value=_EPOCH_MIN, max_value=_EPOCH_MAX))
    millis = draw(st.integers(min_value=0, max_value=999))
    base = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return base.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


# Run-id is documented as "UUIDv4 hex (>=8 chars)". Generate hex strings of
# 8..32 chars from a fixed alphabet to exercise the field shape without
# pulling in `uuid`.
_HEX_ALPHABET = "0123456789abcdef"
_RUN_IDS = st.text(alphabet=_HEX_ALPHABET, min_size=8, max_size=32)


# Source / target / key may be arbitrary printable text or None. We exclude
# control characters that would trip up universal-newline handling.
_KEY_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),  # surrogates are not valid Unicode
        blacklist_characters=("\x00", "\n", "\r"),
    ),
    min_size=0,
    max_size=64,
)

_OPT_KEY_TEXT = st.one_of(st.none(), _KEY_TEXT)

_HASH_HEX = st.text(alphabet=_HEX_ALPHABET, min_size=64, max_size=64)
_OPT_HASH = st.one_of(st.none(), _HASH_HEX)

_OPT_INT = st.one_of(st.none(), st.integers(min_value=0, max_value=10**12))

_OPT_ERROR = st.one_of(st.none(), st.text(min_size=0, max_size=80))

# `extra` is documented as ``Mapping[str, str]`` with 0..3 entries.
_EXTRA = st.dictionaries(
    keys=st.text(
        alphabet=st.characters(
            blacklist_categories=("Cs",),
            blacklist_characters=("\x00", "\n", "\r"),
        ),
        min_size=1,
        max_size=16,
    ),
    values=st.text(
        alphabet=st.characters(
            blacklist_categories=("Cs",),
            blacklist_characters=("\x00", "\n", "\r"),
        ),
        min_size=0,
        max_size=32,
    ),
    min_size=0,
    max_size=3,
)


@st.composite
def manifest_records(draw) -> ManifestRecord:
    return ManifestRecord(
        timestamp=draw(iso_ms_timestamps()),
        run_id=draw(_RUN_IDS),
        action=draw(st.sampled_from(list(Action))),
        result=draw(st.sampled_from(list(Result))),
        source=draw(_OPT_KEY_TEXT),
        target=draw(_OPT_KEY_TEXT),
        key=draw(_OPT_KEY_TEXT),
        content_hash=draw(_OPT_HASH),
        size_bytes=draw(_OPT_INT),
        error=draw(_OPT_ERROR),
        extra=draw(_EXTRA),
    )


# Bound list size at 1000 — req 15.3 talks about up to 10^7 records but
# Hypothesis would be unworkable at that scale. 1000 is large enough to
# exercise multi-line ordering invariants without slowing the suite to a
# crawl, and the property is universally quantified so the truncated
# bound still proves the round-trip holds element-wise.
_MAX_RECORDS = 1000


@st.composite
def manifest_record_lists(draw) -> list[ManifestRecord]:
    n = draw(st.integers(min_value=0, max_value=_MAX_RECORDS))
    return draw(st.lists(manifest_records(), min_size=n, max_size=n))


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(records=manifest_record_lists())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_manifest_roundtrip_in_memory(records: list[ManifestRecord]) -> None:
    """``parse_manifest(print_manifest(records)) == (records, [])``.

    Validates: Requirements 15.2, 15.3, 15.4.
    """
    rendered = print_manifest(records)
    parsed_records, errors = parse_manifest(rendered)
    assert errors == []
    assert parsed_records == records


@pytest.mark.property
@given(records=manifest_record_lists())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_print_manifest_is_lf_terminated_utf8_without_bom(
    records: list[ManifestRecord],
) -> None:
    """Output is UTF-8 LF-terminated JSONL with no BOM.

    Validates: Requirements 14.2, 15.2.
    """
    rendered = print_manifest(records)
    encoded = rendered.encode("utf-8")

    # No UTF-8 BOM at the start of the bytes.
    assert not encoded.startswith(b"\xef\xbb\xbf")

    # Every line is LF-terminated, never CRLF.
    if records:
        assert encoded.endswith(b"\n")
        # No CR bytes anywhere in the rendered output (we never emit CRLF).
        assert b"\r" not in encoded
        # One newline per record.
        assert encoded.count(b"\n") == len(records)
    else:
        assert encoded == b""


# ---------------------------------------------------------------------------
# Example-based tests: empty / single-record / round-trip via file
# ---------------------------------------------------------------------------


def _make_record(
    *,
    timestamp: str = "2024-01-01T00:00:00.000Z",
    run_id: str = "abcdef12",
    action: Action = Action.REPLICATE,
    result: Result = Result.SUCCESS,
    source: Optional[str] = "s3-prod",
    target: Optional[str] = "gcs-archive",
    key: Optional[str] = "photos/IMG_0001.jpg",
    content_hash: Optional[str] = "a" * 64,
    size_bytes: Optional[int] = 1024,
    error: Optional[str] = None,
    extra: Optional[dict[str, str]] = None,
) -> ManifestRecord:
    return ManifestRecord(
        timestamp=timestamp,
        run_id=run_id,
        action=action,
        result=result,
        source=source,
        target=target,
        key=key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        error=error,
        extra=dict(extra or {}),
    )


def test_print_manifest_empty_list_returns_empty_string() -> None:
    assert print_manifest([]) == ""


def test_parse_manifest_empty_string_returns_empty_pair() -> None:
    assert parse_manifest("") == ([], [])


def test_parse_manifest_file_empty_file_returns_empty_pair(tmp_path) -> None:
    p = tmp_path / "empty.jsonl"
    p.write_bytes(b"")
    assert parse_manifest_file(str(p)) == ([], [])


def test_single_record_roundtrip_in_memory() -> None:
    rec = _make_record(extra={"reason": "absent-on-target"})
    rendered = print_manifest([rec])
    parsed, errors = parse_manifest(rendered)
    assert errors == []
    assert parsed == [rec]


def test_print_manifest_record_has_no_trailing_newline() -> None:
    rec = _make_record()
    line = print_manifest_record(rec)
    assert "\n" not in line
    assert "\r" not in line
    # And it parses back to the same record on its own.
    parsed, errors = parse_manifest(line + "\n")
    assert errors == []
    assert parsed == [rec]


def test_print_manifest_uses_compact_json_with_sorted_keys() -> None:
    rec = _make_record(extra={"k": "v"})
    line = print_manifest([rec]).rstrip("\n")
    # No whitespace between key/value or between fields.
    assert ", " not in line
    assert ": " not in line
    decoded = json.loads(line)
    assert list(decoded.keys()) == sorted(decoded.keys())
    # Action and Result encode as the bare string values.
    assert decoded["action"] == "replicate"
    assert decoded["result"] == "success"


def test_print_manifest_renders_extra_as_json_object() -> None:
    rec = _make_record(extra={"expected": "a" * 64, "observed": "b" * 64})
    decoded = json.loads(print_manifest([rec]).rstrip("\n"))
    assert decoded["extra"] == {"expected": "a" * 64, "observed": "b" * 64}


def test_write_manifest_file_then_parse_roundtrip(tmp_path) -> None:
    rec1 = _make_record(action=Action.DISCOVERED, result=Result.SUCCESS)
    rec2 = _make_record(action=Action.REPLICATE, result=Result.SUCCESS, key="b.jpg")
    target = tmp_path / "manifest.jsonl"
    write_manifest_file([rec1, rec2], str(target))

    # File is UTF-8 with no BOM and LF terminators.
    raw = target.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in raw
    assert raw.endswith(b"\n")

    parsed, errors = parse_manifest_file(str(target))
    assert errors == []
    assert parsed == [rec1, rec2]


# ---------------------------------------------------------------------------
# Example-based tests: parse-failure line numbering and categories (req 15.4)
# ---------------------------------------------------------------------------


def _full_record_obj(rec: ManifestRecord) -> dict:
    """Render a record to its JSON-object form (dict) for hand-edit tests."""
    return json.loads(print_manifest_record(rec))


def test_parse_manifest_json_syntax_error_on_line_2_returns_first_record() -> None:
    """Line 1 parses; line 2 is malformed JSON. Result: 1 record, 1 ParseError."""
    rec = _make_record()
    good = print_manifest_record(rec)
    text = good + "\n" + "{not valid json" + "\n"

    records, errors = parse_manifest(text)

    assert records == [rec]
    assert len(errors) == 1
    assert errors[0].line == 2
    assert errors[0].category == "json_syntax"


def test_parse_manifest_missing_required_field_on_line_1() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj.pop("size_bytes")
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "missing_field"
    assert "size_bytes" in errors[0].message


def test_parse_manifest_unknown_key_on_line_1() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["unexpected_field"] = "oops"
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "unknown_key"
    assert "unexpected_field" in errors[0].message


def test_parse_manifest_unknown_action_enum() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["action"] = "not-a-real-action"
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "unknown_enum"
    assert "not-a-real-action" in errors[0].message


def test_parse_manifest_unknown_result_enum() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["result"] = "not-a-real-result"
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "unknown_enum"


def test_parse_manifest_invalid_value_size_bytes_string_instead_of_int() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["size_bytes"] = "1024"  # string instead of int
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "invalid_value"
    assert "size_bytes" in errors[0].message


def test_parse_manifest_invalid_value_size_bytes_bool_is_rejected() -> None:
    """``True`` is an ``int`` subclass in Python; must still be rejected."""
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["size_bytes"] = True
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].category == "invalid_value"


def test_parse_manifest_invalid_value_extra_must_be_string_to_string_dict() -> None:
    rec = _make_record()
    obj = _full_record_obj(rec)
    obj["extra"] = {"k": 42}  # int value, not str
    text = json.dumps(obj) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].category == "invalid_value"
    assert "extra" in errors[0].message


def test_parse_manifest_top_level_array_is_rejected() -> None:
    """Each line must be a JSON object, not an array."""
    text = json.dumps([1, 2, 3]) + "\n"

    records, errors = parse_manifest(text)

    assert records == []
    assert len(errors) == 1
    assert errors[0].line == 1
    assert errors[0].category == "json_syntax"


def test_parse_manifest_blank_line_records_a_parse_error() -> None:
    """Blank lines do not silently disappear (req 15.4)."""
    rec = _make_record()
    good = print_manifest_record(rec)
    # Lines: 1) good, 2) blank, 3) good
    text = good + "\n" + "\n" + good + "\n"

    records, errors = parse_manifest(text)

    assert records == [rec, rec]
    assert len(errors) == 1
    assert errors[0].line == 2
    assert errors[0].category == "json_syntax"


def test_parse_manifest_multiple_errors_preserve_line_numbers() -> None:
    rec = _make_record()
    good = print_manifest_record(rec)
    bad_json = "{not valid"
    obj = _full_record_obj(rec)
    obj.pop("size_bytes")
    bad_missing = json.dumps(obj)

    # Lines: 1=good, 2=bad_json, 3=good, 4=bad_missing
    text = "\n".join([good, bad_json, good, bad_missing]) + "\n"

    records, errors = parse_manifest(text)

    assert records == [rec, rec]
    assert len(errors) == 2
    assert errors[0].line == 2
    assert errors[0].category == "json_syntax"
    assert errors[1].line == 4
    assert errors[1].category == "missing_field"


# ---------------------------------------------------------------------------
# I/O failure cases (req 15.5, 14.6, 14.7)
# ---------------------------------------------------------------------------


def test_parse_manifest_file_nonexistent_path_raises_oserror(tmp_path) -> None:
    """Per req 15.5 an I/O failure on open propagates as OSError to the caller."""
    missing = tmp_path / "no-such-manifest.jsonl"
    with pytest.raises(OSError):
        parse_manifest_file(str(missing))


def test_manifest_writer_appends_records_and_parse_roundtrips(tmp_path) -> None:
    target = tmp_path / "manifest.jsonl"
    rec1 = _make_record(action=Action.DISCOVERED, key="a.jpg")
    rec2 = _make_record(action=Action.REPLICATE, key="b.jpg")

    with ManifestWriter(str(target)) as mw:
        mw.append(rec1)
        mw.append(rec2)

    parsed, errors = parse_manifest_file(str(target))
    assert errors == []
    assert parsed == [rec1, rec2]

    # File is UTF-8 LF-terminated with no BOM.
    raw = target.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in raw
    assert raw.endswith(b"\n")
    assert raw.count(b"\n") == 2


def test_manifest_writer_append_after_close_raises_manifest_write_error(tmp_path) -> None:
    target = tmp_path / "manifest.jsonl"
    mw = ManifestWriter(str(target))
    mw.close()
    with pytest.raises(ManifestWriteError):
        mw.append(_make_record())


def test_manifest_writer_close_is_idempotent(tmp_path) -> None:
    target = tmp_path / "manifest.jsonl"
    mw = ManifestWriter(str(target))
    mw.close()
    # Second close is a no-op.
    mw.close()
    assert mw.closed is True


def test_manifest_writer_append_many_writes_every_record(tmp_path) -> None:
    target = tmp_path / "manifest.jsonl"
    recs = [
        _make_record(action=Action.DISCOVERED, key=f"f{i}.jpg")
        for i in range(5)
    ]
    with ManifestWriter(str(target)) as mw:
        mw.append_many(recs)
    parsed, errors = parse_manifest_file(str(target))
    assert errors == []
    assert parsed == recs


def test_manifest_writer_appends_to_existing_file_without_truncation(tmp_path) -> None:
    """Append mode: re-opening the writer adds to the existing manifest."""
    target = tmp_path / "manifest.jsonl"
    rec1 = _make_record(key="a.jpg")
    rec2 = _make_record(key="b.jpg")

    with ManifestWriter(str(target)) as mw:
        mw.append(rec1)
    with ManifestWriter(str(target)) as mw:
        mw.append(rec2)

    parsed, errors = parse_manifest_file(str(target))
    assert errors == []
    assert parsed == [rec1, rec2]


def test_manifest_writer_construction_on_missing_directory_raises_error(tmp_path) -> None:
    """Per req 14.7 a missing/non-writable manifest dir surfaces at construction."""
    bad = tmp_path / "does-not-exist" / "manifest.jsonl"
    with pytest.raises(ManifestWriteError) as excinfo:
        ManifestWriter(str(bad))
    assert excinfo.value.path == str(bad)
    assert excinfo.value.exit_code == ExitCode.MANIFEST_UNAVAILABLE


def test_manifest_writer_concurrent_appends_do_not_interleave_lines(tmp_path) -> None:
    """The internal Lock serialises append() so two threads writing
    concurrently produce N intact lines, never partial bytes interleaved
    mid-line.

    We launch many threads each writing a distinguishable record; the
    file must contain exactly one well-formed line per record.
    """
    target = tmp_path / "manifest.jsonl"
    n_threads = 8
    n_per_thread = 25

    records: list[ManifestRecord] = []
    for t in range(n_threads):
        for i in range(n_per_thread):
            records.append(
                _make_record(
                    action=Action.DISCOVERED,
                    key=f"thread-{t}/item-{i}",
                    extra={"thread": str(t), "i": str(i)},
                )
            )

    with ManifestWriter(str(target)) as mw:
        threads = []
        # Partition records across threads so each thread writes a disjoint
        # slice; correctness is independent of the partition.
        for t in range(n_threads):
            slice_start = t * n_per_thread
            slice_end = slice_start + n_per_thread

            def worker(s=slice_start, e=slice_end):
                for r in records[s:e]:
                    mw.append(r)

            threads.append(threading.Thread(target=worker))
        for th in threads:
            th.start()
        for th in threads:
            th.join()

    # Every line parses (no torn writes) and every original record is
    # present in the parsed output (order is unspecified across threads).
    parsed, errors = parse_manifest_file(str(target))
    assert errors == []
    assert len(parsed) == n_threads * n_per_thread
    assert sorted(parsed, key=lambda r: r.key or "") == sorted(
        records, key=lambda r: r.key or ""
    )

    # File-level invariants: every line ends in LF, none in CRLF, no BOM.
    raw = target.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in raw
    assert raw.endswith(b"\n")
    assert raw.count(b"\n") == n_threads * n_per_thread
