"""Unit tests for the `SourceAdapter` ABC and `FakeSourceAdapter`.

Exercises the contract documented in `mcps/sources/base.py` via the
in-memory `FakeSourceAdapter` so that every requirement on the abstract
interface (listing, streaming reads, atomic writes, tagging, deletion,
read-only semantics) is covered without touching the network.

Validates: Requirements 2.1, 10.8.
"""

from __future__ import annotations

import dataclasses

import pytest

from mcps.errors import ReadOnlySourceError
from mcps.hashing import CHUNK_SIZE
from mcps.sources.base import ObjectMeta, SourceAdapter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# ObjectMeta value semantics
# ---------------------------------------------------------------------------


def _meta(**overrides: object) -> ObjectMeta:
    """Build an `ObjectMeta` with sensible defaults for the tests below."""
    base = dict(
        key="photos/img.jpg",
        size_bytes=1024,
        last_modified="2024-01-01T00:00:00Z",
        content_type="image/jpeg",
        user_metadata={"mcps-source": "s3-prod"},
        etag="d41d8cd98f00b204e9800998ecf8427e",
        provider_hash=None,
    )
    base.update(overrides)
    return ObjectMeta(**base)  # type: ignore[arg-type]


def test_object_meta_is_frozen() -> None:
    """ObjectMeta is a frozen dataclass: assignment raises FrozenInstanceError."""
    meta = _meta()
    with pytest.raises(dataclasses.FrozenInstanceError):
        meta.key = "other.jpg"  # type: ignore[misc]


def test_object_meta_is_hashable() -> None:
    """A frozen dataclass with hashable fields must itself be hashable."""
    meta = _meta()
    # If any field were unhashable, this call would raise TypeError.
    assert hash(meta) == hash(meta)
    # And it must be usable inside a set / dict key.
    assert {meta} == {meta}


def test_object_meta_equal_for_identical_fields() -> None:
    a = _meta()
    b = _meta()
    assert a == b
    assert hash(a) == hash(b)


def test_object_meta_inequal_when_any_field_differs() -> None:
    assert _meta(key="a.jpg") != _meta(key="b.jpg")
    assert _meta(size_bytes=1) != _meta(size_bytes=2)


# ---------------------------------------------------------------------------
# SourceAdapter ABC contract
# ---------------------------------------------------------------------------


def test_source_adapter_is_abstract_and_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError):
        SourceAdapter()  # type: ignore[abstract]


def test_fake_source_adapter_is_a_source_adapter() -> None:
    """The FakeSourceAdapter must be a real subclass of the ABC so any
    code that requires `isinstance(x, SourceAdapter)` accepts it."""
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    assert isinstance(adapter, SourceAdapter)


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


def test_list_objects_yields_one_meta_per_key() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod",
        kind="s3",
        records={
            "a.jpg": b"alpha",
            "b.jpg": b"beta-bytes",
            "subdir/c.png": b"gamma!",
        },
    )

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["a.jpg", "b.jpg", "subdir/c.png"]
    assert [m.size_bytes for m in metas] == [5, 10, 6]


def test_list_objects_empty_records_yields_nothing() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    assert list(adapter.list_objects()) == []


def test_list_objects_records_call_in_log() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"a": b"x"}
    )
    list(adapter.list_objects())
    assert ("list_objects", {}) in adapter.call_log


def test_list_objects_size_matches_record_length() -> None:
    """A 3 MiB payload list-objects must report 3 * 1024 * 1024 bytes."""
    payload = b"x" * (3 * 1024 * 1024)
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"big.bin": payload}
    )

    [meta] = list(adapter.list_objects())
    assert meta.size_bytes == len(payload)


def test_list_objects_includes_user_metadata_and_tags_merged() -> None:
    """Tags and user-metadata both surface in `ObjectMeta.user_metadata`
    so downstream consumers can read both via a single mapping."""
    adapter = FakeSourceAdapter(
        name="s3-prod",
        kind="s3",
        records={"a.jpg": b"alpha"},
        metadata={"a.jpg": {"mcps-source": "s3-prod"}},
        tags={"a.jpg": {"mcps-quarantined-at": "2024-06-01T00:00:00Z"}},
    )

    [meta] = list(adapter.list_objects())

    assert meta.user_metadata["mcps-source"] == "s3-prod"
    assert meta.user_metadata["mcps-quarantined-at"] == "2024-06-01T00:00:00Z"


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------


def test_read_bytes_concatenation_matches_stored_bytes() -> None:
    payload = b"the quick brown fox" * 1000
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": payload}
    )

    chunks = list(adapter.read_bytes("k"))

    assert b"".join(chunks) == payload


def test_read_bytes_empty_record_yields_no_chunks() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"empty": b""}
    )

    assert list(adapter.read_bytes("empty")) == []


def test_read_bytes_uses_chunk_size_for_chunk_boundaries() -> None:
    """A payload exactly twice CHUNK_SIZE produces two chunks; a payload
    one byte smaller produces one chunk."""
    full = b"x" * (2 * CHUNK_SIZE)
    short = b"x" * (2 * CHUNK_SIZE - 1)

    adapter_full = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": full}
    )
    adapter_short = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": short}
    )

    chunks_full = list(adapter_full.read_bytes("k"))
    chunks_short = list(adapter_short.read_bytes("k"))

    assert len(chunks_full) == 2
    assert len(chunks_full[0]) == CHUNK_SIZE
    assert len(chunks_full[1]) == CHUNK_SIZE
    assert b"".join(chunks_full) == full

    assert len(chunks_short) == 2
    assert len(chunks_short[0]) == CHUNK_SIZE
    assert len(chunks_short[1]) == CHUNK_SIZE - 1
    assert b"".join(chunks_short) == short


def test_read_bytes_records_call_in_log() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"x"}
    )
    list(adapter.read_bytes("k"))
    assert ("read_bytes", {"key": "k"}) in adapter.call_log


def test_read_bytes_missing_key_raises_file_not_found() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    with pytest.raises(FileNotFoundError):
        # Iteration triggers the lookup; the generator raises at first next().
        list(adapter.read_bytes("missing"))


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------


def test_write_bytes_stores_bytes_and_metadata() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    payload = b"new content"
    user_metadata = {
        "mcps-source": "s3-prod",
        "mcps-content-sha256": "a" * 64,
    }

    adapter.write_bytes(
        key="photos/new.jpg",
        chunks=iter([payload[:5], payload[5:]]),
        size_bytes=len(payload),
        content_type="image/jpeg",
        user_metadata=user_metadata,
    )

    assert adapter.records["photos/new.jpg"] == payload
    assert adapter.user_metadata["photos/new.jpg"] == user_metadata


def test_write_bytes_get_metadata_reports_correct_size() -> None:
    """A subsequent get_metadata returns the size of the just-written bytes."""
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    payload = b"x" * 4096

    adapter.write_bytes(
        key="k",
        chunks=iter([payload]),
        size_bytes=len(payload),
        content_type=None,
        user_metadata={},
    )

    meta = adapter.get_metadata("k")
    assert meta.size_bytes == len(payload)


def test_write_bytes_records_call_in_log_with_kwargs() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")

    adapter.write_bytes(
        key="k",
        chunks=iter([b"abc"]),
        size_bytes=3,
        content_type="text/plain",
        user_metadata={"mcps-source": "s3-prod"},
    )

    method, kwargs = adapter.call_log[0]
    assert method == "write_bytes"
    assert kwargs["key"] == "k"
    assert kwargs["size_bytes"] == 3
    assert kwargs["content_type"] == "text/plain"
    assert kwargs["user_metadata"] == {"mcps-source": "s3-prod"}


# ---------------------------------------------------------------------------
# set_tag
# ---------------------------------------------------------------------------


def test_set_tag_stores_tag_and_surfaces_in_object_meta() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod",
        kind="s3",
        records={"k": b"data"},
    )

    adapter.set_tag(
        key="k",
        tag_key="mcps-quarantined-at",
        tag_value="2024-06-01T00:00:00Z",
    )

    meta = adapter.get_metadata("k")
    assert meta.user_metadata["mcps-quarantined-at"] == "2024-06-01T00:00:00Z"


def test_set_tag_multiple_tags_accumulate() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"data"}
    )

    adapter.set_tag(key="k", tag_key="t1", tag_value="v1")
    adapter.set_tag(key="k", tag_key="t2", tag_value="v2")

    assert adapter.tags["k"] == {"t1": "v1", "t2": "v2"}


def test_set_tag_records_call_in_log() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"x"}
    )

    adapter.set_tag(key="k", tag_key="t", tag_value="v")

    assert (
        "set_tag",
        {"key": "k", "tag_key": "t", "tag_value": "v"},
    ) in adapter.call_log


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_removes_record_and_metadata_and_tags() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod",
        kind="s3",
        records={"k": b"data"},
        metadata={"k": {"mcps-source": "s3-prod"}},
        tags={"k": {"mcps-quarantined-at": "2024-06-01T00:00:00Z"}},
    )

    adapter.delete(key="k")

    assert "k" not in adapter.records
    assert "k" not in adapter.user_metadata
    assert "k" not in adapter.tags


def test_delete_then_get_metadata_raises_file_not_found() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"data"}
    )

    adapter.delete(key="k")

    with pytest.raises(FileNotFoundError):
        adapter.get_metadata("k")


def test_delete_records_call_in_log() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"x"}
    )
    adapter.delete(key="k")
    assert ("delete", {"key": "k"}) in adapter.call_log


def test_delete_missing_key_raises_file_not_found() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    with pytest.raises(FileNotFoundError):
        adapter.delete(key="missing")


# ---------------------------------------------------------------------------
# get_metadata
# ---------------------------------------------------------------------------


def test_get_metadata_missing_key_raises_file_not_found() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    with pytest.raises(FileNotFoundError):
        adapter.get_metadata("missing")


def test_get_metadata_records_call_in_log() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"x"}
    )
    adapter.get_metadata("k")
    assert ("get_metadata", {"key": "k"}) in adapter.call_log


# ---------------------------------------------------------------------------
# Read-only semantics (Drive parity, req 10.8)
# ---------------------------------------------------------------------------


def test_read_only_adapter_supports_writes_is_false() -> None:
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
    )
    assert adapter.supports_writes is False


def test_read_only_write_bytes_raises() -> None:
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
    )

    with pytest.raises(ReadOnlySourceError) as info:
        adapter.write_bytes(
            key="k",
            chunks=iter([b"x"]),
            size_bytes=1,
            content_type=None,
            user_metadata={},
        )

    assert info.value.adapter == "drive-personal"
    assert info.value.op == "write_bytes"


def test_read_only_set_tag_raises() -> None:
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
        records={"k": b"x"},
    )

    with pytest.raises(ReadOnlySourceError) as info:
        adapter.set_tag(key="k", tag_key="t", tag_value="v")

    assert info.value.adapter == "drive-personal"
    assert info.value.op == "set_tag"


def test_read_only_delete_raises() -> None:
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
        records={"k": b"x"},
    )

    with pytest.raises(ReadOnlySourceError) as info:
        adapter.delete(key="k")

    assert info.value.adapter == "drive-personal"
    assert info.value.op == "delete"


def test_read_only_failed_calls_still_recorded_in_log() -> None:
    """Even when the adapter raises, the call is recorded so tests can
    assert "the Replicator tried to write to a read-only adapter"."""
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
    )

    with pytest.raises(ReadOnlySourceError):
        adapter.write_bytes(
            key="k",
            chunks=iter([b"x"]),
            size_bytes=1,
            content_type=None,
            user_metadata={},
        )

    methods = [m for m, _ in adapter.call_log]
    assert "write_bytes" in methods


def test_read_only_read_path_still_works() -> None:
    """Read-only doesn't mean read-broken: list_objects, read_bytes, and
    get_metadata all still work."""
    adapter = FakeSourceAdapter(
        name="drive-personal",
        kind="google_drive",
        supports_writes=False,
        records={"a.jpg": b"alpha"},
    )

    [meta] = list(adapter.list_objects())
    assert meta.key == "a.jpg"
    assert b"".join(adapter.read_bytes("a.jpg")) == b"alpha"
    assert adapter.get_metadata("a.jpg").size_bytes == 5


def test_writable_adapter_supports_writes_default_true() -> None:
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    assert adapter.supports_writes is True


# ---------------------------------------------------------------------------
# Call-log shape and ordering
# ---------------------------------------------------------------------------


def test_call_log_preserves_call_order() -> None:
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records={"k": b"data"}
    )

    list(adapter.list_objects())
    adapter.get_metadata("k")
    list(adapter.read_bytes("k"))
    adapter.set_tag(key="k", tag_key="t", tag_value="v")
    adapter.delete(key="k")

    methods = [m for m, _ in adapter.call_log]
    assert methods == [
        "list_objects",
        "get_metadata",
        "read_bytes",
        "set_tag",
        "delete",
    ]


def test_call_log_kwargs_are_independent_snapshots() -> None:
    """Mutating user_metadata after a write_bytes call must not retroactively
    change the recorded kwargs in the call log."""
    adapter = FakeSourceAdapter(name="s3-prod", kind="s3")
    user_metadata = {"mcps-source": "s3-prod"}

    adapter.write_bytes(
        key="k",
        chunks=iter([b"x"]),
        size_bytes=1,
        content_type=None,
        user_metadata=user_metadata,
    )

    user_metadata["injected"] = "after-the-fact"

    _, kwargs = adapter.call_log[0]
    assert "injected" not in kwargs["user_metadata"]


# ---------------------------------------------------------------------------
# Multiple records: end-to-end scenario
# ---------------------------------------------------------------------------


def test_multiple_records_round_trip_through_list_read_and_metadata() -> None:
    """A FakeSourceAdapter holding several records lists each one and the
    bytes returned by `read_bytes` match the constructor input."""
    payloads = {
        "a.jpg": b"alpha-bytes",
        "b/c.png": b"beta-bytes-longer",
        "z.bin": b"",  # empty record is valid
    }

    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records=payloads
    )

    metas = {m.key: m for m in adapter.list_objects()}
    assert set(metas) == set(payloads)

    for key, data in payloads.items():
        assert metas[key].size_bytes == len(data)
        assert b"".join(adapter.read_bytes(key)) == data


def test_constructor_defensively_copies_inputs() -> None:
    """Mutating the constructor's `records` dict after construction must not
    affect the adapter — guards against accidental shared state in tests."""
    records = {"a": b"alpha"}
    adapter = FakeSourceAdapter(
        name="s3-prod", kind="s3", records=records
    )

    records["b"] = b"beta"  # mutate after construction

    keys = [m.key for m in adapter.list_objects()]
    assert keys == ["a"]
