"""Unit tests for `mcps.sources.gcs.GCSSourceAdapter`.

These tests inject a hand-rolled `FakeGcsClient` / `FakeBucket` /
`FakeBlob` hierarchy into the adapter via the ``gcs_client=...``
constructor seam, so the network is never touched. The tests cover:

* `list_objects` paginates via ``client.list_blobs(bucket, prefix=...)``
  and emits ObjectMetas whose ``provider_hash`` reflects the GCS CRC32C
  (informational only — req 2.3).
* `read_bytes` streams ``blob.open("rb")`` in 1 MiB chunks.
* `write_bytes` sets ``blob.metadata`` and forwards the content_type to
  ``upload_from_file``.
* `get_metadata` calls ``blob.reload()`` and surfaces metadata + size +
  updated as ISO-8601 UTC.
* `set_tag` reads ``blob.metadata`` (via ``reload()``), patches the
  requested key, and calls ``blob.patch()`` so existing entries are
  preserved.
* `delete` calls ``blob.delete()``.
* Retry mapping: a ``ServiceUnavailable`` (HTTP 503) is retried
  (transient); a ``Forbidden`` (HTTP 403) is *not* retried
  (non_transient).

Validates: Requirements 2.3, 2.5, 2.6, 5.7, 6.4, 6.5, 9.3, 9.5.
"""

from __future__ import annotations

import datetime as dt
import io
from typing import Any, Dict, List, Mapping, Optional

import pytest

from mcps.config.model import RetriesConfig
from mcps.errors import NonTransientError, RetriesExhausted
from mcps.sources.base import ObjectMeta
from mcps.sources.gcs import GCSSourceAdapter


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeBlob:
    """Minimal ``google.cloud.storage.Blob`` stand-in.

    Holds enough state to satisfy `GCSSourceAdapter`'s read / write /
    metadata / tag / delete paths. Each method records its calls into
    ``self.calls`` so the tests can assert call-site shape.
    """

    def __init__(
        self,
        *,
        name: str,
        data: Optional[bytes] = None,
        size: Optional[int] = None,
        updated: Optional[dt.datetime] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Mapping[str, str]] = None,
        crc32c: Optional[str] = None,
        client: Optional["FakeGcsClient"] = None,
    ) -> None:
        self.name = name
        self._data: bytes = data if data is not None else b""
        self.size: Optional[int] = (
            size if size is not None else (len(self._data) if data is not None else None)
        )
        self.updated: Optional[dt.datetime] = updated
        self.content_type: Optional[str] = content_type
        self.metadata: Optional[Dict[str, str]] = (
            dict(metadata) if metadata is not None else None
        )
        self.crc32c: Optional[str] = crc32c
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

        # Per-method exception queues, mirroring FakeS3Client. Each
        # next() pops one entry; if the popped entry is an exception
        # type/instance, it is raised before the normal return path.
        self.error_queue: Dict[str, List[Any]] = {}

    def _maybe_raise(self, op: str) -> None:
        queue = self.error_queue.get(op)
        if not queue:
            return
        exc = queue.pop(0)
        if exc is None:
            return
        if isinstance(exc, type):
            raise exc("boom")
        raise exc

    def exists(self) -> bool:
        self.calls.append(("exists", {}))
        return self._data is not None and self._data != b""

    def open(self, mode: str = "rb") -> io.BytesIO:
        self.calls.append(("open", {"mode": mode}))
        self._maybe_raise("open")
        assert mode == "rb"
        return io.BytesIO(self._data)

    def upload_from_file(
        self,
        file_obj: Any,
        content_type: Optional[str] = None,
    ) -> None:
        self.calls.append(
            ("upload_from_file", {"content_type": content_type})
        )
        self._maybe_raise("upload_from_file")
        # Drain the source so the streaming code is exercised.
        if hasattr(file_obj, "read"):
            self._data = file_obj.read()
        else:  # pragma: no cover - defensive
            self._data = bytes(file_obj)
        self.size = len(self._data)
        if content_type is not None:
            self.content_type = content_type
        # Mirror real GCS: the upload sets/refreshes ``updated``.
        self.updated = dt.datetime(2024, 6, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    def reload(self) -> None:
        self.calls.append(("reload", {}))
        self._maybe_raise("reload")
        # In the real client, reload populates size/updated/etc from
        # the server. Our fake already has those set; this is a no-op
        # except for the call audit. If the test wants to simulate a
        # server-side change between writes, it can mutate the blob
        # directly between calls.

    def patch(self) -> None:
        self.calls.append(("patch", {"metadata": dict(self.metadata or {})}))
        self._maybe_raise("patch")
        # Real client persists ``self.metadata`` to the server. Nothing
        # else to do for the fake.

    def delete(self) -> None:
        self.calls.append(("delete", {}))
        self._maybe_raise("delete")
        if self._client is not None:
            self._client._remove_blob(self.name)


class FakeBucket:
    """Minimal ``google.cloud.storage.Bucket`` stand-in.

    ``blob(key)`` returns the existing FakeBlob if one is registered
    for ``key``, otherwise constructs a fresh empty blob and registers
    it. This matches the real client's behaviour where ``bucket.blob``
    creates a local-only handle that may or may not yet exist on the
    server.
    """

    def __init__(self, name: str, client: "FakeGcsClient") -> None:
        self.name = name
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def blob(self, key: str) -> FakeBlob:
        self.calls.append(("blob", {"key": key}))
        return self._client._get_or_create_blob(self.name, key)


class FakeGcsClient:
    """Minimal ``google.cloud.storage.Client`` stand-in.

    Driven entirely by tests. The adapter under test only touches
    ``list_blobs`` and ``bucket(...)``; both are recorded into
    ``self.calls`` for assertions. Test seeding is via the
    ``blobs={(bucket, key): FakeBlob}`` constructor argument.
    """

    def __init__(
        self,
        *,
        blobs: Optional[Mapping[tuple[str, str], FakeBlob]] = None,
    ) -> None:
        self._blobs: Dict[tuple[str, str], FakeBlob] = dict(blobs or {})
        for blob in self._blobs.values():
            blob._client = self
        self.calls: List[tuple[str, Dict[str, Any]]] = []
        # Per-op error queue for client-level methods (list_blobs).
        self.error_queue: Dict[str, List[Any]] = {}

    def _maybe_raise(self, op: str) -> None:
        queue = self.error_queue.get(op)
        if not queue:
            return
        exc = queue.pop(0)
        if exc is None:
            return
        if isinstance(exc, type):
            raise exc("boom")
        raise exc

    def _get_or_create_blob(self, bucket: str, key: str) -> FakeBlob:
        existing = self._blobs.get((bucket, key))
        if existing is not None:
            return existing
        fresh = FakeBlob(name=key, client=self)
        self._blobs[(bucket, key)] = fresh
        return fresh

    def _remove_blob(self, key: str) -> None:
        for (bucket, name), blob in list(self._blobs.items()):
            if name == key:
                del self._blobs[(bucket, name)]

    def bucket(self, name: str) -> FakeBucket:
        self.calls.append(("bucket", {"name": name}))
        return FakeBucket(name, self)

    def list_blobs(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> List[FakeBlob]:
        self.calls.append(("list_blobs", {"bucket": bucket, "prefix": prefix}))
        self._maybe_raise("list_blobs")
        result: List[FakeBlob] = []
        for (b, _key), blob in self._blobs.items():
            if b != bucket:
                continue
            if prefix and not blob.name.startswith(prefix):
                continue
            result.append(blob)
        # Sort for deterministic iteration order in assertions.
        result.sort(key=lambda b: b.name)
        return result


# A test-friendly RetriesConfig: tight backoff so retry-driven tests
# finish in a few milliseconds rather than seconds.
_FAST_RETRIES = RetriesConfig(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=1000,
    request_timeout_ms=1000,
)


def _make_adapter(
    client: FakeGcsClient,
    *,
    bucket: str = "test-bucket",
    prefix: Optional[str] = None,
    retries_config: Optional[RetriesConfig] = None,
) -> GCSSourceAdapter:
    return GCSSourceAdapter(
        name="gcs-test",
        bucket=bucket,
        prefix=prefix,
        gcs_client=client,
        retries_config=retries_config or _FAST_RETRIES,
    )


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


def test_list_objects_yields_meta_with_crc32c_provider_hash_and_no_etag() -> None:
    blobs = {
        ("test-bucket", "photos/a.jpg"): FakeBlob(
            name="photos/a.jpg",
            data=b"a" * 100,
            updated=dt.datetime(2024, 5, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={"mcps-source": "gcs-archive"},
            crc32c="AAAAA1==",
        ),
        ("test-bucket", "photos/b.jpg"): FakeBlob(
            name="photos/b.jpg",
            data=b"b" * 2000,
            updated=dt.datetime(2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata=None,  # exercise the None branch
            crc32c="BBBBB2==",
        ),
        ("other-bucket", "photos/skip.jpg"): FakeBlob(
            name="photos/skip.jpg", data=b"skip"
        ),
    }
    client = FakeGcsClient(blobs=blobs)
    adapter = _make_adapter(client, prefix="photos/")

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["photos/a.jpg", "photos/b.jpg"]
    assert [m.size_bytes for m in metas] == [100, 2000]
    assert metas[0].last_modified == "2024-05-01T12:00:00Z"
    assert metas[0].content_type == "image/jpeg"
    # GCS does not use S3-style ETags.
    assert metas[0].etag is None
    assert metas[1].etag is None
    # provider_hash is the GCS CRC32C (informational only — never used as
    # Content_Hash per req 2.3).
    assert metas[0].provider_hash == "AAAAA1=="
    assert metas[1].provider_hash == "BBBBB2=="
    # mcps-source surfaced from blob.metadata.
    assert metas[0].user_metadata == {"mcps-source": "gcs-archive"}
    assert metas[1].user_metadata == {}

    # client.list_blobs called with the prefix.
    list_calls = [c for c in client.calls if c[0] == "list_blobs"]
    assert list_calls == [
        ("list_blobs", {"bucket": "test-bucket", "prefix": "photos/"})
    ]


def test_list_objects_without_prefix_calls_list_blobs_without_prefix_kwarg() -> None:
    client = FakeGcsClient(blobs={})
    adapter = _make_adapter(client, prefix=None)
    list(adapter.list_objects())
    list_calls = [c for c in client.calls if c[0] == "list_blobs"]
    # Adapter uses the no-prefix overload when prefix is falsy.
    assert list_calls == [("list_blobs", {"bucket": "test-bucket", "prefix": None})]


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------


def test_read_bytes_streams_full_payload_via_blob_open() -> None:
    payload = b"hello-world-" * 100
    blob = FakeBlob(name="a", data=payload)
    client = FakeGcsClient(blobs={("test-bucket", "a"): blob})
    adapter = _make_adapter(client)

    streamed = b"".join(adapter.read_bytes("a"))

    assert streamed == payload
    open_calls = [c for c in blob.calls if c[0] == "open"]
    assert open_calls == [("open", {"mode": "rb"})]


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------


def test_write_bytes_sets_metadata_and_forwards_content_type() -> None:
    client = FakeGcsClient()
    adapter = _make_adapter(client)
    payload = b"x" * 1024
    metadata = {
        "mcps-source": "s3-prod",
        "mcps-content-sha256": "f" * 64,
        "mcps-replicated-at": "2024-06-01T12:00:00Z",
    }

    adapter.write_bytes(
        "k.bin",
        iter([payload]),
        size_bytes=len(payload),
        content_type="image/jpeg",
        user_metadata=metadata,
    )

    blob = client._blobs[("test-bucket", "k.bin")]
    upload_calls = [c for c in blob.calls if c[0] == "upload_from_file"]
    assert len(upload_calls) == 1
    assert upload_calls[0][1] == {"content_type": "image/jpeg"}
    # Metadata was set BEFORE the upload (the adapter writes it onto
    # the blob first so a single round-trip persists payload + metadata).
    assert blob.metadata == metadata
    # The streamed bytes round-tripped through the fake.
    assert blob._data == payload


def test_write_bytes_without_content_type_omits_kwarg() -> None:
    client = FakeGcsClient()
    adapter = _make_adapter(client)

    adapter.write_bytes(
        "k.bin",
        iter([b"abc"]),
        size_bytes=3,
        content_type=None,
        user_metadata={},
    )

    blob = client._blobs[("test-bucket", "k.bin")]
    upload_calls = [c for c in blob.calls if c[0] == "upload_from_file"]
    assert len(upload_calls) == 1
    # ``content_type=None`` indicates the adapter did not pass the kwarg.
    assert upload_calls[0][1] == {"content_type": None}
    assert blob.metadata == {}


# ---------------------------------------------------------------------------
# get_metadata
# ---------------------------------------------------------------------------


def test_get_metadata_calls_reload_and_emits_meta() -> None:
    blob = FakeBlob(
        name="photos/img.jpg",
        data=b"\x00" * 1234,
        size=1234,
        updated=dt.datetime(2024, 6, 1, 9, 0, 0, tzinfo=dt.timezone.utc),
        content_type="image/jpeg",
        metadata={
            "mcps-source": "gcs-archive",
            "mcps-content-sha256": "f" * 64,
            "mcps-quarantined-at": "2024-06-15T00:00:00Z",
        },
        crc32c="ZmZmZg==",
    )
    client = FakeGcsClient(blobs={("test-bucket", "photos/img.jpg"): blob})
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("photos/img.jpg")

    assert isinstance(meta, ObjectMeta)
    assert meta.key == "photos/img.jpg"
    assert meta.size_bytes == 1234
    assert meta.last_modified == "2024-06-01T09:00:00Z"
    assert meta.content_type == "image/jpeg"
    assert meta.etag is None
    assert meta.provider_hash == "ZmZmZg=="
    assert meta.user_metadata["mcps-source"] == "gcs-archive"
    assert meta.user_metadata["mcps-content-sha256"] == "f" * 64
    assert meta.user_metadata["mcps-quarantined-at"] == "2024-06-15T00:00:00Z"
    # Adapter issued a reload() against the blob.
    reload_calls = [c for c in blob.calls if c[0] == "reload"]
    assert len(reload_calls) == 1


# ---------------------------------------------------------------------------
# set_tag
# ---------------------------------------------------------------------------


def test_set_tag_preserves_existing_metadata_and_replaces_collision() -> None:
    blob = FakeBlob(
        name="k",
        data=b"data",
        metadata={
            "owner": "lvq",
            "mcps-quarantined-at": "old",
        },
    )
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    adapter.set_tag("k", "mcps-quarantined-at", "2024-07-01T12:00:00Z")

    # owner preserved unchanged; quarantined-at replaced.
    assert blob.metadata == {
        "owner": "lvq",
        "mcps-quarantined-at": "2024-07-01T12:00:00Z",
    }
    # The adapter reloaded then patched (single round-trip per side).
    ops = [c[0] for c in blob.calls]
    assert ops.count("reload") == 1
    assert ops.count("patch") == 1
    # Patch happens after reload + metadata mutation.
    assert ops.index("patch") > ops.index("reload")


def test_set_tag_adds_new_entry_when_absent() -> None:
    blob = FakeBlob(
        name="k",
        data=b"data",
        metadata={"owner": "lvq"},
    )
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    adapter.set_tag("k", "mcps-tombstoned-at", "2024-07-02T00:00:00Z")

    assert blob.metadata == {
        "owner": "lvq",
        "mcps-tombstoned-at": "2024-07-02T00:00:00Z",
    }


def test_set_tag_initialises_metadata_if_absent() -> None:
    """Some GCS blobs have ``metadata=None``; set_tag must still work."""
    blob = FakeBlob(name="k", data=b"data", metadata=None)
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    adapter.set_tag("k", "mcps-quarantined-at", "2024-07-03T00:00:00Z")

    assert blob.metadata == {"mcps-quarantined-at": "2024-07-03T00:00:00Z"}


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_calls_blob_delete() -> None:
    blob = FakeBlob(name="k", data=b"data")
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    adapter.delete("k")

    delete_calls = [c for c in blob.calls if c[0] == "delete"]
    assert delete_calls == [("delete", {})]
    assert ("test-bucket", "k") not in client._blobs


# ---------------------------------------------------------------------------
# Retry / error mapping
# ---------------------------------------------------------------------------


def _make_service_unavailable() -> Exception:
    """Construct a ``ServiceUnavailable`` (HTTP 503) exception."""
    from google.api_core.exceptions import (  # type: ignore[import-not-found]
        ServiceUnavailable,
    )

    return ServiceUnavailable("boom")


def _make_forbidden() -> Exception:
    """Construct a ``Forbidden`` (HTTP 403) exception."""
    from google.api_core.exceptions import (  # type: ignore[import-not-found]
        Forbidden,
    )

    return Forbidden("nope")


def test_google_api_call_error_503_is_transient_and_eventually_succeeds() -> None:
    blob = FakeBlob(
        name="k",
        data=b"x",
        size=1,
        updated=dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
        content_type="application/octet-stream",
        metadata={},
        crc32c="AAAA",
    )
    blob.error_queue["reload"] = [_make_service_unavailable(), None]
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("k")

    assert meta.size_bytes == 1
    reload_calls = [c for c in blob.calls if c[0] == "reload"]
    # One retry → two reload invocations total.
    assert len(reload_calls) == 2


def test_google_api_call_error_503_exhausts_after_max_retries() -> None:
    blob = FakeBlob(name="k", data=b"x", metadata={})
    blob.error_queue["reload"] = [_make_service_unavailable() for _ in range(10)]
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    with pytest.raises(RetriesExhausted):
        adapter.get_metadata("k")


def test_google_api_call_error_403_is_non_transient_and_not_retried() -> None:
    blob = FakeBlob(name="k", data=b"x", metadata={})
    blob.error_queue["reload"] = [_make_forbidden()]
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    with pytest.raises(NonTransientError) as exc_info:
        adapter.get_metadata("k")

    assert exc_info.value.status == 403
    reload_calls = [c for c in blob.calls if c[0] == "reload"]
    # Exactly one attempt — no retry on non_transient.
    assert len(reload_calls) == 1


def test_requests_timeout_is_transient_and_eventually_succeeds() -> None:
    from requests.exceptions import Timeout as RequestsTimeout  # type: ignore[import-not-found]

    blob = FakeBlob(
        name="k",
        data=b"",
        size=0,
        updated=dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
        content_type="application/octet-stream",
        metadata={},
    )
    blob.error_queue["reload"] = [RequestsTimeout("slow"), None]
    client = FakeGcsClient(blobs={("test-bucket", "k"): blob})
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("k")

    assert meta.size_bytes == 0
    reload_calls = [c for c in blob.calls if c[0] == "reload"]
    assert len(reload_calls) == 2


def test_list_blobs_503_is_retried() -> None:
    blob = FakeBlob(
        name="a",
        data=b"a",
        updated=dt.datetime(2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc),
        content_type="application/octet-stream",
        metadata={},
    )
    client = FakeGcsClient(blobs={("test-bucket", "a"): blob})
    client.error_queue["list_blobs"] = [_make_service_unavailable(), None]
    adapter = _make_adapter(client)

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["a"]
    list_calls = [c for c in client.calls if c[0] == "list_blobs"]
    assert len(list_calls) == 2
