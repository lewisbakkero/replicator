"""Unit tests for `mcps.sources.s3.S3SourceAdapter`.

These tests inject a hand-rolled `FakeS3Client` into the adapter via the
``s3_client=...`` constructor seam, so the network is never touched.
The tests cover:

* `list_objects` paginates and emits ObjectMetas with the right key,
  size, last_modified shape, ETag (quotes stripped), and provider_hash
  (set only when the ETag is single-part).
* `read_bytes` streams via the response's ``Body.iter_chunks``.
* `write_bytes` chooses ``put_object`` for ≤ 5 MiB and
  ``upload_fileobj`` (multipart) above. Both call paths must populate
  ``Metadata=`` from ``user_metadata``.
* `get_metadata` calls both ``head_object`` and ``get_object_tagging``
  and merges the results.
* `set_tag` preserves the existing tag set when adding a new entry.
* `delete` calls ``delete_object``.
* Retry mapping: a ``ClientError`` with HTTP 503 is retried (transient);
  a ``ClientError`` with HTTP 403 is *not* retried (non_transient); a
  ``ConnectTimeoutError`` is retried.

The moto-backed integration test for the same adapter lands later in
task 34 (`tests/integration/test_s3_adapter.py`).
"""

from __future__ import annotations

import datetime as dt
import io
from typing import Any, Dict, Iterator, List, Mapping, Optional

import pytest

from mcps.config.model import RetriesConfig
from mcps.errors import NonTransientError, RetriesExhausted
from mcps.sources.base import ObjectMeta
from mcps.sources.s3 import MULTIPART_THRESHOLD_BYTES, S3SourceAdapter


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _StreamingBody:
    """Minimal ``StreamingBody`` stand-in implementing ``iter_chunks``."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def iter_chunks(self, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        for i in range(0, len(self._data), chunk_size):
            yield self._data[i : i + chunk_size]


class _Paginator:
    """Tiny stand-in for boto3's S3 paginator.

    Holds the pages a test wants the adapter to see; ``paginate(...)``
    yields them in order, ignoring its arguments beyond recording them
    for assertions.
    """

    def __init__(self, pages: List[Dict[str, Any]]) -> None:
        self._pages = pages
        self.paginate_calls: List[Dict[str, Any]] = []

    def paginate(self, **kwargs: Any) -> Iterator[Dict[str, Any]]:
        self.paginate_calls.append(dict(kwargs))
        for page in self._pages:
            yield page


class FakeS3Client:
    """Minimal boto3-S3-shaped client driven entirely by tests.

    The adapter under test only touches the methods present here:
    ``get_paginator``, ``get_object``, ``put_object``, ``upload_fileobj``,
    ``head_object``, ``get_object_tagging``, ``put_object_tagging``,
    ``delete_object``. Each method records its kwargs into
    ``self.calls`` so assertions can inspect what the adapter passed.

    The ``responses`` and ``error_queue`` knobs let individual tests
    seed canned responses (or canned exceptions) for specific
    operations.
    """

    def __init__(
        self,
        *,
        list_pages: Optional[List[Dict[str, Any]]] = None,
        objects: Optional[Dict[str, bytes]] = None,
        head_responses: Optional[Dict[str, Dict[str, Any]]] = None,
        tag_sets: Optional[Dict[str, List[Dict[str, str]]]] = None,
    ) -> None:
        self._paginator = _Paginator(list_pages or [])
        self.objects: Dict[str, bytes] = dict(objects or {})
        self.head_responses: Dict[str, Dict[str, Any]] = dict(head_responses or {})
        self.tag_sets: Dict[str, List[Dict[str, str]]] = {
            k: list(v) for k, v in (tag_sets or {}).items()
        }

        # Per-method exception queues. Each next() pops one entry; if
        # the popped entry is an exception type/instance, it is raised
        # before the normal return path. None entries are skipped.
        self.error_queue: Dict[str, List[Any]] = {}

        # Audit log: every call appends ``(method, kwargs)``.
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def _maybe_raise(self, op: str) -> None:
        queue = self.error_queue.get(op)
        if not queue:
            return
        exc = queue.pop(0)
        if exc is None:
            return
        if isinstance(exc, type):
            raise exc()
        raise exc

    def get_paginator(self, op_name: str) -> _Paginator:
        self.calls.append(("get_paginator", {"op_name": op_name}))
        assert op_name == "list_objects_v2"
        return self._paginator

    def get_object(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("get_object", dict(kwargs)))
        self._maybe_raise("get_object")
        key = kwargs["Key"]
        if key not in self.objects:
            raise FileNotFoundError(key)
        return {"Body": _StreamingBody(self.objects[key])}

    def put_object(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("put_object", dict(kwargs)))
        self._maybe_raise("put_object")
        body = kwargs["Body"]
        if isinstance(body, (bytes, bytearray)):
            self.objects[kwargs["Key"]] = bytes(body)
        else:
            # Should not happen in the small-write path, but be defensive.
            self.objects[kwargs["Key"]] = body.read()
        return {}

    def upload_fileobj(
        self,
        fileobj: Any,
        Bucket: str,
        Key: str,
        ExtraArgs: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.calls.append(
            (
                "upload_fileobj",
                {
                    "fileobj_type": type(fileobj).__name__,
                    "Bucket": Bucket,
                    "Key": Key,
                    "ExtraArgs": dict(ExtraArgs or {}),
                },
            )
        )
        self._maybe_raise("upload_fileobj")
        # Drain the source so the chunk-streaming code is exercised.
        out = io.BytesIO()
        while True:
            chunk = fileobj.read(64 * 1024)
            if not chunk:
                break
            out.write(chunk)
        self.objects[Key] = out.getvalue()

    def head_object(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("head_object", dict(kwargs)))
        self._maybe_raise("head_object")
        key = kwargs["Key"]
        if key in self.head_responses:
            return self.head_responses[key]
        # Fallback: synthesize a HEAD response from stored bytes.
        if key not in self.objects:
            raise FileNotFoundError(key)
        return {
            "ContentLength": len(self.objects[key]),
            "LastModified": dt.datetime(
                2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc
            ),
            "ContentType": "application/octet-stream",
            "ETag": '"' + ("a" * 32) + '"',
            "Metadata": {},
        }

    def get_object_tagging(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("get_object_tagging", dict(kwargs)))
        self._maybe_raise("get_object_tagging")
        key = kwargs["Key"]
        return {"TagSet": list(self.tag_sets.get(key, []))}

    def put_object_tagging(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("put_object_tagging", dict(kwargs)))
        self._maybe_raise("put_object_tagging")
        key = kwargs["Key"]
        tagging = kwargs["Tagging"]
        self.tag_sets[key] = [dict(t) for t in tagging["TagSet"]]
        return {}

    def delete_object(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("delete_object", dict(kwargs)))
        self._maybe_raise("delete_object")
        key = kwargs["Key"]
        self.objects.pop(key, None)
        self.tag_sets.pop(key, None)
        self.head_responses.pop(key, None)
        return {}


# A test-friendly RetriesConfig: tight backoff so retry-driven tests
# finish in a few milliseconds rather than seconds.
_FAST_RETRIES = RetriesConfig(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=1000,
    request_timeout_ms=1000,
)


def _make_adapter(
    client: FakeS3Client,
    *,
    bucket: str = "test-bucket",
    prefix: Optional[str] = None,
    retries_config: Optional[RetriesConfig] = None,
) -> S3SourceAdapter:
    return S3SourceAdapter(
        name="s3-test",
        bucket=bucket,
        prefix=prefix,
        s3_client=client,
        retries_config=retries_config or _FAST_RETRIES,
    )


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


def test_list_objects_yields_meta_with_quotes_stripped_and_singlepart_provider_hash() -> None:
    pages = [
        {
            "Contents": [
                {
                    "Key": "photos/a.jpg",
                    "Size": 100,
                    "LastModified": dt.datetime(
                        2024, 5, 1, 12, 0, 0, tzinfo=dt.timezone.utc
                    ),
                    "ETag": '"' + ("d" * 32) + '"',
                },
                {
                    "Key": "photos/b.jpg",
                    "Size": 2000,
                    "LastModified": dt.datetime(
                        2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc
                    ),
                    # Multi-part etag (-N suffix): provider_hash should be None.
                    "ETag": '"' + ("e" * 32) + '-2"',
                },
            ]
        },
        {
            "Contents": [
                {
                    "Key": "photos/c.jpg",
                    "Size": 7,
                    "LastModified": dt.datetime(
                        2024, 5, 1, 12, 0, 2, tzinfo=dt.timezone.utc
                    ),
                    "ETag": '"' + ("f" * 32) + '"',
                }
            ]
        },
    ]
    client = FakeS3Client(list_pages=pages)
    adapter = _make_adapter(client, prefix="photos/")

    metas = list(adapter.list_objects())

    assert [m.key for m in metas] == ["photos/a.jpg", "photos/b.jpg", "photos/c.jpg"]
    assert [m.size_bytes for m in metas] == [100, 2000, 7]
    assert metas[0].last_modified == "2024-05-01T12:00:00Z"
    assert metas[0].etag == "d" * 32
    assert metas[0].provider_hash == "d" * 32
    # Multipart ETag → provider_hash None.
    assert metas[1].etag == ("e" * 32) + "-2"
    assert metas[1].provider_hash is None
    assert metas[2].provider_hash == "f" * 32

    # Paginator was driven with the prefix.
    paginate_kwargs = client._paginator.paginate_calls[0]
    assert paginate_kwargs == {"Bucket": "test-bucket", "Prefix": "photos/"}


def test_list_objects_without_prefix_omits_prefix_kwarg() -> None:
    client = FakeS3Client(list_pages=[{"Contents": []}])
    adapter = _make_adapter(client, prefix=None)
    list(adapter.list_objects())
    assert client._paginator.paginate_calls[0] == {"Bucket": "test-bucket"}


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------


def test_read_bytes_streams_full_payload() -> None:
    payload = b"hello-world-" * 100
    client = FakeS3Client(objects={"a": payload})
    adapter = _make_adapter(client)

    streamed = b"".join(adapter.read_bytes("a"))

    assert streamed == payload
    # Confirm the get_object call was issued with the right kwargs.
    get_calls = [c for c in client.calls if c[0] == "get_object"]
    assert get_calls == [("get_object", {"Bucket": "test-bucket", "Key": "a"})]


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------


def test_write_bytes_small_uses_put_object_with_metadata() -> None:
    client = FakeS3Client()
    adapter = _make_adapter(client)
    payload = b"x" * 1024
    metadata = {"mcps-source": "s3-prod", "mcps-content-sha256": "f" * 64}

    adapter.write_bytes(
        "k.bin",
        iter([payload]),
        size_bytes=len(payload),
        content_type="image/jpeg",
        user_metadata=metadata,
    )

    # Exactly one put_object call, no upload_fileobj call.
    put_calls = [c for c in client.calls if c[0] == "put_object"]
    upload_calls = [c for c in client.calls if c[0] == "upload_fileobj"]
    assert len(put_calls) == 1
    assert len(upload_calls) == 0
    kwargs = put_calls[0][1]
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"] == "k.bin"
    assert kwargs["Body"] == payload
    assert kwargs["ContentType"] == "image/jpeg"
    assert kwargs["Metadata"] == metadata


def test_write_bytes_at_threshold_still_uses_put_object() -> None:
    """≤ 5 MiB is the inclusive upper bound for the put_object path."""
    client = FakeS3Client()
    adapter = _make_adapter(client)
    payload = b"y" * MULTIPART_THRESHOLD_BYTES  # exactly 5 MiB
    adapter.write_bytes(
        "boundary",
        iter([payload]),
        size_bytes=len(payload),
        content_type=None,
        user_metadata={},
    )
    assert any(c[0] == "put_object" for c in client.calls)
    assert not any(c[0] == "upload_fileobj" for c in client.calls)


def test_write_bytes_large_uses_upload_fileobj_with_extra_args() -> None:
    client = FakeS3Client()
    adapter = _make_adapter(client)
    # > 5 MiB triggers multipart.
    chunk_a = b"a" * (4 * 1024 * 1024)
    chunk_b = b"b" * (2 * 1024 * 1024)  # total 6 MiB
    metadata = {"mcps-source": "s3-prod", "mcps-content-sha256": "f" * 64}

    adapter.write_bytes(
        "big.bin",
        iter([chunk_a, chunk_b]),
        size_bytes=len(chunk_a) + len(chunk_b),
        content_type="application/octet-stream",
        user_metadata=metadata,
    )

    upload_calls = [c for c in client.calls if c[0] == "upload_fileobj"]
    put_calls = [c for c in client.calls if c[0] == "put_object"]
    assert len(upload_calls) == 1
    assert len(put_calls) == 0
    kwargs = upload_calls[0][1]
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"] == "big.bin"
    assert kwargs["ExtraArgs"]["ContentType"] == "application/octet-stream"
    assert kwargs["ExtraArgs"]["Metadata"] == metadata
    # Body was actually streamed through to the fake.
    assert client.objects["big.bin"] == chunk_a + chunk_b


# ---------------------------------------------------------------------------
# get_metadata
# ---------------------------------------------------------------------------


def test_get_metadata_merges_head_metadata_and_tags() -> None:
    client = FakeS3Client(
        head_responses={
            "photos/img.jpg": {
                "ContentLength": 1234,
                "LastModified": dt.datetime(
                    2024, 6, 1, 9, 0, 0, tzinfo=dt.timezone.utc
                ),
                "ContentType": "image/jpeg",
                "ETag": '"' + ("a" * 32) + '"',
                "Metadata": {
                    "mcps-source": "s3-prod",
                    "mcps-content-sha256": "f" * 64,
                },
            }
        },
        tag_sets={
            "photos/img.jpg": [
                {"Key": "mcps-quarantined-at", "Value": "2024-06-15T00:00:00Z"},
            ]
        },
    )
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("photos/img.jpg")

    assert isinstance(meta, ObjectMeta)
    assert meta.key == "photos/img.jpg"
    assert meta.size_bytes == 1234
    assert meta.last_modified == "2024-06-01T09:00:00Z"
    assert meta.content_type == "image/jpeg"
    assert meta.etag == "a" * 32
    # User-metadata + tag value both surfaced.
    assert meta.user_metadata["mcps-source"] == "s3-prod"
    assert meta.user_metadata["mcps-content-sha256"] == "f" * 64
    assert meta.user_metadata["mcps-quarantined-at"] == "2024-06-15T00:00:00Z"
    # Both calls were made.
    ops = [c[0] for c in client.calls]
    assert "head_object" in ops
    assert "get_object_tagging" in ops


# ---------------------------------------------------------------------------
# set_tag
# ---------------------------------------------------------------------------


def test_set_tag_preserves_existing_tags_and_replaces_collisions() -> None:
    client = FakeS3Client(
        tag_sets={
            "k": [
                {"Key": "owner", "Value": "lvq"},
                {"Key": "mcps-quarantined-at", "Value": "old"},
            ]
        }
    )
    adapter = _make_adapter(client)

    adapter.set_tag("k", "mcps-quarantined-at", "2024-07-01T12:00:00Z")

    new_set = client.tag_sets["k"]
    # owner preserved unchanged.
    assert {"Key": "owner", "Value": "lvq"} in new_set
    # mcps-quarantined-at replaced (one entry, new value).
    quarantine_entries = [t for t in new_set if t["Key"] == "mcps-quarantined-at"]
    assert quarantine_entries == [
        {"Key": "mcps-quarantined-at", "Value": "2024-07-01T12:00:00Z"}
    ]


def test_set_tag_adds_new_entry_when_absent() -> None:
    client = FakeS3Client(tag_sets={"k": [{"Key": "owner", "Value": "lvq"}]})
    adapter = _make_adapter(client)

    adapter.set_tag("k", "mcps-tombstoned-at", "2024-07-02T00:00:00Z")

    new_set = client.tag_sets["k"]
    assert {"Key": "owner", "Value": "lvq"} in new_set
    assert {"Key": "mcps-tombstoned-at", "Value": "2024-07-02T00:00:00Z"} in new_set


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_calls_delete_object() -> None:
    client = FakeS3Client(objects={"k": b"data"})
    adapter = _make_adapter(client)

    adapter.delete("k")

    delete_calls = [c for c in client.calls if c[0] == "delete_object"]
    assert delete_calls == [("delete_object", {"Bucket": "test-bucket", "Key": "k"})]
    assert "k" not in client.objects


# ---------------------------------------------------------------------------
# Retry / error mapping
# ---------------------------------------------------------------------------


def _make_client_error(status: int) -> Exception:
    """Construct a botocore.ClientError with a specific HTTPStatusCode."""
    from botocore.exceptions import ClientError  # type: ignore[import-not-found]

    return ClientError(
        error_response={
            "Error": {"Code": "Boom", "Message": "boom"},
            "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": {}},
        },
        operation_name="HeadObject",
    )


def test_client_error_503_is_transient_and_eventually_succeeds() -> None:
    client = FakeS3Client(
        head_responses={
            "k": {
                "ContentLength": 1,
                "LastModified": dt.datetime(
                    2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc
                ),
                "ContentType": "application/octet-stream",
                "ETag": '"' + ("a" * 32) + '"',
                "Metadata": {},
            }
        },
        tag_sets={"k": []},
    )
    # First HEAD raises 503; second HEAD succeeds.
    client.error_queue["head_object"] = [_make_client_error(503), None]
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("k")

    assert meta.size_bytes == 1
    head_calls = [c for c in client.calls if c[0] == "head_object"]
    # One retry → two head_object invocations total.
    assert len(head_calls) == 2


def test_client_error_503_exhausts_after_max_retries() -> None:
    client = FakeS3Client(tag_sets={"k": []})
    client.error_queue["head_object"] = [
        _make_client_error(503) for _ in range(10)
    ]
    adapter = _make_adapter(client)

    with pytest.raises(RetriesExhausted):
        adapter.get_metadata("k")


def test_client_error_403_is_non_transient_and_not_retried() -> None:
    client = FakeS3Client(tag_sets={"k": []})
    client.error_queue["head_object"] = [_make_client_error(403)]
    adapter = _make_adapter(client)

    with pytest.raises(NonTransientError) as exc_info:
        adapter.get_metadata("k")

    assert exc_info.value.status == 403
    # Exactly one attempt — no retry on non_transient.
    head_calls = [c for c in client.calls if c[0] == "head_object"]
    assert len(head_calls) == 1


def test_connect_timeout_is_transient_and_eventually_succeeds() -> None:
    from botocore.exceptions import (  # type: ignore[import-not-found]
        ConnectTimeoutError,
    )

    client = FakeS3Client(
        head_responses={
            "k": {
                "ContentLength": 0,
                "LastModified": dt.datetime(
                    2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc
                ),
                "ContentType": "application/octet-stream",
                "ETag": '"' + ("a" * 32) + '"',
                "Metadata": {},
            }
        },
        tag_sets={"k": []},
    )
    client.error_queue["head_object"] = [
        ConnectTimeoutError(endpoint_url="https://s3.amazonaws.com"),
        None,
    ]
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("k")

    assert meta.size_bytes == 0
    head_calls = [c for c in client.calls if c[0] == "head_object"]
    assert len(head_calls) == 2


def test_retry_after_header_is_honored_on_429() -> None:
    """A 429 with a Retry-After header is parsed and surfaces to the decorator."""
    from botocore.exceptions import ClientError  # type: ignore[import-not-found]

    client = FakeS3Client(
        head_responses={
            "k": {
                "ContentLength": 0,
                "LastModified": dt.datetime(
                    2024, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc
                ),
                "ContentType": "application/octet-stream",
                "ETag": '"' + ("a" * 32) + '"',
                "Metadata": {},
            }
        },
        tag_sets={"k": []},
    )
    client.error_queue["head_object"] = [
        ClientError(
            error_response={
                "Error": {"Code": "Throttle", "Message": "slow down"},
                "ResponseMetadata": {
                    "HTTPStatusCode": 429,
                    "HTTPHeaders": {"retry-after": "0"},
                },
            },
            operation_name="HeadObject",
        ),
        None,
    ]
    adapter = _make_adapter(client)

    meta = adapter.get_metadata("k")

    assert meta is not None
    head_calls = [c for c in client.calls if c[0] == "head_object"]
    assert len(head_calls) == 2
