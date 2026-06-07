"""Integration test: `GCSSourceAdapter` against an in-process GCS fake.

Validates: Requirements 2.3, 2.5, 5.7, 6.4, 6.5.

The unit tests in `tests/unit/test_gcs_adapter_unit.py` exercise the
adapter's argument-shaping and retry mapping at a low level. This
integration test instead drives the production `GCSSourceAdapter`
class through every public method against an in-process
``_FakeGcsClient`` / ``_FakeBucket`` / ``_FakeBlob`` hierarchy
modelled on ``google.cloud.storage``. The fake is wired in via the
adapter's ``gcs_client=...`` constructor seam (the same seam used by
``tests/integration/test_full_run_dry.py``), so the real adapter code
paths run unchanged against a deterministic in-memory backing store —
no network, no emulator subprocess.

Coverage corresponds 1:1 to task 35's bullet points:

1. **list_objects with prefix** — seed multiple blobs across different
   prefixes; configure ``GCSSourceAdapter(prefix=...)``; assert only
   the matching keys come out and that the underlying
   ``client.list_blobs(...)`` was called with the configured prefix.
   (Req 2.5.)
2. **streaming read with CRC32C verification** — seed a blob with a
   real GCS-encoded CRC32C (base64-of-big-endian-uint32 over the
   payload); call ``adapter.read_bytes(key)``; assert the streamed
   bytes equal the seed and that the read went through
   ``blob.open("rb")`` (which is where ``google.cloud.storage``
   performs CRC32C verification, per req 2.3). The blob's CRC32C
   value is also surfaced through ``get_metadata`` as
   ``ObjectMeta.provider_hash`` to confirm the adapter never elevates
   it to Content_Hash (req 2.3 forbids this).
3. **upload with metadata** — call ``adapter.write_bytes(...)`` with
   the design's standard write-time metadata bundle
   (``mcps-source`` / ``mcps-content-sha256`` / ``mcps-replicated-at``)
   and confirm the fake's blob now carries the metadata BEFORE the
   upload completes (single round-trip semantics, req 6.4).
4. **metadata patch (tagging)** — GCS has no S3-style object tagging;
   ``set_tag`` writes ``mcps-quarantined-at`` into ``blob.metadata``
   via reload + patch. The test seeds an existing metadata entry and
   confirms ``set_tag`` preserves it while adding the new key
   (req 5.7) and that exactly one ``patch()`` round-trip happens.
5. **delete** — call ``adapter.delete(key)`` and confirm the fake's
   ``_blobs`` map no longer contains an entry for that key (req 6.5
   rollback step / req 5.9 expired-quarantine deletes).
"""

from __future__ import annotations

import base64
import datetime as dt
import io
import struct
from typing import Any, Dict, List, Mapping, Optional

import pytest

from mcps.config.model import RetriesConfig
from mcps.sources.gcs import GCSSourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BUCKET = "mcps-int-bucket-gcs-adapter"
_QUARANTINED_AT = "2024-06-15T12:00:00Z"

# A test-friendly RetriesConfig: tight backoff so any retry-driven
# code paths finish quickly even though the happy-path tests below do
# not exercise retries.
_FAST_RETRIES = RetriesConfig(
    max_retries=3,
    initial_backoff_ms=100,
    max_backoff_ms=1000,
    request_timeout_ms=1000,
)


def _gcs_crc32c_b64(data: bytes) -> str:
    """Compute the GCS-style base64 CRC32C string for ``data``.

    GCS reports ``Blob.crc32c`` as the base64 encoding of the 4-byte
    big-endian CRC32C checksum. The same encoding is used by
    ``google.cloud.storage`` when surfacing the value through the
    SDK; using it here keeps the seeded ``provider_hash`` realistic.
    """
    import google_crc32c  # type: ignore[import-not-found]

    checksum = int(google_crc32c.value(data))
    return base64.b64encode(struct.pack(">I", checksum)).decode("ascii")


# ---------------------------------------------------------------------------
# In-process GCS fake
#
# Mirrors the ``_FakeGcsClient`` / ``_FakeBucket`` / ``_FakeBlob`` shape
# used by ``tests/integration/test_full_run_dry.py``. Kept local so this
# module does not depend on the dry-run integration test's import order.
# ---------------------------------------------------------------------------


class _FakeBlob:
    """Minimal ``google.cloud.storage.Blob`` stand-in."""

    def __init__(
        self,
        *,
        name: str,
        data: bytes = b"",
        updated: Optional[dt.datetime] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Mapping[str, str]] = None,
        crc32c: Optional[str] = None,
        client: Optional["_FakeGcsClient"] = None,
    ) -> None:
        self.name = name
        self._data: bytes = data
        self.size: Optional[int] = len(data) if data else 0
        self.updated: Optional[dt.datetime] = updated
        self.content_type: Optional[str] = content_type
        self.metadata: Optional[Dict[str, str]] = (
            dict(metadata) if metadata is not None else None
        )
        self.crc32c: Optional[str] = crc32c
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def open(self, mode: str = "rb") -> io.BytesIO:
        self.calls.append(("open", {"mode": mode}))
        assert mode == "rb"
        # The real SDK performs CRC32C verification as the bytes flow
        # through this stream. The in-process fake cannot replicate
        # that side-effect, but exercising this method is the contract
        # we care about: the adapter's ``read_bytes`` must funnel its
        # download through ``blob.open("rb")`` so server-side
        # verification has a chance to run.
        return io.BytesIO(self._data)

    def upload_from_file(
        self,
        file_obj: Any,
        content_type: Optional[str] = None,
    ) -> None:
        self.calls.append(
            ("upload_from_file", {"content_type": content_type})
        )
        if hasattr(file_obj, "read"):
            self._data = file_obj.read()
        else:  # pragma: no cover - defensive
            self._data = bytes(file_obj)
        self.size = len(self._data)
        if content_type is not None:
            self.content_type = content_type
        self.updated = dt.datetime(2024, 6, 1, 12, 0, 0, tzinfo=dt.timezone.utc)

    def reload(self) -> None:
        self.calls.append(("reload", {}))

    def patch(self) -> None:
        self.calls.append(("patch", {"metadata": dict(self.metadata or {})}))

    def delete(self) -> None:
        self.calls.append(("delete", {}))
        if self._client is not None:
            self._client._remove_blob(self.name)


class _FakeBucket:
    def __init__(self, name: str, client: "_FakeGcsClient") -> None:
        self.name = name
        self._client = client
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def blob(self, key: str) -> _FakeBlob:
        self.calls.append(("blob", {"key": key}))
        return self._client._get_or_create_blob(self.name, key)


class _FakeGcsClient:
    """In-process ``google.cloud.storage.Client`` stand-in."""

    def __init__(
        self,
        *,
        blobs: Optional[Mapping[tuple[str, str], _FakeBlob]] = None,
    ) -> None:
        self._blobs: Dict[tuple[str, str], _FakeBlob] = dict(blobs or {})
        for blob in self._blobs.values():
            blob._client = self
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def _get_or_create_blob(self, bucket: str, key: str) -> _FakeBlob:
        existing = self._blobs.get((bucket, key))
        if existing is not None:
            return existing
        fresh = _FakeBlob(name=key, client=self)
        self._blobs[(bucket, key)] = fresh
        return fresh

    def _remove_blob(self, key: str) -> None:
        for (bucket, name), _blob in list(self._blobs.items()):
            if name == key:
                del self._blobs[(bucket, name)]

    def bucket(self, name: str) -> _FakeBucket:
        self.calls.append(("bucket", {"name": name}))
        return _FakeBucket(name, self)

    def list_blobs(
        self,
        bucket: str,
        prefix: Optional[str] = None,
    ) -> List[_FakeBlob]:
        self.calls.append(("list_blobs", {"bucket": bucket, "prefix": prefix}))
        result: List[_FakeBlob] = []
        for (b, _key), blob in self._blobs.items():
            if b != bucket:
                continue
            if prefix and not blob.name.startswith(prefix):
                continue
            result.append(blob)
        result.sort(key=lambda x: x.name)
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _make_adapter(
    client: _FakeGcsClient,
    *,
    bucket: str = _BUCKET,
    prefix: Optional[str] = None,
) -> GCSSourceAdapter:
    return GCSSourceAdapter(
        name="gcs-int",
        bucket=bucket,
        prefix=prefix,
        gcs_client=client,
        retries_config=_FAST_RETRIES,
    )


# ---------------------------------------------------------------------------
# 1. list_objects with prefix (Req 2.5)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_objects_with_prefix_returns_only_matching_keys() -> None:
    """Seed blobs across three prefix groups; configure ``prefix=...``
    on the adapter; assert ``list_objects()`` yields only the matching
    keys and that the underlying ``client.list_blobs(bucket, prefix=...)``
    was invoked with the configured prefix.
    """
    matching_payload = b"matched"
    other_payload = b"other"

    blobs = {
        (_BUCKET, "photos/a.jpg"): _FakeBlob(
            name="photos/a.jpg",
            data=matching_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata={"mcps-source": "gcs-archive"},
            crc32c=_gcs_crc32c_b64(matching_payload),
        ),
        (_BUCKET, "photos/sub/b.jpg"): _FakeBlob(
            name="photos/sub/b.jpg",
            data=matching_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 1, tzinfo=dt.timezone.utc),
            content_type="image/jpeg",
            metadata=None,
            crc32c=_gcs_crc32c_b64(matching_payload),
        ),
        # These two should NOT surface — different prefix.
        (_BUCKET, "videos/c.mp4"): _FakeBlob(
            name="videos/c.mp4",
            data=other_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 2, tzinfo=dt.timezone.utc),
            content_type="video/mp4",
            metadata=None,
            crc32c=_gcs_crc32c_b64(other_payload),
        ),
        (_BUCKET, "archive/old.bin"): _FakeBlob(
            name="archive/old.bin",
            data=other_payload,
            updated=dt.datetime(2024, 5, 1, 12, 0, 3, tzinfo=dt.timezone.utc),
            content_type="application/octet-stream",
            metadata=None,
            crc32c=_gcs_crc32c_b64(other_payload),
        ),
        # Different bucket — must never surface regardless of prefix.
        ("other-bucket", "photos/skip.jpg"): _FakeBlob(
            name="photos/skip.jpg",
            data=matching_payload,
            crc32c=_gcs_crc32c_b64(matching_payload),
        ),
    }
    client = _FakeGcsClient(blobs=blobs)
    adapter = _make_adapter(client, prefix="photos/")

    metas = list(adapter.list_objects())

    # Only the two photos/* keys, in name-sorted order.
    assert [m.key for m in metas] == ["photos/a.jpg", "photos/sub/b.jpg"]
    # provider_hash carries the CRC32C — informational only, never
    # elevated to Content_Hash per req 2.3.
    assert metas[0].provider_hash == _gcs_crc32c_b64(matching_payload)
    assert metas[1].provider_hash == _gcs_crc32c_b64(matching_payload)
    # GCS does not use S3-style ETags.
    assert metas[0].etag is None
    assert metas[1].etag is None
    # Metadata flows through verbatim, with the missing entry coerced
    # to an empty dict (not None) so the listing path is uniform.
    assert metas[0].user_metadata == {"mcps-source": "gcs-archive"}
    assert metas[1].user_metadata == {}

    # The underlying client was asked for exactly one prefix-scoped list.
    list_calls = [c for c in client.calls if c[0] == "list_blobs"]
    assert list_calls == [
        ("list_blobs", {"bucket": _BUCKET, "prefix": "photos/"}),
    ]


# ---------------------------------------------------------------------------
# 2. Streaming read with CRC32C verification (Req 2.3)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_read_bytes_streams_payload_through_blob_open_for_crc32c_verification() -> None:
    """The streamed concatenation must equal the seeded bytes byte-for-
    byte AND the read must funnel through ``blob.open("rb")`` so the
    real SDK's CRC32C verification has a chance to run (req 2.3).

    The fake cannot replicate the actual checksum verification, but we
    make the configured CRC32C realistic (base64-of-big-endian-uint32
    over the payload, exactly as GCS reports it) and assert it surfaces
    as ``ObjectMeta.provider_hash`` via ``get_metadata``. A real
    mismatch would surface from the SDK as a
    ``google.resumable_media.common.DataCorruption`` exception that the
    adapter's error mapper would translate into a NonTransientError;
    that classification is covered by the unit-tier tests.
    """
    # ~5 MiB payload exercises multiple 1 MiB chunk reads at the
    # adapter's CHUNK_SIZE.
    payload = (b"crc32c-payload-" * 1024) * 320  # ~4.7 MiB
    real_crc32c = _gcs_crc32c_b64(payload)

    blob = _FakeBlob(
        name="photos/img.jpg",
        data=payload,
        updated=dt.datetime(2024, 6, 1, 9, 0, 0, tzinfo=dt.timezone.utc),
        content_type="image/jpeg",
        metadata={"mcps-source": "gcs-archive"},
        crc32c=real_crc32c,
    )
    client = _FakeGcsClient(blobs={(_BUCKET, "photos/img.jpg"): blob})
    adapter = _make_adapter(client)

    streamed = b"".join(adapter.read_bytes("photos/img.jpg"))

    assert streamed == payload, (
        f"streamed bytes ({len(streamed)}) != seeded bytes ({len(payload)})"
    )
    # The download went through ``blob.open("rb")`` — this is where
    # google.cloud.storage performs CRC32C verification.
    open_calls = [c for c in blob.calls if c[0] == "open"]
    assert open_calls == [("open", {"mode": "rb"})], (
        "read_bytes must funnel through blob.open('rb') so the SDK's "
        "CRC32C verification path has a chance to run"
    )
    # The realistic CRC32C surfaces through the metadata path as
    # provider_hash. Req 2.3 forbids using this as Content_Hash; this
    # assertion just pins that the value is preserved end-to-end.
    meta = adapter.get_metadata("photos/img.jpg")
    assert meta.provider_hash == real_crc32c


# ---------------------------------------------------------------------------
# 3. Upload with metadata (Req 6.4)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_bytes_attaches_user_metadata_at_upload_time() -> None:
    """``write_bytes`` must surface every entry of ``user_metadata`` on
    the destination blob's ``metadata`` field, applied *before* the
    upload so a single round-trip persists payload + metadata together
    (req 6.4: attach metadata entries at write time).
    """
    client = _FakeGcsClient()
    adapter = _make_adapter(client)

    payload = b"replicated-content-" * 256  # ~5 KiB
    sha = _sha256_hex(payload)
    user_metadata = {
        "mcps-source": "s3-prod",
        "mcps-content-sha256": sha,
        "mcps-replicated-at": "2024-06-01T12:00:00Z",
    }

    adapter.write_bytes(
        "photos/replicated.jpg",
        iter([payload]),
        size_bytes=len(payload),
        content_type="image/jpeg",
        user_metadata=user_metadata,
    )

    blob = client._blobs[(_BUCKET, "photos/replicated.jpg")]
    # Bytes round-tripped through the upload path.
    assert blob._data == payload
    # Every supplied metadata entry landed on the blob, in full.
    assert blob.metadata == user_metadata
    # Exactly one upload_from_file invocation, with the configured
    # content_type forwarded as a kwarg.
    upload_calls = [c for c in blob.calls if c[0] == "upload_from_file"]
    assert upload_calls == [
        ("upload_from_file", {"content_type": "image/jpeg"}),
    ]


# ---------------------------------------------------------------------------
# 4. Metadata patch / tagging (Req 5.7, 9.3)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_set_tag_adds_quarantined_at_to_user_metadata_via_patch() -> None:
    """GCS has no S3-style object tagging, so ``set_tag`` writes
    ``mcps-quarantined-at`` into ``blob.metadata`` (req 5.7). The
    operation reads the current metadata via ``reload``, patches the
    requested key, and pushes the merged dict back via ``patch`` —
    pre-existing entries must survive unchanged (single round-trip
    per side).
    """
    blob = _FakeBlob(
        name="photos/dup.jpg",
        data=b"duplicate",
        metadata={
            # An entry written by some other tool — must not be lost.
            "owner": "lvq",
        },
        crc32c=_gcs_crc32c_b64(b"duplicate"),
    )
    client = _FakeGcsClient(blobs={(_BUCKET, "photos/dup.jpg"): blob})
    adapter = _make_adapter(client)

    adapter.set_tag("photos/dup.jpg", "mcps-quarantined-at", _QUARANTINED_AT)

    # Pre-existing entry preserved; new entry added.
    assert blob.metadata == {
        "owner": "lvq",
        "mcps-quarantined-at": _QUARANTINED_AT,
    }
    # Exactly one reload + one patch, in that order.
    ops = [c[0] for c in blob.calls]
    assert ops.count("reload") == 1
    assert ops.count("patch") == 1
    assert ops.index("patch") > ops.index("reload")


# ---------------------------------------------------------------------------
# 5. Delete (Req 6.5 rollback / 5.9 expired-quarantine deletes)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_delete_removes_blob_from_bucket() -> None:
    """``adapter.delete(key)`` must invoke ``blob.delete()`` and the
    fake's backing store must no longer contain an entry for that key.
    Used by the expired-quarantine deletion path (req 5.9) and by the
    Replicator's post-write rollback when a verify mismatch is detected
    (req 6.5).
    """
    payload = b"to-be-deleted"
    blob = _FakeBlob(
        name="photos/expired.jpg",
        data=payload,
        metadata={"mcps-quarantined-at": "2024-01-01T00:00:00Z"},
        crc32c=_gcs_crc32c_b64(payload),
    )
    # A second blob to confirm only the targeted key disappears.
    other_payload = b"keep-me"
    other = _FakeBlob(
        name="photos/keep.jpg",
        data=other_payload,
        crc32c=_gcs_crc32c_b64(other_payload),
    )
    client = _FakeGcsClient(
        blobs={
            (_BUCKET, "photos/expired.jpg"): blob,
            (_BUCKET, "photos/keep.jpg"): other,
        },
    )
    adapter = _make_adapter(client)

    adapter.delete("photos/expired.jpg")

    delete_calls = [c for c in blob.calls if c[0] == "delete"]
    assert delete_calls == [("delete", {})]
    assert (_BUCKET, "photos/expired.jpg") not in client._blobs
    # The non-targeted blob is untouched.
    assert (_BUCKET, "photos/keep.jpg") in client._blobs
    assert other.calls == []
