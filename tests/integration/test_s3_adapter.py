"""Integration test: `S3SourceAdapter` against a `moto`-backed S3 backend.

Validates: Requirements 2.2, 2.5, 5.7, 6.4, 6.5.

The unit tests in `tests/unit/test_s3_adapter_unit.py` inject a hand-rolled
`FakeS3Client` to exercise the adapter's argument-shaping and retry logic.
This integration test instead drives the real adapter against `moto`'s
in-process S3 stand-in to exercise the boto3 paginator, the
``StreamingBody`` returned by ``get_object``, and the round-trip between
``Metadata=`` user-metadata and ``put_object_tagging`` tags surfaced via
``head_object`` / ``get_object_tagging``.

Coverage corresponds 1:1 to task 34's bullet points:

1. **List with continuation tokens** — seed 1500 Objects (the S3
   paginator default page size is 1000, so this forces at least one
   continuation token) and assert ``list_objects()`` returns every key
   exactly once. (Req 2.5.)
2. **Streaming read** — write a sizable Object via the moto client and
   confirm the bytes streamed via `read_bytes` reassemble to the seed.
   (Req 2.2: streaming SHA-256 path.)
3. **Write with Metadata** — call ``write_bytes`` with the design's two
   write-time markers (``mcps-content-sha256``, ``mcps-source``) and
   verify ``s3_client.head_object(...)`` reflects them. (Req 6.4.)
4. **set_tag for `mcps-quarantined-at`** — call
   ``set_tag(key, "mcps-quarantined-at", <iso>)`` and verify
   ``s3_client.get_object_tagging(...)``. (Req 5.7.)
5. **delete** — call ``delete(key)`` and assert ``head_object`` raises
   404 for the missing key. (Req 6.5: rollback step.)
6. **head_object round-trip** — after a ``write_bytes(...)`` with
   metadata + ``set_tag(...)``, call ``get_metadata(key)`` on the
   adapter and assert the merged ``ObjectMeta.user_metadata`` includes
   both the metadata key and the tag (req 6.5: post-write verify path).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

# Skip the whole module when moto is unavailable so optional-dep
# environments still collect the rest of the suite.
pytest.importorskip("moto")

import boto3  # type: ignore[import-not-found]  # noqa: E402
from botocore.exceptions import ClientError  # type: ignore[import-not-found]  # noqa: E402
from moto import mock_aws  # type: ignore[import-not-found]  # noqa: E402

from mcps.sources.s3 import S3SourceAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixed run-time values so assertions are stable
# ---------------------------------------------------------------------------

_AWS_REGION = "us-east-1"
_BUCKET = "mcps-int-bucket-s3-adapter"

# A SHA-256 of the seed byte string used for the metadata round-trip
# tests. Computed inline rather than imported so the test does not
# depend on `mcps.hashing` being correct.
_SEED_PAYLOAD = b"integration-payload-bytes-" * 64  # ~1.6 KiB
_QUARANTINED_AT = "2024-06-15T12:00:00Z"


def _sha256_hex(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def s3_environment():
    """Provide a moto-mocked S3 client + a created bucket + the adapter.

    The fixture yields a 3-tuple ``(s3_client, bucket, adapter)``. The
    bucket is created up front so individual tests do not have to
    re-create it.
    """
    with mock_aws():
        s3_client = boto3.client("s3", region_name=_AWS_REGION)
        s3_client.create_bucket(Bucket=_BUCKET)
        adapter = S3SourceAdapter(
            name="s3-int",
            bucket=_BUCKET,
            region=_AWS_REGION,
            s3_client=s3_client,
        )
        yield s3_client, _BUCKET, adapter


# ---------------------------------------------------------------------------
# 1. list_objects across continuation tokens (Req 2.5)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_list_objects_iterates_through_continuation_tokens(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """The paginator default page size is 1000; seeding >1000 keys forces
    at least one continuation token. Every seeded key must surface
    exactly once in the iterator (no drops, no duplicates).
    """
    s3_client, bucket, adapter = s3_environment

    # 1500 objects → 2 pages (1000 + 500). 1-byte bodies keep the seed
    # cheap; the listing path does not read the body.
    expected_keys = {f"page/key-{i:04d}.bin" for i in range(1500)}
    for key in expected_keys:
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"x")

    observed = [meta.key for meta in adapter.list_objects()]

    # Exactly once each, no duplicates, full coverage.
    assert len(observed) == len(expected_keys), (
        f"expected {len(expected_keys)} keys, got {len(observed)} "
        f"(duplicate or dropped pages?)"
    )
    assert set(observed) == expected_keys, (
        "list_objects did not surface every seeded key exactly once"
    )


# ---------------------------------------------------------------------------
# 2. Streaming read (Req 2.2)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_read_bytes_streams_full_payload(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """The streamed concatenation must equal the seeded bytes byte-for-byte."""
    s3_client, bucket, adapter = s3_environment

    # ~5 MiB payload exercises multiple iter_chunks rounds at the
    # adapter's 1 MiB chunk size.
    payload = (b"abcdefgh" * 1024) * 640  # 5 MiB
    key = "stream/payload.bin"
    s3_client.put_object(Bucket=bucket, Key=key, Body=payload)

    streamed = b"".join(adapter.read_bytes(key))

    assert streamed == payload, (
        f"streamed bytes ({len(streamed)}) != seeded bytes ({len(payload)})"
    )


# ---------------------------------------------------------------------------
# 3. Write with Metadata= (Req 6.4)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_bytes_persists_user_metadata(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """write_bytes must surface every entry of `user_metadata` as S3
    user-metadata so the listing path can recover it via head_object.
    """
    s3_client, bucket, adapter = s3_environment

    key = "write/with-metadata.bin"
    sha = _sha256_hex(_SEED_PAYLOAD)
    user_metadata = {
        "mcps-content-sha256": sha,
        "mcps-source": "s3-int",
    }

    adapter.write_bytes(
        key,
        iter([_SEED_PAYLOAD]),
        size_bytes=len(_SEED_PAYLOAD),
        content_type="image/jpeg",
        user_metadata=user_metadata,
    )

    head = s3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(_SEED_PAYLOAD)
    # boto3 lowercases user-metadata keys on the wire, but moto / boto3
    # round-trip them as-given so direct comparison works here.
    head_meta = head.get("Metadata", {}) or {}
    assert head_meta.get("mcps-content-sha256") == sha
    assert head_meta.get("mcps-source") == "s3-int"
    assert head.get("ContentType") == "image/jpeg"


# ---------------------------------------------------------------------------
# 4. set_tag for mcps-quarantined-at (Req 5.7)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_set_tag_writes_quarantined_at_tag(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """The `mcps-quarantined-at` tag must land on the object via
    ``put_object_tagging`` and be readable via ``get_object_tagging``.
    """
    s3_client, bucket, adapter = s3_environment

    key = "tag/target.bin"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"x")

    adapter.set_tag(key, "mcps-quarantined-at", _QUARANTINED_AT)

    tagging = s3_client.get_object_tagging(Bucket=bucket, Key=key)
    tag_set = tagging.get("TagSet", []) or []
    tag_map = {t["Key"]: t["Value"] for t in tag_set}
    assert tag_map.get("mcps-quarantined-at") == _QUARANTINED_AT


@pytest.mark.integration
def test_set_tag_preserves_pre_existing_tags(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """Adding a new tag must not strip tags previously written by some
    other tooling (S3's `put_object_tagging` replaces the entire set;
    the adapter must merge).
    """
    s3_client, bucket, adapter = s3_environment

    key = "tag/with-existing.bin"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"x")
    s3_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={"TagSet": [{"Key": "owner", "Value": "lvq"}]},
    )

    adapter.set_tag(key, "mcps-quarantined-at", _QUARANTINED_AT)

    tagging = s3_client.get_object_tagging(Bucket=bucket, Key=key)
    tag_map = {t["Key"]: t["Value"] for t in tagging.get("TagSet", []) or []}
    assert tag_map.get("owner") == "lvq"
    assert tag_map.get("mcps-quarantined-at") == _QUARANTINED_AT


# ---------------------------------------------------------------------------
# 5. delete (Req 6.5: rollback after a failed verify)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_delete_object_makes_head_raise_404(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """After ``adapter.delete(key)``, ``head_object`` on the same key must
    raise a 404 ClientError.
    """
    s3_client, bucket, adapter = s3_environment

    key = "delete/target.bin"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"x")
    # Sanity: HEAD succeeds before deletion.
    s3_client.head_object(Bucket=bucket, Key=key)

    adapter.delete(key)

    with pytest.raises(ClientError) as exc_info:
        s3_client.head_object(Bucket=bucket, Key=key)
    status = exc_info.value.response.get("ResponseMetadata", {}).get(
        "HTTPStatusCode"
    )
    assert status == 404, f"expected 404, got {status!r}"


# ---------------------------------------------------------------------------
# 6. get_metadata round-trip merges Metadata + Tags (Req 6.5)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_get_metadata_merges_metadata_and_tags_after_write(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """After ``write_bytes`` (with metadata) plus ``set_tag``, the
    adapter's ``get_metadata`` must return an ``ObjectMeta`` whose
    ``user_metadata`` mapping contains BOTH the user-metadata entries
    and the tag entries.
    """
    s3_client, bucket, adapter = s3_environment

    key = "merge/object.bin"
    sha = _sha256_hex(_SEED_PAYLOAD)
    write_metadata = {
        "mcps-content-sha256": sha,
        "mcps-source": "s3-int",
    }

    adapter.write_bytes(
        key,
        iter([_SEED_PAYLOAD]),
        size_bytes=len(_SEED_PAYLOAD),
        content_type="image/jpeg",
        user_metadata=write_metadata,
    )
    adapter.set_tag(key, "mcps-quarantined-at", _QUARANTINED_AT)

    meta = adapter.get_metadata(key)

    assert meta.key == key
    assert meta.size_bytes == len(_SEED_PAYLOAD)
    assert meta.content_type == "image/jpeg"
    # Both views surfaced into user_metadata:
    assert meta.user_metadata.get("mcps-content-sha256") == sha
    assert meta.user_metadata.get("mcps-source") == "s3-int"
    assert meta.user_metadata.get("mcps-quarantined-at") == _QUARANTINED_AT
    # last_modified is stamped to ISO-8601 UTC seconds with trailing Z.
    assert meta.last_modified.endswith("Z")
    assert "T" in meta.last_modified


@pytest.mark.integration
def test_get_metadata_raises_filenotfound_for_missing_key(
    s3_environment: tuple[Any, str, S3SourceAdapter],
) -> None:
    """The Replicator's destination-probe step (req 6.2 / 6.7) relies on
    the adapter mapping a missing key to ``FileNotFoundError`` rather
    than the underlying ``NonTransientError(status=404)``.
    """
    _s3_client, _bucket, adapter = s3_environment

    with pytest.raises(FileNotFoundError):
        adapter.get_metadata("nonexistent/key.bin")
