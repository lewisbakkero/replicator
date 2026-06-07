# Feature: multicloud-photo-sync, Property 13: Drive_Importer pull-only contract
"""Drive_Importer pull-only contract — Property 13.

Property under test (design.md, "Correctness Properties — Property 13:
Drive_Importer pull-only contract"):

  For any set of Drive files generated with arbitrary ``mimeType``,
  ``createdTime``, ``name``, and bytes, after
  ``Drive_Importer.import_files(...)`` completes:

  1. No ``write_bytes``, ``set_tag``, or ``delete`` call is made
     against the Drive (Pull_Only_Source) adapter — Drive is
     read-only (req 10.8).
  2. Every imported destination key matches
     ``^google-drive/(\\d{4}|unknown-year)/(\\d{2}|unknown-month)/[^/]+__[A-Za-z0-9._-]*$``
     (req 10.6 / 10.7). The trailing character class is relaxed to
     ``*`` because a name comprised entirely of forbidden characters
     legitimately sanitises to the empty string.
  3. Files whose ``mimeType`` does not start with ``image/`` or
     ``video/`` produce no destination write (req 10.2).
  4. Files whose ``mimeType`` starts with ``application/vnd.google-apps.``
     produce no destination write (req 10.3).

The strategy generates between 0 and 6 Drive files per example. Each
file gets:

* a ``mimeType`` drawn from a small pool that mixes supported image /
  video types, unsupported types (``text/plain``,
  ``application/pdf``), and Google native-doc types
  (``application/vnd.google-apps.document``);
* a ``createdTime`` drawn from valid ISO-8601 strings, malformed
  strings, and the empty string;
* a ``name`` containing arbitrary printable ASCII so the sanitiser
  has to do real work;
* a payload of arbitrary bytes (0..256).

The Drive adapter is a `FakeSourceAdapter` constructed with
``supports_writes=False`` so any accidental mutating call would
itself raise. The destination adapter is a writable
`FakeSourceAdapter` whose ``call_log`` is asserted against to
verify the destination-key invariants.

Validates: Requirements 10.2, 10.3, 10.6, 10.7, 10.8.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Mapping, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from mcps.catalog.model import Catalog
from mcps.drive_import import DriveImporter
from mcps.manifest.writer import ManifestWriter
from mcps.sources.fake import FakeSourceAdapter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


_FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# Mix of allowed (image/video), explicitly-unsupported, and
# Google-native-doc mime types so the property exercises every
# branch of the filter.
_MIME_POOL: tuple[str, ...] = (
    "image/jpeg",
    "image/png",
    "image/gif",
    "video/mp4",
    "video/quicktime",
    "text/plain",
    "application/pdf",
    "application/octet-stream",
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.folder",  # Drive uses this for folders;
                                           # the importer should treat as
                                           # native-doc and skip
)


# Created-time pool: mostly well-formed ISO-8601 plus a few problem
# values so the unknown-year / unknown-month branch fires regularly.
_CREATED_TIME_POOL: tuple[str, ...] = (
    "2023-04-15T10:30:00Z",
    "2024-01-01T00:00:00Z",
    "2020-12-31T23:59:59.999Z",
    "2025-06-30T08:15:30+00:00",
    "",                                # missing → warn
    "not-a-date",                      # malformed → warn
    "2024-13-40T99:99:99Z",            # invalid components → warn
)


# Destination-key regex per Property 13 (relaxed trailing class to *).
_DST_KEY_RE = re.compile(
    r"^google-drive/(\d{4}|unknown-year)/(\d{2}|unknown-month)/"
    r"[^/]+__[A-Za-z0-9._-]*$"
)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _drive_file(draw) -> dict:
    """Draw one Drive file as a dict ``{file_id, mime, created, name, bytes}``.

    ``file_id`` is constrained to the alphabet Drive actually emits
    (``[A-Za-z0-9_-]+``). Drive's opaque file ids are never URL-unsafe
    and never contain ``/``; this is a precondition of the destination-
    key regex's ``[^/]+`` segment, not something the importer needs to
    sanitise. The strategy reflects the real input space rather than a
    synthetic one.
    """
    file_id = draw(
        st.text(
            alphabet=st.sampled_from(
                "abcdefghijklmnopqrstuvwxyz"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "0123456789_-"
            ),
            min_size=1,
            max_size=12,
        )
    )
    mime = draw(st.sampled_from(_MIME_POOL))
    created = draw(st.sampled_from(_CREATED_TIME_POOL))
    name = draw(
        st.text(
            alphabet=st.characters(min_codepoint=0x20, max_codepoint=0x7E),
            min_size=0,
            max_size=20,
        )
    )
    payload = draw(st.binary(min_size=0, max_size=256))
    return {
        "file_id": file_id,
        "mime": mime,
        "created": created,
        "name": name,
        "bytes": payload,
    }


@st.composite
def _drive_files(draw) -> list:
    """Draw 0..6 Drive files with unique file_ids."""
    files = draw(st.lists(_drive_file(), min_size=0, max_size=6))
    seen: set[str] = set()
    deduped: list[dict] = []
    for i, f in enumerate(files):
        fid = f["file_id"]
        if fid in seen:
            # Force uniqueness by suffixing the index. This mutation
            # keeps Hypothesis's shrinking simple while guaranteeing
            # the FakeSourceAdapter (a key→bytes dict) doesn't collapse
            # two distinct test inputs onto the same record.
            fid = f"{fid}#{i}"
        seen.add(fid)
        deduped.append({**f, "file_id": fid})
    return deduped


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_drive_adapter(files: list) -> FakeSourceAdapter:
    """Build a read-only ``FakeSourceAdapter`` populated with ``files``.

    Each file's ``user_metadata`` carries the keys the
    `GoogleDriveSourceAdapter` would set at listing time
    (``drive_file_id``, ``drive_path``, ``createdTime``). The
    ``content_type`` is keyed off the file's name, but
    `FakeSourceAdapter` derives content_type from the key extension by
    default. To inject the chosen mimeType we set the user_metadata
    entry the importer reads directly. Since `FakeSourceAdapter`
    derives ``content_type`` from the file extension, we instead
    instantiate a small subclass that returns the mimeType we want.
    """
    return _MimePinnedFakeSource(
        name="drive-src",
        kind="google_drive",
        supports_writes=False,
        files=files,
    )


class _MimePinnedFakeSource(FakeSourceAdapter):
    """``FakeSourceAdapter`` whose ``content_type`` is pinned per key.

    The base `FakeSourceAdapter` guesses the content type from the file
    extension. The Drive_Importer needs to see the exact ``mimeType``
    the test draws, so this subclass overrides ``_build_meta`` to
    substitute the pinned value.
    """

    def __init__(self, *, name, kind, supports_writes, files: list) -> None:
        records = {f["file_id"]: f["bytes"] for f in files}
        metadata = {
            f["file_id"]: {
                "drive_file_id": f["file_id"],
                "drive_path": f["name"],
                "createdTime": f["created"],
            }
            for f in files
        }
        super().__init__(
            name=name,
            kind=kind,
            supports_writes=supports_writes,
            records=records,
            metadata=metadata,
        )
        # Pin content_type per key. We keep both the original mime
        # (for assertions) and a flag for native-doc skip.
        self._pinned_mime: dict[str, str] = {
            f["file_id"]: f["mime"] for f in files
        }

    def _build_meta(self, key):  # type: ignore[override]
        meta = super()._build_meta(key)
        # Replace content_type with the pinned mime from the test.
        from mcps.sources.base import ObjectMeta

        return ObjectMeta(
            key=meta.key,
            size_bytes=meta.size_bytes,
            last_modified=meta.last_modified,
            content_type=self._pinned_mime.get(meta.key, meta.content_type),
            user_metadata=dict(meta.user_metadata),
            etag=meta.etag,
            provider_hash=meta.provider_hash,
        )


def _writes(adapter: FakeSourceAdapter) -> list[tuple[str, dict]]:
    """Return the subset of ``adapter.call_log`` that are write_bytes calls."""
    return [(m, k) for (m, k) in adapter.call_log if m == "write_bytes"]


def _mutating_calls(adapter: FakeSourceAdapter) -> list[tuple[str, dict]]:
    """Return mutating calls (write_bytes, set_tag, delete) on ``adapter``."""
    return [
        (m, k)
        for (m, k) in adapter.call_log
        if m in ("write_bytes", "set_tag", "delete")
    ]


# ---------------------------------------------------------------------------
# The Property 13 test
# ---------------------------------------------------------------------------


@pytest.mark.property
@given(files=_drive_files())
@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_drive_importer_pull_only_contract(
    files: list,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """`Drive_Importer.import_files` honours the Property 13 contract.

    1. No write/set_tag/delete on the Drive adapter.
    2. Every imported destination key matches the documented regex.
    3. Files with a non-image/non-video mimeType produce no destination
       write.
    4. Files with a Google-native-doc mimeType produce no destination
       write.

    Validates: Requirements 10.2, 10.3, 10.6, 10.7, 10.8.
    """
    drive = _build_drive_adapter(files)
    destination = FakeSourceAdapter(
        name="s3-dst",
        kind="s3",
        supports_writes=True,
    )

    importer = DriveImporter(
        drive_adapter=drive,
        destination_adapter=destination,
        drive_source_name="drive-src",
        run_id="property13",
        now=lambda: _FIXED_NOW,
    )

    manifest_path = tmp_path_factory.mktemp("manifest") / "manifest.jsonl"
    with ManifestWriter(str(manifest_path)) as mw:
        stats = importer.import_files(
            catalog=Catalog(),
            replicated_source_names=("s3-dst",),
            manifest_writer=mw,
        )

    # ---- (1) Drive adapter is read-only --------------------------------
    drive_mutations = _mutating_calls(drive)
    assert drive_mutations == [], (
        f"Drive adapter received mutating calls: {drive_mutations!r}"
    )

    # ---- (2) Destination keys match the regex --------------------------
    dst_writes = _writes(destination)
    for _method, kwargs in dst_writes:
        key = kwargs["key"]
        assert _DST_KEY_RE.match(key), f"destination key violates regex: {key!r}"

    # ---- (3 + 4) Filtered mimeTypes never produce a destination write --
    written_file_ids: set[str] = set()
    for _method, kwargs in dst_writes:
        # The destination key embeds the file id between the
        # ``<MM>/`` segment and the ``__`` separator. Recover it so
        # we can correlate writes with the input record.
        m = re.match(
            r"^google-drive/(?:\d{4}|unknown-year)/"
            r"(?:\d{2}|unknown-month)/(?P<fid>[^/]+)__[A-Za-z0-9._-]*$",
            kwargs["key"],
        )
        assert m is not None  # guaranteed by the assertion above
        written_file_ids.add(m.group("fid"))

    forbidden_file_ids = {
        f["file_id"]
        for f in files
        if (
            f["mime"].startswith("application/vnd.google-apps.")
            or not (
                f["mime"].startswith("image/") or f["mime"].startswith("video/")
            )
        )
    }
    illegal_writes = written_file_ids & forbidden_file_ids
    assert illegal_writes == set(), (
        f"destination received writes for filtered files: {illegal_writes!r}"
    )

    # Sanity check: stats counts agree with the call log so the
    # property captures the importer's actual side-effects rather
    # than just its return value.
    assert stats.imported == len(dst_writes), (
        f"imported counter ({stats.imported}) disagrees with destination "
        f"write count ({len(dst_writes)})"
    )
