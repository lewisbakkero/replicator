"""Secret-pattern stripping for log records and Manifest records.

The `Redactor` is plumbed into both the `Manifest_Writer` and the JSON
`logging.Formatter` (tasks 7, 9). Every record produced by either component
passes through `Redactor.scrub_record(record)` before serialisation. `scrub`
walks the dict, and for each string value runs a chain of regexes that target
known secret shapes; any match is replaced with the literal ``[REDACTED]``.
Field names known to carry secrets have their entire value replaced with
``[REDACTED]`` regardless of regex match.

The regex chain matches the table in design.md's "Security and Secret
Handling > [REDACTED] enforcement" section. The two patterns documented
there with variable-width lookbehinds (``aws_secret_access_key`` and
``X-Amz-Signature=``) cannot be expressed directly with Python's stdlib
``re`` module, which only supports fixed-width lookbehinds. They are
implemented here as prefix-capture replacements that preserve the prefix
and redact only the secret portion — semantically equivalent to the
lookbehind formulation.

Validates: Requirements 1.6, 14.4.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Iterable, Mapping, Tuple


#: Literal replacement string substituted for every matched secret.
REDACTED = "[REDACTED]"


# ---------------------------------------------------------------------------
# Regex chain — order matters: PEM blocks must run before bare AKIA / ya29
# patterns so that secrets embedded inside a PEM block are excised whole
# rather than partially.
# ---------------------------------------------------------------------------

# Patterns whose entire match is replaced with ``[REDACTED]``.
_FULL_REPLACE_PATTERNS: Tuple[re.Pattern[str], ...] = (
    # PEM private key block (multi-line). ``re.DOTALL`` lets ``.`` cross
    # newlines so the non-greedy ``.*?`` reaches the matching END marker.
    re.compile(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        re.DOTALL,
    ),
    # AWS Access Key Id.
    re.compile(r"AKIA[0-9A-Z]{16}"),
    # Google OAuth access token.
    re.compile(r"ya29\.[A-Za-z0-9_\-]+"),
    # Google refresh token.
    re.compile(r"1//[A-Za-z0-9_\-]+"),
    # Bearer token in Authorization headers.
    re.compile(r"Bearer\s+[A-Za-z0-9_\-\.=]+"),
)

# Patterns with a fixed prefix that must be preserved. Group 1 is the
# prefix (kept verbatim); group 2 is the secret payload (replaced with
# ``[REDACTED]``). Equivalent to the design.md lookbehind formulation but
# expressed without a variable-width lookbehind, which Python's stdlib
# ``re`` does not support.
_PREFIX_REPLACE_PATTERNS: Tuple[Tuple[re.Pattern[str], str], ...] = (
    # AWS secret access key in surrounding context.
    (
        re.compile(r"(aws_secret_access_key[=:\s]+)([A-Za-z0-9/+=]{40})"),
        r"\1" + REDACTED,
    ),
    # S3 presigned-URL signature.
    (
        re.compile(r"(X-Amz-Signature=)([A-Fa-f0-9]+)"),
        r"\1" + REDACTED,
    ),
)


# ---------------------------------------------------------------------------
# Field-name allowlist — case-insensitive comparison. Stored lowercased.
# ---------------------------------------------------------------------------

_FIELD_ALLOWLIST: frozenset[str] = frozenset(
    {
        "aws_secret_access_key",
        "private_key",
        "private_key_id",
        "client_secret",
        "refresh_token",
        "access_token",
        "authorization",
    }
)


class Redactor:
    """Strip credential-shaped substrings and allowlisted-field values.

    The constructor takes no arguments — the regex set and the field-name
    allowlist are fixed by design.md.
    """

    __slots__ = ()

    # ------------------------------------------------------------------
    # String-level scrubbing
    # ------------------------------------------------------------------

    def scrub_string(self, s: str) -> str:
        """Apply the regex chain to ``s`` and return a new string.

        Every regex match is replaced with the literal ``[REDACTED]``.
        Non-string inputs are returned unchanged so callers can use
        ``scrub_string`` defensively.
        """
        if not isinstance(s, str):
            return s
        out = s
        for pattern in _FULL_REPLACE_PATTERNS:
            out = pattern.sub(REDACTED, out)
        for pattern, repl in _PREFIX_REPLACE_PATTERNS:
            out = pattern.sub(repl, out)
        return out

    # ------------------------------------------------------------------
    # Type-dispatched scrubbing
    # ------------------------------------------------------------------

    def scrub_value(self, v: Any) -> Any:
        """Recursively scrub ``v``, dispatching by runtime type.

        - ``None``, ``bool``, ``int``, ``float``, ``Enum``: returned
          unchanged.
        - ``str``: passed through `scrub_string`.
        - ``Mapping``: returned as a new ``dict`` with each value scrubbed,
          subject to the field-name allowlist (entire value replaced with
          ``"[REDACTED]"`` for allowlisted keys regardless of the value's
          shape, even if the value is itself a dict or list).
        - ``list``: returned as a new ``list`` with each element scrubbed.
        - ``tuple``: returned as a new ``tuple`` with each element scrubbed.
        - Anything else: returned unchanged. Realistically the Manifest
          and log records only contain JSON-serializable primitives, so
          this is a defensive default rather than a hot path.
        """
        # ``bool`` is a subclass of ``int`` in Python; keep it first so
        # downstream isinstance checks against ``int`` cannot misclassify
        # booleans.
        if v is None:
            return v
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, Enum):
            return v
        if isinstance(v, str):
            return self.scrub_string(v)
        if isinstance(v, Mapping):
            return self._scrub_mapping(v)
        if isinstance(v, list):
            return [self.scrub_value(item) for item in v]
        if isinstance(v, tuple):
            return tuple(self.scrub_value(item) for item in v)
        # Unknown / complex types (dataclasses, custom objects): pass
        # through. The Manifest and log writers only ever hand us
        # JSON-serializable primitives, so reaching this branch is a
        # caller bug rather than a runtime concern.
        return v

    # ------------------------------------------------------------------
    # Convenience wrapper for log/Manifest records
    # ------------------------------------------------------------------

    def scrub_record(self, record: Mapping[str, Any]) -> dict:
        """Scrub a top-level record (always returns a fresh ``dict``).

        Used by the structured JSON logger and by the `Manifest_Writer`
        to ensure every emitted line passes through the same chain.
        """
        return self._scrub_mapping(record)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scrub_mapping(self, m: Mapping[str, Any]) -> dict:
        """Scrub every value in ``m``, applying the field-name allowlist.

        Returns a new ``dict`` so callers cannot accidentally mutate a
        previously-scrubbed mapping.
        """
        result: dict[str, Any] = {}
        for key, value in m.items():
            if isinstance(key, str) and key.lower() in _FIELD_ALLOWLIST:
                # Allowlisted keys: the *entire* value is replaced,
                # regardless of its type. Even nested dicts/lists are
                # collapsed to the literal ``"[REDACTED]"`` string.
                result[key] = REDACTED
            else:
                result[key] = self.scrub_value(value)
        return result


__all__ = ["Redactor", "REDACTED"]
