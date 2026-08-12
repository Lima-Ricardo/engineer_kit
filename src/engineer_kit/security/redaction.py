"""Best-effort redaction for human-facing logs and error surfaces.

This is defense in depth, not a substitute for keeping secret values out of
logs in the first place. Patterns intentionally target common credential forms
without trying to parse arbitrary application payloads.
"""

from __future__ import annotations

import re
from typing import Iterable

_MAX_LOG_TEXT = 256 * 1024
_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(
        r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|"
        r"password|passwd|account[_-]?key|sas[_-]?token|connection[_-]?string)"
        r"\s*[:=]\s*)[^\s,;]+"
    ),
    re.compile(r"(?i)(https?://[^\s/?#]+/[^\s?#]*\?)[^\s#]+"),
)


def redact_text(text: object, *, known_secrets: Iterable[str] = ()) -> str:
    """Return bounded text with common credential material replaced."""
    result = str(text)
    for secret in known_secrets:
        if secret:
            result = result.replace(secret, "<redacted>")
    for pattern in _PATTERNS:
        result = pattern.sub(r"\1<redacted>", result)
    if len(result) > _MAX_LOG_TEXT:
        result = result[:_MAX_LOG_TEXT] + "\n<output truncated>"
    return result


__all__ = ["redact_text"]
