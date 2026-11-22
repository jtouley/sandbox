"""Keep secrets out of everything we persist.

Meta's paging URLs embed ``access_token=...`` in the query string, so a verbatim response
body contains the token. We redact before hashing and writing, and record how many
substitutions were made so the change to the verbatim body is itself on the record.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "REDACTED"
SECRET_PARAMS = frozenset({"access_token", "appsecret_proof", "client_secret", "code"})
_QUERY_SECRET = re.compile(
    r"\b(" + "|".join(sorted(SECRET_PARAMS)) + r")=([^&\"'\s\\]+)", flags=re.IGNORECASE
)


def redact_text(text: str, token: str | None = None) -> tuple[str, int]:
    """Redact secret query params and any literal occurrence of ``token``.

    Returns the redacted text and the number of secrets replaced.
    """
    count = 0
    if token:
        count += text.count(token)
        text = text.replace(token, REDACTED)

    def _sub(m: re.Match[str]) -> str:
        nonlocal count
        if m.group(2) != REDACTED:
            count += 1
        return f"{m.group(1)}={REDACTED}"

    return _QUERY_SECRET.sub(_sub, text), count


def strip_secret_params(url: str) -> str:
    """Drop secret query params entirely (used for paging URLs we re-request)."""
    parts = urlsplit(url)
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in SECRET_PARAMS
    ]
    return urlunsplit(parts._replace(query=urlencode(query, safe="{}(),.")))
