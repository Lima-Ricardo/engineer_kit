"""Security helpers for the optional localhost UI."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Apply browser hardening headers to every local-lab response."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'",
        )
        return response


def validate_resource_name(name: str) -> str:
    """Validate a name before it participates in a filesystem path/URL."""
    if _RESOURCE_NAME_RE.fullmatch(name) is None:
        raise ValueError(
            "Nome deve comecar por letra/underscore e conter apenas letras, numeros, '_' ou '-'."
        )
    return name


def enforce_same_origin(request: Request) -> None:
    """Block browser cross-site POSTs against the Basic-auth local lab.

    Modern browsers send Sec-Fetch-Site and/or Origin on cross-site form/fetch
    requests. CLI/TestClient requests without browser metadata remain allowed.
    """
    fetch_site = request.headers.get("sec-fetch-site", "").lower()
    if fetch_site == "cross-site":
        raise HTTPException(status_code=403, detail="Requisicao cross-site bloqueada.")

    origin = request.headers.get("origin")
    if not origin:
        return
    try:
        parsed = urlsplit(origin)
        request_host = request.headers.get("host", "").lower()
        origin_host = parsed.netloc.lower()
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=403, detail="Origin invalida.") from exc
    if parsed.scheme not in {"http", "https"} or origin_host != request_host:
        raise HTTPException(status_code=403, detail="Origin nao permitida.")


__all__ = [
    "SecurityHeadersMiddleware",
    "enforce_same_origin",
    "validate_resource_name",
]
