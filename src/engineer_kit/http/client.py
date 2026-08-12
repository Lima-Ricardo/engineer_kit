"""HTTP client with safe defaults for API ingestion.

Guarantees shared by every connector: explicit timeout, bounded retry/backoff,
HTTPS by default and log/error sanitization that never prints request parameter
values, embedded URL credentials or response bodies.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from engineer_kit.http.auth import AuthStrategy, NoAuth

logger = logging.getLogger("engineer_kit.http")


class InsecureUrlError(ValueError):
    """Raised when a URL does not use HTTPS and HTTP was not opted into."""


class HttpRequestError(RuntimeError):
    """HTTP request failure whose message always contains a sanitized target."""


def _safe_request_target(url: str) -> str:
    """Return a useful request target without credentials/query values."""
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        netloc = f"{host}{port}"
        safe = f"{parsed.scheme}://{netloc}{parsed.path or ''}"
        if parsed.query:
            safe += "?<redacted>"
        return safe
    except (TypeError, ValueError):
        return "<invalid-url>"


def _redact_request_target(url: str, params: dict[str, Any] | None) -> str:
    safe = _safe_request_target(url)
    if params and "?<redacted>" not in safe:
        safe += "?<redacted>"
    return safe


def _parameter_keys(params: Any) -> list[str]:
    """Expose only parameter names for diagnostics, never their values."""
    if not isinstance(params, dict):
        return []
    return sorted(str(key) for key in params)


class HttpClient:
    def __init__(
        self,
        auth: AuthStrategy | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        allow_http: bool = False,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout deve ser maior que zero.")
        if max_retries < 0:
            raise ValueError("max_retries nao pode ser negativo.")
        if backoff_factor < 0:
            raise ValueError("backoff_factor nao pode ser negativo.")

        self._auth = auth or NoAuth()
        self._timeout = timeout
        self._allow_http = allow_http
        self._session = requests.Session()

        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _check_scheme(self, url: str) -> None:
        scheme = urlsplit(url).scheme.lower()
        if scheme != "https" and not self._allow_http:
            raise InsecureUrlError(
                f"URL '{_safe_request_target(url)}' nao usa HTTPS. "
                "Passe allow_http=True explicitamente se isso for intencional "
                "(ex: API interna em rede confiavel)."
            )

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._check_scheme(url)
        kwargs.setdefault("timeout", self._timeout)

        # Never log parameter values. Secrets may come from AuthStrategy,
        # static_params, date filters or caller-provided request parameters.
        logger.info(
            "HTTP %s %s param_keys=%s",
            method,
            _safe_request_target(url),
            _parameter_keys(kwargs.get("params")),
        )

        kwargs = self._auth.apply(kwargs)
        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            # Sever the original requests exception deliberately: it may embed
            # a fully-expanded URL containing query-string secrets.
            raise HttpRequestError(
                f"Falha em {method} {_redact_request_target(url, kwargs.get('params'))}: "
                f"{type(exc).__name__}"
            ) from None
        return response

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self._session.close()
