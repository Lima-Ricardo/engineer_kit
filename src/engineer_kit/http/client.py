"""HTTP client with safe defaults for API ingestion.

Guarantees shared by every connector: explicit timeout, bounded retry/backoff,
HTTPS by default, TLS certificate verification by default, bounded response
bodies, same-origin redirects and log/error sanitization that never prints
request parameter values, embedded URL credentials or response bodies.
"""

from __future__ import annotations

import ipaddress
import logging
from typing import Any
from urllib.parse import urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from engineer_kit.http.auth import AuthStrategy, NoAuth

logger = logging.getLogger("engineer_kit.http")

DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_REDIRECTS = 5
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_BLOCKED_HOSTNAMES = {"metadata.google.internal"}


class InsecureUrlError(ValueError):
    """Raised when a URL does not use HTTPS and HTTP was not opted into."""


class InsecureTlsError(ValueError):
    """Raised when TLS verification is disabled without an explicit opt-in."""


class UnsafeUrlError(ValueError):
    """Raised for malformed or security-sensitive request targets."""


class UnsafeRedirectError(RuntimeError):
    """Raised when a redirect crosses origin or exceeds the configured limit."""


class ResponseTooLargeError(RuntimeError):
    """Raised before an API page can consume unbounded process memory."""


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


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port
    if port is None:
        port = 443 if scheme == "https" else 80 if scheme == "http" else None
    return scheme, host, port


def _is_blocked_literal_ip(host: str) -> bool:
    """Block network targets that should never be API destinations by default.

    RFC1918/private addresses remain allowed because internal enterprise APIs
    are a first-class use case. Link-local/cloud-metadata, multicast,
    unspecified and reserved literal IPs are rejected. IPv4-mapped IPv6 is
    normalized first so address notation cannot bypass the same checks.
    Hostname resolution is deliberately not performed here so normal corporate
    DNS/proxy setups keep working; deployments that execute untrusted configs
    should additionally enforce egress policy at the runtime/network layer.
    """
    candidate = host.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return bool(
        address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    )


class HttpClient:
    def __init__(
        self,
        auth: AuthStrategy | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        allow_http: bool = False,
        allow_insecure_tls: bool = False,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        retry_post: bool = False,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout deve ser maior que zero.")
        if max_retries < 0:
            raise ValueError("max_retries nao pode ser negativo.")
        if backoff_factor < 0:
            raise ValueError("backoff_factor nao pode ser negativo.")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes deve ser maior que zero.")
        if max_redirects < 0:
            raise ValueError("max_redirects nao pode ser negativo.")

        self._auth = auth or NoAuth()
        self._timeout = timeout
        self._allow_http = allow_http
        self._allow_insecure_tls = allow_insecure_tls
        self._max_response_bytes = max_response_bytes
        self._max_redirects = max_redirects
        self._session = requests.Session()

        allowed_methods = {"GET"}
        if retry_post:
            # POST retries may duplicate side effects if the remote endpoint is
            # not idempotent, so this remains an explicit opt-in.
            allowed_methods.add("POST")
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(allowed_methods),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    @property
    def auth_identity(self) -> dict[str, Any]:
        """Return stable logical auth metadata without resolving secret values."""
        identity = self._auth.identity()
        return dict(identity)

    def _check_url(self, url: str) -> None:
        try:
            parsed = urlsplit(url)
            scheme = parsed.scheme.lower()
            host = parsed.hostname
            _ = parsed.port  # force malformed-port validation
        except (TypeError, ValueError) as exc:
            raise UnsafeUrlError(f"URL invalida: {_safe_request_target(url)}") from exc

        if scheme not in {"http", "https"} or not host:
            raise UnsafeUrlError(
                f"URL deve ser absoluta e usar http/https: {_safe_request_target(url)}"
            )
        if parsed.username is not None or parsed.password is not None:
            raise UnsafeUrlError(
                "Credenciais embutidas na URL nao sao permitidas; use uma AuthStrategy/SecretProvider."
            )
        if scheme != "https" and not self._allow_http:
            raise InsecureUrlError(
                f"URL '{_safe_request_target(url)}' nao usa HTTPS. "
                "Passe allow_http=True explicitamente se isso for intencional "
                "(ex: API interna em rede confiavel)."
            )

        normalized_host = host.rstrip(".").lower()
        if normalized_host in _BLOCKED_HOSTNAMES or _is_blocked_literal_ip(normalized_host):
            raise UnsafeUrlError(
                f"Destino de rede bloqueado por seguranca: {_safe_request_target(url)}"
            )

    def _consume_bounded_body(self, response: requests.Response, target: str) -> None:
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                advertised = int(content_length)
            except ValueError:
                advertised = -1
            if advertised > self._max_response_bytes:
                response.close()
                raise ResponseTooLargeError(
                    f"Resposta de {_safe_request_target(target)} excede o limite de "
                    f"{self._max_response_bytes} bytes."
                )

        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > self._max_response_bytes:
                response.close()
                raise ResponseTooLargeError(
                    f"Resposta de {_safe_request_target(target)} excede o limite de "
                    f"{self._max_response_bytes} bytes."
                )
            chunks.append(chunk)

        response._content = b"".join(chunks)  # noqa: SLF001
        response._content_consumed = True  # noqa: SLF001

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._check_url(url)
        if kwargs.get("verify") is False and not self._allow_insecure_tls:
            raise InsecureTlsError(
                "verify=False foi recusado. Para um laboratorio com certificado autoassinado, "
                "crie HttpClient(allow_insecure_tls=True) explicitamente; em uso real prefira "
                "uma CA/certificado valido ou passe o caminho do CA bundle em verify=."
            )
        kwargs.setdefault("timeout", self._timeout)
        kwargs["stream"] = True
        kwargs["allow_redirects"] = False

        logger.info(
            "HTTP %s %s param_keys=%s",
            method,
            _safe_request_target(url),
            _parameter_keys(kwargs.get("params")),
        )

        request_kwargs = self._auth.apply(kwargs)
        target = url
        redirects = 0

        while True:
            try:
                response = self._session.request(method, target, **request_kwargs)
            except requests.exceptions.RequestException as exc:
                raise HttpRequestError(
                    f"Falha em {method} "
                    f"{_redact_request_target(target, request_kwargs.get('params'))}: "
                    f"{type(exc).__name__}"
                ) from None

            if response.status_code not in _REDIRECT_STATUSES:
                try:
                    response.raise_for_status()
                except requests.exceptions.RequestException as exc:
                    response.close()
                    raise HttpRequestError(
                        f"Falha em {method} "
                        f"{_redact_request_target(target, request_kwargs.get('params'))}: "
                        f"{type(exc).__name__}"
                    ) from None
                self._consume_bounded_body(response, target)
                return response

            status_code = response.status_code
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise UnsafeRedirectError(
                    f"Redirect sem Location recebido de {_safe_request_target(target)}."
                )
            redirects += 1
            if redirects > self._max_redirects:
                raise UnsafeRedirectError(
                    f"Numero maximo de redirects ({self._max_redirects}) excedido para "
                    f"{_safe_request_target(url)}."
                )

            next_target = urljoin(target, location)
            self._check_url(next_target)
            if _origin(next_target) != _origin(target):
                raise UnsafeRedirectError(
                    "Redirect para outra origem foi bloqueado para evitar vazamento de "
                    f"credenciais: {_safe_request_target(target)} -> "
                    f"{_safe_request_target(next_target)}"
                )

            if status_code == 303 or (
                status_code in {301, 302} and method.upper() == "POST"
            ):
                method = "GET"
                request_kwargs.pop("json", None)
                request_kwargs.pop("data", None)
            target = next_target

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self._session.close()
