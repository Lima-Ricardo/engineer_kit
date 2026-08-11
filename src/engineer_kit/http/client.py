"""Wrapper fino sobre `requests` com as garantias que todo conector precisa:
timeout obrigatorio, retry com backoff exponencial, HTTPS forcado, e log
que nunca imprime o corpo da resposta (pode conter dado sensivel).
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from engineer_kit.http.auth import AuthStrategy, NoAuth

logger = logging.getLogger("engineer_kit.http")


class InsecureUrlError(ValueError):
    """Levantado quando a URL nao usa HTTPS."""


class HttpClient:
    def __init__(
        self,
        auth: AuthStrategy | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        allow_http: bool = False,
    ) -> None:
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
        scheme = urlparse(url).scheme
        if scheme != "https" and not self._allow_http:
            raise InsecureUrlError(
                f"URL '{url}' nao usa HTTPS. Passe allow_http=True explicitamente "
                "se isso for intencional (ex: API interna em rede confiavel)."
            )

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._check_scheme(url)
        kwargs.setdefault("timeout", self._timeout)
        kwargs = self._auth.apply(kwargs)

        logger.info("HTTP %s %s params=%s", method, url, kwargs.get("params"))
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self._session.close()
