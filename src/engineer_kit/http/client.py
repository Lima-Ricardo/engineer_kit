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


class HttpRequestError(RuntimeError):
    """Erro de requisicao HTTP com URL sempre sanitizada.

    Nunca deixa uma excecao do `requests` propagar diretamente: a
    mensagem padrao dele (`raise_for_status`, erros de conexao) inclui
    a URL completa da requisicao, que pode conter um segredo se o
    AuthStrategy usar query param (ApiKeyAuth com location="query").
    Essa excecao e o que efetivamente chega em `Pipeline`/CLI/logs.
    """


def _redact_request_target(url: str, params: dict[str, Any] | None) -> str:
    """`requests` funde `params` na URL internamente -- o segredo de um
    ApiKeyAuth(location="query") nunca aparece na string `url` recebida
    aqui, so no dict `params` pos-auth. Por isso a redacao tem que olhar
    `params`, nao tentar re-parsear `url` (que normalmente nao tem query
    string nenhuma nesse ponto)."""
    base = url.split("?", 1)[0]
    has_query = "?" in url or bool(params)
    return f"{base}?<redacted>" if has_query else base


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

        # loga ANTES de aplicar auth: AuthStrategy pode injetar a chave da
        # API em `params` (ApiKeyAuth com location="query") -- logar depois
        # vazaria o segredo em texto puro no log.
        logger.info("HTTP %s %s params=%s", method, url, kwargs.get("params"))

        kwargs = self._auth.apply(kwargs)
        try:
            response = self._session.request(method, url, **kwargs)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            # corta o vinculo com a excecao original de proposito (from None):
            # a mensagem dela pode conter a URL com segredo em query param,
            # e nao queremos isso reaparecendo em traceback/logger.exception.
            raise HttpRequestError(
                f"Falha em {method} {_redact_request_target(url, kwargs.get('params'))}: {type(exc).__name__}"
            ) from None
        return response

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:
        self._session.close()
