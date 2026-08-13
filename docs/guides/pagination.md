# Paginação

O caminho comum não exige instanciar classes de estratégia. A API pública aceita seletores simples e resolve a estratégia **uma vez antes do hot path**.

## Happy path

```python
RestConnector(
    base_url=url,
    pagination="cursor",
)
```

Também são aceitos:

```python
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

Strings são normalizadas sem diferenciar capitalização, então `Cursor`, `cursor` e `CURSOR` representam a mesma intenção.

## `auto`

`pagination="auto"` é o default. Ele é propositalmente conservador: usa a resposta que a extração já recebeu e reconhece somente padrões com alta confiança, como Link header, next URL e campos de cursor conhecidos.

Ele **não faz requests extras de descoberta** e não tenta adivinhar page/offset quando os parâmetros da request não podem ser determinados com segurança.

Se uma API usa page number, declare isso:

```python
pagination="page"
```

## Página + tamanho

Para:

```text
?page=1&per_page=1000
```

use a forma guiada:

```python
pagination={
    "type": "page",
    "size": 1000,
}
```

Quando os nomes são fora do padrão:

```python
pagination={
    "type": "page",
    "param": "page_number",
    "size_param": "page_limit",
    "size": 1000,
}
```

## Offset + limit

```python
pagination={
    "type": "offset",
    "size": 1000,
}
```

Seletores adicionais continuam disponíveis para nomes fora do padrão.

## Cursor

Resposta comum:

```json
{
  "results": [...],
  "next_cursor": "abc123"
}
```

Basta:

```python
pagination="cursor"
```

Para uma API diferente:

```python
pagination={
    "type": "cursor",
    "cursor": "meta.next_cursor",
    "param": "after",
}
```

## Link header

```python
pagination="link_header"
```

O connector continua aplicando a proteção de origem para URLs fornecidas pela API.

## Next URL

```python
pagination={
    "type": "next_url",
    "field": "paging.next",
}
```

## Modo expert

As classes continuam públicas:

```python
from engineer_kit import CursorPagination

pagination = CursorPagination(
    cursor_param="after",
    cursor_field="next_cursor",
)
```

Implemente `PaginationStrategy` quando nenhuma estratégia oficial representar o contrato da origem.

## Proteções

Independentemente da forma de configuração, o runtime mantém:

- `max_pages` defensivo;
- detecção de loop de paginação;
- bloqueio de paginação cross-origin por padrão;
- reutilização da sessão HTTP/connection pool;
- separação entre API page size e extraction batch size.

```text
page size da API
       ↓
stream de registros
       ↓
extraction batch (25.000 por padrão)
```

A abstração acontece no setup; a execução usa a estratégia já resolvida.
