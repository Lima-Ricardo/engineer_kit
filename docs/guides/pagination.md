# Paginação

Paginação é uma escolha explícita. Não existe uma estratégia escondida porque cada API documenta um contrato diferente.

## Sem paginação

```python
from engineer_kit import NoPagination

pagination = NoPagination()
```

## Página + tamanho

Para:

```text
?page=1&per_page=1000
```

use:

```python
from engineer_kit import PageNumberPagination

pagination = PageNumberPagination(
    page_param="page",
    page_size_param="per_page",
    page_size=1000,
    start_page=1,
)
```

A estratégia encerra quando uma página vem com menos registros do que `page_size`.

## Offset + limit

```python
from engineer_kit import OffsetPagination

pagination = OffsetPagination(
    offset_param="offset",
    limit_param="limit",
    limit=1000,
    start_offset=0,
)
```

## Cursor

Resposta:

```json
{
  "results": [...],
  "next_cursor": "abc123"
}
```

```python
from engineer_kit import CursorPagination

pagination = CursorPagination(
    cursor_param="cursor",
    cursor_field="next_cursor",
)
```

## Link header

Header:

```text
Link: <https://api.example.com/items?page=2>; rel="next"
```

```python
from engineer_kit import LinkHeaderPagination

pagination = LinkHeaderPagination()
```

## Próxima URL no JSON

```json
{
  "results": [...],
  "next": "https://api.example.com/items?page=2"
}
```

```python
from engineer_kit import NextUrlPagination

pagination = NextUrlPagination(next_url_field="next")
```

## Segurança de URLs fornecidas pela API

Uma API pode fornecer a próxima URL. Por padrão, o connector recusa trocar de origem:

```text
https://api.example.com/page/1
           ↓ permitido
https://api.example.com/page/2

https://api.example.com/page/1
           ↓ bloqueado por padrão
https://attacker.example/collect
```

Isso reduz o risco de encaminhar credenciais para outro host.

## Loop infinito

Além da lógica de cada estratégia, a biblioteca aplica:

- detecção de loops de paginação;
- limite máximo de páginas (`max_pages`).

Configure `max_pages` de acordo com o volume esperado, mas evite remover limites em configs não confiáveis.

## Page size não é extraction batch

```text
page_size=1000
extraction_batch_size=25000
```

Nesse exemplo, aproximadamente 25 páginas podem preencher um batch. A API decide como paginar; a sessão decide quanto entregar ao consumidor por vez.

## Paginação customizada

Quando nenhuma estratégia padrão encaixa, implemente `PaginationStrategy`:

```python
class MyPagination(PaginationStrategy):
    def initial_params(self):
        return {...}

    def next_params(self, page, previous_params):
        ...
```

A classe pode ser testada sem rede usando `ParsedPage` construído manualmente.
