# Pagination

The common path does not require constructing strategy classes. The public API accepts friendly selectors and resolves the strategy **once before the hot path**.

## Happy path

```python
RestConnector(
    base_url=url,
    pagination="cursor",
)
```

Also supported:

```python
pagination="page"
pagination="offset"
pagination="link_header"
pagination="next_url"
pagination=False
```

Strings are case-insensitive, so `Cursor`, `cursor`, and `CURSOR` express the same intent.

## `auto`

`pagination="auto"` is the default. It is deliberately conservative: it inspects the response already fetched by extraction and recognizes only high-confidence patterns such as a Link header, next URL, or known cursor fields.

It **does not issue discovery requests** and it does not guess page/offset parameters when request semantics cannot be known safely.

For a page-number API, simply declare:

```python
pagination="page"
```

## Page number + size

For:

```text
?page=1&per_page=1000
```

use:

```python
pagination={
    "type": "page",
    "size": 1000,
}
```

For non-standard parameter names:

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

Additional selectors remain available for non-standard APIs.

## Cursor

Typical response:

```json
{
  "results": [...],
  "next_cursor": "abc123"
}
```

Use:

```python
pagination="cursor"
```

For a different shape:

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

The connector still enforces origin protection for API-provided URLs.

## Next URL

```python
pagination={
    "type": "next_url",
    "field": "paging.next",
}
```

## Expert mode

The strategy classes remain public:

```python
from engineer_kit import CursorPagination

pagination = CursorPagination(
    cursor_param="after",
    cursor_field="next_cursor",
)
```

Implement `PaginationStrategy` when no official strategy represents the source contract.

## Runtime protections

Regardless of configuration style, the runtime keeps:

- defensive `max_pages`;
- pagination-loop detection;
- cross-origin pagination blocked by default;
- HTTP session/connection-pool reuse;
- API page size separate from extraction batch size.

```text
API page size
     ↓
record stream
     ↓
extraction batch (25,000 by default)
```

Convenience work happens during setup; execution uses the already-resolved strategy.
