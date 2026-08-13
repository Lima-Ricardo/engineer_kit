# Pagination

Pagination is explicit because every API defines a different contract. `engineer_kit` does not guess a hidden strategy.

## No pagination

```python
from engineer_kit import NoPagination
pagination = NoPagination()
```

## Page number + page size

For an API using:

```text
?page=1&per_page=1000
```

configure:

```python
from engineer_kit import PageNumberPagination

pagination = PageNumberPagination(
    page_param="page",
    page_size_param="per_page",
    page_size=1000,
    start_page=1,
)
```

This strategy stops when a page contains fewer records than `page_size`.

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

```json
{
  "results": [...],
  "next_cursor": "abc123"
}
```

```python
from engineer_kit import CursorPagination
pagination = CursorPagination(cursor_param="cursor", cursor_field="next_cursor")
```

## Link header

```text
Link: <https://api.example.com/items?page=2>; rel="next"
```

```python
from engineer_kit import LinkHeaderPagination
pagination = LinkHeaderPagination()
```

## Next URL in JSON

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

## Security for API-provided URLs

The connector refuses a change of origin by default:

```text
https://api.example.com/page/1
           ↓ allowed
https://api.example.com/page/2

https://api.example.com/page/1
           ↓ blocked by default
https://attacker.example/collect
```

This reduces the risk of forwarding credentials to another host.

## Infinite-loop protection

The library applies both pagination-loop detection and a defensive `max_pages` limit. Set `max_pages` according to realistic volume, but do not remove limits from untrusted configurations.

## Page size is not extraction batch size

```text
page_size=1000
extraction_batch_size=25000
```

Roughly 25 pages may fill one extraction batch. The API controls paging; `ExtractionSession` controls how much data reaches the consumer at once.

## Custom pagination

When no built-in strategy matches, implement `PaginationStrategy`:

```python
class MyPagination(PaginationStrategy):
    def initial_params(self):
        return {...}

    def next_params(self, page, previous_params):
        ...
```

The strategy can be tested without network access by constructing `ParsedPage` values manually.
