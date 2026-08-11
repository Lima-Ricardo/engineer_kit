-- gerado automaticamente a partir do schema declarado em Python para o endpoint 'github_commits'.
-- revise os tipos (::TIPO) manualmente antes de considerar isto pronto para producao.
select
    "sha"::VARCHAR as sha,
    "commit_author_name"::VARCHAR as commit_author_name,
    "commit_author_email"::VARCHAR as commit_author_email,
    "commit_author_date"::TIMESTAMP as commit_author_date,
    "commit_committer_name"::VARCHAR as commit_committer_name,
    "commit_committer_email"::VARCHAR as commit_committer_email,
    "commit_committer_date"::TIMESTAMP as commit_committer_date,
    "commit_message"::VARCHAR as commit_message,
    "parents"::VARCHAR as parents,
    "html_url"::VARCHAR as html_url,
    "_source" as _source,
    "_endpoint" as _endpoint,
    "_ingested_at" as _ingested_at
from {{ source('bronze', 'github_commits') }}
