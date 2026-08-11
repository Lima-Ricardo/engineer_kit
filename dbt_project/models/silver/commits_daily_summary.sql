-- exemplo de regra de negocio escrita a mao pelo engenheiro sobre o
-- staging gerado automaticamente: contagem de commits por autor/dia.
select
    commit_author_name as author,
    substr(cast(commit_author_date as varchar), 1, 10) as commit_date,
    count(*) as commits_count
from {{ ref('stg_github_commits') }}
group by 1, 2
order by 2 desc, 3 desc
