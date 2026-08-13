# Contribuindo com engineer_kit

Obrigado por considerar uma contribuição.

## Antes de começar

1. Abra uma issue para bugs grandes, mudanças de contrato ou novos adapters.
2. Não inclua tokens, dumps de produção ou dados pessoais.
3. Preserve a separação arquitetural entre source, state, destination e runtime.

## Setup

```bash
git clone https://github.com/Lima-Ricardo/engineer_kit.git
cd engineer_kit
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,all,docs]"
```

## Validação local

```bash
pytest -q -m 'not stress'
ruff check src tests
bandit -q -r src/engineer_kit -ll
pip-audit
mkdocs build --strict
```

Stress:

```bash
pytest -q -m stress tests/test_stress_ingestion.py
```

## Pull requests

- mantenha PRs focados;
- adicione testes para comportamento novo;
- atualize documentação quando o contrato público mudar;
- não quebre secure defaults sem uma justificativa explícita;
- mantenha backends opcionais fora do core.

A branch `main` é protegida; mudanças devem passar pelo fluxo de revisão/CI definido pelo mantenedor.
