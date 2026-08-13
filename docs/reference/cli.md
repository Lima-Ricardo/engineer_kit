# CLI

Depois da instalação:

```bash
engineer_kit --help
```

## Executar configuração YAML

```bash
engineer_kit run-config pipelines/orders.yaml
```

A CLI resolve adapters e abre os recursos locais necessários de acordo com a configuração.

## Listar adapters

```bash
engineer_kit adapters
```

Útil para verificar quais integrações estão instaladas no ambiente.

## Local Lab

```bash
engineer_kit ui --workspace .
```

Use `engineer_kit ui --help` para opções de bind/autenticação. A configuração padrão é orientada a loopback/local.

## Módulos Python legados/customizados

A CLI mantém caminhos de compatibilidade para execução programática/custom. Trate módulos importados explicitamente pelo operador como código confiável; isso não é um sandbox.
