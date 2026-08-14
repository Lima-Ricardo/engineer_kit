# Local Lab / UI

A UI é um laboratório de desenvolvimento e treinamento. Ela ajuda a entender os mesmos contratos usados em código.

## Instalação

```bash
pip install "engineer_kit[local]"
```

## Iniciar

```bash
engineer_kit ui --workspace .
```

A CLI gera uma senha temporária quando necessário e a UI fica em loopback por padrão.

## Dashboard

![Dashboard da Local Lab](../assets/ui/dashboard.png)

O dashboard mostra pipelines, última execução, status e quantidade de registros.

## Editor

![Editor](../assets/ui/pipeline-editor.png)

O formulário separa:

1. source/connector;
2. extraction batch;
3. autenticação;
4. paginação;
5. incremental/state;
6. schema;
7. destination;
8. transformação;
9. auditoria.

## Data Profile

![Data Profile](../assets/ui/data-profile.png)

O perfil usa o mesmo `ProfileReport` da API Python, com temas claro/escuro, PT-BR/EN, KPIs de qualidade, busca e filtros de campos.

## Arquitetura

![Arquitetura](../assets/ui/architecture.svg)

A página de arquitetura explica managed/embedded mode e os adapters.

## Logs

![Run](../assets/ui/run.svg)

A tela de execução usa streaming de logs e redige texto sensível antes de exibir/persistir.

## Exposição remota

Não trate a UI local como um produto multi-tenant. Se precisar expor fora do loopback:

- faça opt-in explícito;
- use credencial forte;
- coloque atrás de TLS/reverse proxy;
- limite a rede de origem;
- não use um workspace com secrets hardcoded reais;
- aplique controles do sistema operacional/container.
