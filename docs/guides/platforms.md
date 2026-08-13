# AWS, Google Cloud e Azure

A biblioteca separa **source protocol** de **runtime/storage**.

```text
RestConnector
     │
     └── funciona igual em qualquer cloud

Destination/storage
├── filesystem/mount
├── S3
├── GCS
├── ADLS/OneLake
└── Delta
```

## Por que não existe `AWSRestConnector`?

Porque AWS não muda a semântica REST da origem. Criar uma classe por cloud duplicaria HTTP, paginação e incremental.

Cloud-specific entra onde realmente muda:

- URI do storage;
- autenticação do object store;
- IAM/workload identity;
- catálogo/metastore;
- paths e mounts;
- rede/egress.

## AWS

Prefira IAM Role / workload identity em vez de chaves long-lived. Quando usar Delta sobre S3, passe as opções exigidas pelo runtime/adapter sem hardcode de credenciais reais.

## Google Cloud

Prefira service account/workload identity gerenciado pelo ambiente. GCS pertence à camada de storage, não ao connector REST.

## Azure / OneLake

Prefira Managed Identity/Workload Identity e permissões mínimas. Fabric e ADLS podem expor paths montados ou Delta de acordo com o runtime.

## Credenciais

Nunca coloque access keys reais em `README`, notebook versionado ou YAML commitado. Use `SecretProvider`, environment, arquivo montado ou identidade da plataforma.
