# AWS, Google Cloud, and Azure

The library separates **source protocol** from **runtime/storage**.

```text
RestConnector
     │
     └── works the same in every cloud

Destination/storage
├── filesystem/mount
├── S3
├── GCS
├── ADLS/OneLake
└── Delta
```

## Why is there no `AWSRestConnector`?

AWS does not change the REST semantics of the source API. A connector per cloud would duplicate HTTP, pagination, and incremental logic.

Cloud-specific behavior belongs where the platform actually changes:

- storage URI;
- object-store authentication;
- IAM/workload identity;
- catalog/metastore;
- paths and mounts;
- network/egress.

## AWS

Prefer IAM roles or workload identity over long-lived keys. When using Delta over S3, pass runtime/adapter storage options without hardcoding real credentials.

## Google Cloud

Prefer service accounts or workload identity managed by the environment. GCS belongs to the storage layer, not the REST connector.

## Azure / OneLake

Prefer Managed Identity/Workload Identity and least-privilege access. Fabric, OneLake, and ADLS can expose mounted paths or Delta depending on the runtime.

## Credentials

Never place real access keys in a README, versioned notebook, or committed YAML. Use `SecretProvider`, environment variables, mounted files, or platform identity.
