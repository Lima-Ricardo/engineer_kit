# CLI

After installation:

```bash
engineer_kit --help
```

## Run a YAML configuration

```bash
engineer_kit run-config pipelines/orders.yaml
```

The CLI resolves adapters and opens local resources according to the configuration.

## List adapters

```bash
engineer_kit adapters
```

Use this to inspect integrations available in the current environment.

## Local Lab

```bash
engineer_kit ui --workspace .
```

Run `engineer_kit ui --help` for bind and authentication options. Defaults are intentionally loopback/local.

## Legacy/custom Python modules

Compatibility paths remain available for programmatic/custom execution. Treat modules explicitly imported by the operator as trusted Python code; the CLI is not a sandbox.
