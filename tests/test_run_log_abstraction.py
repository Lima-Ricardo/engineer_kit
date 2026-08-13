from engineer_kit.orchestration.pipeline import Pipeline
from engineer_kit.storage.destination import LoadResult
from engineer_kit.storage.run_log import RunLogBackend, RunLogEntry
from engineer_kit.storage.schema import EndpointSchema


class MemoryRunLogBackend(RunLogBackend):
    def __init__(self):
        self.entries = []

    def record(self, entry: RunLogEntry) -> None:
        self.entries.append(entry)


class FakeConnector:
    name = "memory_api"

    def __init__(self):
        self.committed = False

    def extract(self):
        yield {"id": "1"}

    def commit_watermark(self):
        self.committed = True


class MemoryDestination:
    def load(self, connector_name, endpoint, schema, records):
        rows = list(records)
        return LoadResult(table="memory.bronze", rows_loaded=len(rows), extra_fields_seen=[])


def test_pipeline_can_audit_without_duckdb():
    connector = FakeConnector()
    run_log = MemoryRunLogBackend()
    pipeline = Pipeline(
        connector=connector,
        schema=EndpointSchema.from_names(["id"]),
        destination=MemoryDestination(),
        run_log_store=run_log,
    )

    result = pipeline.run()

    assert result.success
    assert connector.committed is True
    assert len(run_log.entries) == 1
    assert run_log.entries[0].connector_name == "memory_api"
    assert run_log.entries[0].status == "success"
    assert run_log.entries[0].rows_loaded == 1
