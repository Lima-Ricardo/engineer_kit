from __future__ import annotations

from datetime import date

import pytest

from engineer_kit.adapters.files.state_store import JsonFileStateStore
from engineer_kit.connectors.dedup import ExactRowDeduplicator
from engineer_kit.connectors.rest import RestConnector
from engineer_kit.profiling.engine import UnknownProfileMetricError, profile_records


class _Response:
    headers = {"Content-Type": "application/json"}
    status_code = 200
    content = b"profile-payload"

    def json(self):
        duplicate = {
            "id": 1,
            "email": None,
            "name": "",
            "active": True,
            "tags": [],
            "meta": {"score": 10},
        }
        return {
            "data": [
                duplicate,
                dict(duplicate),
                {
                    "id": 2,
                    "name": "   ",
                    "active": False,
                    "tags": ["new"],
                    "meta": {"score": 20},
                },
            ]
        }


class _Client:
    def __init__(self):
        self.calls = 0

    def request(self, method, **kwargs):
        self.calls += 1
        return _Response()


def _connector(*, dedup: bool = False, client: _Client | None = None, **kwargs):
    return RestConnector(
        base_url="https://example.test/items",
        pagination=False,
        records="data",
        dedup=dedup,
        http_client=client or _Client(),
        **kwargs,
    )


def test_profile_without_selectors_runs_complete_profile_on_native_json_types():
    report = _connector().profile()

    assert report.records_analyzed == 3
    assert report.has("duplicates")
    assert report.has("cardinality")
    assert report.duplicates is not None
    assert report.duplicates.duplicate_rows == 1
    assert report.duplicates.unique_rows == 2
    assert report.fields["id"].types == {"integer": 3}
    assert report.fields["active"].types == {"boolean": 3}
    assert report.fields["email"].nulls == 2
    assert report.fields["email"].missing == 1
    assert report.fields["name"].empty_strings == 2
    assert report.fields["name"].blank_strings == 1
    assert report.fields["tags"].empty_arrays == 2
    assert report.fields["meta"].types == {"object": 3}
    assert report.fields["meta.score"].cardinality is not None
    assert report.fields["meta.score"].cardinality.count == 2


def test_profile_metric_selection_changes_execution_contract_not_only_rendering():
    report = _connector().profile("nulls", "missing")

    assert report.requested_metrics == ("missing", "nulls")
    assert report.duplicates is None
    assert not report.has("duplicates")
    assert report.fields["email"].nulls == 2
    assert report.fields["email"].missing == 1
    assert report.fields["email"].types is None
    assert report.fields["email"].cardinality is None


def test_profile_quality_preset_and_field_filter_are_composable():
    report = _connector().profile("quality", fields=["email"])

    assert set(report.fields) == {"email"}
    assert report.duplicates is not None
    assert report.quality.duplicate_rows == 1
    assert report.quality.fields_with_missing == 1
    assert report.quality.fields_with_nulls == 1


def test_unknown_profile_metric_fails_before_http_request():
    client = _Client()
    connector = _connector(client=client)

    with pytest.raises(UnknownProfileMetricError):
        connector.profile("nullls")

    assert client.calls == 0


def test_profile_does_not_commit_incremental_state(tmp_path):
    state_path = tmp_path / "state.json"
    state = JsonFileStateStore(state_path)
    connector = _connector(
        incremental=True,
        state_store=state,
        initial_start=date(2026, 1, 1),
    )

    report = connector.profile("count", end=date(2026, 1, 2))

    assert report.records_analyzed == 3
    assert state.get_watermark(connector.state_key) is None
    assert not state_path.exists()


def test_dedup_is_off_by_default_and_true_filters_exact_output_rows():
    default_records = _connector().collect()
    dedup_records = _connector(dedup=True).collect()

    assert len(default_records) == 3
    assert len(dedup_records) == 2
    assert default_records[0] == default_records[1]
    assert dedup_records[0] != dedup_records[1]


def test_profile_still_reports_source_duplicates_when_ingestion_dedup_is_enabled():
    connector = _connector(dedup=True)

    report = connector.profile("duplicates")
    records = connector.collect()

    assert report.duplicates is not None
    assert report.duplicates.duplicate_rows == 1
    assert len(records) == 2


def test_dedup_occurs_after_select_on_the_emitted_dataset():
    connector = _connector(dedup=True, select=["active"])

    records = connector.collect()

    assert records == [{"active": "True"}, {"active": "False"}]


def test_nested_array_presence_is_counted_per_record_not_per_element():
    report = profile_records(
        [
            {"items": [{"sku": "A"}, {"sku": "B"}]},
            {"items": [{"sku": "A"}]},
            {},
        ],
        "missing",
        "cardinality",
    )

    sku = report.fields["items[].sku"]
    assert sku.records_present == 2
    assert sku.occurrences == 3
    assert sku.missing == 1
    assert sku.cardinality is not None
    assert sku.cardinality.count == 2


def test_profile_report_terminal_and_html_are_aggregate_only_and_escape_paths():
    secret = "VERY_SECRET_VALUE_123"
    report = profile_records(
        [{"<script>": secret}, {"<script>": secret}],
        "quality",
        "cardinality",
    )

    text = str(report)
    html = report.to_html()

    assert "DATA PROFILE v1" in text
    assert "DATA QUALITY" in text
    assert secret not in text
    assert secret not in html
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_disk_deduplicator_deletes_temporary_store_on_close():
    tracker = ExactRowDeduplicator()
    path = tracker.path
    assert path.exists()
    assert tracker.add({"id": 1}) is True
    assert tracker.add({"id": 1}) is False

    tracker.close()

    assert not path.exists()
