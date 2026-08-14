from __future__ import annotations

from datetime import date

import pytest

from engineer_kit.adapters.files.state_store import JsonFileStateStore
from engineer_kit.connectors.dedup import (
    ExactKeyDeduplicator,
    ExactRowDeduplicator,
    InvalidDeduplicationKeyError,
)
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
                {**duplicate, "name": "changed payload"},
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


def _connector(
    *,
    primary_key=None,
    dedup=False,
    client: _Client | None = None,
    **kwargs,
):
    return RestConnector(
        base_url="https://example.test/items",
        pagination=False,
        records="data",
        primary_key=primary_key,
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
    assert report.duplicates.duplicate_rows == 0
    assert report.duplicates.unique_rows == 3
    assert report.fields["id"].types == {"integer": 3}
    assert report.fields["active"].types == {"boolean": 3}
    assert report.fields["email"].nulls == 2
    assert report.fields["email"].missing == 1
    assert report.fields["name"].empty_strings == 1
    assert report.fields["name"].blank_strings == 1
    assert report.fields["tags"].empty_arrays == 2
    assert report.fields["meta"].types == {"object": 3}
    assert report.fields["meta.score"].cardinality is not None
    assert report.fields["meta.score"].cardinality.count == 2


def test_profile_candidate_pk_reports_duplicate_keys_even_when_rows_differ():
    report = _connector().profile("duplicates", key="id")

    assert report.duplicates is not None
    assert report.duplicates.key_fields == ("id",)
    assert report.duplicates.duplicate_rows == 1
    assert report.duplicates.unique_rows == 2
    assert report.duplicates.invalid_key_rows == 0
    assert report.duplicates.key_complete is True
    assert report.duplicates.key_unique is False


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
    assert report.quality.duplicate_rows == 0
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


def test_primary_key_does_not_enable_dedup_by_itself():
    connector = _connector(primary_key=["id"])

    records = connector.collect()
    report = connector.profile("duplicates")

    assert connector.primary_key == ("id",)
    assert connector.dedup_enabled is False
    assert connector.dedup_keys is None
    assert len(records) == 3
    assert report.duplicates is not None
    assert report.duplicates.key_fields == ("id",)
    assert report.duplicates.duplicate_rows == 1


def test_explain_reports_identity_and_dedup_policy_separately():
    connector = _connector(primary_key=["id"], dedup=False)

    plan = connector.explain()

    assert plan["primary_key"] == ["id"]
    assert plan["dedup"] is False


def test_dedup_is_off_by_default_and_enabled_policy_filters_complete_records_by_pk():
    default_records = _connector(primary_key=["id"]).collect()
    dedup_records = _connector(primary_key=["id"], dedup=True).collect()

    assert len(default_records) == 3
    assert len(dedup_records) == 2
    assert default_records[0]["id"] == default_records[1]["id"]
    assert default_records[0]["name"] != default_records[1]["name"]
    assert [record["id"] for record in dedup_records] == ["1", "2"]


def test_dedup_true_requires_explicit_primary_key():
    with pytest.raises(ValueError, match="dedup=True exige primary_key"):
        _connector(dedup=True)


def test_profile_reuses_configured_primary_key_even_when_dedup_is_disabled():
    connector = _connector(primary_key=["id"], dedup=False)

    report = connector.profile("duplicates")
    records = connector.collect()

    assert report.duplicates is not None
    assert report.duplicates.key_fields == ("id",)
    assert report.duplicates.duplicate_rows == 1
    assert len(records) == 3


def test_dedup_occurs_after_select_on_the_emitted_dataset():
    connector = _connector(
        primary_key=["customer_id"],
        dedup=True,
        select={"id": "customer_id", "active": "active"},
    )

    records = connector.collect()

    assert records == [
        {"customer_id": "1", "active": "True"},
        {"customer_id": "2", "active": "False"},
    ]


def test_primary_key_must_reference_an_emitted_alias_after_select_even_if_dedup_off():
    with pytest.raises(ValueError, match="colunas emitidas"):
        _connector(
            primary_key=["id"],
            dedup=False,
            select={"id": "customer_id", "active": "active"},
        )


def test_legacy_dedup_key_shorthand_is_migrated_with_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="primary_key"):
        connector = RestConnector(
            base_url="https://example.test/items",
            pagination=False,
            records="data",
            dedup=["id"],
            http_client=_Client(),
        )

    assert connector.primary_key == ("id",)
    assert connector.dedup_enabled is True
    assert len(connector.collect()) == 2


def test_dedup_supports_composite_primary_keys():
    tracker = ExactKeyDeduplicator(["tenant_id", "customer_id"])
    try:
        assert tracker.add({"tenant_id": "a", "customer_id": 1, "value": "x"}) is True
        assert tracker.add({"tenant_id": "a", "customer_id": 1, "value": "y"}) is False
        assert tracker.add({"tenant_id": "b", "customer_id": 1, "value": "z"}) is True
    finally:
        tracker.close()


def test_dedup_pk_must_be_present_non_null_non_blank_and_scalar():
    tracker = ExactKeyDeduplicator("id")
    try:
        for record in ({}, {"id": None}, {"id": "  "}, {"id": [1]}):
            with pytest.raises(InvalidDeduplicationKeyError):
                tracker.add(record)
    finally:
        tracker.close()


def test_profile_counts_invalid_candidate_pk_rows_instead_of_aborting():
    report = profile_records(
        [{"id": 1}, {"id": None}, {}, {"id": 1}],
        "duplicates",
        key="id",
    )

    assert report.duplicates is not None
    assert report.duplicates.duplicate_rows == 1
    assert report.duplicates.invalid_key_rows == 2
    assert report.duplicates.unique_rows == 1
    assert (
        report.duplicates.unique_rows
        + report.duplicates.duplicate_rows
        + report.duplicates.invalid_key_rows
        == report.records_analyzed
    )
    assert report.quality.invalid_key_rows == 2


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
    assert 'data-field="<script>"' not in html
    assert "&lt;script&gt;" in html


def test_profile_html_is_self_contained_themed_bilingual_and_keeps_text_compatible():
    report = profile_records([{"id": 1}, {"name": None}], "quality", "cardinality")
    text_before = report.to_text()

    html = report.to_html(language="pt-BR")

    assert '<html lang="pt-BR">' in html
    assert "--bg:#1e1e1e" in html
    assert 'data-theme-choice="light"' in html
    assert 'data-theme-choice="dark"' in html
    assert 'data-lang="pt-BR"' in html
    assert 'data-lang="en"' in html
    assert "ek-language" in html
    assert "ek-theme" in html
    assert "data-profile" not in html  # standalone does not copy Local Lab/sidebar markup
    assert "<link" not in html
    assert "src=" not in html
    assert report.to_text() == text_before


def test_profile_html_rejects_unknown_initial_language():
    report = profile_records([{"id": 1}], "count")

    with pytest.raises(ValueError, match="language"):
        report.to_html(language="auto")


def test_disk_deduplicator_deletes_temporary_store_on_close():
    tracker = ExactRowDeduplicator()
    path = tracker.path
    assert path.exists()
    assert tracker.add({"id": 1}) is True
    assert tracker.add({"id": 1}) is False

    tracker.close()

    assert not path.exists()
