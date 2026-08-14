from __future__ import annotations

import pytest

from engineer_kit.config.pipeline_config import (
    PipelineConfigError,
    pipeline_config_from_dict,
    pipeline_config_to_dict,
)
from engineer_kit.profiling.config import connector_from_config
from engineer_kit.profiling.engine import profile_records


def _config_dict(*, dedup=False):
    return {
        "version": 1,
        "name": "orders",
        "connector": {
            "base_url": "https://example.test/orders",
            "pagination": False,
            "incremental": False,
            "dedup": dedup,
        },
        "run_log": False,
    }


def test_declarative_dedup_defaults_false_and_round_trips_primary_key_list():
    default = pipeline_config_from_dict(_config_dict())
    enabled = pipeline_config_from_dict(_config_dict(dedup=["customer_id"]))

    assert default.connector.dedup is False
    assert enabled.connector.dedup == ["customer_id"]
    assert pipeline_config_to_dict(enabled)["connector"]["dedup"] == ["customer_id"]
    connector = connector_from_config(enabled)
    assert connector.dedup_enabled is True
    assert connector.dedup_keys == ("customer_id",)


def test_declarative_dedup_accepts_single_key_shorthand_and_composite_key():
    single = pipeline_config_from_dict(_config_dict(dedup="customer_id"))
    composite = pipeline_config_from_dict(
        _config_dict(dedup=["tenant_id", "customer_id"])
    )

    assert single.connector.dedup == ["customer_id"]
    assert composite.connector.dedup == ["tenant_id", "customer_id"]


def test_declarative_dedup_rejects_true_without_primary_key():
    with pytest.raises(PipelineConfigError, match="dedup=True e ambiguo"):
        pipeline_config_from_dict(_config_dict(dedup=True))


def test_quality_summary_preserves_not_computed_semantics():
    report = profile_records([{"id": 1}, {"id": 1}], "duplicates")

    assert report.quality.duplicate_rows == 1
    assert report.quality.fields_with_missing is None
    assert report.quality.fields_with_nulls is None
    assert report.quality.fields_with_empty is None
    assert "Fields with missing</strong><span>—" in report.to_html()
