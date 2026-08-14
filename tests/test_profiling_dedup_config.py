from __future__ import annotations

import pytest

from engineer_kit.capabilities import capability_manifest
from engineer_kit.config.pipeline_config import (
    PipelineConfigError,
    pipeline_config_from_dict,
    pipeline_config_to_dict,
)
from engineer_kit.profiling.config import connector_from_config
from engineer_kit.profiling.engine import profile_records


def _config_dict(*, primary_key=None, dedup=False):
    connector = {
        "base_url": "https://example.test/orders",
        "pagination": False,
        "incremental": False,
        "dedup": dedup,
    }
    if primary_key is not None:
        connector["primary_key"] = primary_key
    return {
        "version": 1,
        "name": "orders",
        "connector": connector,
        "run_log": False,
    }


def test_declarative_primary_key_and_dedup_are_independent_and_round_trip():
    default = pipeline_config_from_dict(_config_dict())
    mapped = pipeline_config_from_dict(_config_dict(primary_key=["customer_id"]))
    enabled = pipeline_config_from_dict(
        _config_dict(primary_key=["customer_id"], dedup=True)
    )

    assert default.connector.primary_key is None
    assert default.connector.dedup is False
    assert mapped.connector.primary_key == ["customer_id"]
    assert mapped.connector.dedup is False
    assert enabled.connector.primary_key == ["customer_id"]
    assert enabled.connector.dedup is True

    serialized = pipeline_config_to_dict(enabled)["connector"]
    assert serialized["primary_key"] == ["customer_id"]
    assert serialized["dedup"] is True

    connector = connector_from_config(enabled)
    assert connector.primary_key == ("customer_id",)
    assert connector.dedup_enabled is True


def test_declarative_primary_key_accepts_single_and_composite_identity():
    single = pipeline_config_from_dict(_config_dict(primary_key="customer_id"))
    composite = pipeline_config_from_dict(
        _config_dict(primary_key=["tenant_id", "customer_id"])
    )

    assert single.connector.primary_key == ["customer_id"]
    assert composite.connector.primary_key == ["tenant_id", "customer_id"]
    assert single.connector.dedup is False
    assert composite.connector.dedup is False


def test_declarative_dedup_true_requires_primary_key():
    with pytest.raises(PipelineConfigError, match="dedup=true exige connector.primary_key"):
        pipeline_config_from_dict(_config_dict(dedup=True))


def test_declarative_dedup_must_be_boolean():
    with pytest.raises(PipelineConfigError, match="connector.dedup deve ser booleano"):
        pipeline_config_from_dict(_config_dict(dedup="false"))

    with pytest.raises(PipelineConfigError, match="connector.dedup deve ser booleano"):
        pipeline_config_from_dict(_config_dict(dedup=["customer_id"]))


def test_declarative_primary_key_rejects_boolean_empty_and_non_string_members():
    # Both booleans are invalid identities. In particular, ``false`` must not be
    # interpreted as the historical "dedup disabled" sentinel now that PK and
    # deduplication policy are separate fields.
    for primary_key in (True, False, "", [], ["id", 123]):
        with pytest.raises(PipelineConfigError, match="connector.primary_key invalido"):
            pipeline_config_from_dict(_config_dict(primary_key=primary_key))


def test_configured_primary_key_is_available_to_profile_with_dedup_disabled():
    config = pipeline_config_from_dict(
        _config_dict(primary_key=["customer_id"], dedup=False)
    )
    connector = connector_from_config(config)

    assert connector.primary_key == ("customer_id",)
    assert connector.dedup_enabled is False


def test_capability_manifest_separates_primary_key_identity_from_dedup_policy():
    connector = capability_manifest()["connector"]

    assert "primary_key" in connector["intent_fields"]
    assert connector["primary_key"]["required_for_dedup"] is True
    assert connector["dedup"]["type"] == "boolean"
    assert connector["dedup"]["identity"] == "primary_key"
    assert connector["dedup"]["default"] is False


def test_quality_summary_preserves_not_computed_semantics():
    report = profile_records([{"id": 1}, {"id": 1}], "duplicates")

    assert report.quality.duplicate_rows == 1
    assert report.quality.fields_with_missing is None
    assert report.quality.fields_with_nulls is None
    assert report.quality.fields_with_empty is None
    assert "Fields with missing</strong><span>—" in report.to_html()
