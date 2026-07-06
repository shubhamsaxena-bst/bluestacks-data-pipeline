"""
Unit tests for shared PySpark transformation logic.

Run in CI (see cicd/.github/workflows/ci-databricks.yml) with a local SparkSession — no
Databricks cluster required, so PRs get fast feedback before deployment. Addresses the JD's
"SDLC from design through deployment", "maintainable and efficient ETL logic... following best
practices", and "Github, CI/CD pipelines" requirements.
"""
import sys
import os
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "utils"))
from data_quality_checks import quarantine_invalid_records
from schema_registry import get_schema


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("bsi-unit-tests")
        .getOrCreate()
    )


def test_get_schema_known_source():
    schema = get_schema("crm_leads_opportunities")
    assert "lead_id" in schema.fieldNames()
    assert "stage" in schema.fieldNames()


def test_get_schema_unknown_source_raises():
    with pytest.raises(ValueError):
        get_schema("not_a_real_source")


def test_quarantine_splits_null_required_fields(spark):
    df = spark.createDataFrame(
        [("lead1", "mql", "2026-07-01 00:00:00"), (None, "mql", "2026-07-01 00:00:00")],
        ["lead_id", "stage", "stage_change_ts"],
    )
    clean_df, bad_df = quarantine_invalid_records(df, "crm_leads_opportunities")
    assert clean_df.count() == 1
    assert bad_df.count() == 1
    assert bad_df.first()["_quarantine_reason"] == "missing_required_field"


def test_quarantine_passthrough_for_unregistered_source(spark):
    df = spark.createDataFrame([("a",)], ["col1"])
    clean_df, bad_df = quarantine_invalid_records(df, "unregistered_source")
    assert clean_df.count() == 1
    assert bad_df.count() == 0
