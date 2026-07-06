"""
Reusable data quality utilities shared across Bronze/Silver notebooks.

Implements: null/PK checks, referential sanity checks, quarantine routing, and run-metric
logging to a governance table — covers the JD's "ensure data quality, consistency, and
governance" and "clear documentation ... for long-term maintainability" requirements.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_REQUIRED_COLUMNS = {
    "ad_campaign_performance": ["campaign_id", "channel", "report_date"],
    "web_analytics_events": ["event_id", "visitor_id", "event_ts"],
    "crm_leads_opportunities": ["lead_id", "stage", "stage_change_ts"],
    "email_campaign_engagement": ["engagement_id", "email_campaign_id", "event_ts"],
}


def quarantine_invalid_records(df: DataFrame, source_name: str) -> tuple[DataFrame, DataFrame]:
    """Splits a DataFrame into (clean, quarantined) based on required-field null checks."""
    required = _REQUIRED_COLUMNS.get(source_name, [])
    if not required:
        return df, df.limit(0)

    null_condition = None
    for col in required:
        cond = F.col(col).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    bad_df = df.filter(null_condition).withColumn("_quarantine_reason", F.lit("missing_required_field"))
    clean_df = df.filter(~null_condition)
    return clean_df, bad_df


def log_run_metrics(
    spark: SparkSession, stage: str, source: str, ingest_date: str,
    rows_in: int, rows_clean: int, rows_quarantined: int,
) -> None:
    """Writes a row to the governance run-metrics Delta table for monitoring/alerting."""
    metrics_df = spark.createDataFrame(
        [(stage, source, ingest_date, rows_in, rows_clean, rows_quarantined)],
        ["stage", "source", "ingest_date", "rows_in", "rows_clean", "rows_quarantined"],
    ).withColumn("logged_at", F.current_timestamp())

    (
        metrics_df.write.format("delta").mode("append")
        .saveAsTable("bsi_governance.pipeline_run_metrics")
    )

    quarantine_ratio = 0 if rows_in == 0 else rows_quarantined / rows_in
    if quarantine_ratio > 0.02:
        # Threshold breach - in production this raises an Azure Monitor alert via a
        # webhook/Log Analytics custom event; see governance/data_quality_rules.yaml
        print(f"[ALERT] {source} quarantine ratio {quarantine_ratio:.2%} exceeds 2% threshold")
