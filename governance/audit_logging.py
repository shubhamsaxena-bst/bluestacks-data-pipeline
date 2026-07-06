"""
Cross-pipeline audit logging helper. Every notebook and ADF pipeline writes a structured
event here so the platform has a single, queryable audit trail — required for the JD's
"audit protocols and secure data handling procedures to support compliance".
"""
from datetime import datetime, timezone
from pyspark.sql import SparkSession


def write_audit_event(
    spark: SparkSession,
    pipeline_name: str,
    activity: str,
    status: str,
    row_count: int | None = None,
    details: str | None = None,
):
    event = spark.createDataFrame([{
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "pipeline_name": pipeline_name,
        "activity": activity,
        "status": status,
        "row_count": row_count,
        "details": details,
    }])
    event.write.format("delta").mode("append").saveAsTable("bsi_governance.audit_log")


def get_lineage_note(table_name: str) -> str:
    """
    In production, table-level lineage is captured automatically by Unity Catalog /
    Microsoft Purview scanning the Delta transaction log and notebook/ADF metadata - this
    stub documents the manual fallback note stored alongside each Gold table for auditors.
    """
    lineage = {
        "bsi_gold.campaign_roi_daily": "bsi_bronze.ad_campaign_performance + bsi_bronze.crm_leads_opportunities -> bsi_silver.ad_campaign_performance + bsi_silver.crm_leads_opportunities -> bsi_gold.campaign_roi_daily",
        "bsi_gold.funnel_conversion_by_stage": "bsi_bronze.crm_leads_opportunities -> bsi_silver.crm_leads_opportunities -> bsi_gold.funnel_conversion_by_stage",
        "bsi_gold.attribution_channel_performance": "bsi_bronze.web_analytics_events + bsi_silver.dim_campaign -> bsi_silver.web_analytics_events -> bsi_gold.attribution_channel_performance",
        "bsi_gold.email_engagement_summary": "bsi_bronze.email_campaign_engagement -> bsi_silver.email_campaign_engagement -> bsi_gold.email_engagement_summary",
    }
    return lineage.get(table_name, "lineage not documented - flag for governance review")
