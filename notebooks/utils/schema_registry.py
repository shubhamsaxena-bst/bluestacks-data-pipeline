"""
Centralized schema registry for all Bronze marketing-data sources.

Explicit schemas (rather than inferSchema) give: (1) fast reads with no extra scan pass,
(2) predictable typing, (3) a single point of change when a source's contract evolves —
directly addresses the JD's "data quality, consistency, and governance across legacy and
cloud-based pipelines" and "metadata management" requirements.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, DateType
)

_SCHEMAS = {
    "ad_campaign_performance": StructType([
        StructField("campaign_id", StringType(), False),
        StructField("channel", StringType(), False),          # google_ads | meta_ads
        StructField("report_date", DateType(), False),
        StructField("impressions", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("spend_usd", DoubleType(), True),
        StructField("conversions", IntegerType(), True),
        StructField("currency", StringType(), True),
    ]),
    "web_analytics_events": StructType([
        StructField("event_id", StringType(), False),
        StructField("visitor_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("event_ts", TimestampType(), False),
        StructField("event_type", StringType(), True),  # page_view | goal_completion | form_submit
        StructField("page_url", StringType(), True),
        StructField("utm_campaign_id", StringType(), True),
        StructField("utm_channel", StringType(), True),
        StructField("country_code", StringType(), True),
    ]),
    "crm_leads_opportunities": StructType([
        StructField("lead_id", StringType(), False),
        StructField("opportunity_id", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("stage", StringType(), False),  # new | mql | sql | opportunity | closed_won | closed_lost
        StructField("stage_change_ts", TimestampType(), False),
        StructField("deal_value_usd", DoubleType(), True),
        StructField("owner_region", StringType(), True),
    ]),
    "email_campaign_engagement": StructType([
        StructField("engagement_id", StringType(), False),
        StructField("email_campaign_id", StringType(), False),
        StructField("visitor_id", StringType(), True),
        StructField("event_type", StringType(), True),  # send | open | click | unsubscribe | bounce
        StructField("event_ts", TimestampType(), False),
        StructField("linked_ad_campaign_id", StringType(), True),
    ]),
}


def get_schema(source_name: str) -> StructType:
    if source_name not in _SCHEMAS:
        raise ValueError(f"No registered schema for source '{source_name}'. Update schema_registry.py.")
    return _SCHEMAS[source_name]
