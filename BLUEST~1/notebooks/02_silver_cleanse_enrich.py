# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: Cleansing, Deduplication, Enrichment
# MAGIC
# MAGIC Conforms Bronze marketing data into governed, deduplicated, enriched Silver Delta tables
# MAGIC using MERGE (upsert) semantics — demonstrates Delta Lake ACID compliance, versioning and
# MAGIC time travel for reliable, reprocessable data-lake operations.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

dbutils.widgets.text("source_name", "ad_campaign_performance")
dbutils.widgets.text("ingest_date", "")
source_name = dbutils.widgets.get("source_name")
ingest_date = dbutils.widgets.get("ingest_date")

bronze_table = f"bsi_bronze.{source_name}"
silver_table = f"bsi_silver.{source_name}"
dim_campaign_table = "bsi_silver.dim_campaign"

# COMMAND ----------
# MAGIC %md ### Time-travel read: reprocess only the target ingest date's bronze version, not the
# MAGIC whole table history — cheap, reproducible reruns if a downstream bug is found later.
bronze_df = (
    spark.read.format("delta").table(bronze_table)
    .filter(F.col("_ingest_date") == ingest_date)
)

# COMMAND ----------
# MAGIC %md ### Cleansing & deduplication
# MAGIC Marketing event/log sources are treated as append-only facts (dedup on natural key +
# MAGIC timestamp), not overwritten state, so a lead's full stage-progression history is preserved
# MAGIC for funnel analysis rather than collapsed to "current stage only".
dedup_keys = {
    "ad_campaign_performance": ["campaign_id", "channel", "report_date"],
    "web_analytics_events": ["event_id"],
    "crm_leads_opportunities": ["lead_id", "stage", "stage_change_ts"],
    "email_campaign_engagement": ["engagement_id"],
}[source_name]

window = F.row_number().over(
    Window.partitionBy(*dedup_keys).orderBy(F.col("_ingest_ts").desc())
)

not_null_cols = {
    "ad_campaign_performance": ["campaign_id", "report_date"],
    "web_analytics_events": ["visitor_id", "event_ts"],
    "crm_leads_opportunities": ["lead_id", "stage_change_ts"],
    "email_campaign_engagement": ["email_campaign_id", "event_ts"],
}[source_name]

cleansed_df = (
    bronze_df
    .withColumn("_rn", window)
    .filter("_rn = 1")
    .drop("_rn")
)
for c in not_null_cols:
    cleansed_df = cleansed_df.filter(F.col(c).isNotNull())

# COMMAND ----------
# MAGIC %md ### Enrichment - join with conformed campaign dimension (SCD Type 1 via MERGE)
# MAGIC `dim_campaign` (channel, campaign_name, budget, start/end date) is the single source of
# MAGIC truth for campaign attributes, so "channel" and "campaign_name" are always classified
# MAGIC consistently across ad spend, web traffic, and email engagement marts.
campaign_key_col = {
    "ad_campaign_performance": "campaign_id",
    "web_analytics_events": "utm_campaign_id",
    "crm_leads_opportunities": "campaign_id",
    "email_campaign_engagement": "linked_ad_campaign_id",
}[source_name]

if campaign_key_col in cleansed_df.columns:
    dim_campaign_df = spark.read.format("delta").table(dim_campaign_table)
    enriched_df = (
        cleansed_df.alias("f")
        .join(
            dim_campaign_df.alias("d"),
            F.col(f"f.{campaign_key_col}") == F.col("d.campaign_id"),
            how="left",
        )
        .select(
            "f.*",
            F.coalesce("d.campaign_name", F.lit("unattributed")).alias("campaign_name"),
            F.coalesce("d.channel_type", F.lit("unknown")).alias("channel_type"),
        )
    )
else:
    enriched_df = cleansed_df

# COMMAND ----------
# MAGIC %md ### Delta MERGE upsert - ACID guarantee, idempotent reruns
if DeltaTable.isDeltaTable(spark, f"/mnt/delta/silver/{source_name}"):
    target = DeltaTable.forName(spark, silver_table)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in dedup_keys])
    (
        target.alias("t")
        .merge(enriched_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        enriched_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("_ingest_date")
        .saveAsTable(silver_table)
    )

# COMMAND ----------
# MAGIC %md ### Time travel example (used for audits / incident investigation)
# MAGIC ```sql
# MAGIC -- Compare today's silver table against yesterday's version for reconciliation
# MAGIC SELECT * FROM bsi_silver.ad_campaign_performance VERSION AS OF 12
# MAGIC EXCEPT
# MAGIC SELECT * FROM bsi_silver.ad_campaign_performance VERSION AS OF 11;
# MAGIC
# MAGIC DESCRIBE HISTORY bsi_silver.ad_campaign_performance;
# MAGIC ```

dbutils.notebook.exit(f"OK: silver merge complete for {source_name} on {ingest_date}")
