# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion: BlueStacks Marketing Data
# MAGIC
# MAGIC **Purpose:** Land raw marketing events/records from ADLS Gen2 `/raw` zone into Bronze
# MAGIC Delta tables with schema enforcement, audit columns, and a quarantine path for
# MAGIC malformed records.
# MAGIC
# MAGIC **Orchestrated by:** ADF pipeline `PL_Master_Orchestration` (Databricks Notebook Activity),
# MAGIC parameters injected via `dbutils.widgets` so the same notebook serves every source
# MAGIC (`ad_campaign_performance`, `web_analytics_events`, `crm_leads_opportunities`,
# MAGIC `email_campaign_engagement`).

# COMMAND ----------
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

sys.path.append("/Workspace/Repos/bluestacks-data-pipeline/notebooks/utils")
from data_quality_checks import quarantine_invalid_records, log_run_metrics
from schema_registry import get_schema

# COMMAND ----------
# Widgets: parameters passed from Azure Data Factory (dynamic, per-source, reusable pattern)
dbutils.widgets.text("source_name", "ad_campaign_performance")
dbutils.widgets.text("ingest_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
dbutils.widgets.text("raw_container_path", "abfss://raw@bsimktadls.dfs.core.windows.net")
dbutils.widgets.text("bronze_container_path", "abfss://bronze@bsimktadls.dfs.core.windows.net")

source_name = dbutils.widgets.get("source_name")
ingest_date = dbutils.widgets.get("ingest_date")
raw_path = f"{dbutils.widgets.get('raw_container_path')}/{source_name}/dt={ingest_date}/"
bronze_table = f"bsi_bronze.{source_name}"
quarantine_table = f"bsi_bronze.{source_name}_quarantine"

print(f"[bronze] source={source_name} ingest_date={ingest_date} raw_path={raw_path}")

# COMMAND ----------
# MAGIC %md ### Schema-on-read enforcement (rejects/quarantines drifted or malformed records
# MAGIC rather than silently failing or corrupting downstream tables)
schema: StructType = get_schema(source_name)

raw_df = (
    spark.read
    .format("json" if source_name != "email_campaign_engagement" else "csv")
    .schema(schema)
    .option("header", source_name == "email_campaign_engagement")
    .option("badRecordsPath", f"{dbutils.widgets.get('raw_container_path')}/_badrecords/{source_name}/")
    .load(raw_path)
)

# COMMAND ----------
# Audit / lineage columns required for governance & audit-readiness (per JD: "audit protocols",
# "secure data handling", "long-term maintainability")
bronze_df = (
    raw_df
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingest_date", F.lit(ingest_date))
    .withColumn("_pipeline_run_id", F.lit(dbutils.widgets.get("source_name") + "-" + ingest_date))
)

clean_df, bad_df = quarantine_invalid_records(bronze_df, source_name)

# COMMAND ----------
# MAGIC %md ### Delta Lake write - ACID append, partitioned, schema evolution controlled
(
    clean_df.write
    .format("delta")
    .mode("append")
    .partitionBy("_ingest_date")
    .option("mergeSchema", "false")  # explicit schema control; drift goes to quarantine, not silently merged
    .saveAsTable(bronze_table)
)

if bad_df.count() > 0:
    (
        bad_df.write.format("delta").mode("append")
        .saveAsTable(quarantine_table)
    )

# COMMAND ----------
log_run_metrics(
    spark,
    stage="bronze",
    source=source_name,
    ingest_date=ingest_date,
    rows_in=raw_df.count(),
    rows_clean=clean_df.count(),
    rows_quarantined=bad_df.count(),
)

dbutils.notebook.exit(f"OK: bronze load complete for {source_name} on {ingest_date}")
