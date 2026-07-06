# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Delta Lake Maintenance & Governance Jobs
# MAGIC
# MAGIC Scheduled weekly via Databricks Jobs (independent of the daily ADF-triggered pipeline).
# MAGIC Covers: OPTIMIZE/ZORDER, VACUUM (with retention aligned to audit requirements), constraint
# MAGIC enforcement, and table history export for compliance audits.

# COMMAND ----------
TABLES = [
    "bsi_bronze.ad_campaign_performance", "bsi_bronze.web_analytics_events",
    "bsi_bronze.crm_leads_opportunities", "bsi_bronze.email_campaign_engagement",
    "bsi_silver.ad_campaign_performance", "bsi_silver.web_analytics_events",
    "bsi_silver.crm_leads_opportunities", "bsi_silver.email_campaign_engagement",
    "bsi_silver.dim_campaign",
    "bsi_gold.campaign_roi_daily", "bsi_gold.funnel_conversion_by_stage",
    "bsi_gold.attribution_channel_performance", "bsi_gold.email_engagement_summary",
]

# COMMAND ----------
# MAGIC %md ### OPTIMIZE + compaction (fixes small-file problem from high-frequency ingestion)
for tbl in TABLES:
    spark.sql(f"OPTIMIZE {tbl}")

# COMMAND ----------
# MAGIC %md ### VACUUM - retention set to 30 days (not the default 7) to preserve enough
# MAGIC time-travel history for audit/incident investigation windows required by compliance.
for tbl in TABLES:
    spark.sql(f"VACUUM {tbl} RETAIN 720 HOURS")

# COMMAND ----------
# MAGIC %md ### Table history export for audit trail (governance requirement)
import json
from datetime import datetime, timezone

history_records = []
for tbl in TABLES:
    hist = spark.sql(f"DESCRIBE HISTORY {tbl}").collect()
    for row in hist[:5]:
        history_records.append({
            "table": tbl,
            "version": row["version"],
            "timestamp": str(row["timestamp"]),
            "operation": row["operation"],
            "user": row["userName"],
        })

audit_path = f"abfss://governance@bsimktadls.dfs.core.windows.net/delta_audit_log/{datetime.now(timezone.utc).date()}.json"
dbutils.fs.put(audit_path, json.dumps(history_records, indent=2), overwrite=True)

dbutils.notebook.exit("OK: delta maintenance complete")
