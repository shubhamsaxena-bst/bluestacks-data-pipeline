# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Curated Marketing Analytics Marts
# MAGIC
# MAGIC Produces business-ready aggregates for Power BI / analyst SQL: campaign ROI/ROAS/CPA,
# MAGIC funnel conversion by stage, channel-level attribution performance, and email engagement.
# MAGIC Runs on a Photon-enabled job cluster (heavy aggregation/shuffle workload).

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
# MAGIC %md ### Campaign ROI / ROAS / CPA
# MAGIC Joins ad spend against closed-won deal value attributed to the same campaign to compute
# MAGIC return on ad spend (ROAS) and cost per acquisition (CPA) — the two headline metrics
# MAGIC marketing leadership reviews weekly.
spend = spark.read.format("delta").table("bsi_silver.ad_campaign_performance").filter(
    F.col("report_date") == run_date
)

closed_won = (
    spark.read.format("delta").table("bsi_silver.crm_leads_opportunities")
    .filter((F.col("stage") == "closed_won") & (F.to_date("stage_change_ts") == run_date))
    .groupBy("campaign_id")
    .agg(F.sum("deal_value_usd").alias("revenue_usd"), F.count("*").alias("deals_won"))
)

campaign_roi = (
    spend.groupBy("campaign_id", "channel", "campaign_name", "channel_type")
    .agg(
        F.sum("spend_usd").alias("spend_usd"),
        F.sum("impressions").alias("impressions"),
        F.sum("clicks").alias("clicks"),
        F.sum("conversions").alias("conversions"),
    )
    .join(closed_won, "campaign_id", "left")
    .withColumn("revenue_usd", F.coalesce("revenue_usd", F.lit(0.0)))
    .withColumn("roas", F.round(F.col("revenue_usd") / F.col("spend_usd"), 3))
    .withColumn("cpa_usd", F.round(F.col("spend_usd") / F.greatest(F.col("conversions"), F.lit(1)), 2))
    .withColumn("ctr", F.round(F.col("clicks") / F.greatest(F.col("impressions"), F.lit(1)), 5))
    .withColumn("metric_date", F.lit(run_date))
)

(
    campaign_roi.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"metric_date = '{run_date}'")
    .saveAsTable("bsi_gold.campaign_roi_daily")
)

# COMMAND ----------
# MAGIC %md ### Funnel conversion by stage
# MAGIC Lead -> MQL -> SQL -> Opportunity -> Closed Won, with stage-to-stage conversion rates.
funnel_stages = ["new", "mql", "sql", "opportunity", "closed_won"]

leads = spark.read.format("delta").table("bsi_silver.crm_leads_opportunities").filter(
    F.to_date("stage_change_ts") == run_date
)

stage_counts = (
    leads.groupBy("stage")
    .agg(F.countDistinct("lead_id").alias("lead_count"))
    .withColumn("metric_date", F.lit(run_date))
)

# window-free conversion rate calc via self-join on ordered stage index
stage_index = spark.createDataFrame(
    [(s, i) for i, s in enumerate(funnel_stages)], ["stage", "stage_order"]
)
funnel_ordered = (
    stage_counts.join(stage_index, "stage")
    .orderBy("stage_order")
)

(
    funnel_ordered.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"metric_date = '{run_date}'")
    .saveAsTable("bsi_gold.funnel_conversion_by_stage")
)

# COMMAND ----------
# MAGIC %md ### Channel attribution performance
# MAGIC Website sessions and goal completions by UTM channel, alongside spend, so marketing can
# MAGIC see full-funnel channel performance (not just ad-platform-reported conversions, which are
# MAGIC frequently inflated by the ad platforms' own attribution windows).
web = spark.read.format("delta").table("bsi_silver.web_analytics_events").filter(
    F.to_date("event_ts") == run_date
)

channel_web_perf = (
    web.groupBy("channel_type")
    .agg(
        F.countDistinct("visitor_id").alias("visitors"),
        F.countDistinct("session_id").alias("sessions"),
        F.sum(F.when(F.col("event_type") == "goal_completion", 1).otherwise(0)).alias("goal_completions"),
    )
    .withColumn("conversion_rate", F.round(F.col("goal_completions") / F.greatest(F.col("sessions"), F.lit(1)), 5))
    .withColumn("metric_date", F.lit(run_date))
)

(
    channel_web_perf.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"metric_date = '{run_date}'")
    .saveAsTable("bsi_gold.attribution_channel_performance")
)

# COMMAND ----------
# MAGIC %md ### Email engagement summary
email = spark.read.format("delta").table("bsi_silver.email_campaign_engagement").filter(
    F.to_date("event_ts") == run_date
)

email_summary = (
    email.groupBy("email_campaign_id")
    .pivot("event_type", ["send", "open", "click", "unsubscribe", "bounce"])
    .agg(F.count("*"))
    .withColumn("open_rate", F.round(F.col("open") / F.greatest(F.col("send"), F.lit(1)), 5))
    .withColumn("click_through_rate", F.round(F.col("click") / F.greatest(F.col("open"), F.lit(1)), 5))
    .withColumn("unsubscribe_rate", F.round(F.col("unsubscribe") / F.greatest(F.col("send"), F.lit(1)), 5))
    .withColumn("metric_date", F.lit(run_date))
)

(
    email_summary.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"metric_date = '{run_date}'")
    .saveAsTable("bsi_gold.email_engagement_summary")
)

# COMMAND ----------
# MAGIC %md ### Table maintenance (cost + query performance)
for tbl in [
    "bsi_gold.campaign_roi_daily", "bsi_gold.funnel_conversion_by_stage",
    "bsi_gold.attribution_channel_performance", "bsi_gold.email_engagement_summary",
]:
    spark.sql(f"OPTIMIZE {tbl} ZORDER BY (metric_date)")

dbutils.notebook.exit(f"OK: gold aggregates complete for {run_date}")
