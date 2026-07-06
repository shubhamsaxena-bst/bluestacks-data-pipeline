# Databricks Cluster & Job Configuration Reference

## Interactive (dev) cluster policy
```json
{
  "name": "bsi-dev-shared-policy",
  "definition": {
    "spark_version": {"type": "fixed", "value": "14.3.x-scala2.12"},
    "node_type_id": {"type": "allowlist", "values": ["Standard_DS3_v2", "Standard_DS4_v2"]},
    "autotermination_minutes": {"type": "fixed", "value": 45, "hidden": true},
    "autoscale.min_workers": {"type": "range", "minValue": 1, "maxValue": 2},
    "autoscale.max_workers": {"type": "range", "minValue": 2, "maxValue": 6},
    "custom_tags.team": {"type": "fixed", "value": "data-engineering"},
    "custom_tags.cost_center": {"type": "fixed", "value": "BSI-DE-001"},
    "spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"}
  }
}
```

## Job cluster (ephemeral, cost-optimized, used by ADF Databricks-Notebook activity)
```json
{
  "new_cluster": {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS4_v2",
    "driver_node_type_id": "Standard_DS4_v2",
    "num_workers": 0,
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "spark_conf": {
      "spark.databricks.delta.optimizeWrite.enabled": "true",
      "spark.databricks.delta.autoCompact.enabled": "true",
      "spark.sql.shuffle.partitions": "auto",
      "spark.databricks.io.cache.enabled": "true"
    },
    "azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE", "spot_bid_max_price": -1},
    "custom_tags": {"team": "data-engineering", "pipeline": "bsi-marketing", "cost_center": "BSI-DE-001"},
    "init_scripts": [{"workspace": {"destination": "/Shared/init-scripts/install-libs.sh"}}]
  },
  "libraries": [{"pypi": {"package": "great-expectations==0.18.19"}}]
}
```

**Cost/performance notes**
- Job clusters (not all-purpose) are used for every scheduled ADF/Databricks Jobs run — billed only for
  run duration, terminate automatically, avoiding idle all-purpose cluster spend.
- Spot-with-fallback on worker nodes cuts compute cost ~60-70% for non-time-critical Silver/Gold batch
  jobs; Bronze streaming/near-real-time jobs pin on-demand nodes only.
- `optimizeWrite` + `autoCompact` avoid the small-file problem from high-frequency telemetry ingestion.
- Cluster policies enforce guardrails (node types, max workers, mandatory tags) so analysts/engineers
  cannot spin up oversized clusters — this is how cost efficiency is governed at the platform level,
  not just per-job.
- Photon acceleration is enabled on Gold aggregation jobs (heavy shuffle/aggregation) via
  `spark_conf["spark.databricks.photon.enabled"]` in the job cluster spec used for `03_gold`.
