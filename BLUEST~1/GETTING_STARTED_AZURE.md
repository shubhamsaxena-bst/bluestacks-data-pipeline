# Getting Started: Running This on Real Azure

A step-by-step path to stand up actual Azure resources, load the dummy data in
`sample_data/`, and watch data flow Bronze → Silver → Gold. Two phases:

- **Phase A** — run the notebooks manually against a minimal set of resources. Fastest way to
  see real results and confirm the logic works before touching ADF at all.
- **Phase B** — wire up Azure Data Factory so ingestion is orchestrated/parameterized/triggered
  like a real production pipeline, matching `adf/`.

You can stop after Phase A if your goal is just to see the pipeline work end-to-end.

---

## Prerequisites

- An Azure subscription with permission to create resource groups/resources.
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) installed and logged in
  (`az login`).
- A machine to run these commands from (Cloud Shell in the Azure Portal works too, no local
  install needed).

Set a few variables you'll reuse (pick a globally-unique storage account name):

```bash
export RG=bsi-marketing-poc-rg
export LOCATION=eastus
export STORAGE=bsimktadls$RANDOM          # must be globally unique, lowercase, no dashes
export KEYVAULT=bsi-mkt-poc-kv-$RANDOM
export DATABRICKS_WS=bsi-mkt-poc-dbx
export ADF_NAME=bsi-mkt-poc-adf

az group create --name $RG --location $LOCATION
```

---

## Phase A: Minimal resources + manual notebook run

### 1. Storage account with ADLS Gen2 (hierarchical namespace)

```bash
az storage account create \
  --name $STORAGE --resource-group $RG --location $LOCATION \
  --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true

# Containers used by the pipeline
for c in raw bronze governance; do
  az storage container create --account-name $STORAGE --name $c --auth-mode login
done
```

### 2. Upload the dummy data, preserving folder structure

```bash
cd bluestacks-data-pipeline/sample_data
az storage blob upload-batch \
  --account-name $STORAGE --auth-mode login \
  --destination raw --source raw
```

This lands `raw/ad_campaign_performance/dt=2026-07-04/part-0000.json` etc. directly at
`abfss://raw@$STORAGE.dfs.core.windows.net/...`, matching what `01_bronze_ingest_marketing_data.py`
expects.

### 3. Key Vault

```bash
az keyvault create --name $KEYVAULT --resource-group $RG --location $LOCATION

STORAGE_KEY=$(az storage account keys list --account-name $STORAGE --query "[0].value" -o tsv)
az keyvault secret set --vault-name $KEYVAULT --name adls-gen2-account-key --value "$STORAGE_KEY"
# Placeholder secrets - real values added once you have them (Databricks PAT in step 5,
# Oracle password not needed until you connect a real legacy CRM source):
az keyvault secret set --vault-name $KEYVAULT --name databricks-pat-token --value "placeholder"
az keyvault secret set --vault-name $KEYVAULT --name oracle-crm-password --value "placeholder"
```

### 4. Azure Databricks workspace

```bash
az databricks workspace create \
  --resource-group $RG --name $DATABRICKS_WS --location $LOCATION --sku standard
```

Open the workspace in the Azure Portal (**Launch Workspace**) for the remaining steps — they're
faster done in the Databricks UI than via CLI.

### 5. Databricks: token, secret scope, cluster

1. **User Settings → Developer → Access tokens → Generate new token.** Save it, then:
   ```bash
   az keyvault secret set --vault-name $KEYVAULT --name databricks-pat-token --value "<token>"
   ```
2. **Create a Key Vault-backed secret scope** (needs the Key Vault's Resource ID and DNS name —
   both shown on the Key Vault's Overview/Properties page in the portal):
   ```bash
   databricks secrets create-scope --scope bsi-kv-scope \
     --scope-backend-type AZURE_KEYVAULT \
     --resource-id /subscriptions/<sub-id>/resourceGroups/$RG/providers/Microsoft.KeyVault/vaults/$KEYVAULT \
     --dns-name https://$KEYVAULT.vault.azure.net/
   ```
3. **Create a small cluster** for manual testing: Compute → Create Cluster → single node,
   `14.3.x-scala2.12` (matches `notebooks/00_cluster_and_job_config.md`), smallest node type
   available, autotermination 30–45 min.

### 6. Import the repo into Databricks

**Repos → Add Repo → Git URL** if this project is in a Git remote you control, or
**Workspace → Import** the `notebooks/` folder directly if not. Either way, note the workspace
path — it should end up matching (or you should edit) the `notebookPath` values referenced in
`adf/pipeline/*.json` (e.g. `/Repos/bluestacks-data-pipeline/notebooks/01_bronze_ingest_marketing_data`).

### 7. Create the four Delta schemas

Run once in a scratch notebook cell:

```python
for db in ["bsi_bronze", "bsi_silver", "bsi_gold", "bsi_governance"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
```

### 8. Load the seed data (dim_campaign)

The Silver notebook's enrichment join reads `bsi_silver.dim_campaign` — it must exist before you
run Silver. Upload `sample_data/seed/dim_campaign.json` to DBFS and load it:

```python
# after uploading dim_campaign.json to e.g. /FileStore/seed/dim_campaign.json via
# Data → Add Data → upload file, or `databricks fs cp` from the CLI
df = spark.read.json("/FileStore/seed/dim_campaign.json")
df.write.format("delta").mode("overwrite").saveAsTable("bsi_silver.dim_campaign")
```

### 9. Point the notebooks at your storage account

In each notebook's widgets cell, the default container path is `bsimktadls` — update the
`raw_container_path` / `bronze_container_path` widget defaults (or just override them when you run
the notebook) to match your actual `$STORAGE` account name.

### 10. Run it — Bronze → Silver → Gold, per source, per day

For each of the 3 sample dates (`2026-07-04`, `2026-07-05`, `2026-07-06`) and each of the 4
sources, run `01_bronze_ingest_marketing_data` then `02_silver_cleanse_enrich` with matching
`source_name` / `ingest_date` widget values. Easiest way: use **Run all** with widgets set, or
call `dbutils.notebook.run(...)` from a small driver cell:

```python
sources = ["ad_campaign_performance", "web_analytics_events", "crm_leads_opportunities", "email_campaign_engagement"]
dates = ["2026-07-04", "2026-07-05", "2026-07-06"]

for d in dates:
    for s in sources:
        dbutils.notebook.run("01_bronze_ingest_marketing_data", 600, {"source_name": s, "ingest_date": d})
        dbutils.notebook.run("02_silver_cleanse_enrich", 600, {"source_name": s, "ingest_date": d})

for d in dates:
    dbutils.notebook.run("03_gold_aggregates", 600, {"run_date": d})

dbutils.notebook.run("04_delta_maintenance", 900, {})
```

### 11. Check the results

```sql
SELECT * FROM bsi_bronze.ad_campaign_performance_quarantine;   -- should show the ~1-2% seeded bad rows
SELECT * FROM bsi_gold.campaign_roi_daily ORDER BY roas DESC;
SELECT * FROM bsi_gold.funnel_conversion_by_stage ORDER BY metric_date, stage_order;
SELECT * FROM bsi_gold.attribution_channel_performance;
SELECT * FROM bsi_gold.email_engagement_summary;

-- confirm the "unattributed" fallback worked for CMP-1066..1070
SELECT * FROM bsi_silver.ad_campaign_performance WHERE campaign_name = 'unattributed';
```

If those queries return sensible rows, the core pipeline logic is proven out end-to-end.

---

## Phase B: Wire up Azure Data Factory

### 1. Create the ADF instance

```bash
az extension add --name datafactory
az datafactory create --resource-group $RG --factory-name $ADF_NAME --location $LOCATION
```

### 2. Grant ADF's managed identity access

ADF needs to read secrets from Key Vault and read/write the storage account:

```bash
ADF_PRINCIPAL_ID=$(az datafactory show --resource-group $RG --factory-name $ADF_NAME --query identity.principalId -o tsv)

az keyvault set-policy --name $KEYVAULT --object-id $ADF_PRINCIPAL_ID --secret-permissions get list

STORAGE_ID=$(az storage account show --name $STORAGE --resource-group $RG --query id -o tsv)
az role assignment create --assignee $ADF_PRINCIPAL_ID --role "Storage Blob Data Contributor" --scope $STORAGE_ID
```

### 3. Import the linked services, datasets, pipelines, triggers

Open Azure Data Factory Studio → **Author**. For each file under `adf/linkedService/`,
`adf/dataset/`, `adf/pipeline/`, `adf/trigger/`: create a new object of the matching type, switch
to its JSON/code view, and paste the file's contents in — then fix the placeholders:

| Placeholder in the JSON | Replace with |
|---|---|
| `bsimktadls` (storage account name) | your `$STORAGE` value |
| `bsi-prod-kv.vault.azure.net` | your `$KEYVAULT` name |
| `adb-xxxxxxxxxxxxxxx.azuredatabricks.net` | your Databricks workspace URL |
| `/subscriptions/xxxx/resourceGroups/bsi-prod-rg/...` | your actual subscription/resource group |

Skip `LS_OnPremOracle.json`, `DS_Oracle_CRM_LeadOpportunity.json`, and
`PL_Legacy_Informatica_Coexistence.json` for now — those model the legacy CRM integration and
need a real (or simulated) Oracle endpoint, which isn't part of this dummy-data test.

### 4. Control table for PL_Master_Orchestration (two options)

`PL_Master_Orchestration.json`'s `LookupActiveSources` activity expects an Azure SQL table
(`ctl.active_sources`). Two ways to proceed:

- **Quick path (recommended for a first test):** skip `PL_Master_Orchestration` entirely and just
  run `PL_Ingest_ParamSource` directly via **Debug** with manual parameters
  (`sourceName`, `fileFormat`, `ingestDate`) for each of the 4 sources — no Azure SQL needed.
- **Full path:** create a small Azure SQL Database, create `ctl.active_sources`
  (`source_name varchar, file_format varchar, is_active bit`), load the 4 rows from
  `sample_data/seed/ctl_active_sources.json`, and point `DS_ControlTable` at it.

### 5. Test

Use **Debug** on `PL_Ingest_ParamSource` (or `PL_Master_Orchestration` if you did the full path)
with `ingestDate` set to one of the 3 sample dates, and watch the Databricks Notebook activities
run in the ADF monitoring view. Then trigger `03_gold_aggregates` (directly, or via
`PL_Master_Orchestration`) and re-run the validation queries from Phase A step 11.

Once Debug runs succeed, publish and either run on-demand or let `TR_Daily_Schedule.json` /
`TR_EventBased_BlobCreated.json` take over.

---

## Cleaning up (avoid ongoing cost)

```bash
az group delete --name $RG --yes --no-wait
```

This tears down everything created above (storage, Key Vault, Databricks workspace, ADF) in one
shot. Databricks clusters with autotermination won't rack up compute cost between test runs even
before you delete the resource group.
