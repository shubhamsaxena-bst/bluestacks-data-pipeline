# BlueStacks Marketing Insights Platform — Enterprise Data Pipeline Reference

A reference implementation of an enterprise Azure data engineering stack, built to mirror the
responsibilities and required skills in the **Associate Application Developer (Data/ETL)** job
description: Azure Databricks + PySpark, Delta Lake, Azure Data Factory, ADLS Gen2, data
governance, CI/CD, and coexistence with a legacy Informatica PowerCenter estate.

**Want to actually run this?** See `GETTING_STARTED_AZURE.md` for a step-by-step path to stand up
real Azure resources and load the dummy data in `sample_data/`.

## Business scenario

BlueStacks runs a global marketing function spanning paid acquisition (Google Ads, Meta Ads),
owned web properties, email/lifecycle campaigns, and a sales/marketing CRM. The data engineering
team turns these disparate marketing systems into governed, analytics-ready data products that
answer questions like "what's our ROAS by channel this week?" and "where are leads dropping out
of the funnel?"

| Source | Description | Volume | Landing format |
|---|---|---|---|
| `ad_campaign_performance` | Daily spend, impressions, clicks, conversions from Google Ads & Meta Ads | ~500K rows/day | JSON, API export landed to Blob |
| `web_analytics_events` | Site visits, page views, goal/conversion completions (clickstream) | ~200M events/day | JSON, streamed to ADLS Gen2 landing |
| `crm_leads_opportunities` | Lead creation, MQL/SQL qualification, opportunity stage changes | ~300K rows/day | Legacy Informatica PowerCenter feed from on-prem Oracle CRM |
| `email_campaign_engagement` | Send, open, click, unsubscribe events from the email platform | ~5M events/day | CSV batch export |

The platform produces governed Delta Lake tables that feed a semantic layer for Power BI /
analyst SQL consumption: campaign ROI and ROAS/CPA by channel, multi-touch attribution, funnel
conversion by stage (lead -> MQL -> SQL -> opportunity -> closed-won), and email engagement
performance.

## Architecture (medallion / Bronze-Silver-Gold on Delta Lake)

```
Sources (Ad platform APIs / Blob / Web clickstream / SFTP / Oracle CRM via Informatica)
        |
        v
Azure Data Factory  -- parameterized Copy + orchestration, triggers, monitoring
        |
        v
ADLS Gen2  /raw (bronze landing, immutable, partitioned by ingest date)
        |
        v
Azure Databricks (PySpark)
   Bronze  -> schema-on-read, quarantine bad records, audit columns
   Silver  -> cleansing, dedup, enrichment (conformed campaign dimension), Delta MERGE upsert
   Gold    -> curated marts (campaign ROI, attribution, funnel conversion, email engagement)
        |
        v
Delta Lake tables (ACID, versioned, time-travel, OPTIMIZE/ZORDER, VACUUM)
        |
        v
Power BI / SQL analytics, marketing ops, data science / propensity models
```

Cross-cutting: Azure Key Vault (secrets/credentials), Unity Catalog / Purview (governance,
lineage, audit), Azure Monitor + Log Analytics (observability), GitHub Actions (CI/CD),
Azure AD (RBAC).

## Repository layout

```
bluestacks-data-pipeline/
├── README.md
├── GETTING_STARTED_AZURE.md    # step-by-step: real Azure resources + dummy data + first run
├── sample_data/                 # dummy data generator + generated files + seed data
│   ├── generate_dummy_data.py
│   ├── raw/                     # upload straight into your ADLS Gen2 "raw" container
│   └── seed/                    # dim_campaign + ADF control table seed rows
├── notebooks/                   # PySpark / Databricks notebooks (Bronze/Silver/Gold + utils)
├── adf/                          # Azure Data Factory ARM-style JSON artifacts
│   ├── linkedService/
│   ├── dataset/
│   ├── pipeline/
│   └── trigger/
├── informatica/                  # Legacy PowerCenter coexistence + migration plan
├── governance/                   # Data quality, audit, lineage, Key Vault, RBAC
├── cicd/.github/workflows/       # GitHub Actions CI/CD for Databricks + ADF
└── docs/                         # Architecture document, JD requirement traceability, interview prep
```

## Quick start

1. Read `GETTING_STARTED_AZURE.md` — Phase A gets you a minimal Azure footprint (storage +
   Key Vault + Databricks) and a manual Bronze → Silver → Gold run using the data in
   `sample_data/`, no Azure Data Factory required yet.
2. Phase B in the same doc wires up ADF (linked services, pipelines, triggers) so ingestion runs
   on a schedule/event trigger like the production design in `adf/`.
3. `sample_data/README.md` explains exactly what's in the dummy data (row counts, intentional
   bad records for testing the quarantine path, intentional "unattributed" campaign IDs for
   testing the Silver enrichment fallback).

## How each JD requirement is covered

See `docs/BlueStacks_Marketing_Data_Pipeline_Architecture.docx` for a line-by-line mapping of every
responsibility and required skill in the job description to the concrete artifact in this repo,
and `docs/Marketing_Interview_Prep_QA.docx` for anticipated follow-up interview questions and answers
grounded in this same marketing-data architecture.
