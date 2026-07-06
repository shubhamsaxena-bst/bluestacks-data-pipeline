"""
Generates realistic dummy data for every Bronze source in the BlueStacks Marketing Insights
Platform, matching notebooks/utils/schema_registry.py exactly, so the pipeline can be exercised
end-to-end against real ADLS Gen2 / Databricks / ADF without waiting on live marketing feeds.

Usage:
    python3 generate_dummy_data.py

Output (relative to this script):
    sample_data/raw/<source_name>/dt=<YYYY-MM-DD>/part-0000.<json|csv>
        - upload each dt=... folder to abfss://raw@<storage>.dfs.core.windows.net/<source_name>/
          i.e. mirror the folder structure directly into the ADLS Gen2 "raw" container.
    sample_data/seed/dim_campaign.json
        - one-time seed for bsi_silver.dim_campaign (NOT ingested through Bronze; load directly)
    sample_data/seed/ctl_active_sources.json
        - rows for the ADF control table (ctl.active_sources) read by PL_Master_Orchestration

Design notes:
    - A shared campaign_id namespace (CMP-####) is used across ad_campaign_performance,
      crm_leads_opportunities, and web_analytics_events (utm_campaign_id) / email_campaign_engagement
      (linked_ad_campaign_id) so the Silver-layer dim_campaign join in 02_silver_cleanse_enrich.py
      has real matches to enrich against.
    - A handful of campaign_ids are deliberately left OUT of the dim_campaign seed to exercise the
      "unattributed"/"unknown" coalesce() fallback path in the Silver enrichment join.
    - ~1% of records per source are deliberately malformed (missing a required field) to exercise
      the quarantine logic in notebooks/utils/data_quality_checks.py without tripping the 2%
      quarantine-ratio alert threshold defined in governance/data_quality_rules.yaml.
    - Output is reproducible (fixed random seed) so re-running produces identical files.
"""
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

random.seed(42)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_RAW = os.path.join(SCRIPT_DIR, "raw")
OUT_SEED = os.path.join(SCRIPT_DIR, "seed")

RUN_DATES = ["2026-07-04", "2026-07-05", "2026-07-06"]

# ---------------------------------------------------------------------------
# Campaign master data (drives dim_campaign + is referenced by every source)
# ---------------------------------------------------------------------------
N_PAID_CAMPAIGNS = 70
CAMPAIGN_THEMES = [
    "Summer_Sale", "Back_To_School", "Holiday_Push", "New_User_Acquisition", "Retargeting",
    "Brand_Awareness", "App_Install", "Regional_LatAm", "Regional_EMEA", "Regional_APAC",
    "Gamer_Persona", "Creator_Persona", "Value_Bundle", "Premium_Upsell", "Winback",
]
REGIONS_FOR_NAME = ["US", "EU", "APAC", "LatAm", "Global"]

paid_campaigns = []
for i in range(1, N_PAID_CAMPAIGNS + 1):
    cmp_id = f"CMP-{1000 + i}"
    channel = "google_ads" if i % 2 == 0 else "meta_ads"
    theme = random.choice(CAMPAIGN_THEMES)
    region = random.choice(REGIONS_FOR_NAME)
    paid_campaigns.append({
        "campaign_id": cmp_id,
        "campaign_name": f"{theme}_{region}_{cmp_id[-4:]}",
        "channel": channel,
        "channel_type": "paid_search" if channel == "google_ads" else "paid_social",
        "budget_usd": round(random.uniform(500, 25000), 2),
    })

# Deliberately excluded from dim_campaign seed -> exercises the "unattributed" fallback path
UNATTRIBUTED_CAMPAIGN_IDS = {c["campaign_id"] for c in paid_campaigns[-5:]}

NON_PAID_CAMPAIGNS = [
    {"campaign_id": "CMP-2001", "campaign_name": "Organic_Search", "channel_type": "organic_search", "budget_usd": 0.0},
    {"campaign_id": "CMP-2002", "campaign_name": "Direct_Traffic", "channel_type": "direct", "budget_usd": 0.0},
    {"campaign_id": "CMP-2003", "campaign_name": "Referral_Partners", "channel_type": "referral", "budget_usd": 0.0},
]

ALL_ATTRIBUTABLE_CAMPAIGN_IDS = (
    [c["campaign_id"] for c in paid_campaigns] + [c["campaign_id"] for c in NON_PAID_CAMPAIGNS]
)

VISITOR_POOL = [f"VIS-{uuid.uuid5(uuid.NAMESPACE_DNS, f'visitor-{i}').hex[:10]}" for i in range(1, 401)]
COUNTRIES = ["US", "IN", "BR", "DE", "GB", "ID", "MX", "PH", "VN", "FR"]
OWNER_REGIONS = ["NA", "EMEA", "APAC", "LATAM"]


def rand_ts(date_str: str) -> str:
    base = datetime.strptime(date_str, "%Y-%m-%d")
    offset = timedelta(seconds=random.randint(0, 86399))
    return (base + offset).strftime("%Y-%m-%d %H:%M:%S")


def write_ndjson(path: str, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def write_csv(path: str, records: list[dict], fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)


# ---------------------------------------------------------------------------
# 1. ad_campaign_performance
# ---------------------------------------------------------------------------
def gen_ad_campaign_performance():
    total_bad = 0
    for date_str in RUN_DATES:
        rows = []
        for c in paid_campaigns:
            impressions = random.randint(500, 50000)
            clicks = round(impressions * random.uniform(0.01, 0.08))
            spend = round(clicks * random.uniform(0.3, 2.5), 2)
            conversions = round(clicks * random.uniform(0.02, 0.12))
            rows.append({
                "campaign_id": c["campaign_id"],
                "channel": c["channel"],
                "report_date": date_str,
                "impressions": impressions,
                "clicks": clicks,
                "spend_usd": spend,
                "conversions": conversions,
                "currency": "USD",
            })
        # inject ~1% malformed rows (missing required 'channel') to exercise quarantine
        n_bad = max(1, round(len(rows) * 0.01)) if date_str == RUN_DATES[0] else 0
        for _ in range(n_bad):
            bad = dict(random.choice(rows))
            bad["channel"] = None
            rows.append(bad)
            total_bad += 1
        write_ndjson(os.path.join(OUT_RAW, "ad_campaign_performance", f"dt={date_str}", "part-0000.json"), rows)
    return total_bad


# ---------------------------------------------------------------------------
# 2. web_analytics_events
# ---------------------------------------------------------------------------
def gen_web_analytics_events():
    total_bad = 0
    events_per_day = 150
    for date_str in RUN_DATES:
        rows = []
        for _ in range(events_per_day):
            visitor_id = random.choice(VISITOR_POOL)
            r = random.random()
            if r < 0.65:
                campaign_pool_choice = random.choice([c["campaign_id"] for c in paid_campaigns])
            elif r < 0.85:
                campaign_pool_choice = "CMP-2001"
            else:
                campaign_pool_choice = random.choice(["CMP-2002", "CMP-2003"])
            event_type = random.choices(
                ["page_view", "goal_completion", "form_submit"], weights=[70, 20, 10]
            )[0]
            rows.append({
                "event_id": str(uuid.uuid4()),
                "visitor_id": visitor_id,
                "session_id": f"SESS-{visitor_id[-8:]}-{random.randint(1, 3)}",
                "event_ts": rand_ts(date_str),
                "event_type": event_type,
                "page_url": random.choice([
                    "/home", "/pricing", "/download", "/blog/top-android-games",
                    "/features", "/signup", "/thank-you",
                ]),
                "utm_campaign_id": campaign_pool_choice,
                "utm_channel": next(
                    (c["channel_type"] for c in (paid_campaigns + NON_PAID_CAMPAIGNS) if c["campaign_id"] == campaign_pool_choice),
                    "unknown",
                ),
                "country_code": random.choice(COUNTRIES),
            })
        n_bad = max(1, round(len(rows) * 0.01)) if date_str == RUN_DATES[0] else 0
        for _ in range(n_bad):
            bad = dict(random.choice(rows))
            bad["event_id"] = None
            rows.append(bad)
            total_bad += 1
        write_ndjson(os.path.join(OUT_RAW, "web_analytics_events", f"dt={date_str}", "part-0000.json"), rows)
    return total_bad


# ---------------------------------------------------------------------------
# 3. crm_leads_opportunities (legacy Informatica-fed CRM funnel)
# ---------------------------------------------------------------------------
FUNNEL_STAGES = ["new", "mql", "sql", "opportunity", "closed_won", "closed_lost"]


def gen_crm_leads_opportunities():
    total_bad = 0
    n_leads = 70
    all_records_by_date = {d: [] for d in RUN_DATES}

    for i in range(1, n_leads + 1):
        lead_id = f"LEAD-{2000 + i}"
        opp_id = f"OPP-{2000 + i}" if random.random() < 0.6 else None
        campaign_id = (
            random.choice([c["campaign_id"] for c in paid_campaigns]) if random.random() < 0.8 else None
        )
        owner_region = random.choice(OWNER_REGIONS)

        # how far this lead progresses through the funnel
        max_stage_idx = random.choices(range(len(FUNNEL_STAGES)), weights=[15, 25, 25, 20, 10, 5])[0]
        stages_hit = FUNNEL_STAGES[: max_stage_idx + 1]

        # spread this lead's stage transitions across the 3-day window, in order
        chosen_dates = sorted(random.choices(RUN_DATES, k=len(stages_hit)))
        for stage, date_str in zip(stages_hit, chosen_dates):
            deal_value = None
            if stage in ("opportunity", "closed_won", "closed_lost"):
                deal_value = round(random.uniform(200, 8000), 2)
            record = {
                "lead_id": lead_id,
                "opportunity_id": opp_id if stage in ("opportunity", "closed_won", "closed_lost") else None,
                "campaign_id": campaign_id,
                "stage": stage,
                "stage_change_ts": rand_ts(date_str),
                "deal_value_usd": deal_value,
                "owner_region": owner_region,
            }
            all_records_by_date[date_str].append(record)

    for date_str in RUN_DATES:
        rows = all_records_by_date[date_str]
        n_bad = max(1, round(len(rows) * 0.02)) if date_str == RUN_DATES[0] and rows else 0
        for _ in range(n_bad):
            bad = dict(random.choice(rows))
            bad["lead_id"] = None
            rows.append(bad)
            total_bad += 1
        write_ndjson(os.path.join(OUT_RAW, "crm_leads_opportunities", f"dt={date_str}", "part-0000.json"), rows)
    return total_bad


# ---------------------------------------------------------------------------
# 4. email_campaign_engagement
# ---------------------------------------------------------------------------
EMAIL_CAMPAIGNS = [f"EML-{3000 + i}" for i in range(1, 11)]
EMAIL_TO_AD_CAMPAIGN = {
    eml: (random.choice([c["campaign_id"] for c in paid_campaigns]) if random.random() < 0.6 else None)
    for eml in EMAIL_CAMPAIGNS
}


def gen_email_campaign_engagement():
    total_bad = 0
    fieldnames = ["engagement_id", "email_campaign_id", "visitor_id", "event_type", "event_ts", "linked_ad_campaign_id"]
    for date_str in RUN_DATES:
        rows = []
        recipients_today = random.sample(VISITOR_POOL, 50)
        for visitor_id in recipients_today:
            eml_campaign = random.choice(EMAIL_CAMPAIGNS)
            linked = EMAIL_TO_AD_CAMPAIGN[eml_campaign]
            # funnel: everyone sent, some open, some of those click, rare unsubscribe/bounce
            rows.append({
                "engagement_id": str(uuid.uuid4()), "email_campaign_id": eml_campaign,
                "visitor_id": visitor_id, "event_type": "send", "event_ts": rand_ts(date_str),
                "linked_ad_campaign_id": linked or "",
            })
            if random.random() < 0.05:
                rows.append({
                    "engagement_id": str(uuid.uuid4()), "email_campaign_id": eml_campaign,
                    "visitor_id": visitor_id, "event_type": "bounce", "event_ts": rand_ts(date_str),
                    "linked_ad_campaign_id": linked or "",
                })
                continue
            if random.random() < 0.4:
                rows.append({
                    "engagement_id": str(uuid.uuid4()), "email_campaign_id": eml_campaign,
                    "visitor_id": visitor_id, "event_type": "open", "event_ts": rand_ts(date_str),
                    "linked_ad_campaign_id": linked or "",
                })
                if random.random() < 0.3:
                    rows.append({
                        "engagement_id": str(uuid.uuid4()), "email_campaign_id": eml_campaign,
                        "visitor_id": visitor_id, "event_type": "click", "event_ts": rand_ts(date_str),
                        "linked_ad_campaign_id": linked or "",
                    })
                if random.random() < 0.03:
                    rows.append({
                        "engagement_id": str(uuid.uuid4()), "email_campaign_id": eml_campaign,
                        "visitor_id": visitor_id, "event_type": "unsubscribe", "event_ts": rand_ts(date_str),
                        "linked_ad_campaign_id": linked or "",
                    })
        n_bad = max(1, round(len(rows) * 0.01)) if date_str == RUN_DATES[0] else 0
        for _ in range(n_bad):
            bad = dict(random.choice(rows))
            bad["email_campaign_id"] = ""
            rows.append(bad)
            total_bad += 1
        write_csv(os.path.join(OUT_RAW, "email_campaign_engagement", f"dt={date_str}", "part-0000.csv"), rows, fieldnames)
    return total_bad


# ---------------------------------------------------------------------------
# Seeds: dim_campaign + ADF control table
# ---------------------------------------------------------------------------
def gen_seeds():
    dim_rows = []
    for c in paid_campaigns:
        if c["campaign_id"] in UNATTRIBUTED_CAMPAIGN_IDS:
            continue  # deliberately excluded - exercises the "unattributed" fallback
        dim_rows.append({
            "campaign_id": c["campaign_id"],
            "campaign_name": c["campaign_name"],
            "channel_type": c["channel_type"],
            "budget_usd": c["budget_usd"],
            "start_date": "2026-06-01",
            "end_date": "2026-12-31",
        })
    for c in NON_PAID_CAMPAIGNS:
        dim_rows.append({**c, "start_date": "2020-01-01", "end_date": "2099-12-31"})
    write_ndjson(os.path.join(OUT_SEED, "dim_campaign.json"), dim_rows)

    ctl_rows = [
        {"source_name": "ad_campaign_performance", "file_format": "json", "is_active": 1},
        {"source_name": "web_analytics_events", "file_format": "json", "is_active": 1},
        {"source_name": "crm_leads_opportunities", "file_format": "json", "is_active": 1},
        {"source_name": "email_campaign_engagement", "file_format": "csv", "is_active": 1},
    ]
    write_ndjson(os.path.join(OUT_SEED, "ctl_active_sources.json"), ctl_rows)
    return len(dim_rows), len(ctl_rows)


if __name__ == "__main__":
    bad_ad = gen_ad_campaign_performance()
    bad_web = gen_web_analytics_events()
    bad_crm = gen_crm_leads_opportunities()
    bad_email = gen_email_campaign_engagement()
    n_dim, n_ctl = gen_seeds()

    print("Dummy data generated under sample_data/raw/ and sample_data/seed/")
    print(f"  ad_campaign_performance : {N_PAID_CAMPAIGNS} campaigns x {len(RUN_DATES)} days (+{bad_ad} malformed)")
    print(f"  web_analytics_events    : ~150/day x {len(RUN_DATES)} days (+{bad_web} malformed)")
    print(f"  crm_leads_opportunities : 70 leads, variable stage depth (+{bad_crm} malformed)")
    print(f"  email_campaign_engagement: ~50 recipients/day x {len(RUN_DATES)} days (+{bad_email} malformed)")
    print(f"  dim_campaign seed rows  : {n_dim} (of {N_PAID_CAMPAIGNS + len(NON_PAID_CAMPAIGNS)} total campaigns - "
          f"{len(UNATTRIBUTED_CAMPAIGN_IDS)} intentionally unattributed: {sorted(UNATTRIBUTED_CAMPAIGN_IDS)})")
    print(f"  ctl_active_sources rows : {n_ctl}")
