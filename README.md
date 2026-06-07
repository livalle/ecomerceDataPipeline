E-Commerce Data Pipeline
Incident Report · Architecture Decision Record · Engineering Runbook
Status: RESOLVED
Severity: P1 — Silent data loss in production
Detection: 2024-02-03 06:47 UTC
Resolution: 2024-02-05 14:20 UTC
Author: Lidia Vale · Analytics Engineer
Stack: Python · Apache Airflow · dbt · Snowflake · Great Expectations
Table of Contents
Incident Summary
Architecture Overview
Problem Identification
Root Cause Analysis
Solution Design & Trade-offs
Data Quality Decisions
Cost vs Performance Trade-offs
How to Run
Monitoring & Alerts
Lessons Learned
1. Incident Summary
Field
Detail
Impact
~3,400 order records silently dropped over 11 days
Revenue exposure
R$2.1M in untracked transactions
Root cause
Burst-triggered HTTP 429s with no retry; delete-on-error instead of quarantine
Contributing factors
Mixed date formats after API migration; BR locale monetary strings
Mean time to detect
11 days (no DQ monitoring at the time)
Mean time to resolve
48 hours
2. Architecture Overview
┌─────────────────────────────────────────────────────────────────┐
│  INGESTION                                                       │
│  API Source → Token Bucket Rate Limiter → Bronze (NDJSON/S3)    │
│                         ↘ Dead Letter Queue (failed requests)   │
├─────────────────────────────────────────────────────────────────┤
│  TRANSFORMATION                                                  │
│  Bronze → [Python Cleaner] → Silver (Parquet/Snowflake)         │
│              ↘ Quarantine (invalid records, not deleted)        │
│  Silver → [dbt models] → Gold (aggregated KPIs)                 │
├─────────────────────────────────────────────────────────────────┤
│  ORCHESTRATION                                                   │
│  Airflow DAG (daily 06:00 BRT) → dbt → Great Expectations       │
│              ↘ Slack alerts on DQ failure or SLA breach         │
├─────────────────────────────────────────────────────────────────┤
│  CONSUMPTION                                                     │
│  Gold → Dashboard (Revenue · Margin · Channel breakdown)        │
└─────────────────────────────────────────────────────────────────┘
Repository structure
ecommerce-data-pipeline/
├── ingestion/
│   └── api_ingestion.py        # Rate limiter, retry, DLQ
├── cleaning/
│   └── data_cleaning.py        # Bronze → Silver cleaner
├── orchestration/
│   ├── dag_ecommerce_pipeline.py
│   └── dbt_models/
│       ├── stg_orders.sql      # Silver staging model
│       └── fct_revenue_margin.sql  # Gold fact table
├── dashboard/
│   └── dashboard.html          # Interactive revenue & margin dashboard
├── tests/
│   └── test_pipeline.py        # Unit tests (pytest)
├── docs/
│   └── data_dictionary.md
└── README.md                   ← you are here
3. Problem Identification
3.1 How we detected the issue
Finance raised a discrepancy during month-end reconciliation:
the orders table in Snowflake showed R$9.48M but the source system reported R$11.59M.
Gap: R$2,110,000 (~18.2% of total revenue)
Affected date range: 2024-01-23 to 2024-02-03
3.2 Investigation steps
# Step 1: compare record counts between source and Bronze layer
SELECT date_trunc('day', order_date), count(*)
FROM bronze.orders
GROUP BY 1 ORDER BY 1;
-- Gaps found: Jan 24, 25, 27, Feb 1, 2, 3

# Step 2: check Airflow logs for those dates
# Result: DAG showed SUCCESS on all days — no errors surfaced

# Step 3: inspect raw ingestion logs
grep "HTTP 429" logs/ingestion_2024-01-24.log | wc -l
# Output: 847
# No retry was implemented — 429s were silently swallowed
3.3 Secondary issues discovered during investigation
#
Issue
Scope
Severity
1
Revenue stored as BR locale string (R$ 1.290,50)
100% of rows
P1
2
Mixed date formats (ISO vs dd/mm/yyyy)
~22% of rows after API migration
P1
3
Null customer_id for guest checkouts
~8% of rows
P2
4
Duplicate order_id from webhook retry
~1.2% of rows
P2
5
Negative revenue from partial-refund accounting
~0.3% of rows
P2
6
Corporate orders >R$500k hard-deleted (not quarantined)
3 records
P1
4. Root Cause Analysis
4.1 Rate limiting (primary)
The upstream API has a ceiling of 10 requests/second. The original client used a fixed time.sleep(0.1) between requests — which works at baseline but collapses under parallel execution or lag accumulation.
On Jan 24 the pipeline ran 6 minutes late due to an upstream deployment. The catch-up burst triggered 429s. Because no retry was implemented, the requests returned None and the pipeline continued, logging nothing.
Fix implemented: Token bucket algorithm (TokenBucketRateLimiter) capped at 2 rps (20% of ceiling) with exponential backoff + jitter on 429. Jitter prevents the "thundering herd" pattern where all retrying threads wake simultaneously.
4.2 Silent data loss (contributing)
The ingestion client used continue on any non-200 response. There was no Dead Letter Queue, no counter, no alert. Failures were invisible until a downstream anomaly surfaced them 11 days later.
Fix implemented: All non-retryable failures are written to a DLQ (NDJSON on S3). DLQ size is monitored via CloudWatch; a threshold alert fires when DLQ accumulates >50 records in a single run.
4.3 Hard-delete of outliers
The cleaner's first version called df.dropna() and df[df['revenue'] < 500000] without saving the removed rows. Three legitimate bulk corporate orders (R$280k each) were permanently lost.
Fix implemented: Invalid records are moved to a quarantine zone, not deleted. A domain expert reviews quarantine weekly and can reclassify records back into Silver.
5. Solution Design & Trade-offs
5.1 Rate limiter: token bucket vs sliding window
Approach
Pros
Cons
Decision
Fixed sleep
Simple
Collapses under burst; no burst tolerance
❌ Rejected
Token bucket
Smooth, burst-aware, cheap
Slightly more complex
✅ Chosen
Sliding window counter
Precise to provider's algorithm
Requires shared state (Redis)
⬜ Future
Trade-off: Token bucket is stateless (single process). For multi-worker deployments, a Redis-backed sliding window counter would be needed to coordinate rate across workers. Accepted as tech debt — current load is single-process.
5.2 Storage format: NDJSON (Bronze) vs Parquet (Silver)
Layer
Format
Rationale
Bronze
NDJSON
Schema-on-read; preserves original structure for debugging
Silver
Parquet (Snappy)
Columnar scan; ~4x compression vs JSON; Snowflake COPY-optimised
Gold
Snowflake table
Aggregated; direct BI query target
Cost decision: Bronze is stored on S3 Standard-IA after 30 days (lifecycle rule). At current volume (~500MB/day) this saves ~40% vs Standard. Gold is intentionally kept small (aggregated KPIs only) to reduce Snowflake compute on dashboard queries.
5.3 Medallion architecture vs single-layer
A single-layer approach (ingest directly to Gold) would be faster to build but would make debugging impossible — any data quality issue would require re-running the full pipeline from source. The Bronze/Silver/Gold separation means:
Bronze: replayable at zero cost (just re-read S3)
Silver: replayable from Bronze (no API calls needed)
Gold: rebuilt from Silver in minutes with dbt run
This decoupling was critical during the incident — we replayed 11 days of data from Bronze without touching the API.
6. Data Quality Decisions
6.1 Null handling strategy
Column
Null %
Strategy
Rationale
customer_id
8%
fill: 'UNKNOWN'
Guest checkouts are valid orders; drop would lose revenue
shipping_fee
12%
fill: 0
Free-shipping promos arrive as null by design
discount_pct
6%
fill: 0
No discount = 0% is semantically correct
product_category
0.4%
fill: 'UNKNOWN'
Uncategorised products; analyst can reclassify
order_date
0%
drop
Cannot compute any KPI without a date; row is unusable
6.2 Monetary parsing
Source delivers revenue as Brazilian locale strings: "R$ 1.290,50".
Regex strategy: strip R$ and whitespace → detect if both . and , present (BR thousands/decimal convention) → swap separators → float().
Edge cases handled: None, NaN, "", negative values, plain float strings already in US format.
6.3 Date parsing
Source switched format mid-year during a backend migration (ISO 8601 → dd/mm/yyyy). Rather than fixing at source (SLA: weeks), the cleaner tries 8 format patterns in order of frequency. Unresolved dates return None (logged, not crashed).
6.4 Quarantine threshold calibration
Revenue ceiling (R$500k) was set at the 99.97th percentile of the historical 6-month dataset. Anything above is flagged, not deleted. Post-fix, the three corporate orders in quarantine were manually validated and promoted back to Silver.
7. Cost vs Performance Trade-offs
Decision
Cost impact
Performance impact
Choice
Rate limit at 2 rps (20% of ceiling)
+15 min ingestion time
Zero 429s in 90-day post-fix period
✅ Accepted
Parquet + Snappy compression
-40% S3 storage
+slight CPU on write
✅ Accepted
Incremental dbt models
-70% Snowflake compute vs full refresh
Requires unique_key maintenance
✅ Accepted
Bronze lifecycle to S3-IA after 30d
-40% Bronze storage cost
Slightly slower replay (ms)
✅ Accepted
Quarantine instead of delete
+small S3 cost
No performance impact
✅ Accepted (compliance)
Great Expectations in CI (lightweight)
Free
Covers 80% of critical checks
✅ Accepted; full GE in roadmap
8. How to Run
Prerequisites
python >= 3.11
pip install pandas pyarrow requests apache-airflow dbt-snowflake great-expectations pytest
Environment variables
export ECOMMERCE_API_KEY="your_api_key"
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/..."
Run ingestion manually
from ingestion.api_ingestion import ApiIngestionClient, IngestionConfig

config = IngestionConfig()
client = ApiIngestionClient(config)
summary = client.run_full_load(date_from="2024-01-01", date_to="2024-01-31")
print(summary)
Run cleaning manually
from pathlib import Path
from cleaning.data_cleaning import OrdersCleaner, CleaningConfig

config = CleaningConfig()
cleaner = OrdersCleaner(config)
silver_path, report = cleaner.clean(Path("data/bronze/orders/2024-01/orders_2024-01-01_2024-01-31.ndjson"))
print(report.to_dict())
Run dbt
dbt run --select silver+ gold+ --profiles-dir ./profiles
dbt test --select silver+ gold+
Run tests
pytest tests/ -v --tb=short --cov=ingestion --cov=cleaning
9. Monitoring & Alerts
Signal
Threshold
Action
DLQ records per run
> 50
Slack P2 alert
Silver pass rate
< 90%
Airflow branch → halt Gold build → Slack P1 alert
Quarantine records per run
> 200
Slack P2 + email to data team
DAG SLA breach
> 1h 30min
PagerDuty P2
Snowflake query duration (Gold)
> 5 min
Slack warning
10. Lessons Learned
What went well after the fix:
Bronze replay capability proved its value immediately — 11 days of data rebuilt in 23 minutes with zero API calls.
Quarantine-not-delete saved the three corporate orders that an earlier version of the fix would have permanently lost.
Token bucket + jitter eliminated all 429s in the 90 days post-deployment.
What we would do differently:
Monitoring before launch. DQ checks and DLQ alerting should be part of the MVP, not an incident afterthought.
Contract testing with source API. A Pact-style consumer contract would have caught the date format change before it hit production.
Canary ingestion. Run a 1% sample with full DQ validation before opening the full load window.
Open items:
[ ] Redis-backed sliding window counter for multi-worker rate limiting
[ ] Full Great Expectations DataContext with HTML reports
[ ] dbt source freshness check wired to Airflow sensor
[ ] Automated quarantine review workflow (Slack approval bot)
Pipeline version 1.0.0 · Last updated 2026-06-05 · Li Barros · linkedin.com/in/libarros
