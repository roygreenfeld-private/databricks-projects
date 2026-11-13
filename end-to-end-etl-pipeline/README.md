# End-to-End Transactions ETL Pipeline (Bronze → Silver → Gold)

A Lakeflow Spark Declarative Pipeline that turns raw transaction records into clean, analysis-ready data and daily business metrics, following the medallion architecture.

## Problem and Objective

Raw transaction data arrives messy: padded and inconsistently-cased IDs and product names, unrounded amounts, and unusable helper columns. The goal is a reliable, automated flow that:

1. Ingests new transactions incrementally from S3.
2. Cleans and validates every row.
3. Produces daily KPIs (volume, revenue, customers) for reporting.

## Architecture

```text
S3 (s3://data-alex-project/transactions)
   │  hourly Auto Loader ingestion
   ▼
transactions (source streaming table)
   │
   ▼  bronze_transactions   — raw copy, untouched
   ▼  silver_transactions   — cleaned + quality-checked
   ▼  daily_transactions_gold — one row per day (materialized view)
```

An orchestration job (`job_to_run_end_to_end_pipline`) is triggered on table update: whenever the source table receives new rows, the pipeline refreshes bronze → silver → gold automatically.

## Implementation

| Layer | File | Type | What it does |
| --- | --- | --- | --- |
| Bronze | `transformations/bronze/bronze_transactions.py` | Streaming table | Streams the source table as-is — no cleaning, so any downstream issue is traceable to the source |
| Silver | `transformations/silver/silver_transactions.py` | Streaming table | Normalizes IDs (trim + uppercase), title-cases and collapses whitespace in text fields, rounds amounts to 2 decimals, drops the empty `_rescued_data` column |
| Gold | `transformations/gold/daily_transactions_gold.py` | Materialized view | Aggregates silver into daily metrics: transaction count, unique customers, units sold, total revenue, average order value (clustered by `transaction_day`) |

## Deployment

The `deployment/` folder documents the Databricks-side configuration (which normally lives only in the workspace, not in code):

| File | What it captures |
| --- | --- |
| `deployment/pipeline.json` | Pipeline settings: publishing catalog/schema, serverless + Photon, library glob (`transformations/**`), trigger mode |
| `deployment/job.json` | Orchestration job: table-update trigger on the source table, queueing, and the pipeline task |

Together with the source ingestion (an hourly scheduled streaming table created by the file-upload UI), these define the full automated chain: **S3 → source table → job trigger → pipeline → bronze → silver → gold**.

## Validation and Reliability

Data quality expectations enforced in silver:

- `expect_or_fail` — run fails if `transaction_id`, `customer_id`, or `product_id` is null.
- `expect_or_drop` — rows with `quantity < 1` or `unit_price <= 0` are dropped.
- `expect` (warn) — flags rows where `total_amount` deviates from `quantity × unit_price` by more than a cent.

Operational lessons baked into the design:

- When a pipeline's publishing catalog changes, its internal tables must be cleaned up first, or initialization fails with cross-catalog permission errors.
- Hidden internal tables (`__materialization_*`, event logs) are invisible in Catalog Explorer but must be accounted for during cleanup.

## Results

On the sample dataset (Jan 6–26 2025), the gold layer produces 21 daily rows with consistent counts, revenue, and average order value per day.

## Reproduction Guide

1. Create a Lakeflow Spark Declarative Pipeline pointing at the `transformations/` folder (glob: `transformations/**`), publishing to a Unity Catalog catalog/schema — see `deployment/pipeline.json`.
2. Ensure the source streaming table `transactions` exists (created via file upload / Auto Loader from S3).
3. Create a job with a **table update trigger** on the source table so every new ingestion automatically refreshes bronze → silver → gold — see `deployment/job.json`.

**Technologies:** Databricks Lakeflow, PySpark, Delta Lake, Unity Catalog, Auto Loader, AWS S3, Jobs.
