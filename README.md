# Crypto Data Pipeline

[![Pipeline Smoke Test](https://github.com/salvi9/crypto-data-pipeline/actions/workflows/pipeline-smoke.yml/badge.svg)](https://github.com/salvi9/crypto-data-pipeline/actions/workflows/pipeline-smoke.yml)

A production-style data pipeline that ingests live stock market data, processes it through a Bronze → Silver → Gold medallion architecture, orchestrates daily runs with Apache Airflow, and archives raw snapshots to AWS S3.

---

## Portfolio Summary

This project demonstrates end-to-end data engineering skills across the full pipeline lifecycle:

**What was built:**
- A live data ingestion layer that fetches daily OHLCV market data from Alpha Vantage for three symbols (AAPL, MSFT, NVDA)
- A three-stage medallion data architecture backed by PostgreSQL — Bronze for raw ingest, Silver for quality-validated records, Gold for hourly and daily aggregated metrics
- An Apache Airflow DAG that orchestrates the three stages on a daily schedule with task-level visibility and retry logic
- An AWS S3 archival layer that stores a raw JSON snapshot of every Bronze fetch, giving the pipeline a cloud-based audit trail separate from the operational database
- A CI/CD pipeline via GitHub Actions that runs a smoke test, DAG import validation, and negative-path constraint tests on every push

**Skills demonstrated:**
- Data pipeline design (ingestion, cleaning, aggregation)
- Medallion architecture (Bronze / Silver / Gold)
- Workflow orchestration with Apache Airflow (Docker-hosted, DAG authoring, task dependencies)
- Cloud storage integration with AWS S3 using boto3
- Relational database design with PostgreSQL (constraints, indexes, OHLCV schema)
- Automated testing with pytest (happy path, constraint rejection, DAG validation)
- CI/CD with GitHub Actions
- Containerisation with Docker Compose
- Secrets management with dotenv

---

## Architecture

```
Alpha Vantage API
      |
      v
  [Bronze]  stock_data_raw        — raw OHLCV records, duplicates rejected by DB
      |      |
      |      v
      |   AWS S3                  — raw JSON snapshot archived per run
      v
  [Silver]  stock_data_clean      — quality-checked rows with a score and validity flag
      |
      v
  [Gold]    stock_metrics_hourly  — hourly OHLCV aggregates + price change & volatility
            stock_data_daily      — daily OHLCV aggregates + return % and intraday range
```

---

## Project Structure

```
crypto-data-pipeline/
├── src/
│   ├── db_connector.py       # PostgreSQL connection manager and all CRUD methods
│   ├── fetch_market_data.py  # Alpha Vantage API client — fetches daily OHLCV data
│   ├── pipeline_runner.py    # End-to-end Bronze → Silver → Gold runner
│   └── s3_archive.py         # AWS S3 helper — archives Bronze snapshots as JSON
├── dags/
│   └── crypto_pipeline_dag.py    # Airflow DAG — schedules and orchestrates the pipeline
├── tests/
│   ├── schema.sql                # Creates all 4 tables (used in CI)
│   ├── test_pipeline_smoke.py    # Happy-path insert tests
│   ├── test_dag_import.py        # Validates DAG loads and tasks exist
│   └── test_negative_paths.py   # Constraint rejection tests
├── docker-compose.yml        # PostgreSQL + Airflow services
├── requirements.txt
└── .github/workflows/
    └── pipeline-smoke.yml    # CI: runs all test suites on every push
```

---

## Requirements

- Python 3.11
- PostgreSQL 13+
- A `.env` file in the project root (see below)

---

## Setup

**1. Clone the repo**

```bash
git clone https://github.com/salvi9/crypto-data-pipeline.git
cd crypto-data-pipeline
```

**2. Create and activate a virtual environment**

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Create a `.env` file**

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crypto_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

ALPHAVANTAGE_API_KEY=your_key

AWS_ACCESS_KEY_ID=your_key_id
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your_bucket_name
```

**5. Bootstrap the database schema**

```bash
psql -U your_user -d crypto_db -f tests/schema.sql
```

---

## Running the Pipeline

```bash
python src/pipeline_runner.py
```

This runs all three stages in sequence and prints a stats summary:

```
Pipeline started at 2026-04-01T10:00:00

=== BRONZE INGEST ===
Inserted Bronze row: AAPL @ 2026-04-01 10:00:00
Inserted Bronze row: MSFT @ 2026-04-01 10:01:00
Inserted Bronze row: NVDA @ 2026-04-01 10:02:00
Skipped Bronze row:  AAPL @ 2026-04-01 10:00:00  (duplicate)
Bronze summary: {'attempted': 4, 'inserted': 3, 'failed': 1}

=== SILVER CLEANING ===
Rows fetched for cleaning: 3
Cleaned row: AAPL @ ... (valid=True, score=1.0)
...
Silver summary: {'processed': 3, 'inserted': 3, 'invalid': 0}

=== GOLD AGGREGATION ===
...
Gold summary: {'rows_for_metrics': 3, 'hourly_inserted': 3, 'daily_inserted': 3}

=== PIPELINE STATS ===
Before: {'bronze': 10, 'silver': 10, 'gold_hourly': 5, 'gold_daily': 5}
After:  {'bronze': 13, 'silver': 13, 'gold_hourly': 8, 'gold_daily': 8}

=== DELTAS ===
bronze:      10 -> 13 (delta: +3)
silver:      10 -> 13 (delta: +3)
gold_hourly:  5 ->  8 (delta: +3)
gold_daily:   5 ->  8 (delta: +3)
```

---

## Airflow Runbook (Local)

Use this when you want scheduled/orchestrated runs instead of running scripts manually.

**1. Start services**

```bash
docker compose up -d
```

**2. Open Airflow UI**

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

**3. Run the DAG manually (first-time check)**

1. Open DAG: `crypto_data_pipeline`
2. Toggle DAG on
3. Click `Trigger DAG`
4. Go to `Grid` or `Graph` view

**4. Expected task flow**

The DAG has 3 tasks that run in order:

1. `bronze_ingest`
2. `silver_clean`
3. `gold_aggregate`

**5. View logs for a task run**

1. In `Grid`, click the task square for a run
2. Click `Log`

You should see stage summaries like:

- Bronze summary (`attempted/inserted/failed`)
- Silver summary (`processed/inserted/invalid`)
- Gold summary (`rows_for_metrics/hourly_inserted/daily_inserted`)

**6. Common behavior you may see**

- `inserted: 0` with successful run:
    This is normal when no new records are available (idempotent behavior).
- Rate limit message:
    Free Alpha Vantage usage is limited; the pipeline now prints one summary line when limits are hit.

**7. Important local scheduling note**

This Airflow setup runs on your machine. If your machine (or Docker) is off at schedule time, the run will not execute.

---

## Running Tests

```bash
python -m pytest tests/test_pipeline_smoke.py -v
python -m pytest tests/test_negative_paths.py -v
python -m pytest tests/test_dag_import.py -v
```

The smoke test covers happy-path inserts across all three layers. The negative-path test confirms the DB rejects bad data (negative prices, invalid OHLC, duplicates). The DAG import test validates the Airflow DAG loads correctly and all three task IDs exist.

---

## CI / GitHub Actions

Every push and pull request to `main` runs the full test suite automatically via `.github/workflows/pipeline-smoke.yml`. It spins up a fresh PostgreSQL 13 container, bootstraps the schema, and runs the smoke test, DAG import check, and negative-path test.

---

## Database Schema

| Table | Layer | Description |
|---|---|---|
| `stock_data_raw` | Bronze | Raw ingest. Unique on `(symbol, timestamp)`. |
| `stock_data_clean` | Silver | Cleaned rows with `data_quality_score` and `is_valid`. |
| `stock_metrics_hourly` | Gold | Hourly OHLCV + volatility and price change. |
| `stock_data_daily` | Gold | Daily OHLCV + daily return % and intraday range. |

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| PostgreSQL 13 | Operational storage |
| psycopg2 | PostgreSQL driver |
| Apache Airflow 2.7.3 | Pipeline orchestration (Docker) |
| AWS S3 + boto3 | Bronze snapshot archival |
| Alpha Vantage API | Market data source |
| python-dotenv | Environment config |
| pytest | Automated testing |
| GitHub Actions | CI/CD |
| Docker Compose | Local service orchestration |
