# Crypto Data Pipeline

[![Pipeline Smoke Test](https://github.com/salvi9/crypto-data-pipeline/actions/workflows/pipeline-smoke.yml/badge.svg)](https://github.com/salvi9/crypto-data-pipeline/actions/workflows/pipeline-smoke.yml)

A data pipeline that ingests stock/crypto market data and processes it through a Bronze → Silver → Gold medallion architecture, backed by PostgreSQL.

---

## Architecture

```
Alpha Vantage API
      |
      v
  [Bronze]  stock_data_raw        — raw OHLCV records, duplicates rejected by DB
      |
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
│   └── pipeline_runner.py    # End-to-end Bronze → Silver → Gold runner
├── tests/
│   ├── schema.sql            # Creates all 4 tables (used in CI)
│   ├── test_pipeline_smoke.py    # Happy-path insert tests
│   └── test_negative_paths.py   # Constraint rejection tests
├── config/
│   └── pipeline-config.yaml  # Data source and DB config
├── dags/                     # Airflow DAGs (coming soon)
├── docker-compose.yml        # PostgreSQL + Airflow services
├── requirements.txt
└── .github/workflows/
    └── pipeline-smoke.yml    # CI: runs both test suites on every push
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

## Running Tests

```bash
python -m pytest tests/test_pipeline_smoke.py -v
python -m pytest tests/test_negative_paths.py -v
```

The smoke test covers happy-path inserts across all three layers. The negative-path test confirms the DB rejects bad data (negative prices, invalid OHLC, duplicates).

---

## CI / GitHub Actions

Every push and pull request to `main` runs the full test suite automatically via `.github/workflows/pipeline-smoke.yml`. It spins up a fresh PostgreSQL 13 container, bootstraps the schema, and runs both test files.

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
| PostgreSQL 13 | Storage |
| psycopg2 | PostgreSQL driver |
| python-dotenv | Environment config |
| pytest | Testing |
| GitHub Actions | CI/CD |
| Docker / Airflow | Orchestration (in progress) |
