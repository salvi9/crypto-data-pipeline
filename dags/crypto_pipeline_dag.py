"""
================================================================================
AIRFLOW DAG: Crypto Data Pipeline
================================================================================
Purpose:
    Schedules the crypto data pipeline to run automatically on a daily basis.
    Fetches market data, cleans it, and aggregates it into hourly/daily metrics.

How it works:
    1. This file defines a workflow (DAG) that Airflow reads
    2. Airflow shows it in the web UI at localhost:8080
    3. On schedule, Airflow runs the task and monitors it
    4. You can see logs, retry failures, and track history
================================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================================
# AIRFLOW DAG CONFIGURATION
# ============================================================================
# These settings tell Airflow when and how to run the pipeline.

default_args = {
    # Owner: Who is responsible for this DAG (just for organization)
    'owner': 'data-team',

    # Retries: If the pipeline fails, Airflow will retry it this many times
    'retries': 1,

    # Retry delay: Wait 5 minutes before retrying after a failure
    'retry_delay': timedelta(minutes=5),

    # Start date: The date when this DAG starts scheduling (must be in the past)
    # Set to today; Airflow won't retroactively run missed schedules after this
    'start_date': datetime(2026, 4, 1),

    # Email on failure: If your email is set up, Airflow can notify you
    'email_on_failure': False,
    'email_on_retry': False,
}


# ============================================================================
# DAG DEFINITION
# ============================================================================
# This creates the actual DAG that Airflow will see.

dag = DAG(
    # DAG ID: Unique name for this workflow (used in the UI and CLI)
    dag_id='crypto_data_pipeline',

    # Description: Shows up in Airflow UI so you remember what it does
    description='Daily pipeline: fetch OHLCV data, clean it, and aggregate to metrics',

    # Schedule: When to run this DAG
    # '0 2 * * *' means "at 2:00 AM every day" (cron format)
    # Breakdown: minute(0) hour(2) day(*) month(*) weekday(*)
    schedule_interval='0 2 * * *',

    # If False, Airflow only runs future schedules from this point forward.
    catchup=False,

    # Default args: Apply the above settings to all tasks in this DAG
    default_args=default_args,

    # Tags: Organize your DAGs in the UI (optional, but helpful)
    tags=['crypto', 'pipeline', 'daily'],
)


# ============================================================================
# TASKS
# ============================================================================
# Each task is a step in the workflow.

# TASK 1: Bronze (ingest raw data)
bronze_task = BashOperator(
    task_id='bronze_ingest',
    bash_command='cd /opt/airflow && python src/pipeline_runner.py --stage bronze',
    dag=dag,
)

# TASK 2: Silver (clean/validate)
silver_task = BashOperator(
    task_id='silver_clean',
    bash_command='cd /opt/airflow && python src/pipeline_runner.py --stage silver',
    dag=dag,
)

# TASK 3: Gold (aggregate metrics)
gold_task = BashOperator(
    task_id='gold_aggregate',
    bash_command='cd /opt/airflow && python src/pipeline_runner.py --stage gold',
    dag=dag,
)


# ============================================================================
# TASK DEPENDENCIES (if you had multiple tasks)
# ============================================================================
# Tasks can depend on each other. Example:
# task_1 >> task_2 >> task_3   means: do task_1, then task_2, then task_3
#
# Run stages in order: Bronze -> Silver -> Gold
bronze_task >> silver_task >> gold_task