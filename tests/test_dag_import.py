"""
DAG import test.

NOTE: This test requires Airflow 2.7.3.  If you have a different version of
Airflow in your local venv it will fail locally.  It is validated correctly
in CI (GitHub Actions) which installs Airflow 2.7.3 via constraints.
Run via CI or install: pip install "apache-airflow==2.7.3".
"""
import importlib.util
from pathlib import Path

import pytest


pytest.importorskip("airflow")


def test_crypto_dag_loads_with_expected_tasks():
    project_root = Path(__file__).resolve().parents[1]
    dag_file = project_root / "dags" / "crypto_pipeline_dag.py"

    spec = importlib.util.spec_from_file_location("crypto_pipeline_dag", dag_file)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)

    dag = getattr(module, "dag", None)
    assert dag is not None
    assert dag.dag_id == "crypto_data_pipeline"

    task_ids = set(dag.task_dict.keys())
    assert {"bronze_ingest", "silver_clean", "gold_aggregate"}.issubset(task_ids)
