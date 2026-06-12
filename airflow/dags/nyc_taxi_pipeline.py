from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# astronomer-cosmos handles DBT ↔ Airflow integration
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import ExecutionMode

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

DBT_PROJECT_PATH = Path(os.environ.get(
    "DBT_PROJECT_PATH",
    "/opt/airflow/dbt/nyc-taxi"
))

# Connection to postgres database
DB_CONN_STRING = (
    f"postgresql+psycopg2://"
    f"{os.environ.get('DBT_USER', 'postgres')}:"
    f"{os.environ.get('DBT_PASSWORD', 'postgres')}@"
    f"{os.environ.get('DBT_HOST', 'postgres')}:5432/"
    f"{os.environ.get('DBT_DBNAME', 'medallion_db')}"
)

# Data source link - navigation based on container
SOURCE_DATA_PATH = Path("/opt/airflow/data/yellow_tripdata_2024-03.parquet")

logger = logging.getLogger(__name__)


# Cosmos profile config, for DBT usage
profile_config = ProfileConfig(
    profile_name="airflow_dagster",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_medallion",
        profile_args={"schema": "analytics"},
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

# DAG definiton
@dag(
    dag_id="medallion_pipeline",
    description="ELT pipeline: ingest > bronze > silver > gold",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def medallion_pipeline():

    @task(task_id="ingest_raw_data")
    def ingest_raw_data() -> dict:

        engine = create_engine(DB_CONN_STRING)

        # Ensure raw schema exists
        with engine.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
            conn.commit()

        df = pd.read_parquet(SOURCE_DATA_PATH)

        # Add ingestion metadata columns
        df["_ingested_at"] = datetime.now()
        df["_source_file"] = str(SOURCE_DATA_PATH)

        rows_loaded = len(df)
        df.to_sql(
            name="taxi_trips",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=10_000,
            method="multi",
        )

        logger.info("Ingested %d rows into raw.orders", rows_loaded)
        return {"rows_loaded": rows_loaded, "source": str(SOURCE_DATA_PATH)}

    # Bronze layer
    bronze_models = DbtTaskGroup(
        group_id="dbt_bronze",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "select": "bronze.*",
            "dbt_cmd_flags": ["--full-refresh"],
        },
    )

    # Silver layer
    silver_models = DbtTaskGroup(
        group_id="dbt_silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "select": "silver.*",
        },
    )

    # Gold layer
    gold_models = DbtTaskGroup(
        group_id="dbt_gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "select": "gold.*",
        },
    )

    # Tests
    @task(task_id="dbt_test_all")
    def run_dbt_tests():
        import subprocess
        result = subprocess.run(
            ["dbt", "test", "--project-dir", str(DBT_PROJECT_PATH),
             "--profiles-dir", str(DBT_PROJECT_PATH)],
            capture_output=True, text=True, check=False,
        )
        logger.info(result.stdout)
        if result.returncode != 0:
            logger.error(result.stderr)
            raise RuntimeError("dbt test failed — see logs above for details.")
        return result.returncode


    ingest = ingest_raw_data()
    tests  = run_dbt_tests()

    ingest >> bronze_models >> silver_models >> gold_models >> tests

medallion_pipeline()