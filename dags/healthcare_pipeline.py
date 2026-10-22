"""
healthcare_pipeline -- the full chain in one DAG.

    Synthea CSVs -> S3 raw/ -> Spark clean -> S3 processed/ -> Snowflake raw

Three mapped stages, one task per table in each:

    extract (11)  ->  validate_then_clean (11)  ->  load (11)

WHY ONE DAG
-----------
Separate DAGs forced `process_raw_data` to guess which ingestion_date=
partition to read by listing S3. That guesswork existed only because the
stages were decoupled. In one DAG run there is a single logical date,
resolved once, and every stage receives the same value.

Consequence: triggering this re-ingests as well as re-processes. That is
intended -- the run is self-consistent.

STAGE BARRIERS
--------------
Chaining mapped tasks means every clean waits for every extract. At 11 tables
that is fine (extraction is ~1 minute total). If extraction ever became the
long pole, per-table chaining would be the fix.
"""
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/spark_jobs")
sys.path.insert(0, "/opt/airflow/include")

from ingest.upload_raw import upload_one_file  # noqa: E402
from schemas import TABLES  # noqa: E402

SPARK_JOBS = "/opt/airflow/spark_jobs"
SNOWFLAKE_CONN = "snowflake_default"
STAGE = "@processed_stage"
FILE_FORMAT = "healthcare.raw.parquet_ff"


def spark_submit(script: str, table: str, ingestion_date: str) -> None:
    cmd = [
        "spark-submit",
        f"{SPARK_JOBS}/{script}",
        "--table",
        table,
        "--ingestion-date",
        ingestion_date,
    ]
    print("RUN:", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def alert_on_failure(context):
    """Alerting stub -- swap for Slack/email in Week 5."""
    ti = context.get("task_instance")
    print(f"FAILED: {ti.dag_id}.{ti.task_id} (run_id={ti.run_id})")


@dag(
    dag_id="healthcare_pipeline",
    schedule=None,          # manual while developing; "@daily" once it's green
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=2,     # each transform task runs a Spark JVM
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_on_failure,
    },
    tags=["pipeline", "s3", "spark", "snowflake"],
)
def healthcare_pipeline():
    # One date for the whole run, resolved once and threaded through every stage.
    run_date = "{{ dag_run.run_after | ds }}"

    @task
    def extract(table: str, ingestion_date: str):
        return upload_one_file(table, ingestion_date)

    @task
    def transform(table: str, ingestion_date: str):
        spark_submit("validate_table.py", table, ingestion_date)
        spark_submit("clean_table.py", table, ingestion_date)
        return f"{table}@{ingestion_date}"

    @task
    def load(table: str):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        # Full refresh. Makes a retry safe, and stops the accumulating
        # _ingested_date=.../ directories from loading duplicates.
        hook.run(f"TRUNCATE TABLE IF EXISTS healthcare.raw.{table}")
        hook.run(
            f"COPY INTO healthcare.raw.{table} FROM {STAGE}/{table}/ "
            f"FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}') "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            "PATTERN = '.*[.]parquet' "
            "ON_ERROR = 'ABORT_STATEMENT'"
        )
        return table

    extracted = extract.partial(ingestion_date=run_date).expand(table=sorted(TABLES))
    transformed = transform.partial(ingestion_date=run_date).expand(table=sorted(TABLES))
    loaded = load.expand(table=sorted(TABLES))

    extracted >> transformed >> loaded


healthcare_pipeline()