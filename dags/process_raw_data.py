"""
process_raw_data -- validate then clean every raw table with Spark.

One task per table, via dynamic task mapping. Each task runs two sequential
spark-submit calls:

    validate_table.py   does the declared schema actually fit the data?
    clean_table.py      dedupe, stamp _ingested_date, write Parquet

Per-table granularity is the whole point: claims_transactions (902,713 rows) is
far slower than payers (10 rows), so a failure on one table retries that table
alone rather than re-running all eleven.

WHY THE INGESTION DATE IS DISCOVERED, NOT TEMPLATED
---------------------------------------------------
The obvious move is {{ ds }} / {{ dag_run.run_after | ds }}, but that is the
date the DAG RUNS, not the date the raw data was LANDED. Trigger this today and
it would look for today's partition while the data sits under an older one. So
each task asks S3 for the newest ingestion_date= partition under raw/<table>/
and processes that.

WHY THIS SHELLS OUT INSTEAD OF IMPORTING THE SPARK CODE
-------------------------------------------------------
spark-submit runs the job in its own JVM, so the Spark job stays independent of
Airflow's Python environment. It is also exactly the command that was tested by
hand, so what runs here is what was verified -- no second code path.
"""
import os
import subprocess
import sys
from datetime import timedelta

import boto3
import pendulum
from airflow.sdk import dag, task

# schemas.py is plain data (a dataclass plus DDL strings) and imports no Spark,
# so pulling the table list from it costs nothing and keeps one source of truth.
sys.path.insert(0, "/opt/airflow/spark_jobs")
from schemas import TABLES  # noqa: E402

SPARK_JOBS = "/opt/airflow/spark_jobs"


def latest_ingestion_date(bucket: str, table: str) -> str:
    """Newest ingestion_date= partition under raw/<table>/."""
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"raw/{table}/", Delimiter="/")
    dates = [
        p["Prefix"].rstrip("/").split("ingestion_date=")[-1]
        for p in resp.get("CommonPrefixes", [])
        if "ingestion_date=" in p["Prefix"]
    ]
    if not dates:
        raise ValueError(f"no ingestion_date= partition under raw/{table}/")
    return max(dates)


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
    subprocess.run(cmd, check=True)  # non-zero exit -> task fails


def alert_on_failure(context):
    """Alerting stub -- swap for Slack/email in Week 5."""
    ti = context.get("task_instance")
    print(f"FAILED: {ti.dag_id}.{ti.task_id} (run_id={ti.run_id})")


@dag(
    dag_id="process_raw_data",
    schedule=None,          # manual while developing; "@daily" once it's green
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    # Each task starts a Spark JVM that wants the machine's cores. Running all
    # eleven at once would thrash, so throttle.
    max_active_tasks=2,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_on_failure,
    },
    tags=["spark", "processed", "week2"],
)
def process_raw_data():
    @task
    def validate_then_clean(table: str):
        date = latest_ingestion_date(os.environ["S3_BUCKET"], table)
        spark_submit("validate_table.py", table, date)
        spark_submit("clean_table.py", table, date)
        return f"{table}@{date}"

    validate_then_clean.expand(table=sorted(TABLES))


process_raw_data()